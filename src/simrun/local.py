"""The local service of a project: the service with the local backend in `.simrun/`.

`.simrun/` of a project directory holds the store, the index, the token of the user
and, while the service runs, `service.json` (pid, port, token, url). `simrun serve
--local` runs the service in the foreground; `ensure_local` finds the running one or
starts it in the background, which exits after `IDLE_EXIT_S` without a request and
without a live run. The service listens on `127.0.0.1` only, and its end cancels the
runs of the local backend, so no process of a run outlives it.
"""

import asyncio
import contextlib
import dataclasses
import fcntl
import getpass
import json
import logging
import os
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
import tomllib
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import httpx
import uvicorn
import yaml
from fastapi import FastAPI

import simrun
from simrun.config import ClusterConfig
from simrun.service.app import ServiceSettings, create_app
from simrun.service.auth import load_tokens
from simrun.service.local_backend import LocalBackend

logger = logging.getLogger(__name__)

LOCAL_DIR = ".simrun"
"""The directory of the local service in a project directory."""
SERVICE_FILE = "service.json"
LOG_FILE = "service.log"
START_LOCK = "start.lock"
SERVICE_LOCK = "service.lock"
HOST = "127.0.0.1"
IDLE_EXIT_S = 1800
"""Seconds without a request and without a live run after which a service started
in the background exits."""
GRACEFUL_SHUTDOWN_S = 5
"""Seconds the open requests, e.g. event streams, have at the end of the service."""
PACKAGE_WEB = Path(simrun.__file__).parent / "web"
"""The built web app in the package, put there by the release before the wheel."""
CHECKOUT = Path(simrun.__file__).parents[2]
"""The checkout of the repository simrun is imported from, if it is one."""
INSTANCE_PATH = "/local/instance"
"""The route which answers the instance of the local service, without a token."""
EXIT_RUNNING = 3
"""The exit code of `simrun serve --local` when the service of the directory runs."""
_CHECK_S = 0.25


class ServiceRunning(RuntimeError):
    """The local service of the directory runs already."""


@dataclasses.dataclass(frozen=True)
class LocalService:
    """A running local service, as its `service.json` names it.

    Attributes:
        instance: A random value of this start of the service, which it answers at
            `INSTANCE_PATH` without a token: a client checks it before it sends the
            token.
        pid: The process of the service.
        port: The port on `127.0.0.1`.
        token: The bearer token of the user.
        url: The URL of the API, `http://127.0.0.1:<port>`.
    """

    instance: str
    pid: int
    port: int
    token: str
    url: str


def web_dir() -> Path | None:
    """The built web app: the one in the package, else the one of a checkout.

    Returns:
        `simrun/web` of the installed package if it has an `index.html`, else
        `frontend/dist` of the checkout of simrun it is imported from (a
        `pyproject.toml` of the project simrun beside `frontend/`), else None.
    """
    if (PACKAGE_WEB / "index.html").is_file():
        return PACKAGE_WEB
    checkout = CHECKOUT / "frontend" / "dist"
    if (checkout / "index.html").is_file() and _is_simrun(CHECKOUT / "pyproject.toml"):
        return checkout
    return None


def _is_simrun(pyproject: Path) -> bool:
    """Whether a `pyproject.toml` is the one of simrun."""
    try:
        data = tomllib.loads(pyproject.read_text())
    except OSError, tomllib.TOMLDecodeError, UnicodeDecodeError:
        return False
    project = data.get("project")
    return isinstance(project, dict) and project.get("name") == "simrun"


def local_dir(root: Path) -> Path:
    """`.simrun/` of a project directory, created with a `.gitignore` of everything."""
    directory = root.absolute() / LOCAL_DIR
    directory.mkdir(parents=True, exist_ok=True)
    ignore = directory / ".gitignore"
    if not ignore.is_file():
        _write_private(ignore, "*\n", mode=0o644)
    return directory


def _write_private(path: Path, text: str, mode: int = 0o600) -> None:
    """Write a file atomically with `mode`, replacing an older one."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(text)
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _create_once(path: Path, text: str) -> None:
    """Create a private file unless it exists; of two concurrent calls one wins."""
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w") as file:
            file.write(text)
        with contextlib.suppress(FileExistsError):
            os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def local_settings(root: Path) -> ServiceSettings:
    """The settings of the local service of a project directory.

    The token file is generated once with a random token of the user, a person who
    owns the service.

    Args:
        root: The project directory.

    Returns:
        The settings: the store, the index and the tokens in `.simrun/`.
    """
    directory = local_dir(root)
    tokens = directory / "tokens.yaml"
    if not tokens.is_file():
        user = getpass.getuser()
        identity = {"name": user, "kind": "person"}
        _create_once(tokens, yaml.safe_dump({secrets.token_urlsafe(32): identity}))
    os.chmod(tokens, 0o600)  # the token runs commands as the user
    return ServiceSettings(
        store=directory / "store",
        db=directory / "simrun.sqlite",
        tokens=tokens,
        owner=getpass.getuser(),
        cluster=ClusterConfig(registry="local"),
        poll_s=0.5,
    )


class _Activity:
    """ASGI middleware: the time since the last request, the web app not counted.

    A request counts from its start to its end, so an open event stream is activity.
    It answers `INSTANCE_PATH` itself, without a token and without activity.
    """

    def __init__(self, app: Callable[..., Any], instance: str = "") -> None:
        self.app = app
        self.instance = instance
        self.last = time.monotonic()
        self.open = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope.get("path", "")
        if scope["type"] == "http" and path == INSTANCE_PATH:
            await _plain(send, self.instance)
            return
        static = path == "/ui" or path.startswith("/ui/")
        if scope["type"] not in ("http", "websocket") or static:
            await self.app(scope, receive, send)
            return
        self.open += 1
        self.last = time.monotonic()
        try:
            await self.app(scope, receive, send)
        finally:
            self.open -= 1
            self.last = time.monotonic()

    def idle_s(self) -> float:
        """Seconds since the end of the last request, 0 while one is open."""
        return 0.0 if self.open else time.monotonic() - self.last


async def _plain(send: Any, text: str) -> None:
    """Send `text` as a plain response of status 200."""
    body = text.encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class _Server(uvicorn.Server):
    """A uvicorn server which calls `cleanup` at the end of its shutdown.

    Uvicorn raises a caught SIGTERM again after its shutdown, which ends the process
    before code after `run` would run; the cleanup runs within the shutdown.
    """

    def __init__(self, config: uvicorn.Config, cleanup: Callable[[], None]) -> None:
        super().__init__(config)
        self.cleanup = cleanup

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        """Shut down, then clean up."""
        try:
            await super().shutdown(sockets)
        finally:
            await asyncio.to_thread(self.cleanup)


def _idle(activity: _Activity, idle_exit_s: float, app: FastAPI) -> bool:
    """Whether no request came for `idle_exit_s` and every run has ended.

    A failure to read the index counts as a live run.
    """
    if activity.idle_s() < idle_exit_s:
        return False
    try:
        return not app.state.service.store.index.any_unended()
    except Exception:
        logger.exception("reading the live runs failed")
        return False


def _user_token(settings: ServiceSettings) -> str:
    """The token of the owner, the person of the user, in the tokens of the settings.

    Raises:
        RuntimeError: The tokens file has no token of the owner.
    """
    for token, identity in load_tokens(settings.tokens).items():
        if identity.kind == "person" and identity.name == settings.owner:
            return token
    raise RuntimeError(f"{settings.tokens} has no token of {settings.owner}")


@contextlib.contextmanager
def _service_lock(directory: Path) -> Generator[None]:
    """Hold `service.lock` while the service runs; RuntimeError if one runs."""
    with open(directory / SERVICE_LOCK, "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            found = read_service(directory.parent)
            running = f" (pid {found.pid}, {found.url})" if found else ""
            raise ServiceRunning(
                f"the local service of {directory.parent} runs already{running}"
            ) from None
        yield


def serve_local(
    root: Path,
    port: int = 0,
    idle_exit_s: float | None = None,
    ready: Callable[[LocalService], None] | None = None,
) -> None:
    """Run the local service of a project directory until stopped.

    Writes `.simrun/service.json` once the service listens and removes it at the end;
    the end cancels every run of the local backend and waits for its processes.

    Args:
        root: The project directory.
        port: The port on `127.0.0.1`, 0 for a free one.
        idle_exit_s: Exit after this many seconds without a request and without a
            live run; None runs until stopped.
        ready: Called with the service once it listens.

    Raises:
        ServiceRunning: The local service of the directory runs already.
        RuntimeError: The tokens file has no token of the user.
        OSError: The port cannot be bound.
    """
    directory = local_dir(root)
    with _service_lock(directory):
        settings = local_settings(root)
        token = _user_token(settings)
        backend = LocalBackend(settings.store, directory / "scratch")
        app = create_app(settings, backend)
        instance = secrets.token_urlsafe(16)
        activity = _Activity(app, instance)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind((HOST, port))
        except OSError:
            listener.close()
            raise
        bound = listener.getsockname()[1]
        service = LocalService(
            instance=instance,
            pid=os.getpid(),
            port=bound,
            token=token,
            url=f"http://{HOST}:{bound}",
        )
        path = directory / SERVICE_FILE
        # the service file is written once listening and removed at the end, never
        # after it
        file_lock = threading.Lock()
        done = threading.Event()

        def cleanup() -> None:
            with file_lock:
                if done.is_set():
                    return
                done.set()
                if read_service(root) == service:
                    path.unlink(missing_ok=True)
            backend.close()
            # the cancelled runs are recorded before the service ends
            try:
                app.state.controller.reconcile()
            except Exception:
                logger.exception("recording the runs at the end of the service failed")

        config = uvicorn.Config(
            activity,
            log_level="warning",
            timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_S,
        )
        server = _Server(config, cleanup)

        def watch() -> None:
            while not server.started:
                if server.should_exit or done.is_set():
                    return
                time.sleep(0.02)
            with file_lock:
                if done.is_set():
                    return
                _write_private(path, json.dumps(dataclasses.asdict(service)))
            if ready is not None:
                ready(service)
            while not server.should_exit:
                time.sleep(_CHECK_S)
                if idle_exit_s is not None and _idle(activity, idle_exit_s, app):
                    logger.warning("idle for %s s, the service exits", idle_exit_s)
                    server.should_exit = True

        watcher = threading.Thread(target=watch, name="local-service", daemon=True)
        watcher.start()
        hangup = _on_hangup(server)
        try:
            server.run(sockets=[listener])
        finally:
            cleanup()
            listener.close()
            if hangup is not None:
                signal.signal(signal.SIGHUP, hangup)


def _on_hangup(server: uvicorn.Server) -> Any:
    """Stop the server on SIGHUP as on SIGTERM, e.g. when its terminal closes.

    Returns:
        The handler before, None outside of the main thread, where no handler can
        be set.
    """
    if threading.current_thread() is not threading.main_thread():
        return None
    return signal.signal(
        signal.SIGHUP, lambda *_: server.handle_exit(signal.SIGTERM, None)
    )


def read_service(root: Path) -> LocalService | None:
    """The service `.simrun/service.json` names.

    Returns:
        The service, None without a readable file or with a URL which is not the one
        of its port on `127.0.0.1`.
    """
    try:
        data = json.loads((root.absolute() / LOCAL_DIR / SERVICE_FILE).read_text())
        service = LocalService(**data)
    except OSError, ValueError, TypeError:
        return None
    if service.url != f"http://{HOST}:{service.port}":
        return None
    return service


def _answers(service: LocalService) -> bool:
    """Whether the service of the file runs and `GET /me` answers.

    The token goes only to a server which answers the instance of the file: a pid
    reused by another process or a port bound by another program never gets it.
    """
    try:
        os.kill(service.pid, 0)
    except ProcessLookupError, PermissionError:
        return False  # gone, or a process of another user
    try:
        with httpx.Client(timeout=2.0) as client:
            instance = client.get(f"{service.url}{INSTANCE_PATH}")
            if instance.status_code != 200 or not secrets.compare_digest(
                instance.content, service.instance.encode()
            ):
                return False
            response = client.get(
                f"{service.url}/me",
                headers={"Authorization": f"Bearer {service.token}"},
            )
    except httpx.HTTPError:
        return False
    return response.status_code == 200


def _running(root: Path) -> LocalService | None:
    found = read_service(root)
    return found if found is not None and _answers(found) else None


def _spawn(directory: Path) -> subprocess.Popen[bytes]:
    """Start the service of `directory.parent` detached, its output to the log."""
    with open(directory / LOG_FILE, "ab") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "simrun",
                "serve",
                "--local",
                "--idle-exit",
                str(IDLE_EXIT_S),
            ],
            cwd=directory.parent,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    # reaped when it ends, so no zombie of a long-lived caller passes for alive
    threading.Thread(target=process.wait, daemon=True).start()
    return process


def _held(directory: Path) -> bool:
    """Whether a service holds the lock of the directory."""
    with open(directory / SERVICE_LOCK, "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(lock, fcntl.LOCK_UN)
        return False


def _log_tail(directory: Path, lines: int = 20) -> str:
    try:
        text = (directory / LOG_FILE).read_text(errors="replace")
    except OSError:
        return ""
    return "".join(text.splitlines(True)[-lines:])


def ensure_local(root: Path, timeout_s: float = 10.0) -> LocalService:
    """The local service of a project directory, started in the background if none runs.

    A service runs when the pid of `service.json` is alive, the server answers the
    instance of the file and `GET /me` answers. The start runs under an exclusive
    lock of `.simrun/start.lock`, so concurrent calls start one service; a start
    refused while an old service still ends is repeated once it has ended.

    Args:
        root: The project directory.
        timeout_s: Seconds to wait for a started service.

    Returns:
        The running service.

    Raises:
        RuntimeError: The service did not start in time or exited; the message has
            the end of `.simrun/service.log`.
    """
    found = _running(root)
    if found is not None:
        return found
    directory = local_dir(root)
    with open(directory / START_LOCK, "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        found = _running(root)
        if found is not None:
            return found
        process = _spawn(directory)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            found = _running(root)
            if found is not None:
                return found
            code = process.poll()
            # while a service holds the lock it starts or ends: wait for either
            if code is not None and not _held(directory):
                if code != EXIT_RUNNING:
                    raise RuntimeError(
                        f"the local service of {root} exited with {code}:"
                        f"\n{_log_tail(directory)}"
                    )
                process = _spawn(directory)  # refused by a service which has ended
            time.sleep(0.05)
        if process.poll() is None:
            process.terminate()
        raise RuntimeError(
            f"the local service of {root} did not start within {timeout_s} s:"
            f"\n{_log_tail(directory)}"
        )
