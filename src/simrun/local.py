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
import socket
import subprocess
import sys
import threading
import time
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
from simrun.service.index import TERMINAL
from simrun.service.local_backend import LocalBackend
from simrun.store import RunState

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
CHECKOUT_WEB = Path(simrun.__file__).parents[2] / "frontend" / "dist"
"""The built web app of a checkout of the repository."""
_LIVE = tuple(state for state in RunState if state not in TERMINAL)
_CHECK_S = 0.25


@dataclasses.dataclass(frozen=True)
class LocalService:
    """A running local service, as its `service.json` names it.

    Attributes:
        pid: The process of the service.
        port: The port on `127.0.0.1`.
        token: The bearer token of the user.
        url: The URL of the API, `http://127.0.0.1:<port>`.
    """

    pid: int
    port: int
    token: str
    url: str


def web_dir() -> Path | None:
    """The built web app: the one in the package, else the one of a checkout.

    Returns:
        `simrun/web` of the installed package if it has an `index.html`, else
        `frontend/dist` of the checkout simrun is imported from, else None.
    """
    for directory in (PACKAGE_WEB, CHECKOUT_WEB):
        if (directory / "index.html").is_file():
            return directory
    return None


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
    """

    def __init__(self, app: Callable[..., Any]) -> None:
        self.app = app
        self.last = time.monotonic()
        self.open = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope.get("path", "")
        if scope["type"] not in ("http", "websocket") or path.startswith("/ui"):
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
    index = app.state.service.store.index
    try:
        return not any(index.runs(state=state) for state in _LIVE)
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
            raise RuntimeError(
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
        RuntimeError: The local service of the directory runs already.
        OSError: The port cannot be bound.
    """
    directory = local_dir(root)
    with _service_lock(directory):
        settings = local_settings(root)
        token = _user_token(settings)
        backend = LocalBackend(settings.store, directory / "scratch")
        app = create_app(settings, backend)
        activity = _Activity(app)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind((HOST, port))
        except OSError:
            listener.close()
            raise
        bound = listener.getsockname()[1]
        service = LocalService(
            pid=os.getpid(), port=bound, token=token, url=f"http://{HOST}:{bound}"
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
        try:
            server.run(sockets=[listener])
        finally:
            cleanup()
            listener.close()


def read_service(root: Path) -> LocalService | None:
    """The service `.simrun/service.json` names, None without a readable file."""
    try:
        data = json.loads((root.absolute() / LOCAL_DIR / SERVICE_FILE).read_text())
        return LocalService(**data)
    except OSError, ValueError, TypeError:
        return None


def _answers(service: LocalService) -> bool:
    """Whether the process of the service runs and `GET /me` answers."""
    try:
        os.kill(service.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass  # a process of another user: the request decides
    try:
        response = httpx.get(
            f"{service.url}/me",
            headers={"Authorization": f"Bearer {service.token}"},
            timeout=2.0,
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

    A service runs when the pid of `service.json` is alive and `GET /me` answers.
    The start runs under an exclusive lock of `.simrun/start.lock`, so concurrent
    calls start one service.

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
            if process.poll() is not None and not _held(directory):
                raise RuntimeError(
                    f"the local service of {root} exited with {process.returncode}:"
                    f"\n{_log_tail(directory)}"
                )
            time.sleep(0.05)
        if process.poll() is None:
            process.terminate()
        raise RuntimeError(
            f"the local service of {root} did not start within {timeout_s} s:"
            f"\n{_log_tail(directory)}"
        )
