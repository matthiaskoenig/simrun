"""The local service of a project: the service with the local backend in `.simrun/`.

`.simrun/` of a project directory holds the store, the index, the token of the user
and, while the service runs, `service.json` (pid, port, token, url and the
environment it runs with). `simrun serve --local` runs the service in the
foreground; `ensure_local` finds the running one or starts it in the background,
which exits after `IDLE_EXIT_S` without a request and without a live run. A service
in the background which runs with another environment than the command (python,
simrun, task types) is restarted when it is idle. The service listens on
`127.0.0.1` only, and its end cancels the runs of the local backend, so no process
of a run outlives a graceful stop; a service which starts kills the processes one
which died left behind (`LocalBackend.kill_orphans`).
"""

import asyncio
import contextlib
import dataclasses
import fcntl
import getpass
import hashlib
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
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

import httpx
import uvicorn
import yaml
from fastapi import FastAPI

import simrun
from simrun import worker
from simrun.config import ClusterConfig
from simrun.plugins import ENTRY_POINT_GROUP
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
STOP_PATH = "/local/stop"
"""The route which stops the local service, with the token of the user; with
`?idle=1` only when no run is live and no other request is open (409 otherwise)."""
STOP_MARGIN_S = 30.0
"""Seconds a stop waits for the end of the service beyond the grace of the
processes of its runs."""
EXIT_RUNNING = 3
"""The exit code of `simrun serve --local` when the service of the directory runs."""
_CHECK_S = 0.25


class ServiceRunning(RuntimeError):
    """The local service of the directory runs already."""


class ServiceBusy(RuntimeError):
    """The local service has a live run or an open request, so it was not stopped."""


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
        python: The interpreter of the service.
        version: The version of simrun of the service.
        types: The hash of the task types the service found (`environment`).
        background: Whether it was started in the background (`ensure_local`),
            which exits when idle and is restarted for another environment.
    """

    instance: str
    pid: int
    port: int
    token: str
    url: str
    python: str = ""
    version: str = ""
    types: str = ""
    background: bool = False


def environment() -> dict[str, str]:
    """The environment of this process as a local service records it.

    Returns:
        `python` (the interpreter), `version` (of simrun) and `types`, a hash of the
        entry points of the task types with the names and versions of their
        distributions, so a new, removed or upgraded plugin changes it.
    """
    found = sorted(
        f"{ep.name}={ep.value} {ep.dist.name if ep.dist else ''} "
        f"{ep.dist.version if ep.dist else ''}"
        for ep in entry_points(group=ENTRY_POINT_GROUP)
    )
    digest = hashlib.sha256("\n".join(found).encode()).hexdigest()[:16]
    return {"python": sys.executable, "version": simrun.__version__, "types": digest}


def _differences(service: LocalService) -> list[str]:
    """How the environment of a service differs from the one of this process."""
    own = environment()
    differences: list[str] = []
    if service.python != own["python"]:
        differences.append(f"python {service.python or '?'} (not {own['python']})")
    if service.version != own["version"]:
        differences.append(f"simrun {service.version or '?'} (not {own['version']})")
    if service.types != own["types"]:
        differences.append("other task types")
    return differences


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
    It answers `INSTANCE_PATH` itself, without a token and without activity, and
    `STOP_PATH` with the token of the user (`token`): it calls `stop`, with
    `?idle=1` only when no other request is open and `live` says no run is.
    """

    def __init__(
        self,
        app: Callable[..., Any],
        instance: str = "",
        token: str = "",
        live: Callable[[], bool] = lambda: False,
    ) -> None:
        self.app = app
        self.instance = instance
        self.token = token
        self.live = live
        self.stop: Callable[[], None] | None = None
        self.last = time.monotonic()
        self.open = 0

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope.get("path", "")
        if scope["type"] == "http" and path == INSTANCE_PATH:
            await _plain(send, self.instance)
            return
        if scope["type"] == "http" and path == STOP_PATH:
            await self._stop(scope, send)
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

    async def _stop(self, scope: dict[str, Any], send: Any) -> None:
        """Answer `STOP_PATH`: 401 without the token, 405 but for POST, 409 busy."""
        headers = dict(scope.get("headers", []))
        given = headers.get(b"authorization", b"")
        if not self.token or not secrets.compare_digest(
            given, f"Bearer {self.token}".encode()
        ):
            await _plain(send, "a valid bearer token is required", 401)
            return
        if scope.get("method") != "POST":
            await _plain(send, "POST stops the service", 405)
            return
        if b"idle=1" in scope.get("query_string", b"").split(b"&") and (
            self.open or await asyncio.to_thread(self.live)
        ):
            await _plain(send, "the service has a live run or an open request", 409)
            return
        if self.stop is not None:
            self.stop()
        await _plain(send, "stopping")


async def _plain(send: Any, text: str, status: int = 200) -> None:
    """Send `text` as a plain response."""
    body = text.encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
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

    def __init__(
        self,
        config: uvicorn.Config,
        cleanup: Callable[[], None],
        starting: Callable[[], None] = lambda: None,
    ) -> None:
        super().__init__(config)
        self.cleanup = cleanup
        self.starting = starting

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        """Say what the shutdown waits for, shut down, then clean up."""
        try:
            self.starting()
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
    stopping: Callable[[str], None] | None = None,
) -> None:
    """Run the local service of a project directory until stopped.

    Writes `.simrun/service.json` once the service listens and removes it at the end;
    the end cancels every run of the local backend and waits for its processes. At
    the start it kills the processes a service before left running when it died.

    Args:
        root: The project directory.
        port: The port on `127.0.0.1`, 0 for a free one.
        idle_exit_s: Exit after this many seconds without a request and without a
            live run; None runs until stopped. Set for a service in the background.
        ready: Called with the service once it listens.
        stopping: Called with what the end of the service waits for, the open
            requests and the processes of the runs, when it waits for any.

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
        killed = backend.kill_orphans()
        if killed:
            logger.warning("killed %d processes of runs of a service before", killed)
        app = create_app(settings, backend)
        instance = secrets.token_urlsafe(16)

        def live() -> bool:
            return bool(app.state.service.store.index.any_unended())

        activity = _Activity(app, instance, token, live)
        notify = stopping or (lambda message: logger.warning("%s", message))
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
            background=idle_exit_s is not None,
            **environment(),
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
            running = backend.placements()
            if running:
                runs = len({p.workflow for p in running})
                notify(
                    f"cancelling {runs} runs: waiting up to {worker.GRACE_S:.0f} s "
                    f"for their {len(running)} processes to end"
                )
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

        def starting() -> None:
            if activity.open:
                notify(
                    f"waiting up to {GRACEFUL_SHUTDOWN_S} s for {activity.open} "
                    "open requests, e.g. event streams"
                )

        server = _Server(config, cleanup, starting)

        def stop() -> None:
            server.should_exit = True

        activity.stop = stop

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


def stop_local(
    root: Path,
    idle: bool = False,
    stopping: Callable[[LocalService], None] | None = None,
    timeout_s: float = worker.GRACE_S + STOP_MARGIN_S,
) -> LocalService | None:
    """Stop the local service of a project directory and wait for its end.

    The end of the service cancels its runs, whose processes get `worker.GRACE_S`.

    Args:
        root: The project directory.
        idle: Stop it only when no run is live and no request is open.
        stopping: Called with the service once it stops, before the wait.
        timeout_s: Seconds to wait for its end.

    Returns:
        The service which was stopped, None when none runs.

    Raises:
        ServiceBusy: With `idle`, the service has a live run or an open request.
        RuntimeError: The service refused to stop or did not end in time.
    """
    found = _running(root)
    if found is None:
        return None
    try:
        response = httpx.post(
            f"{found.url}{STOP_PATH}",
            params={"idle": "1"} if idle else None,
            headers={"Authorization": f"Bearer {found.token}"},
            timeout=10.0,
        )
    except httpx.HTTPError:
        response = None  # ended meanwhile, or ending
    if response is not None and response.status_code == 409:
        raise ServiceBusy(response.text)
    if response is not None and response.status_code != 200:
        raise RuntimeError(
            f"the local service of {root} did not stop: {response.status_code} "
            f"{response.text}"
        )
    if stopping is not None:
        stopping(found)
    directory = local_dir(root)
    deadline = time.monotonic() + timeout_s
    while _held(directory) or _pid_alive(found.pid):
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"the local service of {root} (pid {found.pid}) did not end within "
                f"{timeout_s:.0f} s"
            )
        time.sleep(0.05)
    return found


def _pid_alive(pid: int) -> bool:
    """Whether a process runs, a zombie not counted."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


def ensure_local(
    root: Path,
    timeout_s: float = 10.0,
    warn: Callable[[str], None] | None = None,
) -> LocalService:
    """The local service of a project directory, started in the background if none runs.

    A service runs when the pid of `service.json` is alive, the server answers the
    instance of the file and `GET /me` answers. The start runs under an exclusive
    lock of `.simrun/start.lock`, so concurrent calls start one service; a start
    refused while an old service still ends is repeated once it has ended.

    A service whose environment (python, simrun, task types) differs from the one of
    this process runs code this command does not: one in the background without a
    live run and without an open request is stopped and started again; else it is
    used with a warning.

    Args:
        root: The project directory.
        timeout_s: Seconds to wait for a started service.
        warn: Called with a warning for the user, by default logged.

    Returns:
        The running service.

    Raises:
        RuntimeError: The service did not start in time or exited; the message has
            the end of `.simrun/service.log`.
    """
    found = _running(root)
    if found is not None:
        differences = _differences(found)
        if not differences:
            return found
        what = ", ".join(differences)
        say = warn or (lambda message: logger.warning("%s", message))
        if not found.background:
            say(
                f"the local service of {root} (pid {found.pid}) runs in the "
                f"foreground with {what}: restart it to use this environment"
            )
            return found
        try:
            stop_local(root, idle=True)
        except ServiceBusy:
            say(
                f"the local service of {root} (pid {found.pid}) runs with {what} "
                "and is busy, so it keeps running; `simrun serve --local --stop` "
                "stops it"
            )
            return found
        logger.info("the local service of %s ran with %s: restarted", root, what)
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
