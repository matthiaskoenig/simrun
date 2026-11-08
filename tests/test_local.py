"""The local service of a project: settings in .simrun, started on demand."""

import asyncio
import fcntl
import getpass
import http.server
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, ClassVar

import httpx
import pytest
import yaml

import simrun
from simrun import local
from simrun.config import ClusterConfig
from simrun.local import (
    LOCAL_DIR,
    LocalService,
    ensure_local,
    local_settings,
    serve_local,
    web_dir,
)
from simrun.service.app import ServiceSettings

DEADLINE_S = 30.0
USER = getpass.getuser()


def _alive(pid: int) -> bool:
    """Whether a process runs; a zombie counts as ended."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


def _wait_until(condition: Callable[[], bool], deadline_s: float = DEADLINE_S) -> None:
    deadline = time.monotonic() + deadline_s
    while not condition():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)


def _service_file(root: Path) -> Path:
    return root / LOCAL_DIR / "service.json"


def _stop(pid: int) -> None:
    """SIGTERM a service and wait for its end."""
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    _wait_until(lambda: not _alive(pid), 90)


@pytest.fixture
def root(tmp_path: Path) -> Iterator[Path]:
    """A project directory whose services are stopped at the end of the test."""
    project = tmp_path / "project"
    project.mkdir()
    started: list[int] = []
    spawn = local._spawn

    def spawning(directory: Path) -> subprocess.Popen[bytes]:
        process = spawn(directory)
        started.append(process.pid)
        return process

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(local, "_spawn", spawning)
        yield project
    path = _service_file(project)
    if path.is_file():
        started.append(json.loads(path.read_text())["pid"])
    for pid in started:
        if pid != os.getpid():
            _stop(pid)


def _get(service: LocalService, path: str) -> httpx.Response:
    return httpx.get(
        f"{service.url}{path}",
        headers={"Authorization": f"Bearer {service.token}"},
        timeout=10,
    )


def _submit(service: LocalService, code: str) -> str:
    task = {
        "id": "sleep",
        "type": "command",
        "settings": {
            "image": "localhost:5000/tools:1",
            "args": [sys.executable, "-c", code],
            "cores": 1,
            "memory_gb": 0.5,
        },
    }
    response = httpx.post(
        f"{service.url}/runs",
        json={"workflow": {"name": "local", "tasks": [task]}},
        headers={"Authorization": f"Bearer {service.token}"},
        timeout=10,
    )
    assert response.status_code == 201, response.text
    return response.json()["id"]


def _state(service: LocalService, run_id: str) -> str:
    return _get(service, f"/runs/{run_id}").json()["state"]


def test_local_settings_keeps_the_tokens_private(tmp_path):
    tokens = local_settings(tmp_path).tokens
    tokens.chmod(0o644)
    assert local_settings(tmp_path).tokens.stat().st_mode & 0o777 == 0o600


def test_local_settings_creates_the_tokens_once(tmp_path):
    settings = local_settings(tmp_path)
    directory = tmp_path / LOCAL_DIR
    assert settings.store == directory / "store"
    assert settings.db == directory / "simrun.sqlite"
    assert settings.tokens == directory / "tokens.yaml"
    assert settings.owner == USER
    assert settings.poll_s == 0.5
    assert settings.cluster == ClusterConfig(registry="local")
    assert (directory / ".gitignore").read_text() == "*\n"
    text = settings.tokens.read_text()
    tokens = yaml.safe_load(text)
    assert len(tokens) == 1
    token, identity = next(iter(tokens.items()))
    assert len(token) >= 32
    assert identity == {"name": USER, "kind": "person"}
    assert settings.tokens.stat().st_mode & 0o777 == 0o600
    assert local_settings(tmp_path).tokens.read_text() == text


def test_web_dir_prefers_the_package(tmp_path, monkeypatch):
    package, repository = tmp_path / "package", tmp_path / "repository"
    checkout = repository / "frontend" / "dist"
    monkeypatch.setattr(local, "PACKAGE_WEB", package)
    monkeypatch.setattr(local, "CHECKOUT", repository)
    assert web_dir() is None
    checkout.mkdir(parents=True)
    assert web_dir() is None  # no index.html
    (checkout / "index.html").write_text("<p>dist</p>")
    assert web_dir() is None  # no checkout of simrun
    (repository / "pyproject.toml").write_text('[project]\nname = "other"\n')
    assert web_dir() is None
    (repository / "pyproject.toml").write_text('[project]\nname = "simrun"\n')
    assert web_dir() == checkout
    package.mkdir()
    (package / "index.html").write_text("<p>package</p>")
    assert web_dir() == package
    settings = ServiceSettings(
        store=tmp_path,
        db=tmp_path / "db",
        tokens=tmp_path / "tokens.yaml",
        owner="mk",
        cluster=ClusterConfig(registry="local"),
    )
    assert settings.web == package
    assert settings.model_copy(update={"web": None}).web is None


def test_ensure_local_starts_one_service(root):
    service = ensure_local(root)
    assert re.fullmatch(r"http://127\.0\.0\.1:\d+", service.url)
    got = _get(service, "/me")
    assert got.status_code == 200
    assert got.json() == {"name": USER, "kind": "person", "owner": USER}
    path = _service_file(root)
    assert path.stat().st_mode & 0o777 == 0o600
    assert json.loads(path.read_text()) == {
        "instance": service.instance,
        "pid": service.pid,
        "port": service.port,
        "token": service.token,
        "url": service.url,
        "background": True,
        **local.environment(),
    }
    assert ensure_local(root) == service
    assert (root / LOCAL_DIR / ".gitignore").read_text() == "*\n"
    assert (root / LOCAL_DIR / "service.log").is_file()
    # the web app is served without a token when it is built
    if web_dir() is not None:
        assert httpx.get(f"{service.url}/ui/", timeout=10).status_code == 200


def test_a_stale_service_file_starts_a_new_service(root):
    dead = subprocess.Popen(["true"])
    dead.wait()
    path = _service_file(root)
    path.parent.mkdir()
    path.write_text(
        json.dumps(
            {
                "instance": "i",
                "pid": dead.pid,
                "port": 1,
                "token": "t",
                "url": "http://127.0.0.1:1",
            }
        )
    )
    service = ensure_local(root)
    assert service.pid != dead.pid
    assert _get(service, "/me").status_code == 200


class _Recorder(http.server.BaseHTTPRequestHandler):
    seen: ClassVar[list[str]] = []

    def do_GET(self) -> None:
        self.seen.append(f"{self.path} {self.headers.get('Authorization')}")
        body = b"not simrun"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


def test_the_token_goes_only_to_the_service_of_the_file(root):
    """A live pid and another listener on the port of service.json get no token."""
    listener = http.server.HTTPServer(("127.0.0.1", 0), _Recorder)
    thread = threading.Thread(target=listener.serve_forever, daemon=True)
    thread.start()
    try:
        port = listener.server_address[1]
        path = _service_file(root)
        path.parent.mkdir()
        path.write_text(
            json.dumps(
                {
                    "instance": "the-instance",
                    "pid": os.getpid(),
                    "port": port,
                    "token": "secret-token",
                    "url": f"http://127.0.0.1:{port}",
                }
            )
        )
        service = ensure_local(root)
    finally:
        listener.shutdown()
        listener.server_close()
    assert service.port != port
    assert service.pid != os.getpid()
    assert _Recorder.seen  # it was asked for its instance
    assert not any("secret-token" in request for request in _Recorder.seen)
    assert _get(service, "/me").status_code == 200


def test_ensure_local_waits_for_the_end_of_an_old_service(root, monkeypatch):
    """A start refused while an old service still holds the lock is repeated."""
    spawned: list[int] = []
    spawn = local._spawn

    def counting(directory: Path) -> subprocess.Popen[bytes]:
        process = spawn(directory)
        spawned.append(process.pid)
        return process

    monkeypatch.setattr(local, "_spawn", counting)
    directory = local.local_dir(root)
    lock = open(directory / local.SERVICE_LOCK, "a")  # noqa: SIM115
    fcntl.flock(lock, fcntl.LOCK_EX)
    found: list[LocalService] = []
    thread = threading.Thread(target=lambda: found.append(ensure_local(root, 20)))
    thread.start()
    _wait_until(lambda: len(spawned) == 1 and not _alive(spawned[0]))
    lock.close()  # the old service ends
    thread.join(DEADLINE_S)
    assert len(found) == 1
    assert len(spawned) == 2
    assert found[0].pid == spawned[1]


def test_concurrent_calls_start_one_service(root, monkeypatch):
    spawned: list[Path] = []
    spawn = local._spawn

    def counting(directory: Path) -> subprocess.Popen[bytes]:
        spawned.append(directory)
        return spawn(directory)

    monkeypatch.setattr(local, "_spawn", counting)
    found: list[LocalService] = []

    def call() -> None:
        found.append(ensure_local(root))

    threads = [threading.Thread(target=call) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(DEADLINE_S)
    assert len(found) == 3
    assert len({s.pid for s in found}) == 1
    assert len(spawned) == 1


def test_a_second_service_of_the_directory_is_refused(root):
    service = ensure_local(root)
    with pytest.raises(RuntimeError, match=f"runs already.*{service.pid}"):
        serve_local(root)


def test_the_service_exits_when_idle_but_not_during_a_run(root):
    process = subprocess.Popen(
        [sys.executable, "-m", "simrun", "serve", "--local", "--idle-exit", "2"],
        cwd=root,
        stdout=subprocess.DEVNULL,
    )
    try:
        path = _service_file(root)
        _wait_until(path.is_file)
        data = json.loads(path.read_text())
        service = LocalService(**data)
        run_id = _submit(service, "import time; time.sleep(4)")
        time.sleep(3)
        assert process.poll() is None  # the run is live
        _wait_until(lambda: _state(service, run_id) == "succeeded")
        assert process.wait(DEADLINE_S) == 0
        assert not path.exists()
    finally:
        process.kill()
        process.wait()


def _start_sleeper(service: LocalService, root: Path) -> int:
    """Submit a task which sleeps, the pid of its process once it runs."""
    pid_file = root / "task.pid"
    code = (
        "import os, pathlib, time; "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid())); "
        "time.sleep(600)"
    )
    _submit(service, code)
    _wait_until(lambda: pid_file.is_file() and pid_file.read_text() != "")
    task = int(pid_file.read_text())
    assert _alive(task)
    return task


def test_stopping_the_service_ends_the_processes_of_a_run(root):
    service = ensure_local(root)
    task = _start_sleeper(service, root)
    [run_id] = [
        r for r in os.listdir(root / LOCAL_DIR / "store" / "runs") if r[0] != "."
    ]
    _stop(service.pid)
    assert not _alive(task)
    assert not _service_file(root).exists()
    record = json.loads(
        (root / LOCAL_DIR / "store" / "runs" / run_id / "run.json").read_text()
    )
    assert record["state"] == "failed"


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGHUP])
def test_serve_local_in_the_foreground(root, signum):
    process = subprocess.Popen(
        [sys.executable, "-m", "simrun", "serve", "--local"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        line = process.stdout.readline()
        match = re.fullmatch(
            r"simrun at (http://127\.0\.0\.1:(\d+))/ui/ "
            r"\(token in \.simrun/service\.json\)\n",
            line,
        )
        assert match, line
        data = json.loads(_service_file(root).read_text())
        assert data["pid"] == process.pid
        assert data["url"] == match[1]
        assert data["port"] == int(match[2])
        task = _start_sleeper(LocalService(**data), root)
        process.send_signal(signum)
        _, errors = process.communicate(timeout=DEADLINE_S)
        assert not _service_file(root).exists()
        assert not _alive(task)
        # what the end waits for
        assert "cancelling 1 runs: waiting up to 60 s for their 1 processes" in errors
    finally:
        process.kill()
        process.wait()


def test_requests_to_the_web_app_are_no_activity():
    async def app(scope: dict[str, Any], receive: Any, send: Any) -> None:
        await asyncio.sleep(0)

    activity = local._Activity(app)
    activity.last -= 100
    asyncio.run(activity({"type": "http", "path": "/ui/index.html"}, None, None))
    asyncio.run(activity({"type": "http", "path": "/ui"}, None, None))
    assert activity.idle_s() >= 100
    asyncio.run(activity({"type": "http", "path": "/uix"}, None, None))
    assert activity.idle_s() < 100
    activity.last -= 100
    asyncio.run(activity({"type": "http", "path": "/me"}, None, None))
    assert activity.idle_s() < 100


def _other_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let this process have another simrun than the services it starts."""
    own = local.environment()
    monkeypatch.setattr(local, "environment", lambda: {**own, "version": "9.9.9"})


def test_a_service_with_another_environment_is_restarted_when_idle(root, monkeypatch):
    service = ensure_local(root)
    _other_environment(monkeypatch)
    warnings: list[str] = []
    again = ensure_local(root, warn=warnings.append)
    assert again.pid != service.pid
    assert not _alive(service.pid)
    assert _get(again, "/me").status_code == 200
    assert warnings == []


def test_a_busy_service_with_another_environment_keeps_running(root, monkeypatch):
    service = ensure_local(root)
    _start_sleeper(service, root)
    _other_environment(monkeypatch)
    warnings: list[str] = []
    assert ensure_local(root, warn=warnings.append) == service
    [warning] = warnings
    assert f"simrun {simrun.__version__} (not 9.9.9)" in warning
    assert "simrun serve --local --stop" in warning


def test_a_foreground_service_with_another_environment_is_kept(root, monkeypatch):
    process = subprocess.Popen(
        [sys.executable, "-m", "simrun", "serve", "--local"],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_until(_service_file(root).is_file)
        _other_environment(monkeypatch)
        warnings: list[str] = []
        assert ensure_local(root, warn=warnings.append).pid == process.pid
        assert "runs in the foreground" in warnings[0]
    finally:
        process.terminate()
        process.wait(DEADLINE_S)


def test_the_stop_route_needs_the_token_and_post(root):
    service = ensure_local(root)
    url = f"{service.url}{local.STOP_PATH}"
    assert httpx.post(url, timeout=10).status_code == 401
    bearer = {"Authorization": "Bearer nope"}
    assert httpx.post(url, headers=bearer, timeout=10).status_code == 401
    assert _get(service, local.STOP_PATH).status_code == 405
    assert _alive(service.pid)


def test_stop_local_cancels_the_runs_and_waits(root):
    service = ensure_local(root)
    task = _start_sleeper(service, root)
    with pytest.raises(local.ServiceBusy):
        local.stop_local(root, idle=True)
    seen: list[LocalService] = []
    assert local.stop_local(root, stopping=seen.append) == service
    assert seen == [service]
    assert not _alive(service.pid)
    assert not _alive(task)
    assert not _service_file(root).exists()
    assert local.stop_local(root) is None


def test_a_new_service_kills_the_processes_a_dead_one_left(root):
    service = ensure_local(root)
    task = _start_sleeper(service, root)
    os.kill(service.pid, signal.SIGKILL)  # no cleanup: the task runs on
    _wait_until(lambda: not _alive(service.pid))
    assert _alive(task)
    ensure_local(root)
    _wait_until(lambda: not _alive(task))
    store = root / LOCAL_DIR / "store"
    assert list(store.glob("runs/*/tasks/*/logs/*.pid")) == []
