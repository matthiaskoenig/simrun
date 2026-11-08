import dataclasses
import os
import socket
import subprocess
import sys
import threading
import time
import tomllib
from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, Literal

import httpx
import pytest
import tomlkit
import uvicorn
import yaml
from fastapi.testclient import TestClient
from pydantic import BaseModel

from simrun.client import Client
from simrun.config import ClusterConfig
from simrun.demo import SQUARES, SquaresSettings
from simrun.environment import BUILD_CONSTRAINTS
from simrun.plugins import ResourceHint, TaskType
from simrun.plugins_builtin import COMMAND
from simrun.service.app import ServiceSettings, create_app
from simrun.service.controller import Controller
from simrun.service.index import IndexedRunStore
from simrun.testing import TOKENS, FakeBackend


@pytest.fixture(scope="session", autouse=True)
def _uv_cache_with_the_build_backends(
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """Put the pinned build backends and simrun's dependencies into the uv cache.

    The tests of the environments, the submissions and the installs of snapshots
    run uv offline (`UV_OFFLINE=1`), as a pod installs without an index, so they
    read from the cache of uv the build backends at the pins of
    `build_constraints.txt` and the dependencies of simrun, whose wheel the
    environments of the tests lock. A universal lock reads the metadata of other
    wheels than an install, so a project of all of them is locked and synced. A
    cold cache, e.g. of a fresh runner of continuous integration, gets them from
    the index, once per worker; a warm one is not asked, so the tests run without
    a network once the cache has them.
    """
    project = tmp_path_factory.mktemp("uv-cache")
    with (Path(__file__).parents[1] / "pyproject.toml").open("rb") as f:
        dependencies = tomllib.load(f)["project"]["dependencies"]
    pins = [
        line.strip()
        for line in BUILD_CONSTRAINTS.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    (project / "pyproject.toml").write_text(
        tomlkit.dumps(
            {
                "project": {
                    "name": "uv-cache",
                    "version": "0",
                    "requires-python": ">=3.14",
                    # pytest: the dev group of a model of the tests
                    "dependencies": [*dependencies, *pins, "pytest"],
                }
            }
        )
    )
    sync = ["uv", "sync", "-q", "--no-install-project", "--python", sys.executable]
    offline = {**os.environ, "UV_OFFLINE": "1"}
    if subprocess.run(sync, cwd=project, env=offline, check=False).returncode != 0:
        subprocess.run(sync, cwd=project, check=True)


class ReportSettings(BaseModel):
    title: str = "report"


# an unsplit plugin type: its command runs through `simrun-exec`
REPORT = TaskType(
    name="demo.report",
    settings=ReportSettings,
    command="report --settings {settings} --out {out}",
    hint=ResourceHint(memory_base_gb=0.5, memory_per_worker_gb=1.0),
)


class SubsetSettings(SquaresSettings):
    subset: str = "all"


# a split type whose cost depends on a setting: its observations are kept apart
SUBSET = dataclasses.replace(
    SQUARES, name="demo.subset", settings=SubsetSettings, cost_setting="subset"
)
TYPES = {
    SQUARES.name: SQUARES,
    COMMAND.name: COMMAND,
    REPORT.name: REPORT,
    SUBSET.name: SUBSET,
}


def pytest_xdist_auto_num_workers(config: pytest.Config) -> int | None:
    """One worker for the tests on the k3d cluster, which share its nodes."""
    return 1 if os.environ.get("SIMRUN_K3D") == "1" else None


class Service:
    """A running app with its fake backend, and helpers of the tests."""

    def __init__(
        self, client: TestClient, backend: FakeBackend, settings: ServiceSettings
    ) -> None:
        self.client = client
        self.backend = backend
        self.settings = settings

    @staticmethod
    def auth(token: str = "t-mk") -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    @staticmethod
    def workflow(n: int = 4, **task: Any) -> dict[str, Any]:
        return {
            "name": "demo",
            "tasks": [
                {"id": "squares", "type": "demo.squares", "settings": {"n": n}, **task}
            ],
        }

    @property
    def controller(self) -> Controller:
        return self.client.app.state.controller  # ty: ignore[unresolved-attribute]

    @property
    def store(self) -> IndexedRunStore:
        """The store of the service: a record written through it is indexed."""
        return self.controller.store

    def get(self, path: str, token: str = "t-mk", **kw: Any) -> Any:
        return self.client.get(path, headers=self.auth(token), **kw)

    def post(self, path: str, body: Any = None, token: str = "t-mk") -> Any:
        return self.client.post(path, json=body, headers=self.auth(token))

    def submit(
        self, token: str = "t-mk", wait: bool = True, **kw: Any
    ) -> dict[str, Any]:
        """Submit a run; with `wait` the record once its submission ended."""
        body = {"workflow": kw.pop("workflow", self.workflow()), "sources": [], **kw}
        response = self.post("/runs", body, token)
        assert response.status_code == 201, response.text
        summary = response.json()
        assert summary["state"] == "submitting"
        return self.submitted(summary["id"]) if wait else summary

    def submitted(self, run_id: str, timeout_s: float = 60.0) -> dict[str, Any]:
        """The record of a run once its submission in the background ended."""
        deadline = time.monotonic() + timeout_s
        while self.controller.in_flight(run_id):
            assert time.monotonic() < deadline, f"run {run_id} is still submitted"
            time.sleep(0.01)
        return self.get(f"/runs/{run_id}").json()

    def name(self, run_id: str) -> str:
        return f"simrun-{run_id}"

    def run_dir(self, run_id: str) -> Path:
        return self.settings.store / "runs" / run_id


def make_settings(tmp_path: Path, **kw: Any) -> ServiceSettings:
    tokens = tmp_path / "tokens.yaml"
    tokens.write_text(yaml.safe_dump(TOKENS))
    values: dict[str, Any] = {
        "store": tmp_path / "store",
        "db": tmp_path / "db" / "resources.sqlite",
        "tokens": tokens,
        "owner": "mk",
        "cluster": ClusterConfig(registry="localhost:5000"),
        "poll_s": 3600.0,
        "default_image": "localhost:5000/base:1",
        "web": None,
    }
    values.update(kw)
    return ServiceSettings(**values)


@contextmanager
def _running(settings: ServiceSettings, backend: FakeBackend) -> Generator[Service]:
    with TestClient(create_app(settings, backend, TYPES)) as client:
        yield Service(client, backend, settings)


@pytest.fixture
def service_factory(
    tmp_path: Path,
) -> Callable[..., AbstractContextManager[Service]]:
    """Start an app: `backend=` a fake backend, other keywords are settings."""

    def factory(
        backend: FakeBackend | None = None, **kw: Any
    ) -> AbstractContextManager[Service]:
        return _running(make_settings(tmp_path, **kw), backend or FakeBackend())

    return factory


@pytest.fixture
def service(
    service_factory: Callable[..., AbstractContextManager[Service]],
) -> Iterator[Service]:
    with service_factory() as svc:
        yield svc


class InProcessTransport(httpx.BaseTransport):
    """Sends the requests of an `httpx.Client` to the app of a `TestClient`."""

    def __init__(self, client: TestClient) -> None:
        self.client = client
        self.requests: list[httpx.Request] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        request.read()  # a streamed body, e.g. an upload of a file
        response = self.client.request(
            request.method,
            request.url.raw_path.decode(),
            content=request.content,
            headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
        )
        return httpx.Response(
            response.status_code,
            headers={
                k: v
                for k, v in response.headers.items()
                if k.lower() not in ("content-length", "content-encoding")
            },
            content=response.content,
        )


@pytest.fixture
def make_client(
    tmp_path: Path,
) -> Callable[..., Client]:
    """A `Client` of a running `Service`: `make_client(service, token=, transport=)`."""

    def factory(
        service: Service,
        token: str = "t-mk",
        transport: httpx.BaseTransport | None = None,
        sources: Literal["store", "upload"] = "store",
    ) -> Client:
        return Client(
            "http://testserver",
            token,
            service.settings.store if sources == "store" else None,
            sources,
            transport=transport or InProcessTransport(service.client),
        )

    return factory


@pytest.fixture
def in_process() -> Callable[[Service], InProcessTransport]:
    """An `InProcessTransport` to the app of a `Service`, which records requests."""
    return lambda service: InProcessTransport(service.client)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A git repository with a model file."""
    path = tmp_path / "model"
    path.mkdir()
    for args in (
        ["init", "-q", "-b", "main"],
        ["config", "user.email", "a@b.c"],
        ["config", "user.name", "T"],
    ):
        subprocess.run(["git", "-C", str(path), *args], check=True)
    (path / "model.txt").write_text("model\n")
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-q", "-m", "init"], check=True)
    return path


AUTH = {"Authorization": "Bearer t-mk"}


class Live:
    """A running app on a socket, for streams which are read while they change."""

    def __init__(self, base: str, app: Any, backend: FakeBackend, settings: Any):
        self.base = base
        self.app = app
        self.backend = backend
        self.settings = settings
        self.http = httpx.Client(base_url=base, headers=AUTH, timeout=30.0)

    def submit(self) -> str:
        workflow = {
            "name": "demo",
            "tasks": [{"id": "squares", "type": "demo.squares", "settings": {"n": 4}}],
        }
        response = self.http.post("/runs", json={"workflow": workflow, "sources": []})
        assert response.status_code == 201, response.text
        run_id = response.json()["id"]
        deadline = time.monotonic() + 60
        while self.app.state.controller.in_flight(run_id):
            assert time.monotonic() < deadline
            time.sleep(0.01)
        return run_id

    def name(self, run_id: str) -> str:
        return f"simrun-{run_id}"

    def stream(
        self, path: str, headers: dict[str, str] | None = None
    ) -> Iterator[tuple[str, str]]:
        """The events `(name, data)` of a stream, until the server ends it."""
        with httpx.stream(
            "GET", self.base + path, headers=headers or AUTH, timeout=30.0
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            assert response.headers["cache-control"] == "no-cache"
            assert response.headers["x-accel-buffering"] == "no"
            name = None
            for line in response.iter_lines():
                if line.startswith("event: "):
                    name = line[7:]
                elif line.startswith("data: ") and name is not None:
                    yield name, line[6:]
                    name = None


@pytest.fixture
def live(tmp_path: Path) -> Iterator[Callable[..., Live]]:
    """Start an app on a socket: `live(**settings)` gives a `Live`."""
    started: list[tuple[uvicorn.Server, threading.Thread, Live]] = []

    def start(**kw: Any) -> Live:
        settings: ServiceSettings = make_settings(tmp_path, **kw)
        backend = FakeBackend()
        app = create_app(settings, backend, TYPES)
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        )
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 30
        while not server.started:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        item = Live(f"http://127.0.0.1:{port}", app, backend, settings)
        started.append((server, thread, item))
        return item

    yield start
    for server, thread, item in started:
        item.http.close()
        server.should_exit = True
        thread.join(30)
