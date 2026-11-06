import os
import subprocess
from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from fastapi.testclient import TestClient

from simrun.client import Client
from simrun.config import ClusterConfig
from simrun.demo import SQUARES
from simrun.plugins_builtin import COMMAND
from simrun.service.app import ServiceSettings, create_app
from simrun.service.controller import Controller
from simrun.testing import FakeBackend

TYPES = {SQUARES.name: SQUARES, COMMAND.name: COMMAND}


def pytest_xdist_auto_num_workers(config: pytest.Config) -> int | None:
    """One worker for the tests on the k3d cluster, which share its nodes."""
    return 1 if os.environ.get("SIMRUN_K3D") == "1" else None


TOKENS = {
    "t-mk": {"name": "mk", "kind": "person"},
    "t-mk-bot": {"name": "mk-bot", "kind": "agent", "owner": "mk"},
    "t-alice": {"name": "alice", "kind": "person"},
    "t-alice-bot": {"name": "alice-bot", "kind": "agent", "owner": "alice"},
}


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

    def get(self, path: str, token: str = "t-mk", **kw: Any) -> Any:
        return self.client.get(path, headers=self.auth(token), **kw)

    def post(self, path: str, body: Any = None, token: str = "t-mk") -> Any:
        return self.client.post(path, json=body, headers=self.auth(token))

    def submit(self, token: str = "t-mk", **kw: Any) -> dict[str, Any]:
        body = {"workflow": kw.pop("workflow", self.workflow()), "sources": [], **kw}
        response = self.post("/runs", body, token)
        assert response.status_code == 201, response.text
        return response.json()

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
    ) -> Client:
        return Client(
            "http://testserver",
            token,
            service.settings.store,
            transport or InProcessTransport(service.client),
        )

    return factory


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
