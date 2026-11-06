import importlib.util
import json
import shutil
import socket
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn

from simrun.service.app import ServiceSettings, create_app
from simrun.service.controller import TERMINAL_RUN_STATES
from simrun.testing import FakeBackend

_spec = importlib.util.spec_from_file_location(
    "root_conftest", Path(__file__).parents[1] / "conftest.py"
)
assert _spec is not None and _spec.loader is not None
_conftest = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_conftest)
TYPES, make_settings = _conftest.TYPES, _conftest.make_settings

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


def test_events_first_snapshot(live):
    web = live(events_max_s=1.5, poll_s=0.2)
    run_id = web.submit()
    started = time.monotonic()
    got = list(web.stream("/events"))
    assert time.monotonic() - started < 10
    assert [name for name, _ in got[:2]] == ["runs", "servers"]

    runs = json.loads(got[0][1])
    assert [r["id"] for r in runs] == [run_id]
    assert runs[0]["owner"] == "mk"
    assert len(json.loads(got[1][1])) == 12


def test_events_send_changes_only(live):

    web = live(events_max_s=5, poll_s=0.2)
    run_id = web.submit()
    seen: list[str] = []
    first = None
    for name, data in web.stream("/events"):
        seen.append(name)
        if name != "runs":
            continue
        state = json.loads(data)[0]["state"]
        if first is None:
            first = state
            web.backend.start(web.name(run_id))
        else:
            assert state != first
            break
    assert first is not None
    assert seen.count("servers") == 1
    assert seen.count("runs") == 2


def test_run_events_end_with_a_terminal_run(live):

    web = live(events_max_s=30, poll_s=0.2)
    run_id = web.submit()
    web.backend.start(web.name(run_id))
    web.backend.finish(web.name(run_id))
    deadline = time.monotonic() + 30
    while (record := web.http.get(f"/runs/{run_id}").json())[
        "state"
    ] not in TERMINAL_RUN_STATES:
        assert time.monotonic() < deadline, record["state"]
        time.sleep(0.05)
    started = time.monotonic()
    got = list(web.stream(f"/runs/{run_id}/events"))
    assert time.monotonic() - started < 10
    assert sorted(name for name, _ in got) == ["progress", "run", "workers"]
    by_name = dict(got)
    assert json.loads(by_name["run"])["id"] == run_id
    assert list(json.loads(by_name["progress"])) == ["squares"]
    assert list(json.loads(by_name["workers"])) == ["squares"]


def test_events_need_a_token(live):
    web = live(events_max_s=1.5, poll_s=0.2)
    run_id = web.submit()
    assert httpx.get(web.base + "/events").status_code == 401
    assert httpx.get(web.base + f"/runs/{run_id}/events").status_code == 401
    assert httpx.get(web.base + "/runs/nope/events", headers=AUTH).status_code == 404


def test_run_events_end_when_the_run_is_gone(live):
    web = live(events_max_s=30, poll_s=0.2)
    run_id = web.submit()
    names = []
    for name, _ in web.stream(f"/runs/{run_id}/events"):
        names.append(name)
        if name == "workers":
            shutil.rmtree(web.settings.store / "runs" / run_id)
    assert names == ["run", "progress", "workers"]
