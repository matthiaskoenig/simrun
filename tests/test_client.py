import dataclasses
import json
import sys
import time
import types
from pathlib import Path

import httpx
import pytest

import simrun.client
from simrun.client import ClientError
from simrun.demo import SQUARES, SquaresSettings
from simrun.testing import FakeBackend
from simrun.workflow import Workflow

WORKFLOW = {
    "name": "demo",
    "tasks": [{"id": "squares", "type": "demo.squares", "settings": {"n": 4}}],
}


class Flaky(httpx.BaseTransport):
    """Fails the first `failures` requests of a path with a connection error."""

    def __init__(self, inner: httpx.BaseTransport, path: str, failures: int) -> None:
        self.inner = inner
        self.path = path
        self.failures = failures

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if self.path in request.url.path and self.failures > 0:
            self.failures -= 1
            raise httpx.ConnectError("dropped", request=request)
        return self.inner.handle_request(request)


def test_submit_snapshots_then_posts(service, make_client, repo: Path) -> None:
    client = make_client(service)
    summary = client.submit(
        _write(repo.parent / "wf.yaml"), {"model": repo}, group="study"
    )
    record = client.status(summary.id)
    (source,) = record.sources
    assert source.name == "model"
    assert (service.settings.store / source.snapshot / "model.txt").is_file()
    assert record.group == "study"
    assert [s.id for s in client.list_runs(group="study")] == [summary.id]
    assert [s.id for s in client.list_runs(owner="mk")] == [summary.id]
    assert client.list_runs(owner="alice") == []


def _plan(settings: SquaresSettings) -> SquaresSettings:
    return settings.model_copy(update={"sleep_s": 0.25 * settings.n})


@pytest.fixture
def planning(monkeypatch) -> None:
    """`demo.squares` is installed locally with a `plan` which sets `sleep_s`."""
    module = types.ModuleType("simrun_test_plan")
    module.plan = _plan  # ty: ignore[unresolved-attribute]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    planned = dataclasses.replace(SQUARES, plan=f"{module.__name__}:plan")
    monkeypatch.setattr(
        simrun.client, "load_task_types", lambda: {SQUARES.name: planned}
    )


def test_client_plans_tasks_of_local_types(
    service, make_client, repo: Path, planning
) -> None:
    client = make_client(service)
    workflow = Workflow(**WORKFLOW)
    body = client._request(workflow, {}, None, None, None)
    (task,) = body["workflow"]["tasks"]
    assert task["settings"] == SquaresSettings(n=4, sleep_s=1.0).model_dump(mode="json")
    # the workflow of the caller is not changed
    assert workflow.tasks[0].settings == {"n": 4}
    # the service writes the planned settings
    summary = client.submit(workflow, {"model": repo})
    task_dir = service.run_dir(summary.id) / "tasks/squares"
    assert json.loads((task_dir / "settings.json").read_text())["sleep_s"] == 1.0


def test_client_plans_before_the_snapshots(
    service, make_client, repo: Path, planning
) -> None:
    task = {"id": "squares", "type": "demo.squares", "settings": {"n": "x"}}
    bad = {"name": "demo", "tasks": [task]}
    with pytest.raises(ValueError, match=r"task 'squares' of type 'demo\.squares'"):
        make_client(service).submit(Workflow(**bad), {"model": repo})
    assert not (service.settings.store / "snapshots").exists()


def test_client_keeps_tasks_of_unknown_types(service, make_client, monkeypatch) -> None:
    client = make_client(service)
    workflow = Workflow(**WORKFLOW)
    # a type not installed locally is submitted as written
    monkeypatch.setattr(simrun.client, "load_task_types", dict)
    body = client._request(workflow, {}, None, None, None)
    assert body["workflow"]["tasks"][0]["settings"] == {"n": 4}
    # so is a type installed locally without a `plan`
    monkeypatch.setattr(
        simrun.client, "load_task_types", lambda: {SQUARES.name: SQUARES}
    )
    body = client._request(workflow, {}, None, None, None)
    assert body["workflow"]["tasks"][0]["settings"] == {"n": 4}


def test_submit_refused_has_detail(service, make_client) -> None:
    bad = {"name": "x", "tasks": [{"id": "a", "type": "nope"}]}
    from simrun.workflow import Workflow

    with pytest.raises(ClientError) as error:
        make_client(service).submit(Workflow(**bad), {})
    assert error.value.status == 422
    assert "nope" in error.value.detail


def test_wait_survives_disconnect(service_factory, make_client, monkeypatch) -> None:
    monkeypatch.setattr(simrun.client.time, "sleep", lambda s: None)
    with service_factory(FakeBackend(finish_after=0.2), poll_s=0.05) as service:
        run_id = service.submit()["id"]
        inner = make_client(service)._http._transport
        flaky = Flaky(inner, "/wait", 2)
        client = make_client(service, transport=flaky)
        assert client.wait(run_id, timeout_s=30).state == "succeeded"
        assert flaky.failures == 0


def test_wait_times_out_with_state(service, make_client) -> None:
    run_id = service.submit()["id"]
    start = time.monotonic()
    summary = make_client(service).wait(run_id, timeout_s=0.5)
    assert summary.state != "succeeded"
    assert time.monotonic() - start < 5


class Down(httpx.BaseTransport):
    """Answers like `inner` for `after` requests, then drops every request."""

    def __init__(self, inner: httpx.BaseTransport, after: int = 0) -> None:
        self.inner = inner
        self.after = after

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if self.after <= 0:
            raise httpx.ConnectError("down", request=request)
        self.after -= 1
        return self.inner.handle_request(request)


def test_wait_timeout_in_outage_returns_last_summary(
    service, make_client, monkeypatch
) -> None:
    run_id = service.submit()["id"]
    down = Down(make_client(service)._http._transport, after=1)
    monkeypatch.setattr(simrun.client, "POLL_S", 0.2)
    summary = make_client(service, transport=down).wait(run_id, timeout_s=0.6)
    assert summary.id == run_id
    assert summary.state != "succeeded"


def test_wait_outage_without_summary_raises(service, make_client) -> None:
    run_id = service.submit()["id"]
    down = Down(make_client(service)._http._transport)
    with pytest.raises(httpx.TransportError):
        make_client(service, transport=down).wait(run_id, timeout_s=0.1)


def test_wait_unknown_run(service, make_client) -> None:
    with pytest.raises(ClientError) as error:
        make_client(service).wait("nope", timeout_s=1)
    assert error.value.status == 404


def test_cancel_logs_usage_cleanup(service, make_client) -> None:
    client = make_client(service)
    run_id = service.submit()["id"]
    name = f"simrun-{run_id}"
    service.backend.add_log(name, "squares", "squares-w0(0)", "a\nb\nc\n")
    assert client.logs(run_id, "squares", tail_lines=1).endswith("<==\nc\n")
    assert client.logs(run_id, "squares").endswith("<==\na\nb\nc\n")
    assert client.cancel(run_id).state == "cancelled"
    with pytest.raises(ClientError) as error:
        client.cancel(run_id)
    assert error.value.status == 409
    assert run_id in client.usage().runs
    assert client.cleanup(0, dry_run=True).runs == [run_id]
    assert client.compare([run_id]).rows[0]["run"] == run_id


def _write(path: Path) -> Path:
    import yaml

    path.write_text(yaml.safe_dump(WORKFLOW))
    return path
