import time
from pathlib import Path

import httpx
import pytest

import simrun.client
from simrun.client import ClientError
from simrun.testing import FakeBackend

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


def test_wait_unknown_run(service, make_client) -> None:
    with pytest.raises(ClientError) as error:
        make_client(service).wait("nope", timeout_s=1)
    assert error.value.status == 404


def test_cancel_logs_usage_cleanup(service, make_client) -> None:
    client = make_client(service)
    run_id = service.submit()["id"]
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
