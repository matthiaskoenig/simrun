import dataclasses
import importlib
import json
import sys
import time
import types
from pathlib import Path

import httpx
import pytest

import simrun.client
from simrun.client import Client, ClientError
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
    # what was given and what the plan set; the defaults stay with the snapshot
    assert task["settings"] == {"n": 4, "sleep_s": 1.0}
    # the workflow of the caller is not changed
    assert workflow.tasks[0].settings == {"n": 4}
    # the service writes the planned settings
    summary = client.submit(workflow, {"model": repo})
    task_dir = service.run_dir(summary.id) / "tasks/squares"
    settings = json.loads((task_dir / "settings.json").read_text())
    assert settings == {"n": 4, "sleep_s": 1.0}


class StampedSettings(SquaresSettings):
    stamp: str | None = None


def _stamp(settings: StampedSettings) -> StampedSettings:
    return settings.model_copy(update={"stamp": "planned"})


def test_validate_plans_as_submit(
    service, make_client, repo: Path, planning, monkeypatch
) -> None:
    client = make_client(service)
    workflow = Workflow(**WORKFLOW)
    # the local plugin knows `stamp`, the one of the service does not
    module = sys.modules["simrun_test_plan"]
    monkeypatch.setattr(module, "plan", _stamp)
    stamped = dataclasses.replace(
        SQUARES, settings=StampedSettings, plan="simrun_test_plan:plan"
    )
    monkeypatch.setattr(
        simrun.client, "load_task_types", lambda: {SQUARES.name: stamped}
    )
    result = client.validate(workflow)
    assert result.ok is False
    assert result.errors == ["task 'squares': unknown setting 'stamp'"]
    with pytest.raises(ClientError, match="unknown setting 'stamp'"):
        client.submit(workflow, {"model": repo})
    # an unset default of the local plugin is not sent
    monkeypatch.setattr(module, "plan", _plan)
    assert client.validate(workflow).ok is True


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


def test_client_records_provenance_only_without_a_store(
    service, repo: Path, tmp_path: Path
) -> None:
    client = Client(
        "http://testserver",
        "t-mk",
        None,
        sources="provenance",
        transport=httpx.MockTransport(lambda r: httpx.Response(200)),
    )
    prov = client.source(repo, "model")
    assert prov.snapshot == "" and prov.patch is None
    assert not (tmp_path / "snapshots").exists()
    with pytest.raises(RuntimeError, match="not available"):
        Client("http://x", "t", None, "store").source(repo, "model")


def test_output_archive_extracts_and_file_reads(service, make_client, tmp_path):
    run_id = service.submit()["id"]
    out = service.run_dir(run_id) / "tasks/squares/out"
    (out / "squares").mkdir(parents=True)
    (out / "squares/0.json").write_text('{"i": 0}')
    (out / ".simrun-merged").touch()
    client = make_client(service)
    assert client.output_file(run_id, "squares", "squares/0.json") == b'{"i": 0}'
    to = tmp_path / "dest"
    written = client.output_archive(run_id, "squares", to)
    assert written == [to / "squares/0.json"]
    assert (to / "squares/0.json").read_text() == '{"i": 0}'


def test_from_env_without_a_store_uploads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIMRUN_URL", "http://x")
    monkeypatch.setenv("SIMRUN_TOKEN", "t")
    monkeypatch.delenv("SIMRUN_STORE", raising=False)
    client = Client.from_env()
    assert (client.store, client.sources) == (None, "upload")
    monkeypatch.setenv("SIMRUN_STORE", "/mnt/cluster")
    client = Client.from_env()
    assert (client.store, client.sources) == (Path("/mnt/cluster"), "store")
    monkeypatch.delenv("SIMRUN_TOKEN")
    with pytest.raises(RuntimeError, match="SIMRUN_TOKEN"):
        Client.from_env()


def test_upload_retries_when_the_store_lost_a_file(
    service, make_client, tmp_path: Path, repo: Path
) -> None:
    import shutil

    client = make_client(service, sources="upload")
    first = client.source(repo, "model")
    # the latest snapshot is gone between `missing` and the upload: a conflict, sent
    # again with the files the store then lacks
    real = client._call
    removed = []

    def call(method, path, **kw):
        response = real(method, path, **kw)
        if path.endswith("/missing") and not removed:
            store = service.settings.store
            shutil.rmtree(store / first.snapshot)
            removed.append(path)
        return response

    client._call = call
    (repo / "extra.txt").write_text("x\n")
    second = client.source(repo, "model")
    assert removed
    assert (service.settings.store / second.snapshot / "model.txt").exists()


def _change_after_missing(client: Client, repo: Path, times: int) -> list[str]:
    """Change model.txt after each of the first `times` answers of `missing`."""
    real = client._call
    changed: list[str] = []

    def call(method, path, **kw):
        response = real(method, path, **kw)
        if path.endswith("/missing") and len(changed) < times:
            (repo / "model.txt").write_text(f"saved {len(changed)}\n")
            changed.append(path)
        return response

    client._call = call
    return changed


def test_upload_scans_again_once_after_a_file_changed(
    service, make_client, repo: Path
) -> None:
    client = make_client(service, sources="upload")
    (repo / "model.txt").write_text("edited\n")
    changed = _change_after_missing(client, repo, 1)
    prov = client.source(repo, "model")
    assert len(changed) == 1
    assert (service.settings.store / prov.snapshot / "model.txt").read_text() == (
        "saved 0\n"
    )


def test_upload_fails_when_a_file_changes_twice(
    service, make_client, repo: Path
) -> None:
    client = make_client(service, sources="upload")
    (repo / "model.txt").write_text("edited\n")
    changed = _change_after_missing(client, repo, 2)
    with pytest.raises(ClientError) as error:
        client.source(repo, "model")
    assert (
        error.value.status == 422 and "changed during the upload" in error.value.detail
    )
    assert len(changed) == 2


@pytest.mark.parametrize(
    ("status", "detail"), [(404, "Not Found"), (405, "Method Not Allowed")]
)
def test_upload_to_a_service_without_uploads_names_the_cause(
    repo: Path, status: int, detail: str
) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(status, json={"detail": detail})
    )
    client = Client("http://old", "t", None, "upload", transport=transport)
    with pytest.raises(ClientError) as error:
        client.source(repo, "model")
    assert error.value.status == status
    assert "too old for uploads" in error.value.detail
    assert "SIMRUN_STORE" in error.value.detail


def _plan_from_the_root(settings: SquaresSettings) -> SquaresSettings:
    # a plan which imports the project of the model, e.g. `examples.<model>` at the
    # root of a checkout of sbmlsim, which no installed package has
    probe = importlib.import_module("simrun_root_probe")
    return settings.model_copy(update={"sleep_s": probe.SLEEP_S})


def test_the_plan_imports_from_the_root_of_the_model(
    service, make_client, repo: Path, monkeypatch
) -> None:
    (repo / "simrun_root_probe.py").write_text("SLEEP_S = 2.5\n")
    module = types.ModuleType("simrun_test_root_plan")
    module.plan = _plan_from_the_root  # ty: ignore[unresolved-attribute]
    monkeypatch.setitem(sys.modules, module.__name__, module)
    planned = dataclasses.replace(SQUARES, plan=f"{module.__name__}:plan")
    monkeypatch.setattr(
        simrun.client, "load_task_types", lambda: {SQUARES.name: planned}
    )
    path = list(sys.path)
    try:
        body = make_client(service)._request(
            Workflow(**WORKFLOW), {"model": repo}, None, None, None
        )
    finally:
        sys.modules.pop("simrun_root_probe", None)
    (task,) = body["workflow"]["tasks"]
    assert task["settings"]["sleep_s"] == 2.5
    # the root is importable while planning only
    assert sys.path == path
