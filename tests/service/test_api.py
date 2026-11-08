import json
import os
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from simrun.demo import SQUARES
from simrun.resources import ResourceStore, resource_key
from simrun.service import app as app_module
from simrun.service import controller as controller_module
from simrun.service.app import serve, unknown_settings
from simrun.service.auth import Identity, load_tokens, priority_of
from simrun.service.backend import Placement
from simrun.service.controller import task_plans
from simrun.snapshot import SourceProvenance
from simrun.store import RunStore
from simrun.testing import FakeBackend
from simrun.worker import WorkerReport
from simrun.workflow import Priority

DAY = 86400.0


def provenance(name: str, snapshot: str) -> dict:
    return SourceProvenance(
        name=name,
        branch="develop",
        commit="0" * 40,
        dirty=True,
        changed_files=["M a.py", "?? b.py"],
        diff_sha256="ab" * 32,
        snapshot=snapshot,
        patch=None,
    ).model_dump()


def test_no_documentation_routes(service):
    for path in ("/docs", "/redoc", "/openapi.json"):
        assert service.client.get(path).status_code == 404
        assert service.get(path).status_code == 404


def test_requires_token(service):
    assert service.client.get("/runs").status_code == 401
    response = service.client.get("/runs", headers=service.auth("wrong"))
    assert response.status_code == 401
    assert service.client.post("/validate", json={}).status_code == 401


def test_load_tokens(tmp_path: Path):
    path = tmp_path / "tokens.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "a": {"name": "p", "kind": "person"},
                "b": {"name": "x", "kind": "agent", "owner": "p"},
            }
        )
    )
    tokens = load_tokens(path)
    assert tokens["a"] == Identity(name="p", kind="person", owner="p")
    assert tokens["b"].owner == "p"
    with pytest.raises(ValueError, match="owner"):
        Identity.model_validate({"name": "x", "kind": "agent"})
    with pytest.raises(ValueError, match="owner"):
        Identity(name="p", kind="person", owner="q")


def test_validate(service):
    ok = service.post("/validate", service.workflow()).json()
    assert ok == {"ok": True, "errors": []}
    bad = service.workflow()
    bad["tasks"][0]["type"] = "nope"
    result = service.post("/validate", bad).json()
    assert result["ok"] is False
    assert "unknown task type 'nope'" in result["errors"][0]
    bad = service.workflow()
    bad["tasks"][0]["settings"] = {}
    assert "squares" in service.post("/validate", bad).json()["errors"][0]
    cycle = {
        "name": "c",
        "tasks": [{"id": "a", "type": "command", "inputs": {"x": "a.out"}}],
    }
    assert "cycle" in service.post("/validate", cycle).json()["errors"][0]


def test_a_command_cannot_stage(service):
    workflow = service.workflow()
    workflow["tasks"].append(
        {
            "id": "sum",
            "type": "command",
            "settings": {"image": "t:1", "args": ["sum"], "cores": 1, "memory_gb": 1},
            "inputs": {"a": "squares.squares"},
            "stage": ["a"],
        }
    )
    result = service.post("/validate", workflow).json()
    assert result["errors"] == ["task 'sum': a command cannot stage 'a'"]


def test_unknown_settings_are_errors(service):
    misspelled = service.workflow()
    misspelled["tasks"][0]["settings"] = {"n": 4, "sleep": 2}
    result = service.post("/validate", misspelled).json()
    assert result["ok"] is False
    assert result["errors"] == ["task 'squares': unknown setting 'sleep'"]
    response = service.post("/runs", {"workflow": misspelled, "sources": []})
    assert response.status_code == 422
    assert "unknown setting 'sleep'" in response.json()["detail"]
    assert not (service.settings.store / "runs").exists()


def test_submit_returns_id_at_once(service):
    response = service.post(
        "/runs", {"workflow": service.workflow(), "sources": [], "group": "g1"}
    )
    assert response.status_code == 201, response.text
    summary = response.json()
    # the environment, the image and the workflow follow in the background
    assert summary["state"] == "submitting"
    assert summary["group"] == "g1"
    assert summary["tasks"] == {"squares": "waiting"}
    run_dir = service.run_dir(summary["id"])
    # as submitted: the worker validates them with the plugin of the snapshot
    settings = json.loads((run_dir / "tasks/squares/settings.json").read_text())
    assert settings == {"n": 4}
    record = service.submitted(summary["id"])
    assert record["state"] == "queued"
    assert record["backend_name"] == service.name(summary["id"])
    assert record["image"] == "localhost:5000/base:1"
    assert record["built"] is not None
    assert record["plans"]["squares"]["units"] == 4
    assert service.name(summary["id"]) in service.backend.manifests


def test_task_dir_has_provenance_json(service):
    store = service.settings.store
    _snapshot(store, "snapshots/model/a", 10)
    _snapshot(store, "snapshots/lib/b", 10)
    sources = [
        provenance("model", "snapshots/model/a"),
        provenance("lib", "snapshots/lib/b"),
    ]
    run_id = service.submit(sources=sources)["id"]
    path = service.run_dir(run_id) / "tasks/squares/provenance.json"
    assert json.loads(path.read_text()) == [
        SourceProvenance.model_validate(s).model_dump(mode="json") for s in sources
    ]
    # without sources, an empty list
    run_id = service.submit()["id"]
    path = service.run_dir(run_id) / "tasks/squares/provenance.json"
    assert json.loads(path.read_text()) == []


@pytest.mark.parametrize(
    ("token", "priority", "agent"),
    [
        ("t-mk", "owner", None),
        ("t-mk-bot", "owner", "mk-bot"),
        ("t-alice", "standard", None),
        ("t-alice-bot", "agent", "alice-bot"),
    ],
)
def test_priority_from_identity(service, token, priority, agent):
    summary = service.submit(token)
    assert summary["priority"] == priority
    assert summary["agent"] == agent
    manifest = service.backend.manifests[service.name(summary["id"])]
    assert f"simrun-{priority}-base" in json.dumps(manifest)


def test_priority_of():
    person = Identity.model_validate({"name": "alice", "kind": "person"})
    assert priority_of(person, "alice") == Priority.OWNER
    assert priority_of(person, "mk") == Priority.STANDARD


def test_plan_error_is_422(service):
    body = {
        "workflow": service.workflow(resources={"memory_per_worker_gb": 2000}),
        "sources": [],
    }
    response = service.post("/runs", body)
    assert response.status_code == 422
    # the task is named: a workflow has several
    assert response.json()["detail"].startswith("task 'squares': ")
    assert "one worker needs 2000.0 GiB" in response.json()["detail"]
    assert "largest node srv-01" in response.json()["detail"]
    assert not (service.settings.store / "runs").exists()


def test_missing_snapshot_is_422(service):
    body = {
        "workflow": service.workflow(),
        "sources": [provenance("model", "snapshots/model/gone")],
    }
    for route in ("/runs", "/estimate"):
        response = service.post(route, body)
        assert response.status_code == 422, response.text
        detail = response.json()["detail"]
        assert "snapshots/model/gone" in detail
        assert "SIMRUN_STORE" in detail
    assert not (service.settings.store / "runs").exists()


class _RegistryDown(FakeBackend):
    def has_image(self, ref):
        raise RuntimeError(f"the registry answered 401 for {ref}")


def test_registry_error_lands_in_the_record(service_factory, tmp_path, monkeypatch):
    # the registry is asked in the background: its error is the message of the run
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = _wheel(tmp_path)
    with service_factory(_RegistryDown(), simrun_wheel=wheel, default_image=None) as (
        service
    ):
        record = service.submit(sources=[_model(service.settings.store)])
        assert record["state"] == "failed"
        assert "the registry cannot tell whether it has" in record["message"]
        assert "answered 401" in record["message"]


def test_estimate(service):
    response = service.post(
        "/estimate", {"workflow": service.workflow(n=100), "sources": []}
    )
    assert response.status_code == 200, response.text
    squares = response.json()["squares"]
    assert squares["plan"]["units"] == 100
    # 100 units fill 3 pods of 36 processes, no pod is left without units
    assert squares["plan"]["pods"] == 3
    assert squares["resources"]["source"] == "plugin hint"
    assert squares["seconds"] is None
    single = {
        "workflow": service.workflow(n=100),
        "sources": [],
        "split": "single-server",
    }
    assert service.post("/estimate", single).json()["squares"]["plan"]["pods"] == 1


def test_a_backend_without_extra_workers_plans_one_pod(service):
    service.backend.extra_workers = False  # the local backend: one machine
    response = service.post(
        "/estimate", {"workflow": service.workflow(n=100), "sources": []}
    )
    assert response.json()["squares"]["plan"]["pods"] == 1


def test_estimate_from_records(service):
    # the reviewer's example: 360 units of 10 s on 36 processes took 100 s
    report = WorkerReport(
        worker="squares-w0",
        node="srv-02",
        processes=36,
        units_done=list(range(360)),
        seconds=100.0,
        unit_seconds=3600.0,
    )
    resources = ResourceStore(service.settings.db)
    resources.record(resource_key(SQUARES, {"n": 360}, ""), report, base_gb=0.5)
    single = {
        "workflow": service.workflow(n=360, resources={"memory_per_worker_gb": 4.0}),
        "sources": [],
        "split": "single-server",
    }
    squares = service.post("/estimate", single).json()["squares"]
    assert squares["resources"]["seconds_per_unit"] == 10.0
    plan = squares["plan"]
    # one pod of a server of 40 cores: nine rounds of 10 s
    assert (plan["pods"], plan["shape"]["processes"]) == (1, 40)
    assert squares["seconds"] == 90.0


def test_list_filters(service):
    a = service.submit("t-mk", group="g1")["id"]
    b = service.submit("t-mk-bot", group="g2")["id"]
    c = service.submit("t-alice", group="g1")["id"]
    ids = lambda **q: [r["id"] for r in service.get("/runs", params=q).json()]  # noqa: E731
    assert ids() == sorted([a, b, c])
    assert ids(group="g1") == sorted([a, c])
    assert ids(agent="mk-bot") == [b]
    assert ids(owner="mk") == sorted([a, b])
    assert ids(state="queued", group="g2") == [b]
    assert ids(state="succeeded") == []


def test_unknown_run_404(service):
    for path in ("/runs/nope", "/runs/nope/wait", "/runs/nope/progress"):
        assert service.get(path).status_code == 404
    assert service.post("/runs/nope/cancel").status_code == 404


def test_wait_returns_at_terminal_state(service_factory):
    with service_factory(FakeBackend(finish_after=0.2), poll_s=0.05) as service:
        run_id = service.submit()["id"]
        start = time.monotonic()
        response = service.get(f"/runs/{run_id}/wait", params={"timeout": 5})
        assert time.monotonic() - start < 1.0
        assert response.json()["state"] == "succeeded"
        assert response.json()["tasks"] == {"squares": "done"}
        assert response.json()["finished"] is not None


def test_wait_times_out(service_factory):
    with service_factory(poll_s=0.05) as service:
        run_id = service.submit()["id"]
        service.backend.start(service.name(run_id))
        start = time.monotonic()
        response = service.get(f"/runs/{run_id}/wait", params={"timeout": 0.5})
        assert 0.5 <= time.monotonic() - start < 2.0
        assert response.json()["state"] == "running"
        assert (
            service.get(f"/runs/{run_id}/wait", params={"timeout": 301}).status_code
            == 422
        )


def test_wait_after_restart(service_factory):
    with service_factory() as service:
        run_id = service.submit()["id"]
        service.backend.finish(service.name(run_id))
        service.controller.reconcile()
    with service_factory(FakeBackend()) as service:
        start = time.monotonic()
        response = service.get(f"/runs/{run_id}/wait", params={"timeout": 5})
        assert time.monotonic() - start < 0.5
        assert response.json()["state"] == "succeeded"


def test_record_and_progress(service):
    run_id = service.submit()["id"]
    record = service.get(f"/runs/{run_id}").json()
    assert record["workflow"]["name"] == "demo"
    task_dir = service.run_dir(run_id) / "tasks/squares"
    for state in ("pending", "claimed", "done"):
        (task_dir / "units" / state).mkdir(parents=True)
    (task_dir / "units/size").write_text("4")
    (task_dir / "units/pending/3").touch()
    (task_dir / "units/claimed/2@squares-w0").touch()
    (task_dir / "units/done/0").touch()
    (task_dir / "units/done/1").touch()
    line = {"worker": "squares-w0", "event": "unit_done", "unit": 1}
    (task_dir / "progress.jsonl").write_text(
        json.dumps({"unit": 0}) + "\n" + json.dumps(line) + "\n"
    )
    progress = service.get(f"/runs/{run_id}/progress").json()
    assert progress["squares"]["counts"] == {
        "pending": 1,
        "claimed": 1,
        "done": 2,
        "total": 4,
    }
    assert progress["squares"]["first"] == {"unit": 0}
    assert progress["squares"]["last"] == line


def test_progress_without_lines(service):
    run_id = service.submit()["id"]
    progress = service.get(f"/runs/{run_id}/progress").json()
    assert progress["squares"] == {"counts": None, "first": None, "last": None}
    task_dir = service.run_dir(run_id) / "tasks/squares"
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "progress.jsonl").write_text("not json\n")
    progress = service.get(f"/runs/{run_id}/progress").json()
    assert progress["squares"]["first"] is None
    assert progress["squares"]["last"] is None


def test_outputs_relative_to_store(service):
    run_id = service.submit()["id"]
    out = service.run_dir(run_id) / "tasks/squares/out"
    (out / "squares").mkdir(parents=True)
    (out / "summary.json").write_text("{}")
    (out / "squares/0.json").write_text('{"i": 0}')
    files = service.get(f"/runs/{run_id}/outputs/squares").json()
    assert files == [
        {"path": f"runs/{run_id}/tasks/squares/out/squares/0.json", "bytes": 8},
        {"path": f"runs/{run_id}/tasks/squares/out/summary.json", "bytes": 2},
    ]
    assert service.get(f"/runs/{run_id}/outputs/nope").status_code == 404


def _lines(n: int, prefix: str) -> str:
    return "".join(f"{prefix} {i}\n" for i in range(n))


def test_logs(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.add_log(name, "squares", "squares-w0(0)", _lines(3, "a"))
    service.backend.add_log(name, "squares", "squares-w0(1)", _lines(500, "b"))
    text = service.get(f"/runs/{run_id}/logs/squares").text
    # the last 300 lines of every pod by default, each under a header
    header, *lines = text.split("\n==> ")[1].splitlines()
    assert text.startswith(f"==> squares-w0(0) ({name}-squares-w0(0)) <==\na 0\n")
    assert header == f"squares-w0(1) ({name}-squares-w0(1)) <=="
    assert (len(lines), lines[0], lines[-1]) == (300, "b 200", "b 499")
    short = service.get(f"/runs/{run_id}/logs/squares", params={"tail_lines": 2})
    assert "a 0" not in short.text and "a 1\na 2\n" in short.text
    full = service.get(f"/runs/{run_id}/logs/squares", params={"tail_lines": 0})
    assert "b 0\n" in full.text
    assert service.get(f"/runs/{run_id}/logs/nope").status_code == 404


def test_logs_are_kept_in_the_store(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.add_log(name, "squares", "squares-w0(0)", _lines(2, "w0"))
    service.backend.add_log(name, "squares", "squares-merge(0)", _lines(2, "m"))
    service.backend.start(name)
    service.controller.reconcile()
    logs = service.run_dir(run_id) / "tasks/squares/logs"
    assert not logs.exists()  # the task runs
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    # copied when the task was final, before the workflow was stopped
    calls = service.backend.calls
    assert calls.index(("logs", name, "squares")) < calls.index(("stop", name))
    assert sorted(p.name for p in logs.iterdir()) == [
        "index.json",
        f"{name}-squares-merge(0).log",
        f"{name}-squares-w0(0).log",
    ]
    # the workflow and its pods are gone: the logs come from the store
    service.backend.delete(name)
    service.backend.pod_logs.clear()
    text = service.get(f"/runs/{run_id}/logs/squares").text
    assert "w0 1\n" in text and "m 1\n" in text
    assert text.index("squares-w0(0)") < text.index("squares-merge(0)")
    assert service.get(
        f"/runs/{run_id}/logs/squares", params={"tail_lines": 1}
    ).text == (
        f"==> squares-w0(0) ({name}-squares-w0(0)) <==\nw0 1\n\n"
        f"==> squares-merge(0) ({name}-squares-merge(0)) <==\nm 1\n"
    )


def test_logs_are_kept_before_a_cancellation(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.add_log(name, "squares", "squares-w0(0)", "running\n")
    service.backend.start(name)
    service.post(f"/runs/{run_id}/cancel")
    calls = service.backend.calls
    assert calls.index(("logs", name, "squares")) < calls.index(("cancel", name))
    service.backend.pod_logs.clear()
    assert "running" in service.get(f"/runs/{run_id}/logs/squares").text


def test_cancel(service):
    run_id = service.submit(reserve="srv-03")["id"]
    response = service.post(f"/runs/{run_id}/cancel")
    assert response.json()["state"] == "cancelled"
    assert response.json()["tasks"] == {"squares": "skipped"}
    assert ("cancel", service.name(run_id)) in service.backend.calls
    assert ("unreserve", "srv-03", run_id) in service.backend.calls
    assert service.post(f"/runs/{run_id}/cancel").status_code == 409


def test_delete_running_run_409(service):
    run_id = service.submit()["id"]
    service.backend.start(service.name(run_id))
    service.controller.reconcile()
    assert (
        service.client.delete(f"/runs/{run_id}", headers=service.auth()).status_code
        == 409
    )
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    response = service.client.delete(f"/runs/{run_id}", headers=service.auth())
    assert response.status_code == 200
    assert response.json()["runs"] == [run_id]
    assert not service.run_dir(run_id).exists()
    assert service.get(f"/runs/{run_id}").status_code == 404


def _snapshot(store: Path, rel: str, size: int) -> None:
    path = store / rel
    path.mkdir(parents=True)
    (path / "data").write_bytes(b"x" * size)
    old = time.time() - 10 * DAY
    os.utime(path, (old, old))


def _age(store: RunStore, run_id: str, days: float) -> None:
    record = store.read(run_id)
    created = datetime.fromtimestamp(time.time() - days * DAY, UTC)
    store.write(record.model_copy(update={"created": created, "finished": created}))


def test_cleanup_dry_run_and_delete(service, monkeypatch):
    store_root = service.settings.store
    _snapshot(store_root, "snapshots/model/shared", 100)
    _snapshot(store_root, "snapshots/model/own", 1000)
    (store_root / "snapshots/model/own.files.json").write_text("{}")
    (store_root / "snapshots/model/shared.files.json").write_text("{}")
    shared = provenance("model", "snapshots/model/shared")
    own = provenance("lib", "snapshots/model/own")
    failed = service.submit(workflow=service.workflow(), sources=[shared, own])["id"]
    succeeded = service.submit(sources=[shared])["id"]
    service.backend.fail(service.name(failed), "squares")
    service.backend.finish(service.name(succeeded))
    service.controller.reconcile()
    store = service.store
    _age(store, failed, 5)
    _age(store, succeeded, 5)
    young = service.submit()["id"]

    dry = service.post("/cleanup", {"older_than_days": 1, "dry_run": True}).json()
    assert dry["snapshots"] == []  # created within the grace, a client may submit them
    monkeypatch.setattr(app_module, "SNAPSHOT_GRACE_S", 0.0)
    dry = service.post("/cleanup", {"older_than_days": 1, "dry_run": True}).json()
    assert dry["dry_run"] is True
    assert dry["runs"] == sorted([failed, succeeded])
    assert dry["snapshots"] == ["snapshots/model/own", "snapshots/model/shared"]
    assert dry["bytes_freed"] >= 1100
    assert service.run_dir(failed).exists()

    result = service.post(
        "/cleanup", {"older_than_days": 1, "failed_only": True}
    ).json()
    assert result["runs"] == [failed]
    assert result["snapshots"] == ["snapshots/model/own"]
    assert result["bytes_freed"] >= 1000
    assert not service.run_dir(failed).exists()
    assert not (store_root / "snapshots/model/own").exists()
    assert not (store_root / "snapshots/model/own.files.json").exists()
    assert (store_root / "snapshots/model/shared/data").exists()
    assert (store_root / "snapshots/model/shared.files.json").exists()
    assert service.run_dir(succeeded).exists()
    assert service.run_dir(young).exists()


def test_usage(service):
    a = service.submit(group="g1")["id"]
    b = service.submit()["id"]
    (service.run_dir(a) / "tasks/squares/out").mkdir(parents=True)
    (service.run_dir(a) / "tasks/squares/out/x").write_bytes(b"x" * 5000)
    usage = service.get("/usage").json()
    assert usage["runs"][a] >= 5000
    assert usage["groups"]["g1"] == usage["runs"][a]
    assert usage["groups"][""] == usage["runs"][b]


def test_the_run_records_how_the_local_backend_runs_its_commands(service):
    response = service.post(
        "/runs",
        {"workflow": service.workflow(), "sources": [], "local_commands": "docker"},
    )
    assert response.status_code == 201, response.text
    assert service.submitted(response.json()["id"])["local_commands"] == "docker"
    response = service.post("/runs", {"workflow": service.workflow(), "sources": []})
    assert service.submitted(response.json()["id"])["local_commands"] is None
    response = service.post(
        "/runs",
        {"workflow": service.workflow(), "sources": [], "local_commands": "podman"},
    )
    assert response.status_code == 422


def test_reserved_node_must_be_free(service):
    service.submit(reserve="srv-02")
    response = service.post(
        "/runs", {"workflow": service.workflow(), "sources": [], "reserve": "srv-02"}
    )
    assert response.status_code == 409
    response = service.post(
        "/runs", {"workflow": service.workflow(), "sources": [], "reserve": "nope"}
    )
    assert response.status_code == 422


def _wheel(tmp_path: Path) -> Path:
    wheel = tmp_path / "simrun-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    return wheel


def _model(store: Path, lock: bool = True) -> dict:
    model = store / "snapshots/model/a"
    model.mkdir(parents=True, exist_ok=True)
    (model / "pyproject.toml").write_text(
        '[project]\nname = "model"\nversion = "0.1"\nrequires-python = ">=3.14"\n'
        "dependencies = []\n"
    )
    if lock:
        (model / "uv.lock").write_text(
            'version = 1\nrevision = 5\nrequires-python = ">=3.14"\n'
        )
    return provenance("model", "snapshots/model/a")


def test_image_built_once(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = _wheel(tmp_path)
    with service_factory(simrun_wheel=wheel, default_image=None) as service:
        source = _model(service.settings.store)
        first = service.submit(sources=[source])["id"]
        second = service.submit(sources=[source])["id"]
    builds = [c for c in service.backend.calls if c[0] == "build_image"]
    assert len(builds) == 1
    _, ref, files = builds[0]
    assert ref.startswith("localhost:5000/simrun-env:")
    assert files == ["Dockerfile", wheel.name]
    store = service.store
    assert store.read(first).image == store.read(second).image == ref
    assert (
        'name = "pydantic"' in (service.run_dir(first) / "environment.lock").read_text()
    )


def test_a_new_wheel_of_the_same_version_builds_a_new_image(
    service_factory, tmp_path, monkeypatch
):
    # the local test system builds the wheel of a working tree under one version:
    # its images must not keep the harness of the wheel before
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = _wheel(tmp_path)
    with service_factory(simrun_wheel=wheel, default_image=None) as service:
        source = _model(service.settings.store)
        first = service.submit(sources=[source])["id"]
        wheel.write_bytes(wheel.read_bytes() + b"\0")
        second = service.submit(sources=[source])["id"]
    builds = [c for c in service.backend.calls if c[0] == "build_image"]
    assert len(builds) == 2
    assert service.store.read(first).image != service.store.read(second).image


class _SlowBuild(FakeBackend):
    """A build which waits until the test releases it."""

    def __init__(self) -> None:
        super().__init__()
        self.building = threading.Event()
        self.release = threading.Event()

    def build_image(self, context, ref):
        self.building.set()
        assert self.release.wait(30)
        super().build_image(context, ref)


def test_submission_returns_before_the_build(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    backend = _SlowBuild()
    wheel = _wheel(tmp_path)
    with service_factory(backend, simrun_wheel=wheel, default_image=None) as service:
        try:
            source = _model(service.settings.store)
            first = service.submit(sources=[source], wait=False)["id"]
            assert backend.building.wait(30)
            assert service.get(f"/runs/{first}").json()["state"] == "submitting"
            # a build longer than the grace: the controller leaves the run alone
            monkeypatch.setattr(controller_module, "SUBMIT_GRACE_S", 0.0)
            service.controller.reconcile()
            assert service.get(f"/runs/{first}").json()["state"] == "submitting"
            monkeypatch.setattr(controller_module, "SUBMIT_GRACE_S", 120.0)
            # the same new environment again while it is built: built once
            second = service.submit(sources=[source], wait=False)["id"]
        finally:
            backend.release.set()
        for run_id in (first, second):
            record = service.submitted(run_id)
            assert record["state"] == "queued", record["message"]
            assert record["built"] is not None
    assert len([c for c in backend.calls if c[0] == "build_image"]) == 1


def test_cancel_during_the_build(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    backend = _SlowBuild()
    wheel = _wheel(tmp_path)
    with service_factory(backend, simrun_wheel=wheel, default_image=None) as service:
        try:
            source = _model(service.settings.store)
            run_id = service.submit(sources=[source], wait=False)["id"]
            assert backend.building.wait(30)
            response = service.post(f"/runs/{run_id}/cancel")
            assert response.json()["state"] == "cancelled"
            # cancelled before it had a workflow: no logs to keep, none followed
            assert not service.store.read(run_id).pending_logs
            assert not (service.run_dir(run_id) / "tasks/squares/logs").exists()
        finally:
            backend.release.set()
        assert service.submitted(run_id)["state"] == "cancelled"
    assert ("submit", service.name(run_id)) not in backend.calls


def test_failed_environment_lands_in_the_record(service_factory, tmp_path):
    with service_factory(simrun_wheel=_wheel(tmp_path), default_image=None) as service:
        source = _model(service.settings.store, lock=False)
        record = service.submit(sources=[source], reserve="srv-02")
        assert record["state"] == "failed"
        assert "uv.lock" in record["message"]
        assert record["tasks"] == {"squares": "skipped"}
        assert not [c for c in service.backend.calls if c[0] in ("submit", "reserve")]


class _FailingBuild(FakeBackend):
    def build_image(self, context, ref):
        raise RuntimeError("buildkit is down")


def test_failed_build_lands_in_the_record(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = _wheel(tmp_path)
    with service_factory(_FailingBuild(), simrun_wheel=wheel, default_image=None) as (
        service
    ):
        record = service.submit(sources=[_model(service.settings.store)])
        assert record["state"] == "failed"
        assert "building the image" in record["message"]
        assert "buildkit is down" in record["message"]


def test_no_image_is_422(service_factory):
    with service_factory(default_image=None) as service:
        response = service.post(
            "/runs", {"workflow": service.workflow(), "sources": []}
        )
        assert response.status_code == 422
        assert "default_image" in response.json()["detail"]


def test_serve_reads_config(tmp_path, monkeypatch):
    config = tmp_path / "service.yaml"
    tokens = tmp_path / "tokens.yaml"
    tokens.write_text("{}")
    config.write_text(
        yaml.safe_dump(
            {
                "store": str(tmp_path / "store"),
                "db": str(tmp_path / "db.sqlite"),
                "tokens": str(tokens),
                "owner": "mk",
                "cluster": {"registry": "localhost:5000"},
                "backend": "simrun.testing:FakeBackend",
            }
        )
    )
    monkeypatch.setenv("SIMRUN_CONFIG", str(config))
    app = serve()
    assert app.state.settings.owner == "mk"
    assert isinstance(app.state.backend, FakeBackend)


class _FailingSubmit(FakeBackend):
    def submit(self, record, plans, types, cluster):
        raise RuntimeError("no cluster")


def test_failed_submission_releases_node(service_factory):
    with service_factory(_FailingSubmit()) as service:
        record = service.submit(reserve="srv-02")
        assert "no cluster" in record["message"]
        assert ("unreserve", "srv-02", record["id"]) in service.backend.calls
        assert record["state"] == "failed"
        assert record["tasks"] == {"squares": "skipped"}


def test_pending_work_blocks_deletion(service, monkeypatch):
    monkeypatch.setattr(app_module, "SNAPSHOT_GRACE_S", 0.0)
    run_id = service.submit()["id"]
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    store = service.store
    record = store.read(run_id)
    old = datetime.fromtimestamp(time.time() - 5 * DAY, UTC)
    pending = record.model_copy(
        update={"pending_release": "srv-02", "created": old, "finished": old}
    )
    store.write(pending)
    response = service.client.delete(f"/runs/{run_id}", headers=service.auth())
    assert response.status_code == 409
    assert "release its node" in response.json()["detail"]
    for dry_run in (True, False):
        body = {"older_than_days": 1, "dry_run": dry_run}
        assert service.post("/cleanup", body).json()["runs"] == []
    assert service.run_dir(run_id).exists()
    store.write(
        pending.model_copy(update={"pending_release": None, "pending_cancel": True})
    )
    response = service.client.delete(f"/runs/{run_id}", headers=service.auth())
    assert response.status_code == 409


def test_unknown_settings_by_name_and_alias():
    class Aliased(BaseModel):
        model_config = ConfigDict(populate_by_name=True)
        rate: float = Field(1.0, alias="k")
        dose: float = Field(1.0, validation_alias=AliasChoices("d", "amount"))

    class Open(BaseModel):
        model_config = ConfigDict(extra="allow")

    settings = {"rate": 1, "k": 2, "d": 3, "amount": 4, "dose": 5, "x": 6}
    assert unknown_settings(Aliased, settings) == ["x"]
    assert unknown_settings(Open, {"anything": 1}) == []


class _FlakyLogs(FakeBackend):
    """The logs of a pod cannot be read the first two times (the copy before the
    stop and the one of the settlement in the same poll)."""

    def __init__(self) -> None:
        super().__init__()
        self.failures = 2

    def logs(self, name, task_id, tail_lines=None):
        if self.failures:
            self.failures -= 1
            raise RuntimeError("the API is down")
        return super().logs(name, task_id, tail_lines)


def test_logs_are_kept_after_a_failed_copy(service_factory):
    with service_factory(_FlakyLogs()) as service:
        run_id = service.submit()["id"]
        name = service.name(run_id)
        service.backend.add_log(name, "squares", "squares-w0(0)", "done\n")
        service.backend.finish(name)
        service.controller.reconcile()
        record = service.store.read(run_id)
        # the run ended in the poll whose copy failed: it stays followed
        assert record.state == "succeeded"
        assert record.pending_logs
        assert service.store.index.live_ids() == [run_id]
        service.controller.reconcile()
        assert not service.store.read(run_id).pending_logs
        assert service.store.index.live_ids() == []
        logs = service.run_dir(run_id) / "tasks/squares/logs"
        assert (logs / "index.json").is_file()


def test_logs_of_a_skipped_task_are_kept_once_its_pods_stopped(service):
    workflow = service.workflow()
    command = {"image": "x:1", "args": ["true"], "cores": 1, "memory_gb": 1.0}
    workflow["tasks"].append({"id": "cmd", "type": "command", "settings": command})
    run_id = service.submit(workflow=workflow)["id"]
    name = service.name(run_id)
    backend = service.backend
    backend.add_log(name, "squares", "squares-w0(0)", "unit 1\n", done=False)
    backend.add_log(name, "cmd", "cmd(0)", "failed\n")
    backend.start(name)
    backend.set_node(name, "cmd", "Failed")
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert record.tasks == {"squares": "skipped", "cmd": "failed"}
    assert ("stop", name) in backend.calls
    tasks = service.run_dir(run_id) / "tasks"
    assert (tasks / "cmd/logs/index.json").is_file()
    # the pod of the skipped task still stops: its log is kept once it ended
    assert not (tasks / "squares/logs").exists()
    (pod,) = backend.pod_logs[(name, "squares")]
    pod.text, pod.done = "unit 1\nstopped on SIGTERM\n", True
    service.controller.reconcile()
    assert (
        "stopped on SIGTERM" in (tasks / "squares/logs" / f"{pod.pod}.log").read_text()
    )
    assert not service.store.read(run_id).pending_logs


def test_no_logs_are_kept_for_a_run_without_a_workflow(service_factory, tmp_path):
    with service_factory(simrun_wheel=_wheel(tmp_path), default_image=None) as service:
        source = _model(service.settings.store, lock=False)
        record = service.submit(sources=[source])
        assert record["state"] == "failed"
        assert not record["pending_logs"]
        assert not (service.run_dir(record["id"]) / "tasks/squares/logs").exists()


def test_a_restart_resumes_a_submission_in_flight(
    service_factory, tmp_path, monkeypatch
):
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = _wheel(tmp_path)
    first_backend = _SlowBuild()
    try:
        with service_factory(
            first_backend, simrun_wheel=wheel, default_image=None
        ) as first:
            run_id = first.submit(sources=[_model(first.settings.store)], wait=False)[
                "id"
            ]
            assert first_backend.building.wait(30)
            stopping = time.monotonic()
        # the service stops without waiting for the build
        assert time.monotonic() - stopping < 5
        backend = FakeBackend()
        with service_factory(backend, simrun_wheel=wheel, default_image=None) as second:
            deadline = time.monotonic() + 30
            while second.store.read(run_id).state == "submitting":
                assert time.monotonic() < deadline, "the submission was not resumed"
                time.sleep(0.05)
            record = second.store.read(run_id)
            assert record.state == "queued", record.message
            name = second.name(run_id)
            assert [c[0] for c in backend.calls] == ["build_image", "submit"]
            assert ("submit", name) in backend.calls
    finally:
        first_backend.release.set()
    # the job of the first service finds the run changed and submits nothing
    deadline = time.monotonic() + 30
    while first.controller.in_flight(run_id):
        assert time.monotonic() < deadline
        time.sleep(0.05)
    assert not [c for c in first_backend.calls if c[0] == "submit"]


def test_a_workflow_submitted_twice_is_submitted_once(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.start(name)
    # a resumed submission submits the workflow of the run again
    record = service.store.read(run_id)
    cluster = service.settings.cluster
    submit = (record, task_plans(record), service.controller.types, cluster)
    assert service.backend.submit(*submit) == name
    assert service.backend.status(name).phase == "Running"


def test_the_build_constraints_of_the_service(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    constraints = tmp_path / "constraints.txt"
    constraints.write_text("setuptools==1.0.0\n")
    with service_factory(
        simrun_wheel=_wheel(tmp_path),
        default_image=None,
        build_constraints=constraints,
    ) as service:
        # the model has no build system: the legacy backend of setuptools, pinned
        record = service.submit(sources=[_model(service.settings.store)])
        assert record["state"] == "failed"
        assert "setuptools==1.0.0" in record["message"]


def test_fake_placements_end_with_the_workflow(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    placement = Placement(
        workflow=name,
        dag_task="squares-w0",
        pod="p",
        node="n1",
        phase="Running",
        cores=2.0,
        memory_gb=1.0,
    )
    service.backend.place(placement)
    service.backend.start(name)
    assert service.backend.placements() == [placement]
    service.backend.finish(name)
    assert service.backend.placements() == []


class _NoImages(FakeBackend):
    runs_images = False

    def submit(self, record, plans, types, cluster):
        self.calls.append(("image", record.image))
        record = record.model_copy(update={"image": "x"})  # for the manifest
        return super().submit(record, plans, types, cluster)


def test_a_backend_without_images_does_not_build(service_factory, tmp_path):
    with service_factory(_NoImages(), simrun_wheel=_wheel(tmp_path)) as service:
        source = _model(service.settings.store)
        run_id = service.submit(sources=[source])["id"]
        record = service.store.read(run_id)
    assert record.state == "queued"
    assert record.image is None
    assert ("image", None) in service.backend.calls
    assert not [c for c in service.backend.calls if c[0] == "build_image"]
    assert not (service.run_dir(run_id) / "environment.lock").exists()


def test_a_source_recorded_without_a_snapshot_runs_on_a_backend_without_images(
    service_factory,
):
    # the local mode records a working tree without copying it: no snapshot
    source = {**provenance("model", "snapshots/model/a"), "snapshot": "", "patch": None}
    with service_factory(_NoImages()) as service:
        record = service.store.read(service.submit(sources=[source])["id"])
    assert record.state == "queued"
    assert record.sources[0].snapshot == ""


def test_a_source_without_a_snapshot_is_refused_where_runs_need_the_code(service):
    source = {**provenance("model", "snapshots/model/a"), "snapshot": "", "patch": None}
    response = service.post(
        "/runs", {"workflow": service.workflow(), "sources": [source]}
    )
    assert response.status_code == 422
    assert "source 'model' has no snapshot" in response.json()["detail"]


def test_a_run_the_backend_refuses_fails_with_its_message(service, monkeypatch):
    def refuse(self, record, plans, types, cluster):
        raise ValueError("no image for task x")

    monkeypatch.setattr(FakeBackend, "submit", refuse)
    summary = service.submit()
    assert summary["state"] == "failed"
    assert summary["message"] == "no image for task x"


def _outputs(service):
    run_id = service.submit()["id"]
    out = service.run_dir(run_id) / "tasks/squares/out"
    (out / "squares").mkdir(parents=True)
    (out / "summary.json").write_text("{}")
    (out / "squares/0.json").write_text('{"i": 0}')
    (out / ".simrun-merged").touch()
    return run_id, out


def test_output_file_and_archive(service):
    import io
    import tarfile

    run_id, _ = _outputs(service)
    base = f"/runs/{run_id}/outputs/squares"
    got = service.get(f"{base}/file", params={"path": "squares/0.json"})
    assert got.status_code == 200 and got.content == b'{"i": 0}'
    archive = service.get(f"{base}/archive")
    assert archive.status_code == 200
    assert archive.headers["content-type"] == "application/x-tar"
    with tarfile.open(fileobj=io.BytesIO(archive.content)) as tar:
        assert sorted(m.name for m in tar.getmembers() if m.isfile()) == [
            "squares/0.json",
            "summary.json",
        ]


def test_output_file_refuses_paths_outside_the_output(service, tmp_path):
    run_id, out = _outputs(service)
    (out / "link").symlink_to(service.run_dir(run_id) / "run.json")
    (out / "dirlink").symlink_to(tmp_path)
    base = f"/runs/{run_id}/outputs/squares/file"
    for path in (
        "../run.json",
        "../../../run.json",
        "/etc/passwd",
        "link",
        "dirlink/x",
        "nope.json",
        "squares",
        ".simrun-merged",
        "a\0b",
    ):
        assert service.get(base, params={"path": path}).status_code == 404, path
    assert (
        service.get(
            f"/runs/{run_id}/outputs/nope/file", params={"path": "x"}
        ).status_code
        == 404
    )
    assert service.get(f"/runs/{run_id}/outputs/nope/archive").status_code == 404


def test_output_archive_leaves_out_escaping_links(service):
    import io
    import tarfile

    run_id, out = _outputs(service)
    (out / "link").symlink_to(service.run_dir(run_id) / "run.json")
    archive = service.get(f"/runs/{run_id}/outputs/squares/archive")
    with tarfile.open(fileobj=io.BytesIO(archive.content)) as tar:
        assert "link" not in tar.getnames()
        assert ".simrun-merged" not in tar.getnames()


def test_output_archive_is_streamed_in_chunks(service, monkeypatch):
    import io
    import tarfile

    monkeypatch.setattr(app_module, "TAR_CHUNK", 7)
    run_id, out = _outputs(service)
    body = bytes(range(256)) * 9
    (out / "big.bin").write_bytes(body)
    (out / "big.bin").chmod(0o640)
    archive = service.get(f"/runs/{run_id}/outputs/squares/archive")
    assert archive.status_code == 200
    assert archive.headers["content-disposition"] == (
        f'attachment; filename="{run_id}-squares.tar"'
    )
    assert len(archive.content) % tarfile.RECORDSIZE == 0
    with tarfile.open(fileobj=io.BytesIO(archive.content)) as tar:
        member = tar.getmember("big.bin")
        assert member.mode == 0o640
        big = tar.extractfile(member)
        small = tar.extractfile("squares/0.json")
        assert big is not None and small is not None
        assert big.read() == body
        assert small.read() == b'{"i": 0}'


# --- snapshots through the API ----------------------------------------------


def _git_repo(path: Path) -> Path:
    import subprocess

    path.mkdir()
    for args in (
        ["init", "-q", "-b", "main"],
        ["config", "user.email", "a@b.c"],
        ["config", "user.name", "T"],
    ):
        subprocess.run(["git", "-C", str(path), *args], check=True)
    (path / "model.txt").write_text("model\n")
    (path / "sub").mkdir()
    (path / "sub" / "x.py").write_text("x = 1\n")
    (path / "sub").chmod(0o750)
    (path / "run.sh").write_text("#!/bin/sh\n")
    (path / "run.sh").chmod(0o755)
    (path / "link").symlink_to("sub/x.py")
    subprocess.run(["git", "-C", str(path), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(path), "commit", "-q", "-m", "i"], check=True)
    (path / "model.txt").write_text("changed\n")
    (path / "new.txt").write_text("new\n")
    return path


def _tar(members: Mapping[str, bytes | str]) -> bytes:
    import io
    import tarfile

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for name, content in members.items():
            info = tarfile.TarInfo(name)
            if isinstance(content, str):
                info.type = tarfile.SYMTYPE
                info.linkname = content
                tar.addfile(info)
            else:
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _post_snapshot(service, name: str, scan, tar: bytes, token: str = "t-mk"):
    return service.client.post(
        f"/snapshots/{name}",
        files={
            "scan": ("scan.json", scan.model_dump_json(), "application/json"),
            "files": ("files.tar", tar, "application/x-tar"),
        },
        headers=service.auth(token),
    )


def _scan_of(files: dict[str, bytes], links: dict[str, str] | None = None):
    import hashlib

    from simrun.snapshot import FileEntry, Scan

    entries = [
        FileEntry(path=p, mode=0o444, sha256=hashlib.sha256(c).hexdigest(), link=None)
        for p, c in files.items()
    ]
    entries += [
        FileEntry(path=p, mode=0o777, sha256=None, link=t)
        for p, t in (links or {}).items()
    ]
    return Scan(
        branch="main",
        commit="c" * 40,
        dirty=False,
        changed_files=[],
        patch=b"",
        files=entries,
    )


def test_upload_twice_transfers_the_files_once(
    service, make_client, in_process, tmp_path: Path
):
    from simrun.snapshot import scan

    repo = _git_repo(tmp_path / "model")
    transport = in_process(service)
    client = make_client(service, transport=transport, sources="upload")
    entries = [f.model_dump() for f in scan(repo).files]
    assert "model.txt" in service.post("/snapshots/model/missing", entries).json()
    first = client.source(repo, "model")
    assert service.post("/snapshots/model/missing", entries).json() == []
    second = client.source(repo, "model")
    assert first == second
    # the second upload sent an empty tar
    uploads = [r for r in transport.requests if r.url.path == "/snapshots/model"]
    assert len(uploads) == 2
    assert b"changed\n" in uploads[0].content
    assert b"changed\n" not in uploads[1].content
    assert (service.settings.store / first.snapshot / "model.txt").read_text() == (
        "changed\n"
    )


def test_upload_equals_a_direct_snapshot(service, make_client, tmp_path: Path):
    from simrun.snapshot import snapshot

    repo = _git_repo(tmp_path / "model")
    uploaded = make_client(service, sources="upload").source(repo, "model")
    direct = snapshot(repo, tmp_path / "direct", "model")
    assert uploaded.model_dump(exclude={"patch"}) == direct.model_dump(
        exclude={"patch"}
    )
    assert uploaded.patch == direct.patch
    assert uploaded.patch is not None and direct.patch is not None
    store = service.settings.store
    assert (store / uploaded.patch).read_bytes() == (
        tmp_path / "direct" / direct.patch
    ).read_bytes()
    up, di = store / uploaded.snapshot, tmp_path / "direct" / direct.snapshot
    paths = sorted(p.relative_to(di) for p in di.rglob("*"))
    assert paths == sorted(p.relative_to(up) for p in up.rglob("*"))
    for rel in paths:
        a, b = (up / rel).lstat(), (di / rel).lstat()
        assert (a.st_mode, a.st_size) == (b.st_mode, b.st_size), rel
        if (di / rel).is_symlink():
            assert os.readlink(up / rel) == os.readlink(di / rel)
        elif (di / rel).is_file():
            assert (up / rel).read_bytes() == (di / rel).read_bytes()


def test_a_direct_snapshot_lets_an_upload_send_nothing(service, tmp_path: Path):
    from simrun.snapshot import scan, snapshot

    repo = _git_repo(tmp_path / "model")
    snapshot(repo, service.settings.store, "model")
    entries = [f.model_dump() for f in scan(repo).files]
    assert service.post("/snapshots/model/missing", entries).json() == []


def test_upload_refuses_a_member_outside_the_tree(service):
    scan = _scan_of({"a.txt": b"a"})
    response = _post_snapshot(service, "model", scan, _tar({"../x": b"a"}))
    assert response.status_code == 422, response.text
    assert not (service.settings.store / "x").exists()
    assert not list((service.settings.store / "snapshots" / "model").glob("*"))


@pytest.mark.parametrize(
    "path",
    ["/abs", "a/../b", "../a", "a\0b", ".git/config", "a/.git/x", "./a", "", ".", "a/"],
)
def test_snapshot_routes_refuse_a_path_outside_the_tree(service, path):
    scan = _scan_of({path: b"a"})
    entries = [f.model_dump() for f in scan.files]
    assert service.post("/snapshots/model/missing", entries).status_code == 422
    assert _post_snapshot(service, "model", scan, _tar({})).status_code == 422


@pytest.mark.parametrize(
    "links",
    [
        {"l": "/etc/passwd"},
        {"l": "../out"},
        {"l": "sub/../../out"},
        {"d/l": "../../out"},
        {"l": ""},
        # lexically inside, but `b` is the root: `b/..` is its parent
        {"b": ".", "e": "b/../x"},
        {"a": "b", "b": "a"},  # a loop
        {"b": "/", "e": "b/x"},
    ],
)
def test_upload_refuses_a_link_out_of_the_tree(service, links):
    scan = _scan_of({}, links)
    assert _post_snapshot(service, "model", scan, _tar({})).status_code == 422


def test_upload_takes_links_within_the_tree(service):
    links = {"d/l": "../a.txt", "b": ".", "e": "b/a.txt", "f": "d/../a.txt"}
    scan = _scan_of({"a.txt": b"a"}, links)
    response = _post_snapshot(service, "model", scan, _tar({"a.txt": b"a"}))
    assert response.status_code == 200, response.text
    root = service.settings.store / response.json()["snapshot"]
    assert (root / "e").read_text() == "a" and (root / "d" / "l").read_text() == "a"


def test_upload_refuses_a_hash_which_does_not_match(service):
    scan = _scan_of({"a.txt": b"a"})
    response = _post_snapshot(service, "model", scan, _tar({"a.txt": b"b"}))
    assert response.status_code == 422
    assert "hash" in response.json()["detail"]


def test_upload_refuses_a_member_which_is_not_a_listed_file(service):
    scan = _scan_of({"a.txt": b"a"}, {"l": "a.txt"})
    for members in ({"a.txt": b"a", "b.txt": b"b"}, {"a.txt": b"a", "l": "a.txt"}):
        assert _post_snapshot(service, "model", scan, _tar(members)).status_code == 422


def test_upload_refuses_a_file_below_another_entry(service):
    scan = _scan_of({"l/x": b"a"}, {"l": "."})
    assert _post_snapshot(service, "model", scan, _tar({"l/x": b"a"})).status_code == (
        422
    )


def test_upload_refuses_a_mode_with_write_or_special_bits(service):
    scan = _scan_of({"a.txt": b"a"})
    for mode in (0o644, 0o4555):
        scan.files[0].mode = mode
        response = _post_snapshot(service, "model", scan, _tar({"a.txt": b"a"}))
        assert response.status_code == 422


def test_upload_without_the_content_of_a_file_conflicts(service):
    scan = _scan_of({"a.txt": b"a"})
    response = _post_snapshot(service, "model", scan, _tar({}))
    assert response.status_code == 409
    assert "a.txt" in response.json()["detail"]


@pytest.mark.parametrize("name", ["..", ".hidden", "a b", "a%2Fb"])
def test_snapshot_routes_refuse_a_bad_name(service, name):
    assert service.post(f"/snapshots/{name}/missing", []).status_code in (404, 422)
    response = _post_snapshot(service, name, _scan_of({}), _tar({}))
    assert response.status_code in (404, 422)


def test_snapshot_routes_need_a_token(service):
    assert service.client.post("/snapshots/model/missing", json=[]).status_code == 401
    assert _post_snapshot(
        service, "model", _scan_of({}), _tar({}), "bad"
    ).status_code == (401)


def test_upload_is_bounded(service_factory):
    with service_factory(max_upload_bytes=16384) as svc:  # a tar has 10240 bytes
        scan = _scan_of({"a.bin": b"x" * 32768})
        response = _post_snapshot(svc, "model", scan, _tar({"a.bin": b"x" * 32768}))
        assert response.status_code == 413
        small = _post_snapshot(svc, "model", _scan_of({"a": b"a"}), _tar({"a": b"a"}))
        assert small.status_code == 200, small.text


def test_upload_refuses_a_bad_directory(service):
    for dirs in (
        {"d": 0o4755},
        {"d": 0o1777},
        {"a.txt": 0o755},
        {"l/d": 0o755},
        {"../d": 0o755},
    ):
        scan = _scan_of({"a.txt": b"a"}, {"l": "."})
        scan.dirs = dirs
        response = _post_snapshot(service, "model", scan, _tar({"a.txt": b"a"}))
        assert response.status_code == 422, dirs


def test_upload_renames_the_new_files_into_place(service, monkeypatch):
    import simrun.snapshot as snapshot_module

    real = snapshot_module._copy

    def copy(source, dst):
        assert not isinstance(source, Path), f"{source} copied, not renamed"
        return real(source, dst)

    monkeypatch.setattr(snapshot_module, "_copy", copy)
    scan = _scan_of({"a.txt": b"a", "d/b.txt": b"b"})
    scan.dirs = {"d": 0o750}
    response = _post_snapshot(
        service, "model", scan, _tar({"a.txt": b"a", "d/b.txt": b"b"})
    )
    assert response.status_code == 200, response.text
    root = service.settings.store / response.json()["snapshot"]
    assert (root / "d" / "b.txt").read_text() == "b"
    assert (root / "d").stat().st_mode & 0o7777 == 0o750
    assert (root / "a.txt").stat().st_mode & 0o7777 == 0o444


def test_missing_bounds_the_file_list(service, monkeypatch):
    monkeypatch.setattr(app_module, "FILE_LIST_MAX_BYTES", 1024)
    entries = [
        f.model_dump() for f in _scan_of({f"f{i}": b"x" for i in range(50)}).files
    ]
    assert service.post("/snapshots/model/missing", entries).status_code == 413
    assert service.post("/snapshots/model/missing", entries[:2]).status_code == 200


def test_a_too_large_upload_closes_its_spooled_files(live, monkeypatch):
    from tempfile import SpooledTemporaryFile

    import starlette.formparsers

    made = []

    class Recorded(SpooledTemporaryFile):
        def __init__(self, *args, **kw):
            super().__init__(*args, **kw)
            made.append(self)

    monkeypatch.setattr(starlette.formparsers, "SpooledTemporaryFile", Recorded)
    # a limit above one read of the socket (64 KiB), so the parser has spooled the
    # start of the tar when the limit trips, and a body far above it
    server = live(max_upload_bytes=1024 * 1024)
    body = b"x" * (4 * 1024 * 1024)
    scan = _scan_of({"a.bin": body})
    request = httpx.Request(
        "POST",
        f"{server.base}/snapshots/model",
        files={
            "scan": ("scan.json", scan.model_dump_json(), "application/json"),
            "files": ("files.tar", _tar({"a.bin": body}), "application/x-tar"),
        },
    )
    data = request.read()
    # chunks without a Content-Length: the limit counts the stream, after the
    # parser spooled the start of the tar
    try:
        response = server.http.post(
            "/snapshots/model",
            content=(data[i : i + 4096] for i in range(0, len(data), 4096)),
            headers={"Content-Type": request.headers["Content-Type"]},
        )
        assert response.status_code == 413, response.text
    except httpx.TransportError:
        pass  # the service answered and closed while the body was still sent
    assert made and all(f.closed for f in made)
