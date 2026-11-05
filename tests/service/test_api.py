import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from simrun.service import app as app_module
from simrun.service.app import serve
from simrun.service.auth import Identity, load_tokens, priority_of
from simrun.snapshot import SourceProvenance
from simrun.store import RunStore
from simrun.testing import FakeBackend
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


def test_submit_returns_id_at_once(service):
    summary = service.submit(group="g1")
    assert summary["state"] == "queued"
    assert summary["group"] == "g1"
    assert summary["tasks"] == {"squares": "waiting"}
    run_dir = service.run_dir(summary["id"])
    record = json.loads((run_dir / "run.json").read_text())
    assert record["backend_name"] == service.name(summary["id"])
    assert record["image"] == "localhost:5000/base:1"
    assert record["plans"]["squares"]["units"] == 4
    settings = json.loads((run_dir / "tasks/squares/settings.json").read_text())
    assert settings["n"] == 4
    assert settings["sleep_s"] == 0.0
    assert service.name(summary["id"]) in service.backend.manifests


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
    assert "one worker needs 2000.0 GiB" in response.json()["detail"]
    assert "largest node srv-01" in response.json()["detail"]
    assert not (service.settings.store / "runs").exists()


def test_estimate(service):
    response = service.post(
        "/estimate", {"workflow": service.workflow(n=100), "sources": []}
    )
    assert response.status_code == 200, response.text
    squares = response.json()["squares"]
    assert squares["plan"]["units"] == 100
    assert squares["plan"]["pods"] == 8
    assert squares["resources"]["source"] == "plugin hint"
    assert squares["seconds"] is None
    single = {
        "workflow": service.workflow(n=100),
        "sources": [],
        "split": "single-server",
    }
    assert service.post("/estimate", single).json()["squares"]["plan"]["pods"] == 1


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
    assert progress["squares"]["last"] == line


def test_outputs_relative_to_store(service):
    run_id = service.submit()["id"]
    out = service.run_dir(run_id) / "tasks/squares/out"
    (out / "units").mkdir(parents=True)
    (out / "summary.json").write_text("{}")
    (out / "units/0.json").write_text('{"i": 0}')
    files = service.get(f"/runs/{run_id}/outputs/squares").json()
    assert files == [
        {"path": f"runs/{run_id}/tasks/squares/out/summary.json", "bytes": 2},
        {"path": f"runs/{run_id}/tasks/squares/out/units/0.json", "bytes": 8},
    ]
    assert service.get(f"/runs/{run_id}/outputs/nope").status_code == 404


def test_logs(service):
    run_id = service.submit()["id"]
    service.backend.logs_text[(service.name(run_id), "squares")] = "hello"
    assert service.get(f"/runs/{run_id}/logs/squares").text == "hello"


def test_cancel(service):
    run_id = service.submit(reserve="srv-03")["id"]
    response = service.post(f"/runs/{run_id}/cancel")
    assert response.json()["state"] == "cancelled"
    assert response.json()["tasks"] == {"squares": "skipped"}
    assert ("cancel", service.name(run_id)) in service.backend.calls
    assert ("unreserve", "srv-03") in service.backend.calls
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
    shared = provenance("model", "snapshots/model/shared")
    own = provenance("lib", "snapshots/model/own")
    failed = service.submit(workflow=service.workflow(), sources=[shared, own])["id"]
    succeeded = service.submit(sources=[shared])["id"]
    service.backend.fail(service.name(failed), "squares")
    service.backend.finish(service.name(succeeded))
    service.controller.reconcile()
    store = RunStore(store_root)
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
    assert (store_root / "snapshots/model/shared/data").exists()
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


def test_image_built_once(service_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("UV_OFFLINE", "1")
    wheel = tmp_path / "simrun-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    with service_factory(simrun_wheel=wheel, default_image=None) as service:
        model = service.settings.store / "snapshots/model/a"
        model.mkdir(parents=True)
        (model / "pyproject.toml").write_text(
            '[project]\nname = "model"\nversion = "0.1"\nrequires-python = ">=3.14"\n'
            "dependencies = []\n"
        )
        (model / "uv.lock").write_text(
            'version = 1\nrevision = 5\nrequires-python = ">=3.14"\n'
        )
        source = provenance("model", "snapshots/model/a")
        first = service.submit(sources=[source])["id"]
        second = service.submit(sources=[source])["id"]
    builds = [c for c in service.backend.calls if c[0] == "build_image"]
    assert len(builds) == 1
    _, ref, files = builds[0]
    assert ref.startswith("localhost:5000/simrun-env:")
    assert files == ["Dockerfile", wheel.name]
    store = RunStore(service.settings.store)
    assert store.read(first).image == store.read(second).image == ref
    assert (
        'name = "pydantic"' in (service.run_dir(first) / "environment.lock").read_text()
    )


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
    def submit(self, manifest):
        raise RuntimeError("no cluster")


def test_failed_submission_releases_node(service_factory):
    with service_factory(_FailingSubmit()) as service:
        body = {"workflow": service.workflow(), "sources": [], "reserve": "srv-02"}
        response = service.post("/runs", body)
        assert response.status_code == 502
        assert "no cluster" in response.json()["detail"]
        assert ("unreserve", "srv-02") in service.backend.calls
        (record,) = service.get("/runs").json()
        assert record["state"] == "failed"
        assert record["tasks"] == {"squares": "skipped"}
