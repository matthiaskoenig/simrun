import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from simrun.service.index import IndexedRunStore, RunIndex
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.workflow import Priority, Workflow

WORKFLOW = Workflow(
    name="demo",
    tasks=[{"id": "squares", "type": "demo.squares", "settings": {"n": 4}}],
)


def _record(run_id: str, **update) -> RunRecord:
    record = RunRecord(
        id=run_id,
        submitted_by="mk",
        owner="mk",
        agent=None,
        priority=Priority.OWNER,
        group="g1",
        workflow=WORKFLOW,
        sources=[
            SourceProvenance(
                name="model",
                branch="develop",
                commit="0" * 40,
                dirty=False,
                changed_files=[],
                diff_sha256="00",
                snapshot=f"snapshots/model/{run_id}",
                patch=None,
            )
        ],
        image="localhost:5000/base:1",
        reserve=None,
        state=RunState.QUEUED,
        tasks={"squares": TaskState.WAITING},
        plans={},
        backend_name=f"simrun-{run_id}",
        created=datetime(2026, 11, 6, tzinfo=UTC),
        finished=None,
        message=None,
    )
    return record.model_copy(update=update)


def test_write_updates_the_index(tmp_path: Path) -> None:
    index = RunIndex(tmp_path / "db.sqlite")
    store = IndexedRunStore(tmp_path / "store", index)
    store.write(_record("a"))
    store.write(_record("b", owner="alice", agent="alice-bot", group="g2"))
    store.write(_record("c", state=RunState.SUCCEEDED))
    store.write(_record("d", state=RunState.FAILED, pending_release="srv-02"))
    store.write(_record("e", state=RunState.CANCELLED, pending_cancel=True))
    # the runs the controller follows: not ended, or with a release or a
    # cancellation pending
    assert index.live_ids() == ["a", "b", "d", "e"]
    assert [r.summary.id for r in index.runs()] == ["a", "b", "c", "d", "e"]
    assert [r.summary.id for r in index.runs(owner="alice")] == ["b"]
    assert [r.summary.id for r in index.runs(agent="alice-bot")] == ["b"]
    assert [r.summary.id for r in index.runs(group="g1", state=RunState.SUCCEEDED)] == [
        "c"
    ]
    (b,) = index.runs(owner="alice")
    assert b.summary.agent == "alice-bot"
    assert b.workflow == "demo"
    assert b.snapshots == ("snapshots/model/b",)
    assert not b.pending
    store.forget("a")
    assert index.live_ids() == ["b", "d", "e"]


def test_rebuilt_from_the_store(tmp_path: Path) -> None:
    plain = RunStore(tmp_path / "store")
    plain.write(_record("a"))
    plain.write(_record("b", state=RunState.SUCCEEDED))
    (tmp_path / "store/runs/broken").mkdir()
    (tmp_path / "store/runs/broken/run.json").write_text("{")
    index = RunIndex(tmp_path / "db.sqlite")
    index.put(_record("gone"))
    store = IndexedRunStore(tmp_path / "store", index)
    store.rebuild()
    assert [r.summary.id for r in index.runs()] == ["a", "b"]
    assert index.live_ids() == ["a"]


def test_the_record_is_written_before_the_index(tmp_path: Path) -> None:
    index = RunIndex(tmp_path / "db.sqlite")
    (tmp_path / "file").write_text("")
    # the root of the store is a file: the record cannot be written
    with pytest.raises(OSError):
        IndexedRunStore(tmp_path / "file", index).write(_record("x"))
    assert index.runs() == []


def _reads(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    read: list[str] = []
    original = RunStore.read

    def counted(self: RunStore, run_id: str) -> RunRecord:
        read.append(run_id)
        return original(self, run_id)

    monkeypatch.setattr(RunStore, "read", counted)
    return read


def test_reconcile_reads_only_the_live_runs(service, monkeypatch):
    ended = service.submit()["id"]
    live = service.submit()["id"]
    service.backend.finish(service.name(ended))
    service.controller.reconcile()
    assert service.store.read(ended).state == "succeeded"
    read = _reads(monkeypatch)
    service.controller.reconcile()
    assert set(read) == {live}


def test_list_filters_in_the_index(service, monkeypatch):
    a = service.submit("t-mk", group="g1")["id"]
    b = service.submit("t-alice-bot", group="g2")["id"]
    read = _reads(monkeypatch)
    listed = service.get("/runs", params={"owner": "alice"}).json()
    assert [r["id"] for r in listed] == [b]
    assert listed[0]["agent"] == "alice-bot"
    assert listed[0]["state"] == "queued"
    assert [r["id"] for r in service.get("/runs").json()] == sorted([a, b])
    assert read == []


def test_index_is_rebuilt_at_the_start(service_factory, tmp_path):
    with service_factory() as service:
        run_id = service.submit(group="g1")["id"]
    db = service.settings.db
    db.unlink()
    for suffix in ("-wal", "-shm"):
        db.with_name(db.name + suffix).unlink(missing_ok=True)
    # the same backend: the workflow of the run still exists for the controller
    with service_factory(service.backend) as restarted:
        listed = restarted.get("/runs", params={"group": "g1"}).json()
        assert [r["id"] for r in listed] == [run_id]
        assert restarted.store.index.live_ids() == [run_id]


def test_a_failed_index_write_is_retried(service, monkeypatch):
    index = service.store.index
    original = RunIndex.put

    def locked(self, record):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(RunIndex, "put", locked)
    # the record is written, the submission and the controller go on
    run_id = service.submit()["id"]
    assert service.store.read(run_id).state == "queued"
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    assert service.store.read(run_id).state == "succeeded"
    # the run ended: the workflow is stopped although its index row was not written
    assert ("stop", name) in service.backend.calls
    assert index.runs() == []
    monkeypatch.setattr(RunIndex, "put", original)
    # the next poll indexes the run from its record
    service.controller.reconcile()
    (run,) = index.runs()
    assert (run.summary.id, run.summary.state) == (run_id, "succeeded")


def test_a_record_which_cannot_be_read_at_the_start_is_followed(
    service_factory, caplog
):
    with service_factory() as service:
        run_id = service.submit()["id"]
    record = service.run_dir(run_id) / "run.json"
    good = record.read_text()
    record.write_text("{")
    with service_factory(service.backend) as restarted:
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            restarted.controller.reconcile()
            restarted.controller.reconcile()
        warned = [r for r in caplog.records if run_id in r.getMessage()]
        assert len(warned) >= 2  # at every poll
        assert restarted.store.index.runs() == []
        record.write_text(good)
        restarted.controller.reconcile()
        assert restarted.store.index.live_ids() == [run_id]


def test_reindex_does_not_rewrite_the_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = IndexedRunStore(tmp_path / "store", RunIndex(tmp_path / "index.db"))
    RunStore.write(store, _record("r1"))
    store._unindexed.add("r1")
    read = store.read

    def read_then_cancel(run_id: str) -> RunRecord:
        record = read(run_id)
        # a write of the service between the read of reindex and its index put
        changed = record.model_copy(update={"pending_cancel": True})
        RunStore.write(store, changed)
        return record

    monkeypatch.setattr(store, "read", read_then_cancel)
    assert store.reindex() == []
    monkeypatch.undo()
    assert store.read("r1").pending_cancel
