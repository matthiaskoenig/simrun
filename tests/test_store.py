from datetime import UTC, datetime
from pathlib import Path

import pytest

from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.workflow import Priority, Workflow


def make_record(run_id: str) -> RunRecord:
    return RunRecord(
        id=run_id,
        submitted_by="u",
        owner="o",
        agent=None,
        priority=Priority.STANDARD,
        group=None,
        workflow=Workflow(name="w", tasks=[]),
        sources=[],
        image=None,
        reserve=None,
        state=RunState.QUEUED,
        tasks={"t": TaskState.WAITING},
        plans={"t": {"k": 1}},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
    )


def test_record_round_trip(tmp_path: Path) -> None:
    store = RunStore(tmp_path)
    rid = store.new_run_id()
    assert len(rid.split("-")) == 3 and len(rid.split("-")[2]) == 6
    rec = make_record(rid)
    store.write(rec)
    assert store.read(rid) == rec
    assert list(store.iter_records()) == [rec]
    assert not list(store.run_dir(rid).glob("*.tmp*"))


def test_read_unknown_raises(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        RunStore(tmp_path).read("nope")


def test_task_paths(tmp_path: Path) -> None:
    p = RunStore(tmp_path).task_paths("r1", "t", {"x": tmp_path / "i"}, tmp_path / "s")
    assert p.run_dir == tmp_path / "runs" / "r1"
    assert p.task_dir == p.run_dir / "tasks" / "t"
    assert p.out == p.task_dir / "out"
    assert p.store == tmp_path
