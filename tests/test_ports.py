"""Output ports: a task writes a port `<port>` into `out/<port>`, which a downstream
task reads as the input `<task>.<port>`."""

import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel

from simrun import podspec
from simrun.demo import SQUARES, SquaresSettings
from simrun.planner import PodShape
from simrun.plugins import TaskType
from simrun.service.app import validate_workflow
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.worker import paths_from_env, run_merge, run_worker
from simrun.workflow import Priority, Workflow

RUN = "20261106-000000-abcdef"


class TotalSettings(BaseModel):
    pass


TOTAL = TaskType(
    name="demo.total", settings=TotalSettings, inputs=("squares",), command="total"
)
TYPES = {SQUARES.name: SQUARES, TOTAL.name: TOTAL}
SHAPE = PodShape(1, 1, 1, 0.5, 1, 0.75, 0.0, 0)


def _workflow(ref: str) -> Workflow:
    return Workflow(
        name="ports",
        tasks=[
            {"id": "sq", "type": "demo.squares", "settings": {"n": 5}},
            {"id": "total", "type": "demo.total", "inputs": {"squares": ref}},
        ],
    )


def test_a_port_is_a_directory_of_out(tmp_path: Path) -> None:
    store = RunStore(tmp_path)
    workflow = _workflow("sq.squares")
    assert validate_workflow(workflow, TYPES) == []
    record = RunRecord(
        id=RUN,
        submitted_by="u",
        owner="u",
        agent=None,
        priority=Priority.OWNER,
        group=None,
        workflow=workflow,
        sources=[],
        image="localhost:5000/base:1",
        reserve=None,
        state=RunState.RUNNING,
        tasks=dict.fromkeys(("sq", "total"), TaskState.WAITING),
        plans={},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
    )
    # the demo computes and merges its units
    settings = SquaresSettings(n=5)
    paths = store.task_paths(RUN, "sq", {}, tmp_path / "scratch")
    run_worker(
        SQUARES,
        settings,
        paths,
        "sq-w0",
        cores=2,
        memory_bytes=None,
        base_gb=0.5,
        per_worker_gb=0.5,
    )
    run_merge(SQUARES, settings, paths)
    # the downstream task sees the port in its environment
    total = workflow.task("total")
    env = podspec.env_values(record, total, TOTAL, SHAPE, str(tmp_path), "/scratch")
    downstream = paths_from_env(env)
    port = downstream.inputs["squares"]
    assert port == paths.out / "squares"
    squares = [json.loads(p.read_text())["square"] for p in port.glob("*.json")]
    assert sorted(squares) == [0, 1, 4, 9, 16]
    # the summary of the task, which `compare` reads, is no port
    assert json.loads((paths.out / "summary.json").read_text()) == {"sum": 30, "n": 5}


def test_a_port_the_type_does_not_have() -> None:
    errors = validate_workflow(_workflow("sq.out"), TYPES)
    assert errors == ["input 'squares' of task 'total': task 'sq' has no output 'out'"]


def _source(name: str, snapshot: str, path: str | None = None) -> SourceProvenance:
    return SourceProvenance(
        name=name,
        branch="main",
        commit="c" * 40,
        dirty=False,
        changed_files=[],
        diff_sha256="0" * 64,
        snapshot=snapshot,
        patch=None,
        path=path,
    )


def _with_sources(sources: list[SourceProvenance]) -> RunRecord:
    return RunRecord(
        id=RUN,
        submitted_by="u",
        owner="u",
        agent=None,
        priority=Priority.OWNER,
        group=None,
        workflow=_workflow("sq.squares"),
        sources=sources,
        image="localhost:5000/base:1",
        reserve=None,
        state=RunState.RUNNING,
        tasks={},
        plans={},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
    )


def test_a_task_imports_from_the_root_of_the_model(tmp_path: Path) -> None:
    # as `python -m` in a checkout: the root of the model, e.g. a checkout of
    # sbmlsim with its `examples`, comes first on the path of the task
    record = _with_sources(
        [_source("model", "snapshots/model/a"), _source("lib", "snapshots/lib/b")]
    )
    task = record.workflow.task("sq")
    env = podspec.env_values(record, task, SQUARES, SHAPE, "/store", "/scratch")
    assert env["PYTHONPATH"] == "/store/snapshots/model/a"
    # a working tree recorded without a snapshot (the local mode) by its path
    local = _with_sources([_source("model", "", path=str(tmp_path))])
    env = podspec.env_values(local, task, SQUARES, SHAPE, "/store", "/scratch")
    assert env["PYTHONPATH"] == str(tmp_path)
    # a run without sources has no root
    env = podspec.env_values(
        _with_sources([]), task, SQUARES, SHAPE, "/store", "/scratch"
    )
    assert "PYTHONPATH" not in env
