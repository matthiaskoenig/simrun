import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from simrun.cwl import export_cwl
from simrun.demo import SQUARES
from simrun.planner import PodShape, TaskPlan
from simrun.store import RunRecord, RunState, TaskState
from simrun.workflow import Priority, Workflow

RUN = "20261105-120000-abc123"
TYPES = {SQUARES.name: SQUARES}
N = 20


def _shape(cores: int) -> PodShape:
    return PodShape(
        cores=cores,
        memory_gb=1,
        processes=cores,
        per_worker_gb=0.5,
        retry_memory_gb=2,
        retry_per_worker_gb=0.75,
        scratch_gb=0.0,
        gpus=0,
    )


def _record() -> RunRecord:
    workflow = Workflow(
        name="demo",
        tasks=[{"id": "squares", "type": "demo.squares", "settings": {"n": N}}],
    )
    return RunRecord(
        id=RUN,
        submitted_by="u",
        owner="o",
        agent=None,
        priority=Priority.OWNER,
        group=None,
        workflow=workflow,
        sources=[],
        image="localhost:5000/env:1",
        reserve=None,
        state=RunState.QUEUED,
        tasks={"squares": TaskState.WAITING},
        plans={},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
    )


def _plans(pods: int) -> dict[str, TaskPlan]:
    return {
        "squares": TaskPlan(
            "squares", pods, _shape(1), "simrun-owner-base", "simrun-owner-extra", N
        )
    }


def test_cwl_validates(tmp_path: Path) -> None:
    path = export_cwl(_record(), _plans(2), TYPES, tmp_path / "run.cwl")
    assert path.exists()


def _run(tmp_path: Path, pods: int, parallel: bool) -> bytes:
    store = tmp_path / f"store{pods}{parallel}"
    task_dir = store / "runs" / RUN / "tasks" / "squares"
    task_dir.mkdir(parents=True)
    (task_dir / "settings.json").write_text(json.dumps({"n": N}))
    cwl = export_cwl(_record(), _plans(pods), TYPES, tmp_path / f"run{pods}.cwl")
    cwltool = Path(sys.executable).parent / "cwltool"
    command = [str(cwltool), "--no-container", "--outdir", str(tmp_path / "o")]
    if parallel:
        command.append("--parallel")
    command += [
        str(cwl),
        "--store",
        str(store),
        "--scratch",
        str(tmp_path / "scratch"),
    ]
    env = {**os.environ, "PATH": f"{Path(sys.executable).parent}:{os.environ['PATH']}"}
    done = subprocess.run(
        command, capture_output=True, text=True, timeout=300, env=env, check=False
    )
    assert done.returncode == 0, done.stderr[-3000:]
    return (task_dir / "out" / "summary.json").read_bytes()


@pytest.mark.parametrize("parallel", [False, True])
def test_cwltool_runs_demo(tmp_path: Path, parallel: bool) -> None:
    summary = json.loads(_run(tmp_path, 2, parallel))
    assert summary == {"sum": sum(i * i for i in range(N)), "n": N}


def test_same_result_as_unsplit(tmp_path: Path) -> None:
    assert _run(tmp_path, 1, False) == _run(tmp_path, 4, True)
