import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from simrun.cwl import export_cwl, to_cwl
from simrun.demo import SQUARES
from simrun.planner import PodShape, TaskPlan
from simrun.plugins_builtin import COMMAND
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


def _cwltool(
    tmp_path: Path, cwl: Path, store: Path, parallel: bool
) -> subprocess.CompletedProcess[str]:
    cwltool = Path(sys.executable).parent / "cwltool"
    command = [str(cwltool), "--no-container", "--outdir", str(tmp_path / "o")]
    if parallel:
        command.append("--parallel")
    command += [str(cwl), "--store", str(store), "--scratch", str(tmp_path / "s")]
    env = {**os.environ, "PATH": f"{Path(sys.executable).parent}:{os.environ['PATH']}"}
    return subprocess.run(
        command, capture_output=True, text=True, timeout=300, env=env, check=False
    )


def _store(tmp_path: Path, name: str, settings: dict[str, Any]) -> Path:
    store = tmp_path / name
    task_dir = store / "runs" / RUN / "tasks" / "squares"
    task_dir.mkdir(parents=True)
    (task_dir / "settings.json").write_text(json.dumps(settings))
    return store


def _summary(store: Path) -> Path:
    return store / "runs" / RUN / "tasks" / "squares" / "out" / "summary.json"


def _run(tmp_path: Path, pods: int, parallel: bool) -> bytes:
    store = _store(tmp_path, f"store{pods}{parallel}", {"n": N})
    cwl = export_cwl(_record(), _plans(pods), TYPES, tmp_path / f"run{pods}.cwl")
    done = _cwltool(tmp_path, cwl, store, parallel)
    assert done.returncode == 0, done.stderr[-3000:]
    return _summary(store).read_bytes()


@pytest.mark.parametrize("parallel", [False, True])
def test_cwltool_runs_demo(tmp_path: Path, parallel: bool) -> None:
    summary = json.loads(_run(tmp_path, 2, parallel))
    assert summary == {"sum": sum(i * i for i in range(N)), "n": N}


def test_same_result_as_unsplit(tmp_path: Path) -> None:
    assert _run(tmp_path, 1, False) == _run(tmp_path, 4, True)


def _failing_extra(tmp_path: Path, codes: list[int]) -> Path:
    document = to_cwl(_record(), _plans(3), TYPES)
    tool = document["steps"]["squares-extra"]["run"]
    tool["arguments"] = ["sh", "-c", f"exit {codes[0]}"]
    path = tmp_path / "failing.cwl"
    path.write_text(yaml.safe_dump(document))
    return path


@pytest.mark.parametrize("code", [3, 4, 137, 143])
def test_failing_extra_workers_do_not_fail_the_run(tmp_path: Path, code: int) -> None:
    store = _store(tmp_path, "store", {"n": N})
    done = _cwltool(tmp_path, _failing_extra(tmp_path, [code]), store, False)
    assert done.returncode == 0, done.stderr[-3000:]
    assert json.loads(_summary(store).read_text())["n"] == N


def test_failing_extra_worker_with_another_code_fails_the_run(tmp_path: Path) -> None:
    store = _store(tmp_path, "store", {"n": N})
    done = _cwltool(tmp_path, _failing_extra(tmp_path, [1]), store, False)
    assert done.returncode != 0


@pytest.mark.parametrize("parallel", [False, True])
def test_unit_failure_fails_the_run_through_w0(tmp_path: Path, parallel: bool) -> None:
    store = _store(tmp_path, "store", {"n": N, "fail_at": 3})
    cwl = export_cwl(_record(), _plans(2), TYPES, tmp_path / "run.cwl")
    done = _cwltool(tmp_path, cwl, store, parallel)
    assert done.returncode != 0
    assert not _summary(store).exists()


def _record_with_command() -> RunRecord:
    record = _record()
    script = (
        'mkdir -p "$SIMRUN_TASK_DIR/out" && '
        'cat "$SIMRUN_TASK_DIR/../squares/out/summary.json" '
        '> "$SIMRUN_TASK_DIR/out/copy.json"'
    )
    workflow = Workflow(
        name="demo",
        tasks=[
            {"id": "squares", "type": "demo.squares", "settings": {"n": N}},
            {
                "id": "copy",
                "type": "command",
                "settings": {
                    "image": "localhost:5000/tools:1",
                    "args": ["sh", "-c", script],
                    "cores": 1,
                    "memory_gb": 1.0,
                },
                "inputs": {"sums": "squares.squares"},
            },
        ],
    )
    return record.model_copy(update={"workflow": workflow})


@pytest.mark.parametrize("parallel", [False, True])
def test_unsplit_task_runs_after_the_merge_it_reads(
    tmp_path: Path, parallel: bool
) -> None:
    record = _record_with_command()
    plans = {**_plans(3), "copy": TaskPlan("copy", 1, _shape(1), "b", "e", None)}
    types = {**TYPES, COMMAND.name: COMMAND}
    store = _store(tmp_path, "store", {"n": N})
    cwl = export_cwl(record, plans, types, tmp_path / "run.cwl")
    document = yaml.safe_load(cwl.read_text())
    assert document["steps"]["copy"]["in"]["after"]["source"] == ["squares-merge/log"]
    done = _cwltool(tmp_path, cwl, store, parallel)
    assert done.returncode == 0, done.stderr[-3000:]
    copy = store / "runs" / RUN / "tasks" / "copy" / "out" / "copy.json"
    assert json.loads(copy.read_text()) == {"sum": sum(i * i for i in range(N)), "n": N}
