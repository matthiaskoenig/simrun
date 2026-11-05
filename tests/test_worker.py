import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from simrun import worker
from simrun.demo import SQUARES, SquaresSettings
from simrun.ledger import Ledger
from simrun.plugins import TaskPaths
from simrun.worker import processes, run_merge, run_worker

WORKER = """
import json, sys
from pathlib import Path
from simrun.demo import SQUARES, SquaresSettings
from simrun.plugins import TaskPaths
from simrun.worker import run_worker

if __name__ == "__main__":
    task_dir = Path(sys.argv[1])
    settings = SquaresSettings.model_validate_json(sys.argv[3])
    paths = TaskPaths(
        store=task_dir.parent,
        run_dir=task_dir.parent,
        task_dir=task_dir,
        out=task_dir / "out",
        inputs={},
        scratch=task_dir.parent / "scratch",
    )
    run_worker(
        SQUARES,
        settings,
        paths,
        sys.argv[2],
        cores=int(sys.argv[4]),
        memory_bytes=None,
        base_gb=0.5,
        per_worker_gb=0.5,
        grace_s=float(sys.argv[5]),
    )
"""


def task_paths(task_dir: Path) -> TaskPaths:
    return TaskPaths(
        store=task_dir.parent,
        run_dir=task_dir.parent,
        task_dir=task_dir,
        out=task_dir / "out",
        inputs={},
        scratch=task_dir.parent / "scratch",
    )


def start_worker(
    task_dir: Path,
    worker_id: str,
    settings: SquaresSettings,
    cores: int,
    grace_s: float = 60.0,
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            WORKER,
            str(task_dir),
            worker_id,
            settings.model_dump_json(),
            str(cores),
            str(grace_s),
        ]
    )


def read_report(task_dir: Path, worker_id: str) -> dict:
    return json.loads((task_dir / "workers" / f"{worker_id}.json").read_text())


def test_processes_memory_bound() -> None:
    assert processes(40, 188 * 2**30, 2.0, 5.1) == 36
    assert processes(40, 188 * 2**30, 2.0, 7.65) == 24
    assert processes(4, None, 2.0, 6.0) == 4


def test_processes_too_small_raises() -> None:
    with pytest.raises(ValueError):
        processes(4, 4 * 2**30, 2.0, 6.0)


def test_two_workers_share_a_task(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=200)
    workers = [start_worker(task_dir, f"w{i}", settings, cores=2) for i in range(2)]
    try:
        for process in workers:
            assert process.wait(timeout=120) == 0
    finally:
        for process in workers:
            process.kill()
            process.wait()
    done = [set(read_report(task_dir, f"w{i}")["units_done"]) for i in range(2)]
    assert done[0].isdisjoint(done[1])
    assert done[0] | done[1] == set(range(200))
    lines = (task_dir / "progress.jsonl").read_text().splitlines()
    events = [json.loads(line) for line in lines]
    assert sorted(e["unit"] for e in events) == list(range(200))
    assert {e["event"] for e in events} == {"unit_done"}
    run_merge(SQUARES, settings, task_paths(task_dir))
    summary = json.loads((task_dir / "out" / "summary.json").read_text())
    assert summary["sum"] == sum(i * i for i in range(200))


def test_sigterm_releases_running_units(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=50, sleep_s=0.5)
    progress = task_dir / "progress.jsonl"
    process = start_worker(task_dir, "w0", settings, cores=4, grace_s=0.0)
    try:
        deadline = time.monotonic() + 60
        while not (progress.exists() and progress.read_text().strip()):
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=30) != 0
    finally:
        process.kill()
        process.wait()
    report = read_report(task_dir, "w0")
    ledger = Ledger(task_dir / "units", 50)
    done = sorted(int(name) for name in os.listdir(task_dir / "units" / "done"))
    assert 0 < len(report["units_done"]) < 50
    assert sorted(report["units_done"]) == done
    assert os.listdir(task_dir / "units" / "claimed") == []
    assert ledger.counts().pending == 50 - len(done)

    run_worker(
        SQUARES,
        settings.model_copy(update={"sleep_s": 0.0}),
        task_paths(task_dir),
        "w1",
        cores=2,
        memory_bytes=None,
        base_gb=0.5,
        per_worker_gb=0.5,
    )
    assert ledger.complete_ledger()
    run_merge(SQUARES, settings, task_paths(task_dir))
    summary = json.loads((task_dir / "out" / "summary.json").read_text())
    assert summary["sum"] == sum(i * i for i in range(50))


@pytest.fixture
def worker_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    task_dir = tmp_path / "runs" / "r1" / "squares"
    task_dir.mkdir(parents=True)
    monkeypatch.setenv("SIMRUN_TASK_DIR", str(task_dir))
    monkeypatch.setenv("SIMRUN_TASK_TYPE", "demo.squares")
    monkeypatch.setenv("SIMRUN_WORKER", "squares-w0")
    monkeypatch.setenv("SIMRUN_MEMORY_BASE_GB", "0.5")
    monkeypatch.setenv("SIMRUN_MEMORY_PER_WORKER_GB", "0.5")
    monkeypatch.setenv("SIMRUN_SCRATCH", str(tmp_path / "scratch"))
    for name in ("SIMRUN_STAGE", "SIMRUN_EDITABLE", "SIMRUN_INPUTS"):
        monkeypatch.delenv(name, raising=False)
    # main sets the thread variables; monkeypatch restores them afterwards
    for name in worker.THREAD_VARIABLES:
        monkeypatch.setenv(name, "4")
    return task_dir


def test_unit_error_exit_code(worker_env: Path) -> None:
    (worker_env / "settings.json").write_text(
        SquaresSettings(n=6, fail_at=3).model_dump_json()
    )
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 3
    assert "3" in os.listdir(worker_env / "units" / "pending")
    assert os.listdir(worker_env / "units" / "claimed") == []
    assert all(os.environ[name] == "1" for name in worker.THREAD_VARIABLES)


def test_main_done(worker_env: Path) -> None:
    (worker_env / "settings.json").write_text(SquaresSettings(n=5).model_dump_json())
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 0
    report = read_report(worker_env, "squares-w0")
    assert sorted(report["units_done"]) == list(range(5))


def test_main_configuration_error(worker_env: Path) -> None:
    (worker_env / "settings.json").write_text(json.dumps({"n": "many"}))
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 4


def test_main_stages_inputs(worker_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = worker_env.parent.parent.parent
    source = store / "runs" / "r1" / "mesh" / "out"
    source.mkdir(parents=True)
    (source / "mesh.txt").write_text("mesh")
    monkeypatch.setenv("SIMRUN_STAGE", "runs/r1/mesh/out=mesh")
    (worker_env / "settings.json").write_text(SquaresSettings(n=1).model_dump_json())
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 0
    scratch = Path(os.environ["SIMRUN_SCRATCH"])
    assert (scratch / "mesh" / "mesh.txt").read_text() == "mesh"


def test_paths_from_env(tmp_path: Path) -> None:
    task_dir = tmp_path / "runs" / "r1" / "fit"
    paths = worker.paths_from_env(
        {
            "SIMRUN_TASK_DIR": str(task_dir),
            "SIMRUN_INPUTS": "model=runs/r1/model/out,mesh=/scratch/mesh",
            "SIMRUN_SCRATCH": "/scratch/r1",
        }
    )
    assert paths.store == tmp_path
    assert paths.run_dir == tmp_path / "runs" / "r1"
    assert paths.out == task_dir / "out"
    assert paths.inputs == {
        "model": tmp_path / "runs" / "r1" / "model" / "out",
        "mesh": Path("/scratch/mesh"),
    }
    assert paths.scratch == Path("/scratch/r1")


def test_install_editable_command(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], check: bool) -> None:
        assert check
        calls.append(command)

    monkeypatch.setattr(worker.subprocess, "run", run)
    monkeypatch.setattr(worker.site, "addsitedir", lambda directory: None)
    worker.install_editable([])
    assert calls == []
    worker.install_editable(["/store/snapshots/a/1", "/store/snapshots/b/2"])
    assert calls == [
        [
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--no-deps",
            "--no-index",
            "-e",
            "/store/snapshots/a/1",
            "-e",
            "/store/snapshots/b/2",
        ]
    ]


def test_merge_incomplete_raises(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=3)
    ledger = Ledger.create(task_dir / "units", 3)
    index = ledger.claim("w0")
    assert index is not None
    ledger.complete(index, "w0")
    with pytest.raises(RuntimeError, match="1, 2"):
        run_merge(SQUARES, settings, task_paths(task_dir))
