"""`simrun-exec`: the harness of an unsplit plugin command."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from simrun import worker

GIB = 2**30


@pytest.fixture
def exec_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "report"
    task_dir.mkdir(parents=True)
    monkeypatch.setenv("SIMRUN_TASK_DIR", str(task_dir))
    monkeypatch.setenv("SIMRUN_TASK_TYPE", "demo.report")
    monkeypatch.setenv("SIMRUN_MEMORY_BASE_GB", "0.5")
    monkeypatch.setenv("SIMRUN_MEMORY_PER_WORKER_GB", "1.5")
    monkeypatch.setenv("SIMRUN_ATTEMPT", "1")
    monkeypatch.setenv("SIMRUN_SCRATCH", str(tmp_path / "scratch"))
    for name in (
        "SIMRUN_STAGE",
        "SIMRUN_EDITABLE",
        "SIMRUN_INPUTS",
        "SIMRUN_WORKER",
        worker.SOURCES_INSTALLED,
    ):
        monkeypatch.delenv(name, raising=False)
    for name in worker.THREAD_VARIABLES:
        monkeypatch.setenv(name, "4")
    monkeypatch.setattr(worker.cgroup, "memory_anon", lambda: 3 * GIB)
    monkeypatch.setattr(worker.cgroup, "memory_limit", lambda: 2 * GIB)
    monkeypatch.setattr(worker.cgroup, "cpu_quota", lambda: 1)
    return task_dir


def run_exec(monkeypatch: pytest.MonkeyPatch, *command: str) -> int | str | None:
    monkeypatch.setattr(sys, "argv", ["simrun-exec", *command])
    with pytest.raises(SystemExit) as raised:
        worker.exec_main()
    return raised.value.code


def python(code: str, *args: str) -> list[str]:
    return [sys.executable, "-c", code, *args]


def test_runs_the_command_and_reports(
    exec_env: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    installed: list[list[str]] = []
    restarts: list[str] = []
    monkeypatch.setattr(worker, "install_editable", installed.append)
    # the harness restarts after an install; here it goes on in the same process
    monkeypatch.setattr(worker, "restart", lambda: restarts.append("restart"))
    monkeypatch.setenv("SIMRUN_EDITABLE", "/store/snapshots/m/1,/store/snapshots/l/2")
    threads = tmp_path / "threads.json"
    code = (
        "import json, os, sys; "
        "names = ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'); "
        "json.dump([os.environ[n] for n in names], open(sys.argv[1], 'w'))"
    )
    assert run_exec(monkeypatch, "--", *python(code, str(threads))) == 0
    assert installed == [["/store/snapshots/m/1", "/store/snapshots/l/2"]]
    assert restarts == ["restart"]
    assert json.loads(threads.read_text()) == ["1", "1", "1"]
    # the report of the task, by its id, so the controller learns its memory
    report = json.loads((exec_env / "workers" / "report-1.json").read_text())
    assert report["worker"] == "report"
    assert report["processes"] == 1
    assert report["units_done"] == []
    assert report["peak_memory_bytes"] == 3 * GIB
    assert report["memory_limit_bytes"] == 2 * GIB
    assert report["per_worker_gb"] == 1.5


def test_failing_command_is_an_analysis_error(
    exec_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert run_exec(monkeypatch, *python("raise SystemExit(1)")) == 3
    assert (exec_env / "workers" / "report-1.json").is_file()


def test_killed_command_is_a_kill_for_memory(
    exec_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    counts = iter([0, 1])
    monkeypatch.setattr(worker.cgroup, "memory_oom_kills", lambda: next(counts))
    code = "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
    assert run_exec(monkeypatch, *python(code)) == 137
    # the evidence of the out of memory killer, which the controller reads
    report = json.loads((exec_env / "workers" / "report-1.json").read_text())
    assert report["oom_kills"] == 1


@pytest.mark.parametrize("command", [[], ["--"], ["/nonexistent/simrun-test-command"]])
def test_configuration_errors(
    exec_env: Path, monkeypatch: pytest.MonkeyPatch, command: list[str]
) -> None:
    assert run_exec(monkeypatch, *command) == 4


def test_without_task_dir(exec_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIMRUN_TASK_DIR")
    assert run_exec(monkeypatch, *python("pass")) == 4


def test_sigterm_reaches_the_command(tmp_path: Path) -> None:
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "report"
    task_dir.mkdir(parents=True)
    started = tmp_path / "started"
    stopped = tmp_path / "stopped"
    command = python(
        "import pathlib, signal, sys, time\n"
        "def stop(signum, frame):\n"
        "    pathlib.Path(sys.argv[2]).touch()\n"
        "    sys.exit(0)\n"
        "signal.signal(signal.SIGTERM, stop)\n"
        "pathlib.Path(sys.argv[1]).touch()\n"
        "time.sleep(60)\n",
        str(started),
        str(stopped),
    )
    env = {**os.environ, "SIMRUN_TASK_DIR": str(task_dir)}
    for name in ("SIMRUN_EDITABLE", "SIMRUN_STAGE", "SIMRUN_ATTEMPT"):
        env.pop(name, None)
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "from simrun.worker import exec_main; exec_main()",
            *command,
        ],
        env=env,
    )
    try:
        deadline = time.monotonic() + 30
        while not started.exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        # evicted: retried by the workflow, not done although the command exited 0
        assert process.wait(timeout=30) == 143
    finally:
        process.kill()
        process.wait()
    assert stopped.exists()
    assert (task_dir / "workers" / "report-0.json").is_file()


def test_the_memory_of_the_process_tree(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    code = (
        "import pathlib, sys, time; block = bytearray(200 * 2**20); "
        "pathlib.Path(sys.argv[1]).touch(); time.sleep(60)"
    )
    child = subprocess.Popen([sys.executable, "-c", code, str(ready)])
    try:
        deadline = time.monotonic() + 30
        while not ready.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        own = worker.process_tree_anon(child.pid)
        tree = worker.process_tree_anon(os.getpid())
        assert own is not None and own >= 200 * 2**20
        assert tree is not None and tree >= own
    finally:
        child.kill()
        child.wait()
    assert worker.process_tree_anon(child.pid) is None


def test_the_process_source_ignores_the_cgroup(
    exec_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the root cgroup of a laptop: the memory and the kills of the whole machine
    monkeypatch.setenv("SIMRUN_MEMORY_SOURCE", "process")
    monkeypatch.setenv("SIMRUN_MEMORY_LIMIT_GB", "4")
    counts = iter([0, 5])
    monkeypatch.setattr(worker.cgroup, "memory_oom_kills", lambda: next(counts))
    monkeypatch.setattr(worker.cgroup, "memory_anon", lambda: 64 * GIB)
    assert run_exec(monkeypatch, *python("pass")) == 0
    report = json.loads((exec_env / "workers" / "report-1.json").read_text())
    # the process tree of the harness, here the one of the tests
    assert 0 < report["peak_memory_bytes"] < 4 * GIB
    assert report["memory_limit_bytes"] == 4 * GIB
    assert report["oom_kills"] == 0


def test_an_unknown_memory_source_is_a_configuration_error(
    exec_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SIMRUN_MEMORY_SOURCE", "psi")
    assert run_exec(monkeypatch, *python("pass")) == 4
