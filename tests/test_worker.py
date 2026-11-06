import contextlib
import json
import os
import signal
import subprocess
import sys
import threading
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
        heartbeat_s=float(sys.argv[6]),
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
    heartbeat_s: float = 30.0,
    new_session: bool = False,
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
            str(heartbeat_s),
        ],
        start_new_session=new_session,
    )


def read_report(task_dir: Path, worker_id: str, attempt: int = 0) -> dict:
    path = task_dir / "workers" / f"{worker_id}-{attempt}.json"
    return json.loads(path.read_text())


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
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "squares"
    task_dir.mkdir(parents=True)
    monkeypatch.setenv("SIMRUN_TASK_DIR", str(task_dir))
    monkeypatch.setenv("SIMRUN_TASK_TYPE", "demo.squares")
    monkeypatch.setenv("SIMRUN_WORKER", "squares-w0")
    monkeypatch.setenv("SIMRUN_MEMORY_BASE_GB", "0.5")
    monkeypatch.setenv("SIMRUN_MEMORY_PER_WORKER_GB", "0.5")
    monkeypatch.setenv("SIMRUN_SCRATCH", str(tmp_path / "scratch"))
    for name in (
        "SIMRUN_STAGE",
        "SIMRUN_EDITABLE",
        "SIMRUN_INPUTS",
        "SIMRUN_ATTEMPT",
        "SIMRUN_ROLE",
        "SIMRUN_STALE_S",
    ):
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


def test_main_done(worker_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIMRUN_ATTEMPT", "2")
    (worker_env / "settings.json").write_text(SquaresSettings(n=5).model_dump_json())
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 0
    assert os.listdir(worker_env / "workers") == ["squares-w0-2.json"]
    report = read_report(worker_env, "squares-w0", attempt=2)
    assert sorted(report["units_done"]) == list(range(5))


def test_main_configuration_error(worker_env: Path) -> None:
    (worker_env / "settings.json").write_text(json.dumps({"n": "many"}))
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 4


def test_main_stages_inputs(worker_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = worker_env.parents[3]
    source = store / "runs" / "r1" / "tasks" / "mesh" / "out"
    source.mkdir(parents=True)
    (source / "mesh.txt").write_text("mesh")
    monkeypatch.setenv("SIMRUN_STAGE", "runs/r1/tasks/mesh/out=mesh")
    (worker_env / "settings.json").write_text(SquaresSettings(n=1).model_dump_json())
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 0
    scratch = Path(os.environ["SIMRUN_SCRATCH"])
    assert (scratch / "mesh" / "mesh.txt").read_text() == "mesh"


def test_paths_from_env(tmp_path: Path) -> None:
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "fit"
    paths = worker.paths_from_env(
        {
            "SIMRUN_TASK_DIR": str(task_dir),
            "SIMRUN_INPUTS": "model=runs/r1/tasks/model/out,mesh=/scratch/mesh",
            "SIMRUN_SCRATCH": "/scratch/r1",
        }
    )
    assert paths.store == tmp_path
    assert paths.run_dir == tmp_path / "runs" / "r1"
    assert paths.out == task_dir / "out"
    assert paths.inputs == {
        "model": tmp_path / "runs" / "r1" / "tasks" / "model" / "out",
        "mesh": Path("/scratch/mesh"),
    }
    assert paths.scratch == Path("/scratch/r1")


def test_install_editable_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def run(command: list[str], check: bool) -> None:
        assert check
        calls.append(command)

    monkeypatch.setattr(worker.subprocess, "run", run)
    monkeypatch.setattr(worker.site, "addsitedir", lambda directory: None)
    worker.install_editable([])
    assert calls == []
    snapshots = [tmp_path / "snapshots/a/1", tmp_path / "snapshots/b/2"]
    for snapshot in snapshots:
        (snapshot / "pkg").mkdir(parents=True)
        (snapshot / "pkg/__init__.py").write_text("")
    root = tmp_path / "sources"
    worker.install_editable([str(s) for s in snapshots], root=root)
    # every snapshot is installed from a writable tree of links to its files
    assert calls == [
        [
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--no-deps",
            "--no-index",
            "--no-build-isolation",
            "-e",
            str(root / "0-1"),
            "-e",
            str(root / "1-2"),
        ]
    ]
    link = root / "0-1/pkg/__init__.py"
    assert link.is_symlink() and link.resolve() == snapshots[0] / "pkg/__init__.py"


HATCH_PROJECT = """
[project]
name = "hlib"
version = "0.1"
[project.entry-points."simrun.task_types"]
x = "hlib:X"
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
"""

SETUPTOOLS_PROJECT = """
[project]
name = "slib"
version = "0.1"
dependencies = ["packaging"]
[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"
[tool.setuptools.packages.find]
where = ["."]
"""


def _files(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*"))


def test_install_read_only_snapshots(tmp_path: Path) -> None:
    # as in a pod: the snapshots are read-only, the image has the build backends,
    # no index; offline from the cache of uv
    venv = tmp_path / "venv"
    env = {**os.environ, "UV_OFFLINE": "1"}
    subprocess.run(["uv", "venv", "-q", str(venv)], check=True, env=env)
    python = str(venv / "bin" / "python")
    backends = ["hatchling", "editables", "setuptools", "packaging"]
    install = ["uv", "pip", "install", "-q", "--python", python, *backends]
    if subprocess.run(install, env=env, check=False).returncode != 0:
        pytest.skip("the build backends are not in the cache of uv")
    snapshots = tmp_path / "snapshots"
    hlib = snapshots / "hlib/1"
    (hlib / "src/hlib").mkdir(parents=True)
    (hlib / "pyproject.toml").write_text(HATCH_PROJECT)
    (hlib / "src/hlib/__init__.py").write_text("X = 1\n")
    slib = snapshots / "slib/2"
    (slib / "slib").mkdir(parents=True)
    (slib / "pyproject.toml").write_text(SETUPTOOLS_PROJECT)
    (slib / "slib/__init__.py").write_text("Y = 2\n")
    before = _files(snapshots)
    for path in [snapshots, *snapshots.rglob("*")]:
        path.chmod(path.stat().st_mode & ~0o222)
    try:
        worker.install_editable(
            [str(hlib), str(slib)], python=python, root=tmp_path / "sources"
        )
        assert _files(snapshots) == before
    finally:
        for path in [snapshots, *snapshots.rglob("*")]:
            path.chmod(path.stat().st_mode | 0o200)
    code = (
        "import hlib, slib; from importlib.metadata import entry_points; "
        "assert (hlib.X, slib.Y) == (1, 2); "
        "assert [e.value for e in entry_points(group='simrun.task_types')] == "
        "['hlib:X']"
    )
    subprocess.run([python, "-P", "-c", code], check=True, cwd=tmp_path)


def test_merge_incomplete_raises(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=3)
    ledger = Ledger.create(task_dir / "units", 3)
    index = ledger.claim("w0")
    assert index is not None
    ledger.complete(index, "w0")
    with pytest.raises(RuntimeError, match="1, 2"):
        run_merge(SQUARES, settings, task_paths(task_dir))


MAIN = """
import sys
sys.path.insert(0, sys.argv[1])
import simrun.worker as worker
if sys.argv[2] != "-":
    from plugin import TYPES
    worker.load_task_types = lambda: TYPES
worker.main()
"""

PLUGIN = """
from simrun.demo import SquaresSettings
from simrun.plugins import TaskType


def setup(settings, paths):
    raise RuntimeError("setup failed on request")


TYPES = {
    "failing.setup": TaskType(
        name="failing.setup",
        settings=SquaresSettings,
        count_units="simrun.demo:count_units",
        setup="plugin:setup",
        compute_unit="simrun.demo:compute_unit",
        merge="simrun.demo:merge",
    )
}
"""


def start_main(
    tmp_path: Path,
    settings: SquaresSettings,
    task_type: str = "demo.squares",
    new_session: bool = False,
    extra_env: dict[str, str] | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    """Run `main` in a subprocess; a task type other than the demo is from PLUGIN."""
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "squares"
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / "settings.json").write_text(settings.model_dump_json())
    plugin_dir = tmp_path / "plugin"
    plugin_dir.mkdir()
    (plugin_dir / "plugin.py").write_text(PLUGIN)
    env = {
        **os.environ,
        "SIMRUN_TASK_DIR": str(task_dir),
        "SIMRUN_TASK_TYPE": task_type,
        "SIMRUN_WORKER": "w0",
        "SIMRUN_MEMORY_BASE_GB": "0.5",
        "SIMRUN_MEMORY_PER_WORKER_GB": "0.5",
        "SIMRUN_SCRATCH": str(tmp_path / "scratch"),
        # the processes of the pool import the plugin as well
        "PYTHONPATH": os.pathsep.join(
            filter(None, [str(plugin_dir), os.environ.get("PYTHONPATH")])
        ),
    }
    for name in (
        "SIMRUN_STAGE",
        "SIMRUN_EDITABLE",
        "SIMRUN_INPUTS",
        "SIMRUN_ATTEMPT",
        "SIMRUN_ROLE",
        "SIMRUN_STALE_S",
    ):
        env.pop(name, None)
    env.update(extra_env or {})
    plugin = "-" if task_type == "demo.squares" else "plugin"
    process = subprocess.Popen(
        [sys.executable, "-c", MAIN, str(plugin_dir), plugin],
        env=env,
        start_new_session=new_session,
    )
    return process, task_dir


def children(pid: int) -> set[int]:
    """The child processes of `pid`, from the parents in /proc."""
    found: set[int] = set()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            stat = Path(f"/proc/{entry}/stat").read_text()
        except OSError:
            continue
        if int(stat.rpartition(")")[2].split()[1]) == pid:
            found.add(int(entry))
    return found


def kill_tree(process: subprocess.Popen[bytes], pool: set[int] | None = None) -> None:
    """Kill a process, every process below it and the pool processes given."""
    tree: set[int] = set(pool or ())
    frontier = {process.pid}
    while frontier:
        frontier = {c for p in frontier for c in children(p)}
        tree |= frontier
    process.kill()
    process.wait()
    for pid in tree:
        if alive(pid):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


def ignores_sigint(pid: int) -> bool:
    """Whether a process ignores SIGINT, as the initializer of the pool sets."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except OSError:
        return False
    mask = next(line for line in status.splitlines() if line.startswith("SigIgn:"))
    return bool(int(mask.split()[1], 16) & (1 << (signal.SIGINT - 1)))


def alive(pid: int) -> bool:
    """Whether a process runs, a zombie counting as ended."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rpartition(")")[2].split()[0] != "Z"


def wait_for_pool(process: subprocess.Popen[bytes], claimed: Path) -> set[int]:
    """Wait until every claimed unit runs in a pool process past its initializer.

    Returns:
        The pool processes, children of the forkserver, a child of the worker.
    """
    deadline = time.monotonic() + 60
    while True:
        assert process.poll() is None
        assert time.monotonic() < deadline
        time.sleep(0.05)
        if not (claimed.exists() and os.listdir(claimed)):
            continue
        running = {p for c in children(process.pid) for p in children(c)}
        if len(running) == len(os.listdir(claimed)) and all(
            ignores_sigint(p) for p in running
        ):
            return running


def wait_ended(pids: set[int]) -> None:
    deadline = time.monotonic() + 10
    while any(alive(p) for p in pids):
        assert time.monotonic() < deadline, [p for p in pids if alive(p)]
        time.sleep(0.05)


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="reads /proc")
def test_sigterm_to_the_process_group_keeps_the_grace(tmp_path: Path) -> None:
    process, task_dir = start_main(
        tmp_path, SquaresSettings(n=20, sleep_s=1.0), new_session=True
    )
    claimed = task_dir / "units" / "claimed"
    pool: set[int] = set()
    try:
        pool = wait_for_pool(process, claimed)
        running = {int(name.partition("@")[0]) for name in os.listdir(claimed)}
        # as a container runtime or Argo's emissary signal: the whole group
        os.killpg(process.pid, signal.SIGTERM)
        assert process.wait(timeout=30) == 143
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        kill_tree(process, pool)
    done = {int(name) for name in os.listdir(task_dir / "units" / "done")}
    assert running <= done
    assert os.listdir(claimed) == []
    assert set(read_report(task_dir, "w0")["units_done"]) == done
    wait_ended(pool)


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="reads /proc")
def test_killed_pool_process_exits_137(tmp_path: Path) -> None:
    process, task_dir = start_main(tmp_path, SquaresSettings(n=20, sleep_s=1.0))
    claimed = task_dir / "units" / "claimed"
    pool: set[int] = set()
    try:
        pool = wait_for_pool(process, claimed)
        os.kill(min(pool), signal.SIGKILL)
        assert process.wait(timeout=30) == 137
    finally:
        kill_tree(process, pool)
    assert os.listdir(claimed) == []
    assert read_report(task_dir, "w0")["worker"] == "w0"
    wait_ended(pool)


def test_setup_error_exit_code(tmp_path: Path) -> None:
    process, task_dir = start_main(
        tmp_path, SquaresSettings(n=4), task_type="failing.setup"
    )
    try:
        assert process.wait(timeout=60) == 3
    finally:
        kill_tree(process)
    assert os.listdir(task_dir / "units" / "claimed") == []
    assert len(os.listdir(task_dir / "units" / "pending")) == 4
    assert read_report(task_dir, "w0")["units_done"] == []


def test_stopped_before_the_first_claim(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    stop = threading.Event()
    stop.set()
    with pytest.raises(worker.Terminated):
        run_worker(
            SQUARES,
            SquaresSettings(n=5),
            task_paths(task_dir),
            "w0",
            cores=2,
            memory_bytes=None,
            base_gb=0.5,
            per_worker_gb=0.5,
            stop=stop,
        )
    assert len(os.listdir(task_dir / "units" / "pending")) == 5
    assert read_report(task_dir, "w0")["units_done"] == []


def sigterm_to_self(snapshots: list[str]) -> None:
    os.kill(os.getpid(), signal.SIGTERM)


def test_sigterm_during_preparation_exits_143(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (worker_env / "settings.json").write_text(SquaresSettings(n=5).model_dump_json())
    monkeypatch.setattr(worker, "install_editable", sigterm_to_self)
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 143
    assert not (worker_env / "units" / "done").exists() or not os.listdir(
        worker_env / "units" / "done"
    )
    with pytest.raises(SystemExit) as raised:
        worker.merge_main()
    assert raised.value.code == 143
    assert not (worker_env / "out" / "summary.json").exists()
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_DFL


def coordinate(task_dir: Path, settings: SquaresSettings, stale_s: float) -> dict:
    """Run the coordinator `w0` in this process; returns its report."""
    run_worker(
        SQUARES,
        settings,
        task_paths(task_dir),
        "w0",
        cores=2,
        memory_bytes=None,
        base_gb=0.5,
        per_worker_gb=0.5,
        coordinator=True,
        stale_s=stale_s,
        heartbeat_s=0.1,
        poll_s=0.1,
    )
    return read_report(task_dir, "w0")


def wait_for_claims(
    process: subprocess.Popen[bytes], claimed: Path, worker_id: str, n: int
) -> set[int]:
    """Wait until `worker_id` holds `n` claims; returns their units."""
    deadline = time.monotonic() + 60
    while True:
        assert process.poll() is None
        assert time.monotonic() < deadline
        names = os.listdir(claimed) if claimed.exists() else []
        units = {
            int(i) for i, _, w in (n.partition("@") for n in names) if w == worker_id
        }
        if len(units) >= n:
            return units
        time.sleep(0.05)


def test_coordinator_recomputes_the_units_of_a_dead_worker(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=6, sleep_s=0.3)
    process = start_worker(
        task_dir, "w1", settings, cores=1, heartbeat_s=0.1, new_session=True
    )
    try:
        held = wait_for_claims(process, task_dir / "units" / "claimed", "w1", 1)
        # the pod dies at once: no release, no report, its heartbeat stops
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=30)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        kill_tree(process)
    assert held <= {
        int(n.partition("@")[0]) for n in os.listdir(task_dir / "units" / "claimed")
    }
    start = time.monotonic()
    report = coordinate(task_dir, settings, stale_s=1.0)
    assert Ledger(task_dir / "units", 6).complete_ledger()
    assert held <= set(report["units_done"])
    assert time.monotonic() - start >= 0.5
    run_merge(SQUARES, settings, task_paths(task_dir))
    summary = json.loads((task_dir / "out" / "summary.json").read_text())
    assert summary["sum"] == sum(i * i for i in range(6))


def test_coordinator_waits_for_a_live_worker(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    settings = SquaresSettings(n=4, sleep_s=1.5)
    process = start_worker(task_dir, "w1", settings, cores=4, heartbeat_s=0.1)
    try:
        wait_for_claims(process, task_dir / "units" / "claimed", "w1", 4)
        # stale far below the duration of a unit: only the heartbeats of the thread
        # keep the claims of the slow worker
        report = coordinate(task_dir, settings, stale_s=0.5)
        assert process.wait(timeout=30) == 0
    finally:
        kill_tree(process)
    assert report["units_done"] == []
    assert Ledger(task_dir / "units", 4).complete_ledger()
    assert sorted(read_report(task_dir, "w1")["units_done"]) == [0, 1, 2, 3]


def test_extra_worker_returns_while_others_hold_claims(tmp_path: Path) -> None:
    task_dir = tmp_path / "squares"
    ledger = Ledger.create(task_dir / "units", 3)
    assert ledger.claim("other") == 0
    report = run_worker(
        SQUARES,
        SquaresSettings(n=3),
        task_paths(task_dir),
        "w1",
        cores=2,
        memory_bytes=None,
        base_gb=0.5,
        per_worker_gb=0.5,
    )
    assert report.units_done == [1, 2]
    assert os.listdir(task_dir / "units" / "claimed") == ["0@other"]
    assert (task_dir / "units" / "heartbeats" / "w1").exists()


def test_main_reads_the_role(worker_env: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict] = []

    def fake_run_worker(*args: object, **kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(worker, "run_worker", fake_run_worker)
    (worker_env / "settings.json").write_text(SquaresSettings(n=1).model_dump_json())
    for role, stale, expected in (
        ("base", "90", (True, 90.0)),
        ("extra", None, (False, None)),
        (None, None, (False, None)),
    ):
        if role is None:
            monkeypatch.delenv("SIMRUN_ROLE", raising=False)
        else:
            monkeypatch.setenv("SIMRUN_ROLE", role)
        if stale is None:
            monkeypatch.delenv("SIMRUN_STALE_S", raising=False)
        else:
            monkeypatch.setenv("SIMRUN_STALE_S", stale)
        with pytest.raises(SystemExit) as raised:
            worker.main()
        assert raised.value.code == 0
        assert (calls[-1]["coordinator"], calls[-1]["stale_s"]) == expected
    monkeypatch.setenv("SIMRUN_ROLE", "boss")
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 4


def test_idle_coordinator_stops_on_sigterm(tmp_path: Path) -> None:
    task_dir = tmp_path / "runs" / "r1" / "tasks" / "squares"
    ledger = Ledger.create(task_dir / "units", 2)
    assert [ledger.claim("other"), ledger.claim("other")] == [0, 1]
    ledger.heartbeat("other")
    process, _ = start_main(
        tmp_path,
        SquaresSettings(n=2),
        extra_env={"SIMRUN_ROLE": "base", "SIMRUN_STALE_S": "600"},
    )
    try:
        deadline = time.monotonic() + 60
        while not (task_dir / "units" / "heartbeats" / "w0").exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.05)
        time.sleep(0.2)
        assert process.poll() is None
        start = time.monotonic()
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=30) == 143
        assert time.monotonic() - start < 2.0
    finally:
        kill_tree(process)
    assert sorted(os.listdir(task_dir / "units" / "claimed")) == ["0@other", "1@other"]


def test_merge_main_writes_marker(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (worker_env / "settings.json").write_text(SquaresSettings(n=2).model_dump_json())
    monkeypatch.setattr(worker, "install_editable", lambda snapshots: None)
    marker = worker_env / "out" / worker.MERGED_MARKER
    with pytest.raises(SystemExit) as raised:
        worker.merge_main()  # the ledger is incomplete
    assert raised.value.code == 3
    assert not marker.exists()
    monkeypatch.setattr(worker, "run_merge", lambda *args: None)
    with pytest.raises(SystemExit) as raised:
        worker.merge_main()
    assert raised.value.code == 0
    assert marker.exists()


def test_report_carries_the_peak_of_the_sampled_anonymous_memory(
    worker_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # the samples of `memory.stat` `anon`: the report keeps their maximum, not the
    # `memory.peak` of the cgroup, which counts the page cache
    samples = iter([1 * 2**30, 3 * 2**30])
    monkeypatch.setattr(worker.cgroup, "memory_anon", lambda: next(samples, 2**30))
    monkeypatch.setattr(worker, "SAMPLE_S", 0.01)
    (worker_env / "settings.json").write_text(
        SquaresSettings(n=4, sleep_s=0.05).model_dump_json()
    )
    with pytest.raises(SystemExit) as raised:
        worker.main()
    assert raised.value.code == 0
    assert read_report(worker_env, "squares-w0")["peak_memory_bytes"] == 3 * 2**30


def test_sampler_keeps_the_maximum(monkeypatch: pytest.MonkeyPatch) -> None:
    samples = iter([5, None, 9, 2])
    monkeypatch.setattr(worker.cgroup, "memory_anon", lambda: next(samples))
    sampler = worker.MemorySampler()
    assert sampler.peak is None
    for _ in range(4):
        sampler.sample()
    assert sampler.peak == 9
