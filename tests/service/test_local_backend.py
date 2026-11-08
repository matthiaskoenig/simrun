"""The local backend: the tasks of a run as processes on this machine."""

import dataclasses
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from simrun import worker
from simrun.argo import RETRY_LIMIT
from simrun.config import ClusterConfig
from simrun.planner import TaskPlan, plan_task
from simrun.plugins import TaskType, load_task_types
from simrun.resources import ResolvedResources
from simrun.service import local_backend
from simrun.service.backend import FailedAttempt
from simrun.service.local_backend import LocalBackend
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.worker import MERGED_MARKER, PROVENANCE_FILE, SETTINGS_FILE
from simrun.workflow import Priority, SplitMode, Workflow

PYTHON = sys.executable
TYPES = load_task_types()
DEADLINE_S = 30.0
CLUSTER = ClusterConfig(registry="localhost:5000")


@pytest.fixture
def backends(tmp_path: Path) -> Iterator[Callable[..., LocalBackend]]:
    """Local backends on a store in `tmp_path`, every run cancelled at the end."""
    made: list[LocalBackend] = []

    def make(**kwargs: Any) -> LocalBackend:
        backend = LocalBackend(tmp_path / "store", tmp_path / "scratch", **kwargs)
        made.append(backend)
        return backend

    yield make
    for backend in made:
        backend.close()


def _submit(
    backend: LocalBackend, tasks: list[dict[str, Any]], **record: Any
) -> tuple[RunRecord, str]:
    """Write a run of the tasks into the store of the backend and submit it."""
    store = RunStore(backend.store)
    workflow = Workflow(name="local", tasks=tasks)
    run = RunRecord(
        id=store.new_run_id(),
        submitted_by="mk",
        owner="mk",
        agent=None,
        priority=Priority.OWNER,
        group=None,
        workflow=workflow,
        sources=[],
        image=None,
        reserve=None,
        state=RunState.SUBMITTING,
        tasks=dict.fromkeys((t.id for t in workflow.tasks), TaskState.WAITING),
        plans={},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
        **record,
    )
    plans = {task.id: _plan(backend, task) for task in workflow.tasks}
    run = run.model_copy(
        update={"plans": {k: dataclasses.asdict(p) for k, p in plans.items()}}
    )
    store.write(run)
    for task in workflow.tasks:
        task_dir = store.run_dir(run.id) / "tasks" / task.id
        task_dir.mkdir(parents=True, exist_ok=True)
        (task_dir / SETTINGS_FILE).write_text(json.dumps(task.settings))
        (task_dir / PROVENANCE_FILE).write_text("[]")
    return run, backend.submit(run, plans, TYPES, CLUSTER)


def _plan(backend: LocalBackend, task: Any) -> TaskPlan:
    task_type: TaskType = TYPES[task.type]
    hint = task_type.hint
    resources = ResolvedResources(
        memory_base_gb=hint.memory_base_gb,
        memory_per_worker_gb=hint.memory_per_worker_gb,
        scratch_gb=0.0,
        gpus=0,
        seconds_per_unit=None,
        samples=0,
        source="plugin hint",
    )
    units = None
    if task_type.splittable:
        units = task.settings["n"]
    return plan_task(
        task,
        task_type,
        resources,
        backend.nodes(),
        SplitMode.FASTEST,
        Priority.OWNER,
        units,
    )


def _wait(
    backend: LocalBackend, name: str, until: Callable[[Any], bool] | None = None
) -> Any:
    """Poll the status of a run until `until` holds, by default until it ended."""
    deadline = time.monotonic() + DEADLINE_S
    while True:
        status = backend.status(name)
        if (until or (lambda s: s.phase in ("Succeeded", "Failed")))(status):
            return status
        assert time.monotonic() < deadline, status
        time.sleep(0.02)


def _command(task_id: str, code: str, *args: str, cores: int = 1) -> dict[str, Any]:
    return {
        "id": task_id,
        "type": "command",
        "settings": {
            "image": "localhost:5000/tools:1",
            "args": [PYTHON, "-c", code, *args],
            "cores": cores,
            "memory_gb": 0.5,
        },
    }


def _log(backend: LocalBackend, name: str, task_id: str) -> str:
    return "".join(log.text for log in backend.logs(name, task_id))


def _processes_of(directory: Path) -> list[int]:
    """The processes whose environment names a path below `directory`."""
    found: list[int] = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            environ = Path(f"/proc/{entry}/environ").read_bytes()
        except OSError:
            continue
        if str(directory).encode() in environ:
            found.append(int(entry))
    return found


def test_a_split_task_then_a_command_run_to_their_end(backends, tmp_path):
    backend = backends(cores=2)
    squares = {"id": "squares", "type": "demo.squares", "settings": {"n": 20}}
    total = _command("sum", "import sys; print(sys.argv)", "{input}") | {
        "inputs": {"squares": "squares.squares"}
    }
    run, name = _submit(backend, [squares, total])
    assert name == f"simrun-{run.id}"
    status = _wait(backend, name)
    assert status.phase == "Succeeded", status
    assert status.tasks == {"squares-w0": "Succeeded", "sum": "Succeeded"}
    assert status.failed == {}
    task_dir = tmp_path / "store" / "runs" / run.id / "tasks"
    assert (task_dir / "squares" / "out" / MERGED_MARKER).is_file()
    out = task_dir / "squares" / "out" / "squares"
    assert f"squares={out}" in _log(backend, name, "sum")
    # the coordinator ran with the cores of its plan and wrote its report
    report = json.loads(
        (task_dir / "squares" / "workers" / "squares-w0-0.json").read_text()
    )
    assert report["processes"] == 2
    logs = backend.logs(name, "squares")
    assert [(log.node, log.done) for log in logs] == [("squares-w0", True)]
    assert backend.placements() == []


def test_a_failed_task_omits_its_dependants(backends):
    backend = backends(cores=2)
    squares = {
        "id": "squares",
        "type": "demo.squares",
        "settings": {"n": 6, "fail_at": 3},
    }
    total = _command("sum", "print('sum')", "{input}") | {
        "inputs": {"squares": "squares.squares"}
    }
    other = _command("other", "print('other')")
    _, name = _submit(backend, [squares, total, other])
    status = _wait(backend, name)
    assert status.phase == "Failed"
    assert status.tasks == {
        "squares-w0": "Failed",
        "sum": "Omitted",
        "other": "Succeeded",
    }
    assert status.failed == {
        "squares-w0": [FailedAttempt(attempt=0, exit_code=worker.EXIT_ANALYSIS)]
    }


def test_cancel_ends_every_process_of_the_run(backends, tmp_path):
    backend = backends(cores=2)
    squares = {
        "id": "squares",
        "type": "demo.squares",
        "settings": {"n": 4, "sleep_s": 1.0},
    }
    after = _command("after", "print('after')", "{input}") | {
        "inputs": {"squares": "squares.squares"}
    }
    run, name = _submit(backend, [squares, after])
    claimed = tmp_path / "store" / "runs" / run.id / "tasks" / "squares" / "units"
    _wait(
        backend,
        name,
        lambda s: (
            s.tasks.get("squares-w0") == "Running"
            and (claimed / "claimed").is_dir()
            and bool(os.listdir(claimed / "claimed"))
        ),
    )
    processes = _processes_of(tmp_path / "store")
    assert processes
    groups = {os.getpgid(pid) for pid in processes}
    backend.cancel(name)
    assert backend.status(name).phase == "Failed"
    deadline = time.monotonic() + DEADLINE_S
    for group in groups:
        while True:
            try:
                os.killpg(group, 0)
            except ProcessLookupError:
                break
            assert time.monotonic() < deadline, f"process group {group} is alive"
            time.sleep(0.05)
    status = _wait(backend, name, lambda s: s.tasks.get("squares-w0") == "Failed")
    assert status.phase == "Failed"
    assert status.tasks == {"squares-w0": "Failed", "after": "Omitted"}
    assert backend.placements() == []
    assert _processes_of(tmp_path / "store") == []


def test_cancel_kills_a_process_after_the_grace(backends, monkeypatch):
    monkeypatch.setattr(worker, "GRACE_S", 0.2)
    backend = backends(cores=1)
    stubborn = _command(
        "stubborn",
        "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print('ready', flush=True); time.sleep(30)",
    )
    _, name = _submit(backend, [stubborn])
    _wait(backend, name, lambda s: "ready" in _log(backend, name, "stubborn"))
    backend.stop(name)
    status = _wait(backend, name, lambda s: s.tasks["stubborn"] == "Failed")
    assert status.failed == {
        "stubborn": [FailedAttempt(attempt=0, exit_code=128 + signal.SIGKILL)]
    }


def test_tasks_of_two_runs_share_the_cores(backends):
    backend = backends(cores=2)
    code = "import time; time.sleep(0.5)"
    _, first = _submit(backend, [_command("big", code, cores=2)])
    _, second = _submit(backend, [_command("big", code, cores=2)])
    seen_waiting = False
    deadline = time.monotonic() + DEADLINE_S
    while True:
        a, b = backend.status(first), backend.status(second)
        assert not (a.tasks.get("big") == "Running" == b.tasks.get("big"))
        if a.tasks.get("big") == "Running" and b.tasks.get("big") == "Pending":
            seen_waiting = True
        if a.phase == "Succeeded" and b.phase == "Succeeded":
            break
        assert time.monotonic() < deadline, (a, b)
        time.sleep(0.01)
    assert seen_waiting


def test_a_task_waits_for_its_dependencies_without_a_phase(backends):
    backend = backends(cores=1)
    first = _command("first", "import time; time.sleep(0.5)")
    second = _command("second", "print('second')", "{input}") | {
        "inputs": {"x": "first.x"}
    }
    _, name = _submit(backend, [first, second])
    status = backend.status(name)
    assert status.phase == "Running"
    assert "second" not in status.tasks
    assert _wait(backend, name).phase == "Succeeded"


def test_a_kill_is_retried_with_the_retry_memory(backends):
    backend = backends(cores=1)
    code = (
        "import os, signal; "
        "print('memory', os.environ['SIMRUN_MEMORY_PER_WORKER_GB'], flush=True); "
        "os.environ['SIMRUN_ATTEMPT'] == '0' and os.kill(os.getpid(), signal.SIGKILL)"
    )
    run, name = _submit(backend, [_command("killed", code)])
    status = _wait(backend, name)
    assert status.phase == "Succeeded"
    assert status.failed == {
        "killed": [FailedAttempt(attempt=0, exit_code=128 + signal.SIGKILL)]
    }
    logs = backend.logs(name, "killed")
    assert [log.node for log in logs] == ["killed", "killed"]
    shape = run.plans["killed"]["shape"]
    assert f"memory {float(shape['per_worker_gb'])}" in logs[0].text
    assert f"memory {float(shape['retry_per_worker_gb'])}" in logs[1].text
    assert RETRY_LIMIT > 1


def test_another_exit_fails_the_node(backends):
    backend = backends(cores=1)
    _, name = _submit(backend, [_command("bad", "raise SystemExit(5)")])
    status = _wait(backend, name)
    assert status.phase == "Failed"
    assert status.tasks == {"bad": "Failed"}
    assert status.failed == {"bad": [FailedAttempt(attempt=0, exit_code=5)]}


def test_docker_missing_fails_the_command_with_a_message(
    backends, tmp_path, monkeypatch
):
    empty = tmp_path / "bin"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    backend = backends(cores=1, commands="docker")
    _, name = _submit(backend, [_command("boxed", "print('boxed')")])
    status = _wait(backend, name)
    assert status.phase == "Failed"
    assert status.tasks == {"boxed": "Failed"}
    assert status.failed == {
        "boxed": [FailedAttempt(attempt=0, exit_code=worker.EXIT_CONFIGURATION)]
    }
    assert "docker is not installed" in _log(backend, name, "boxed")


def test_the_run_chooses_docker(backends, tmp_path, monkeypatch):
    """The commands of the run win over the ones of the backend; a fake docker."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(f"#!{PYTHON}\nimport sys\nprint('docker', sys.argv[1:])\n")
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    backend = backends(cores=1)
    _, name = _submit(
        backend, [_command("boxed", "print('boxed')")], local_commands="docker"
    )
    assert _wait(backend, name).phase == "Succeeded"
    text = _log(backend, name, "boxed")
    store, scratch = tmp_path / "store", tmp_path / "scratch"
    expected = [
        "run",
        "--rm",
        "-v",
        f"{store}:{store}",
        "-v",
        f"{scratch}:{scratch}",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "localhost:5000/tools:1",
        PYTHON,
        "-c",
        "print('boxed')",
    ]
    assert f"docker {expected}" in text


def test_the_callback_follows_the_end_of_a_process(backends):
    backend = backends(cores=1)
    seen: list[dict[str, str]] = []
    names: list[str] = []
    ended = threading.Event()

    def callback() -> None:
        # the callback may call the backend: it is not called under its lock
        if names:
            tasks = backend.status(names[0]).tasks
            seen.append(tasks)
            if tasks.get("quick") == "Succeeded":
                ended.set()

    backend.subscribe(callback)
    _, name = _submit(backend, [_command("quick", "pass")])
    names.append(name)
    assert ended.wait(DEADLINE_S), seen
    assert seen[-1] == {"quick": "Succeeded"}


def test_the_machine_is_the_one_node(backends):
    backend = backends(cores=3, memory_gb=7.5)
    [node] = backend.nodes()
    assert (node.cores, node.memory_gb) == (3, 7.5)
    assert backend.runs_images is False
    assert backend.has_image("anything")
    for call in (
        lambda: backend.reserve(node.name, "r"),
        lambda: backend.unreserve(node.name, "r"),
        lambda: backend.build_image(Path("."), "ref"),
    ):
        with pytest.raises(RuntimeError, match="not on the local backend"):
            call()
    with pytest.raises(KeyError):
        backend.status("simrun-unknown")
    default = LocalBackend(Path("/nonexistent"), Path("/nonexistent"))
    [machine] = default.nodes()
    assert machine.cores == os.process_cpu_count()
    assert machine.memory_gb > 0


def test_placements_of_the_running_processes(backends):
    backend = backends(cores=2)
    code = "print('ready', flush=True); import time; time.sleep(30)"
    run, name = _submit(backend, [_command("slow", code, cores=2)])
    _wait(backend, name, lambda s: "ready" in _log(backend, name, "slow"))
    [placement] = backend.placements()
    assert placement.workflow == name
    assert placement.dag_task == "slow"
    assert placement.phase == "Running"
    assert placement.cores == 2
    assert placement.memory_gb == run.plans["slow"]["shape"]["memory_gb"]
    assert placement.attempt == 0
    [log] = backend.logs(name, "slow", tail_lines=1)
    assert log.text == "ready\n"
    assert not log.done


def test_backend_reads_the_settings(tmp_path, monkeypatch):
    config = tmp_path / "simrun.yaml"
    config.write_text(
        f"store: {tmp_path / 'store'}\n"
        f"db: {tmp_path / 'simrun.db'}\n"
        f"tokens: {tmp_path / 'tokens.yaml'}\n"
        "owner: mk\n"
        "cluster: {registry: localhost:5000}\n"
    )
    monkeypatch.setenv("SIMRUN_CONFIG", str(config))
    backend = local_backend.backend()
    assert backend.store == tmp_path / "store"
    assert backend.scratch == tmp_path / "scratch"
