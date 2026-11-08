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
    record.setdefault("id", store.new_run_id())
    run = RunRecord(
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
    # stopped on SIGTERM, which is retried, but not in a cancelled run
    assert status.failed == {
        "squares-w0": [FailedAttempt(attempt=0, exit_code=worker.EXIT_TERMINATED)]
    }
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
        # one consistent view of both runs: the lock of the backend is reentrant
        with backend._cond:
            a, b = backend.status(first), backend.status(second)
            cores = sum(p.cores for p in backend.placements())
        assert cores <= 2
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
        "print('memory', os.environ['SIMRUN_MEMORY_PER_WORKER_GB'], "
        "os.environ['SIMRUN_MEMORY_LIMIT_GB'], flush=True); "
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
    first = f"memory {float(shape['per_worker_gb'])} {float(shape['memory_gb'])}"
    retry = (
        f"memory {float(shape['retry_per_worker_gb'])} "
        f"{float(shape['retry_memory_gb'])}"
    )
    assert first in logs[0].text
    assert retry in logs[1].text
    assert RETRY_LIMIT > 1


def test_a_stop_from_outside_is_retried_with_the_first_memory(backends):
    backend = backends(cores=1)
    code = (
        "import os, signal; "
        "print('memory', os.environ['SIMRUN_MEMORY_PER_WORKER_GB'], flush=True); "
        "os.environ['SIMRUN_ATTEMPT'] == '0' and os.kill(os.getpid(), signal.SIGTERM)"
    )
    run, name = _submit(backend, [_command("stopped", code)])
    status = _wait(backend, name)
    assert status.phase == "Succeeded"
    assert status.failed == {
        "stopped": [FailedAttempt(attempt=0, exit_code=worker.EXIT_TERMINATED)]
    }
    logs = backend.logs(name, "stopped")
    per_worker = float(run.plans["stopped"]["shape"]["per_worker_gb"])
    assert [f"memory {per_worker}" in log.text for log in logs] == [True, True]


def test_a_failed_start_fails_the_node_and_frees_its_cores(backends):
    backend = backends(cores=1)
    run_id = RunStore(backend.store).new_run_id()
    # the logs of the task cannot be created: its directory is a file
    logs = backend.store / "runs" / run_id / "tasks" / "broken" / "logs"
    logs.parent.mkdir(parents=True)
    logs.write_text("no directory")
    _, name = _submit(backend, [_command("broken", "pass")], id=run_id)
    status = _wait(backend, name)
    assert status.tasks == {"broken": "Failed"}
    assert status.failed == {
        "broken": [FailedAttempt(attempt=0, exit_code=worker.EXIT_CONFIGURATION)]
    }
    # the cores are free again: a later run starts
    _, later = _submit(backend, [_command("later", "pass")])
    assert _wait(backend, later).phase == "Succeeded"


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


FAKE_DOCKER = """#!{python}
import json, sys, time
with open({calls!r}, "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
print("docker", sys.argv[1:], flush=True)
if sys.argv[1] == "run" and "localhost:5000/slow:1" in sys.argv:
    time.sleep(30)
"""


@pytest.fixture
def docker_calls(tmp_path, monkeypatch) -> Path:
    """A fake docker on the PATH which records its calls, one JSON list per line."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "docker-calls.jsonl"
    docker = bin_dir / "docker"
    docker.write_text(FAKE_DOCKER.format(python=PYTHON, calls=str(calls)))
    docker.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    return calls


def _calls(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_the_run_chooses_docker(backends, tmp_path, docker_calls):
    """The commands of the run win over the ones of the backend."""
    backend = backends(cores=1)
    run, name = _submit(
        backend, [_command("boxed", "print('boxed')")], local_commands="docker"
    )
    assert _wait(backend, name).phase == "Succeeded"
    [call] = _calls(docker_calls)
    store, scratch = tmp_path / "store", tmp_path / "scratch"
    assert call[:12] == [
        "run",
        "--rm",
        "--init",
        "--name",
        f"simrun-{run.id}-boxed-0",
        "-v",
        f"{store}:{store}",
        "-v",
        f"{scratch}:{scratch}",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "-e",
    ]
    assert call[-4:] == ["localhost:5000/tools:1", PYTHON, "-c", "print('boxed')"]
    options = call[11:-4]
    assert options[::2] == ["-e"] * (len(options) // 2)
    env = dict(value.split("=", 1) for value in options[1::2])
    task_dir = store / "runs" / run.id / "tasks" / "boxed"
    assert env["SIMRUN_TASK_DIR"] == str(task_dir)
    assert env["SIMRUN_ATTEMPT"] == "0"
    assert env["SIMRUN_MEMORY_LIMIT_GB"] == "1.0"
    assert all(key.startswith("SIMRUN_") for key in env)
    assert "boxed" in _log(backend, name, "boxed")


def test_cancel_kills_the_container(backends, docker_calls, monkeypatch):
    monkeypatch.setattr(worker, "GRACE_S", 0.2)
    backend = backends(cores=1, commands="docker")
    task = _command("boxed", "print('boxed')")
    task["settings"]["image"] = "localhost:5000/slow:1"
    run, name = _submit(backend, [task])
    _wait(backend, name, lambda s: bool(_calls(docker_calls)))
    backend.cancel(name)
    container = f"simrun-{run.id}-boxed-0"
    # the SIGTERM of the cancel, in a thread, and the kill after the end of the
    # client, in either order
    expected = [["kill", "--signal", "TERM", container], ["kill", container]]
    deadline = time.monotonic() + DEADLINE_S
    while not all(call in _calls(docker_calls) for call in expected):
        assert time.monotonic() < deadline, _calls(docker_calls)
        time.sleep(0.02)
    status = _wait(backend, name, lambda s: s.tasks["boxed"] == "Failed")
    assert len(status.failed["boxed"]) == 1


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


def test_a_closed_backend_refuses_a_run(backends):
    backend = backends()
    backend.close()
    with pytest.raises(RuntimeError, match="closed"):
        _submit(backend, [_command("late", "print('late')")])
    assert backend.placements() == []
