"""The local backend: the pods of a run as processes on this machine.

`LocalBackend` runs the DAG of a run without a cluster: a split task as its
coordinator (`simrun-worker` with `SIMRUN_ROLE=base`, which merges itself), a plugin
command through `simrun-exec` and a `command` task with its args directly or in its
image with `docker run` (a container named after the run, the node and the
attempt, killed with `docker kill` on a cancel). A task starts when the tasks it reads from succeeded and its
cores fit into the free cores of the machine; the tasks of several runs take them in
the order of their submission. The nodes and phases of its status are the ones of
the Argo workflow of the run, so the controller decides the states of the tasks as
on the cluster. Every process runs in its own process group with its stdout and
stderr in `<task dir>/logs/<node>-<attempt>.log`; while it runs,
`<node>-<attempt>.pid` beside the log names it, so a service which starts after
one which died without ending its processes kills them (`kill_orphans`).

A run with snapshots gets a venv of its own (`simrun.service.local_env`), created when
its first process starts and removed at its end: its snapshots are installed there,
never into the environment of the service.

A daemon thread per run starts the processes of the run, a thread per process waits
for its end; the state lives under one condition, the callbacks of `subscribe` are
called outside of it after every start and end of a process.
"""

import contextlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Literal

from simrun import podspec, worker
from simrun.argo import RETRY_LIMIT, workflow_name
from simrun.config import ClusterConfig
from simrun.planner import Node, PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.service import local_env
from simrun.service.app import CONFIG_ENV, load_settings
from simrun.service.backend import (
    RETRYING_PHASE,
    BackendStatus,
    FailedAttempt,
    Placement,
    PodLog,
)
from simrun.store import RunRecord
from simrun.worker import EXIT_CONFIGURATION, EXIT_POOL_BROKEN, EXIT_TERMINATED, GIB

logger = logging.getLogger(__name__)

PENDING = "Pending"
RUNNING = "Running"
SUCCEEDED = "Succeeded"
FAILED = "Failed"
OMITTED = "Omitted"
# the phases of a node which does not change any more
ENDED = frozenset({SUCCEEDED, FAILED, OMITTED})
# the phases of a node which waits for its cores
WAITING = frozenset({PENDING, RETRYING_PHASE})
WORKER_ARGV = [sys.executable, "-c", "from simrun.worker import main; main()"]
EXEC_ARGV = [sys.executable, "-c", "from simrun.worker import exec_main; exec_main()"]
NOT_LOCAL = "not on the local backend"
# the exits of a stop, retried as on the cluster: a kill and a SIGTERM
RETRIED_EXITS = frozenset({EXIT_POOL_BROKEN, EXIT_TERMINATED})
# seconds a `docker kill` may take
DOCKER_KILL_S = 60.0
ENDED_KEPT = 64
"""The ended runs whose status the backend keeps, the latest; an older one is
unknown (KeyError), which the controller resolves from its record."""

Commands = Literal["direct", "docker"]


@dataclass(eq=False)
class _Node:
    """A node of the DAG of a run: one process per attempt.

    Attributes:
        name: The name of the node as Argo names it, `<task>-w0` or `<task>`; the
            key of its failed attempts.
        task_id: The id of its task.
        dependencies: The names of the nodes it reads from.
        command: The command of its process, inside the container with an image.
        env: The `SIMRUN_*` variables of its process besides the ones of the
            attempt.
        shape: The pod shape of its plan.
        cores: The cores it takes of the machine.
        scratch: Its scratch directory.
        worker: Its `SIMRUN_WORKER`, None for an unsplit task.
        image: The image its command runs in with `docker run`, None to run it
            directly.
        phase: Its phase, None while it waits for its dependencies.
        admitted: Whether its cores are taken and its process is to be started.
        attempt: The attempt of its current or next process.
        retry_memory: Whether the attempt has the retry memory: the one before
            was killed (exit 137).
        process: Its running process.
        container: The name of the container of its current attempt.
        failed: Its failed attempts.
    """

    name: str
    task_id: str
    dependencies: list[str]
    command: list[str]
    env: dict[str, str]
    shape: PodShape
    cores: int
    scratch: Path
    worker: str | None
    image: str | None
    phase: str | None = None
    admitted: bool = False
    attempt: int = 0
    retry_memory: bool = False
    process: subprocess.Popen[bytes] | None = None
    container: str | None = None
    failed: list[FailedAttempt] = field(default_factory=list)


@dataclass(eq=False)
class _Run:
    """A submitted run: its nodes in topological order.

    Attributes:
        name: The name of its workflow.
        run_id: The id of the run.
        nodes: Its nodes by name, in topological order.
        snapshots: The paths of its snapshots, installed into its venv.
        cancelled: Whether it was cancelled.
        thread: The thread which starts its processes.
        python: The python of its venv once created.
        env_error: Why its venv could not be created, for every later process.
    """

    name: str
    run_id: str
    nodes: dict[str, _Node]
    snapshots: list[str] = field(default_factory=list)
    cancelled: bool = False
    thread: threading.Thread | None = None
    python: Path | None = None
    env_error: str | None = None


def _machine_memory_gb() -> float:
    """The physical memory of this machine in GiB."""
    return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / GIB, 1)


def _signal_group(pid: int, signum: int) -> None:
    """Send a signal to the process group of `pid`, which may be gone."""
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signum)


def _docker_kill(container: str, signal_name: str | None = None) -> None:
    """Send a signal (SIGKILL by default) to a container, which may be gone."""
    options = ["--signal", signal_name] if signal_name else []
    try:
        subprocess.run(
            ["docker", "kill", *options, container],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=DOCKER_KILL_S,
            check=False,
        )
    except OSError, subprocess.SubprocessError:
        logger.exception("docker kill %s failed", container)


def _start_time(pid: int) -> int | None:
    """The start of a process in clock ticks since the boot, None when it is gone."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # the fields after the command, from the state on; the start time is the 22nd
    return int(stat.rsplit(")", 1)[1].split()[19])


def _cmdline(pid: int) -> list[str] | None:
    """The command line of a process, None when it is gone."""
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return None
    return [os.fsdecode(part) for part in raw.split(b"\0")[:-1]]


def _container(run_id: str, node: str, attempt: int) -> str:
    """The name of the container of an attempt, valid for docker."""
    return re.sub(r"[^a-zA-Z0-9_.-]", "-", f"simrun-{run_id}-{node}-{attempt}")


def _tail(text: str, lines: int | None) -> str:
    return text if lines is None else "".join(text.splitlines(True)[-lines:])


class LocalBackend:
    """A backend which runs the tasks of a run as processes on this machine.

    Attributes:
        store: The root of the store.
        scratch: The root of the scratch directories of the processes.
        envs: The root of the venvs of the runs with snapshots.
        commands: How a `command` task runs unless its run says otherwise
            (`RunRecord.local_commands`): `direct` its args, `docker` in its image.
        cores: The cores of the machine the tasks share.
        memory_gb: The memory of the machine in GiB.
        hostname: The name of the machine, its one node.
    """

    runs_images = False
    # the machine is one node: a split task is its coordinator with all the cores
    extra_workers = False

    def __init__(
        self,
        store: Path,
        scratch: Path,
        commands: Commands = "direct",
        cores: int | None = None,
        memory_gb: float | None = None,
        envs: Path | None = None,
    ) -> None:
        """Create the backend.

        Args:
            store: The root of the store.
            scratch: The root of the scratch directories.
            commands: How a `command` task runs by default.
            cores: The cores of the machine, `os.process_cpu_count()` by default.
            memory_gb: The memory of the machine in GiB, its physical memory by
                default.
            envs: The root of the venvs of the runs, `envs` beside `scratch` by
                default.
        """
        self.store = store.absolute()
        self.scratch = scratch.absolute()
        self.envs = (envs or scratch.parent / "envs").absolute()
        self.commands = commands
        self.cores = cores or os.process_cpu_count() or 1
        self.memory_gb = memory_gb if memory_gb is not None else _machine_memory_gb()
        self.hostname = socket.gethostname()
        self._cond = threading.Condition()
        self._runs: dict[str, _Run] = {}
        # the runs with a node which has not ended, in the order of submission
        self._active: dict[str, _Run] = {}
        # the ended runs in the order of their end, the latest `ENDED_KEPT`
        self._ended: list[str] = []
        self._used = 0
        self._callbacks: list[Callable[[], None]] = []
        self._closed = False

    # the protocol

    def subscribe(self, callback: Callable[[], None]) -> None:
        """Call `callback` after every start and end of a process."""
        with self._cond:
            self._callbacks.append(callback)

    def submit(
        self,
        record: RunRecord,
        plans: dict[str, TaskPlan],
        types: dict[str, TaskType],
        cluster: ClusterConfig,
    ) -> str:
        """Start the run in a thread of its own; a run submitted before is kept.

        Raises:
            ValueError: A task stages a port which is no input, or its type is
                neither splittable nor has a command.
            RuntimeError: The backend is closed.
        """
        name = workflow_name(record.id)
        with self._cond:
            if self._closed:
                raise RuntimeError("the local backend is closed")
            if name in self._runs:
                return name
        snapshots = [str(self.store / s.snapshot) for s in record.sources if s.snapshot]
        run = _Run(
            name, record.id, self._nodes(record, plans, types, cluster), snapshots
        )
        with self._cond:
            if self._closed:
                raise RuntimeError("the local backend is closed")
            if name in self._runs:
                return name
            self._runs[name] = run
            self._active[name] = run
            self._advance()
        run.thread = threading.Thread(
            target=self._drive, args=(run,), name=f"local-{record.id}", daemon=True
        )
        run.thread.start()
        logger.info("run %s submitted to the local backend", record.id)
        return name

    def status(self, name: str) -> BackendStatus:
        """The phases of the nodes of a run; KeyError for an unknown run."""
        with self._cond:
            run = self._runs[name]
            nodes = list(run.nodes.values())
            if run.cancelled:
                phase = FAILED
            elif any(n.phase not in ENDED for n in nodes):
                phase = RUNNING
            elif all(n.phase == SUCCEEDED for n in nodes):
                phase = SUCCEEDED
            else:
                phase = FAILED
            return BackendStatus(
                phase=phase,
                tasks={n.name: n.phase for n in nodes if n.phase is not None},
                failed={
                    n.name: [a.model_copy() for a in n.failed]
                    for n in nodes
                    if n.failed
                },
            )

    def cancel(self, name: str) -> None:
        """End a run: SIGTERM to its processes, SIGKILL after `worker.GRACE_S`.

        A container gets the same signals with `docker kill`. No process of the run
        starts or is retried any more; KeyError for an unknown run.
        """
        targets: list[tuple[subprocess.Popen[bytes], str | None]] = []
        with self._cond:
            run = self._runs[name]
            run.cancelled = True
            for node in run.nodes.values():
                if node.admitted:
                    node.admitted = False
                    self._used -= node.cores
                if node.phase == RUNNING:
                    if node.process is not None:
                        targets.append((node.process, node.container))
                elif node.phase == RETRYING_PHASE:
                    node.phase = FAILED
                elif node.phase not in ENDED:
                    node.phase = OMITTED
            self._advance()
        self._terminate(targets)
        self._notify()

    def stop(self, name: str) -> None:
        """End a run whose state is decided, as `cancel`."""
        self.cancel(name)

    def logs(
        self, name: str, task_id: str, tail_lines: int | None = None
    ) -> list[PodLog]:
        """The logs of the processes of a task, oldest first."""
        with self._cond:
            run = self._runs.get(name)
            run_id = run.run_id if run else name.removeprefix(workflow_name(""))
            running = (
                {(n.name, n.attempt) for n in run.nodes.values() if n.phase == RUNNING}
                if run
                else set()
            )
        directory = self._task_dir(run_id, task_id) / "logs"
        pattern = re.compile(rf"({re.escape(task_id)}(?:-w0)?)-(\d+)\.log")
        found: list[tuple[int, str, Path]] = []
        if directory.is_dir():
            for path in directory.iterdir():
                match = pattern.fullmatch(path.name)
                if match:
                    found.append((int(match[2]), match[1], path))
        logs: list[PodLog] = []
        for attempt, node, path in sorted(found):
            text = path.read_text(errors="replace")
            logs.append(
                PodLog(
                    node=node,
                    pod=f"{name}-{node}-{attempt}",
                    text=_tail(text, tail_lines),
                    done=(node, attempt) not in running,
                )
            )
        return logs

    def nodes(self) -> list[Node]:
        """This machine."""
        return [Node(self.hostname, self.cores, self.memory_gb)]

    def placements(self) -> list[Placement]:
        """A placement per running process."""
        with self._cond:
            return [
                Placement(
                    workflow=run.name,
                    dag_task=node.name,
                    pod=f"{run.name}-{node.name}-{node.attempt}",
                    node=self.hostname,
                    phase=RUNNING,
                    cores=node.cores,
                    memory_gb=float(
                        node.shape.retry_memory_gb
                        if node.retry_memory
                        else node.shape.memory_gb
                    ),
                    worker=node.worker,
                    attempt=node.attempt,
                )
                for run in self._runs.values()
                for node in run.nodes.values()
                if node.process is not None
            ]

    def reserve(self, node: str, run_id: str) -> None:
        """Refused: there is nothing to reserve locally."""
        raise RuntimeError(NOT_LOCAL)

    def unreserve(self, node: str, run_id: str) -> None:
        """Refused: there is nothing to reserve locally."""
        raise RuntimeError(NOT_LOCAL)

    def build_image(self, context: Path, ref: str) -> None:
        """Refused: the local backend runs no images of runs."""
        raise RuntimeError(NOT_LOCAL)

    def has_image(self, ref: str) -> bool:
        """True: the local backend needs no image."""
        return True

    def close(self) -> None:
        """Refuse new runs, cancel every run and wait for the end of its processes."""
        with self._cond:
            self._closed = True
            runs = list(self._runs.values())
        for run in runs:
            self.cancel(run.name)
        for run in runs:
            if run.thread is not None:
                run.thread.join(worker.GRACE_S + 5)

    # the runs

    def _task_dir(self, run_id: str, task_id: str) -> Path:
        return self.store / "runs" / run_id / "tasks" / task_id

    def _nodes(
        self,
        record: RunRecord,
        plans: dict[str, TaskPlan],
        types: dict[str, TaskType],
        cluster: ClusterConfig,
    ) -> dict[str, _Node]:
        """The nodes of a run in topological order, with commands and environment."""
        commands = record.local_commands or self.commands
        names = {
            t.id: f"{t.id}-w0" if types[t.type].splittable else t.id
            for t in record.workflow.tasks
        }
        nodes: dict[str, _Node] = {}
        for task in record.workflow.order():
            task_type = types[task.type]
            shape = plans[task.id].shape
            name = names[task.id]
            cores = max(1, min(shape.cores, self.cores))
            scratch = self.scratch / record.id / task.id
            if task_type.splittable:
                scratch = scratch / name
            env = podspec.env_values(
                record, task, task_type, shape, str(self.store), str(scratch)
            )
            env |= {
                "SIMRUN_NODE": self.hostname,
                "SIMRUN_CORES": str(cores),
                # the cgroup is the one of the machine or the session, not the task's
                worker.MEMORY_SOURCE_ENV: "process",
            }
            if env["SIMRUN_EDITABLE"]:
                # installed into the venv of the run, which the harness runs in
                env[worker.SOURCES_INSTALLED] = "1"
            image: str | None = None
            worker_id: str | None = None
            if task_type.splittable:
                worker_id = name
                argv = WORKER_ARGV
                env |= {
                    "SIMRUN_WORKER": name,
                    "SIMRUN_ROLE": "base",
                    "SIMRUN_MERGE": "1",
                    "SIMRUN_STALE_S": str(cluster.grace_s + podspec.STALE_MARGIN_S),
                }
            else:
                command_image, argv = podspec.unsplit_command(
                    record, task, task_type, str(self.store), str(scratch)
                )
                if argv[0] == podspec.EXEC_COMMAND:
                    argv = [*EXEC_ARGV, *argv[1:]]
                elif commands == "docker":
                    image = command_image
            nodes[name] = _Node(
                name=name,
                task_id=task.id,
                dependencies=[names[d] for d in task.dependencies()],
                command=argv,
                env=env,
                shape=shape,
                cores=cores,
                scratch=scratch,
                worker=worker_id,
                image=image,
            )
        return nodes

    def _advance(self) -> None:
        """Decide the nodes which wait or are omitted, admit the ones which fit.

        Called with the condition held. A node whose dependencies succeeded waits
        for its cores (`Pending`), one whose dependency failed or was omitted is
        omitted. The waiting nodes take the free cores in the order of their runs
        and of the DAG; the first which does not fit stops the admission, so a
        large task is not passed by smaller ones forever.
        """
        for run in list(self._active.values()):
            for node in run.nodes.values():
                if node.phase is not None:
                    continue
                phases = [run.nodes[d].phase for d in node.dependencies]
                if run.cancelled or any(p in (FAILED, OMITTED) for p in phases):
                    node.phase = OMITTED
                elif all(p == SUCCEEDED for p in phases):
                    node.phase = PENDING
            if all(n.phase in ENDED for n in run.nodes.values()):
                del self._active[run.name]
                self._ended.append(run.name)
        while len(self._ended) > ENDED_KEPT:
            del self._runs[self._ended.pop(0)]
        for run in self._active.values():
            if run.cancelled:
                continue
            for node in run.nodes.values():
                if node.phase not in WAITING or node.admitted:
                    continue
                if node.cores > self.cores - self._used:
                    self._cond.notify_all()
                    return
                node.admitted = True
                self._used += node.cores
        self._cond.notify_all()

    def _drive(self, run: _Run) -> None:
        """Start the admitted nodes of a run until every node of it ended."""
        while True:
            with self._cond:
                while True:
                    starts = [n for n in run.nodes.values() if n.admitted]
                    if starts or all(n.phase in ENDED for n in run.nodes.values()):
                        break
                    self._cond.wait()
                for node in starts:
                    # running from here: a cancel leaves it to `_start`
                    node.admitted = False
                    node.phase = RUNNING
            if not starts:
                if run.snapshots:
                    shutil.rmtree(self.envs / run.run_id, ignore_errors=True)
                logger.info("run %s ended on the local backend", run.run_id)
                return
            for node in starts:
                self._start(run, node)

    def _start(self, run: _Run, node: _Node) -> None:
        """Start the process of the current attempt of a node, unless cancelled.

        A process which cannot start (also for an unexpected error) fails the
        attempt with exit 4, so its cores are free again.
        """
        with self._cond:
            if run.cancelled:
                node.phase = OMITTED if node.attempt == 0 else FAILED
                self._used -= node.cores
                self._advance()
                return
        try:
            process = self._launch(run, node)
        except Exception:
            logger.exception("run %s: %s cannot start", run.run_id, node.name)
            process = None
        if process is None:
            self._finish(run, node, EXIT_CONFIGURATION)
            return
        with self._cond:
            node.process = process
            cancelled = run.cancelled
        if cancelled:
            self._terminate([(process, node.container)])
        threading.Thread(
            target=self._wait,
            args=(run, node, process),
            name=f"local-{run.run_id}-{node.name}",
            daemon=True,
        ).start()
        self._notify()

    def _launch(self, run: _Run, node: _Node) -> subprocess.Popen[bytes] | None:
        """The process of the current attempt of a node.

        The first process of a run with snapshots creates the venv of the run, the
        output of its installer in the log of the process.

        Returns:
            The process, None (and a line in its log) when the command cannot start.
        """
        log = self._task_dir(run.run_id, node.task_id) / "logs"
        log.mkdir(parents=True, exist_ok=True)
        node.scratch.mkdir(parents=True, exist_ok=True)
        shape = node.shape
        values = node.env | {
            "SIMRUN_ATTEMPT": str(node.attempt),
            "SIMRUN_MEMORY_LIMIT_GB": podspec.number(shape.memory_gb),
        }
        if node.retry_memory:
            values |= {
                "SIMRUN_MEMORY_PER_WORKER_GB": podspec.number(
                    shape.retry_per_worker_gb
                ),
                "SIMRUN_MEMORY_LIMIT_GB": podspec.number(shape.retry_memory_gb),
            }
        # the variables of the service, e.g. of a shell, are no business of a task
        env = {k: v for k, v in os.environ.items() if not k.startswith("SIMRUN_")}
        env |= values
        argv = node.command
        with (log / f"{node.name}-{node.attempt}.log").open("ab") as out:
            error: str | None = None
            if node.image is not None:
                container = _container(run.run_id, node.name, node.attempt)
                with self._cond:
                    node.container = container
                argv = [
                    *("docker", "run", "--rm", "--init", "--name", container),
                    *("-v", f"{self.store}:{self.store}"),
                    *("-v", f"{self.scratch}:{self.scratch}"),
                    *("--user", f"{os.getuid()}:{os.getgid()}"),
                    *(o for k, v in values.items() for o in ("-e", f"{k}={v}")),
                    node.image,
                    *node.command,
                ]
                if shutil.which("docker", path=env.get("PATH")) is None:
                    error = f"docker is not installed: task {node.task_id} cannot run"
            elif run.snapshots:
                python = self._environment(run, node, out)
                if python is None:
                    error = run.env_error
                else:
                    if argv[:1] == [sys.executable]:
                        argv = [str(python), *argv[1:]]
                    venv_dir = python.parent.parent
                    env["VIRTUAL_ENV"] = str(venv_dir)
                    env["PATH"] = os.pathsep.join(
                        [str(python.parent), *filter(None, [env.get("PATH")])]
                    )
            if error is None:
                try:
                    process = subprocess.Popen(
                        argv,
                        stdin=subprocess.DEVNULL,
                        stdout=out,
                        stderr=subprocess.STDOUT,
                        env=env,
                        start_new_session=True,
                    )
                except OSError as e:
                    error = f"the command of task {node.task_id} cannot start: {e}"
                else:
                    self._write_pid(log, node, process, argv)
                    return process
            out.write(f"{error}\n".encode())
        logger.error("run %s: %s", run.run_id, error)
        return None

    @staticmethod
    def _pid_file(log: Path, node: _Node) -> Path:
        return log / f"{node.name}-{node.attempt}.pid"

    def _write_pid(
        self,
        log: Path,
        node: _Node,
        process: subprocess.Popen[bytes],
        argv: list[str],
    ) -> None:
        """Name a started process beside its log: its group, start, command line."""
        record = {
            "pid": process.pid,
            "start": _start_time(process.pid),
            "argv": argv,
            "container": node.container,
        }
        try:
            self._pid_file(log, node).write_text(json.dumps(record))
        except OSError:
            logger.exception("the pid file of %s cannot be written", node.name)

    def kill_orphans(self) -> int:
        """Kill the processes of runs which a service before left running.

        A service which died without ending its processes (SIGKILL, out of memory)
        leaves their pid files beside their logs. The process group of each is
        killed, and its container, unless its run is one of this backend or its
        leader is not the process of the file any more (another start time or
        command line: a reused pid); the files are removed.

        Returns:
            The process groups killed.
        """
        with self._cond:
            live = {run.run_id for run in self._runs.values()}
        killed = 0
        for path in self.store.glob("runs/*/tasks/*/logs/*.pid"):
            if path.parents[3].name in live:
                continue
            try:
                record = json.loads(path.read_text())
                pid = int(record["pid"])
            except (OSError, ValueError, KeyError, TypeError) as error:
                logger.warning("pid file %s: %s", path, error)
                path.unlink(missing_ok=True)
                continue
            same = (
                record.get("start") is not None
                and _start_time(pid) == record["start"]
                and _cmdline(pid) == record.get("argv")
            )
            if same:
                logger.warning("killing the orphaned process group %d (%s)", pid, path)
                _signal_group(pid, signal.SIGKILL)
                killed += 1
            container = record.get("container")
            if isinstance(container, str):
                _docker_kill(container)
            path.unlink(missing_ok=True)
        return killed

    def _environment(self, run: _Run, node: _Node, log: BinaryIO) -> Path | None:
        """The python of the venv of a run, created by its first process.

        Called by the thread of the run only, so the venv is created once.

        Returns:
            The python, None when the venv cannot be created (`run.env_error`).
        """
        if run.python is not None or run.env_error is not None:
            return run.python
        try:
            run.python = local_env.create(
                self.envs / run.run_id,
                run.snapshots,
                log,
                lambda: run.cancelled,
            )
        except local_env.RunEnvironmentError as error:
            run.env_error = f"{error} (see the log of {node.name})"
        return run.python

    def _wait(self, run: _Run, node: _Node, process: subprocess.Popen[bytes]) -> None:
        """Wait for the end of a process, then end the rest of its process group."""
        pid_file = self._pid_file(
            self._task_dir(run.run_id, node.task_id) / "logs", node
        )
        code = process.wait()
        # a process killed by a signal exits 128 + the signal, as in a container
        code = 128 - code if code < 0 else code
        _signal_group(process.pid, signal.SIGKILL)
        if node.container is not None and code != 0:
            # a docker client which was killed leaves its container running
            _docker_kill(node.container)
        pid_file.unlink(missing_ok=True)
        shutil.rmtree(node.scratch, ignore_errors=True)
        self._finish(run, node, code)

    def _finish(self, run: _Run, node: _Node, code: int) -> None:
        """Record the end of an attempt: done, retried after a stop, or failed.

        A stop (exit 137 or 143) is retried as on the cluster, after 137 with the
        retry memory; never in a cancelled run.
        """
        with self._cond:
            node.process = None
            node.container = None
            self._used -= node.cores
            if code == 0:
                node.phase = SUCCEEDED
            else:
                node.failed.append(FailedAttempt(attempt=node.attempt, exit_code=code))
                retry = code in RETRIED_EXITS and node.attempt < RETRY_LIMIT
                if retry and not run.cancelled:
                    node.attempt += 1
                    node.retry_memory = code == EXIT_POOL_BROKEN
                    node.phase = RETRYING_PHASE
                else:
                    node.phase = FAILED
            self._advance()
        logger.info("run %s: %s exited %d", run.run_id, node.name, code)
        self._notify()

    def _terminate(
        self, targets: list[tuple[subprocess.Popen[bytes], str | None]]
    ) -> None:
        """SIGTERM to the process groups and containers, SIGKILL after the grace.

        Only the processes alive after the grace are killed; the `docker kill` of
        the containers runs in a thread, so a cancel does not wait for docker.
        """
        if not targets:
            return
        for process, _ in targets:
            _signal_group(process.pid, signal.SIGTERM)
        containers = [c for _, c in targets if c is not None]
        if containers:
            threading.Thread(
                target=lambda: [_docker_kill(c, "TERM") for c in containers],
                daemon=True,
            ).start()

        def kill() -> None:
            for process, container in targets:
                # an ended leader: its waiter killed the rest of its group
                if process.returncode is None:
                    _signal_group(process.pid, signal.SIGKILL)
                    if container is not None:
                        _docker_kill(container)

        timer = threading.Timer(worker.GRACE_S, kill)
        timer.daemon = True
        timer.start()

    def _notify(self) -> None:
        """Call the callbacks of `subscribe`, without the condition held."""
        with self._cond:
            callbacks = list(self._callbacks)
        for callback in callbacks:
            try:
                callback()
            except Exception:
                logger.exception("a callback of the local backend failed")


def backend() -> LocalBackend:
    """The backend of the settings named by `SIMRUN_CONFIG`, for `serve`.

    The store is the one of the settings, the scratch `<store>/../scratch`.
    """
    settings = load_settings(Path(os.environ[CONFIG_ENV]))
    return LocalBackend(settings.store, settings.store.parent / "scratch")
