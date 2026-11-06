"""The worker harness: a pod of a split task computes units of the ledger in a pool.

`simrun-worker` (`main`) claims units of the ledger of its task, computes them in a
local process pool with the functions of the task type and writes a report;
`simrun-merge` (`merge_main`) merges the units once the ledger is complete;
`simrun-exec` (`exec_main`) runs the command of an unsplit plugin task and writes a
report of it. A pod
is evicted with SIGTERM and killed after its grace period, and is restarted with the
same worker id, which releases the claims of the incarnation before.
"""

import contextlib
import importlib
import json
import logging
import math
import multiprocessing
import os
import shutil
import signal
import site
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Generator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from multiprocessing import forkserver
from pathlib import Path

from pydantic import BaseModel

from simrun import cgroup
from simrun.ledger import Ledger
from simrun.plugins import TaskPaths, TaskType, load_task_types, resolve

logger = logging.getLogger(__name__)

GIB = 2**30
UNITS_DIR = "units"
WORKERS_DIR = "workers"
PROGRESS_FILE = "progress.jsonl"
SETTINGS_FILE = "settings.json"
# written to the output of a task by `simrun-merge` after a successful merge, so the
# service knows a task is done also when the workflow is gone
MERGED_MARKER = ".simrun-merged"
DEFAULT_SCRATCH = "/scratch"
THREAD_VARIABLES = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")
POLL_S = 0.2
HEARTBEAT_S = 30.0
# seconds between two samples of the anonymous memory of the cgroup
SAMPLE_S = 1.0
# the directory of the writable trees the snapshots are installed from
SOURCES_DIR = "simrun-sources"
# seconds a command of `simrun-exec` gets after SIGTERM, the grace of a worker
GRACE_S = 60.0
COORDINATOR_POLL_S = 5.0
# a heartbeat is stale this long after the grace of a worker
STALE_MARGIN_S = 60.0
ROLES = {"base": True, "extra": False}

EXIT_DONE = 0
EXIT_ANALYSIS = 3
EXIT_CONFIGURATION = 4
# A process of the pool was killed, as the OOM killer of the cgroup does: the exit
# code of a killed pod, on which the workflow retries with more memory.
EXIT_POOL_BROKEN = 128 + signal.SIGKILL
# Evicted: not done, so the workflow retries the pod.
EXIT_TERMINATED = 128 + signal.SIGTERM


class ConfigurationError(Exception):
    """The environment or the settings of a worker are wrong."""


class UnitError(RuntimeError):
    """A unit failed in the function of the task type; it was released."""

    def __init__(self, index: int) -> None:
        """Create the error of the unit `index`."""
        super().__init__(f"unit {index} failed")
        self.index = index


class Terminated(Exception):
    """The worker stopped on SIGTERM before its units were done."""


@dataclass
class WorkerReport:
    """What a worker did, written as `task_dir/workers/<worker>.json`.

    Attributes:
        worker: The worker id.
        node: The server the worker ran on.
        processes: The number of processes of the pool.
        units_done: The units the worker completed.
        seconds: The duration of the worker.
        peak_memory_bytes: The largest sample of the anonymous memory of the cgroup
            (`MemorySampler`), without the page cache.
        memory_limit_bytes: The memory limit of the cgroup.
        per_worker_gb: The memory per process in GiB the worker was sized with.
    """

    worker: str
    node: str
    processes: int
    units_done: list[int] = field(default_factory=list)
    seconds: float = 0.0
    peak_memory_bytes: int | None = None
    memory_limit_bytes: int | None = None
    per_worker_gb: float = 0.0


def processes(
    cores: int, memory_bytes: int | None, base_gb: float, per_worker_gb: float
) -> int:
    """The number of processes the cores and the memory allow.

    Args:
        cores: The number of cores.
        memory_bytes: The memory limit in bytes, `None` without a limit.
        base_gb: The memory of the task besides its processes in GiB.
        per_worker_gb: The memory per process in GiB.

    Returns:
        `min(cores, floor((memory - base) / per_worker))`, at least 1.

    Raises:
        ValueError: Not even one process fits into the memory.
    """
    if memory_bytes is None:
        return max(1, cores)
    # the tolerance keeps an exact quotient from rounding down to the integer below
    fit = math.floor((memory_bytes / GIB - base_gb) / per_worker_gb + 1e-9)
    if fit < 1:
        raise ValueError(
            f"no process fits into {memory_bytes / GIB:.2f} GiB with "
            f"{base_gb} GiB base and {per_worker_gb} GiB per process"
        )
    return max(1, min(cores, fit))


# The setups which ran in this process of the pool.
_SETUPS_DONE: set[str] = set()


def _init_process() -> None:
    """Start a process of the pool: ignore SIGINT and SIGTERM.

    A SIGTERM to the process group (from a container runtime or Argo's emissary)
    reaches the worker only, which gives the running units their grace. The worker
    ends the processes with SIGKILL, see `_kill`.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    # blocked by the forkserver, see `_start_forkserver`
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM})


def _start_forkserver() -> None:
    """Start the forkserver of the pool with SIGTERM blocked.

    The forkserver and the processes it forks inherit the mask, so a SIGTERM to the
    process group ends neither. The forkserver would die of it, and the executor
    reads the end of every process from the forkserver: the pool would break. A
    SIGTERM to the worker meanwhile is delivered when the mask is restored.
    """
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
    try:
        forkserver.ensure_running()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def _kill(pids: set[int]) -> None:
    """SIGKILL the processes of a pool, without the lock of the executor.

    When a process dies, the manager thread of the executor holds its lock while it
    sends SIGTERM to the others, which ignore it, and joins them; so
    `ProcessPoolExecutor.kill_workers`, which takes that lock, would wait forever.
    """
    for pid in pids:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def _compute(
    setup: str | None, ref: str, settings: BaseModel, index: int, paths: TaskPaths
) -> None:
    """Compute one unit in a process of the pool, after the setup of the process.

    The setup runs in the first unit of a process, so its exception is the exception
    of that unit, and a broken pool only means that a process died.
    """
    if setup is not None and setup not in _SETUPS_DONE:
        resolve(setup)(settings, paths)
        _SETUPS_DONE.add(setup)
    resolve(ref)(settings, index, paths)


@contextmanager
def _stop_on_sigterm(stop: threading.Event) -> Generator[None]:
    """Set `stop` on SIGTERM, in the main thread of this process only."""
    if threading.current_thread() is not threading.main_thread():
        logger.debug("not in the main thread: SIGTERM is not handled")
        yield
        return

    def handler(signum: int, frame: object) -> None:
        stop.set()

    previous = signal.signal(signal.SIGTERM, handler)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, previous)


def _append_progress(path: Path, worker: str, index: int) -> None:
    """Append a `unit_done` line, with one write of a file opened with O_APPEND."""
    line = json.dumps(
        {
            "time": datetime.now(UTC).isoformat(),
            "worker": worker,
            "event": "unit_done",
            "unit": index,
        }
    )
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, f"{line}\n".encode())
    finally:
        os.close(fd)


def write_report(report: WorkerReport, task_dir: Path, attempt: int = 0) -> Path:
    """Write the report as `task_dir/workers/<worker>-<attempt>.json`.

    The file is written as a temporary file and renamed.

    Args:
        report: The report.
        task_dir: The task directory.
        attempt: The attempt of the pod.

    Returns:
        The path of the report.
    """
    directory = task_dir / WORKERS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{report.worker}-{attempt}.json"
    path = directory / name
    tmp = directory / f".{name}.{uuid.uuid4().hex}.tmp"
    tmp.write_text(json.dumps(asdict(report), indent=2))
    os.replace(tmp, path)
    return path


@dataclass
class _Units:
    """The state of the units of one worker."""

    ledger: Ledger
    worker: str
    progress: Path
    done: list[int] = field(default_factory=list)
    failed: int | None = None
    broken: BrokenProcessPool | None = None

    def complete(self, index: int) -> None:
        try:
            self.ledger.complete(index, self.worker)
        except ValueError:
            logger.warning(
                "unit %d was computed, but its claim was lost to a restarted "
                "incarnation of %s",
                index,
                self.worker,
            )
            return
        self.done.append(index)
        _append_progress(self.progress, self.worker, index)

    def release(self, index: int) -> None:
        try:
            self.ledger.release(index, self.worker)
        except ValueError:
            logger.warning(
                "unit %d: its claim was lost to a restarted incarnation of %s",
                index,
                self.worker,
            )

    def settle(self, future: Future[None], index: int) -> None:
        """Complete the unit of a future which succeeded, else release it."""
        if future.done() and not future.cancelled() and future.exception() is None:
            self.complete(index)
        else:
            self.release(index)

    def finished(self, future: Future[None], index: int) -> None:
        """Complete the unit of a finished future, or release it and note why."""
        error = None if future.cancelled() else future.exception()
        if not future.cancelled() and error is None:
            self.complete(index)
            return
        self.release(index)
        if isinstance(error, BrokenProcessPool):
            if self.broken is None:
                logger.error("a process of the pool died: %s", error)
                self.broken = error
        elif error is not None:
            logger.error("unit %d failed", index, exc_info=error)
            if self.failed is None:
                self.failed = index


@dataclass(frozen=True)
class _Coordination:
    """How the coordinator waits for the units of other workers.

    Attributes:
        stale_s: Seconds after which the heartbeat of a worker is stale.
        poll_s: Seconds between two releases of stale claims.
    """

    stale_s: float
    poll_s: float


class MemorySampler:
    """The maximum of the anonymous memory of the cgroup over its samples.

    The anonymous memory (`memory.stat` `anon`) is what the processes allocated;
    `memory.peak` also counts the page cache of staging, installing and writing
    outputs, which the kernel reclaims at the limit instead of killing.

    Attributes:
        peak: The largest sample in bytes, None before one was taken.
    """

    def __init__(self) -> None:
        """Create a sampler without samples."""
        self.peak: int | None = None

    def sample(self) -> None:
        """Read the anonymous memory and keep it when it is the largest."""
        value = cgroup.memory_anon()
        if value is not None and (self.peak is None or value > self.peak):
            self.peak = value


@contextmanager
def _heartbeats(
    ledger: Ledger | None, worker: str, heartbeat_s: float, sampler: MemorySampler
) -> Generator[None]:
    """Heartbeat and sample the memory in a thread until the end.

    The heartbeat of `worker` is touched now and every `heartbeat_s` (not without a
    ledger), the memory is sampled now, every `SAMPLE_S` and at the end.
    """
    if ledger is not None:
        ledger.heartbeat(worker)
    sampler.sample()
    done = threading.Event()

    def beat() -> None:
        next_beat = time.monotonic() + heartbeat_s
        while True:
            timeout = SAMPLE_S
            if ledger is not None:
                timeout = max(0.0, min(timeout, next_beat - time.monotonic()))
            if done.wait(timeout):
                return
            sampler.sample()
            if ledger is None or time.monotonic() < next_beat:
                continue
            next_beat = time.monotonic() + heartbeat_s
            try:
                ledger.heartbeat(worker)
            except OSError as error:
                logger.warning("heartbeat of %s failed: %s", worker, error)

    thread = threading.Thread(target=beat, name=f"heartbeat-{worker}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        done.set()
        thread.join()
        sampler.sample()


def _compute_units(
    task_type: TaskType,
    settings: BaseModel,
    paths: TaskPaths,
    units: _Units,
    n_processes: int,
    stop: threading.Event,
    grace_s: float,
    coordination: _Coordination | None,
) -> None:
    """Keep `n_processes` units in flight until none is pending or the worker stops.

    A coordinator (`coordination` given) continues until the ledger is complete: it
    releases the claims of workers with a stale heartbeat every `poll_s` and computes
    them.
    """
    assert task_type.compute_unit is not None
    _start_forkserver()
    pool = ProcessPoolExecutor(
        max_workers=n_processes,
        mp_context=multiprocessing.get_context("forkserver"),
        initializer=_init_process,
    )
    in_flight: dict[Future[None], int] = {}
    # the processes the pool started, which it starts in `submit` only
    started: set[int] = set()
    deadline: float | None = None
    next_release = 0.0
    try:
        while True:
            if stop.is_set() and deadline is None:
                deadline = time.monotonic() + grace_s
                logger.info(
                    "SIGTERM: %d units running, waiting up to %s s",
                    len(in_flight),
                    grace_s,
                )
            claiming = not stop.is_set() and units.failed is None
            if (
                coordination is not None
                and claiming
                and units.broken is None
                and time.monotonic() >= next_release
            ):
                units.ledger.release_stale(coordination.stale_s, units.worker)
                next_release = time.monotonic() + coordination.poll_s
            while claiming and units.broken is None and len(in_flight) < n_processes:
                index = units.ledger.claim(units.worker)
                if index is None:
                    break
                try:
                    future = pool.submit(
                        _compute,
                        task_type.setup,
                        task_type.compute_unit,
                        settings,
                        index,
                        paths,
                    )
                except BrokenProcessPool as error:
                    units.broken = error
                    units.release(index)
                    break
                in_flight[future] = index
                started.update(pool._processes or ())
            if units.failed is not None or units.broken is not None:
                break
            if not in_flight:
                if (
                    coordination is None
                    or stop.is_set()
                    or units.ledger.complete_ledger()
                ):
                    break
                # other workers hold the rest: wait for them, or for their heartbeat
                # to go stale
                stop.wait(max(0.0, next_release - time.monotonic()))
                continue
            timeout = POLL_S
            if deadline is not None:
                timeout = min(timeout, deadline - time.monotonic())
                if timeout <= 0:
                    break
            finished, _ = wait(in_flight, timeout=timeout, return_when=FIRST_COMPLETED)
            for future in finished:
                units.finished(future, in_flight.pop(future))
    finally:
        if in_flight or units.broken is not None:
            # end the processes before their units are freed, and every process of
            # a broken pool, whose manager thread waits for them
            _kill(started)
        pool.shutdown(wait=True, cancel_futures=True)
        for future, index in in_flight.items():
            units.settle(future, index)


def run_worker(
    task_type: TaskType,
    settings: BaseModel,
    paths: TaskPaths,
    worker: str,
    *,
    cores: int,
    memory_bytes: int | None,
    base_gb: float,
    per_worker_gb: float,
    grace_s: float = GRACE_S,
    attempt: int = 0,
    stop: threading.Event | None = None,
    heartbeat_s: float = HEARTBEAT_S,
    coordinator: bool = False,
    stale_s: float | None = None,
    poll_s: float = COORDINATOR_POLL_S,
) -> WorkerReport:
    """Compute units of the ledger of a task until none is pending.

    The worker touches its heartbeat in the ledger every `heartbeat_s` from its start
    to its end. A coordinator does not return when nothing is pending: until the
    ledger is complete it waits, releases the claims of workers whose heartbeat is
    older than `stale_s` every `poll_s` and computes them.

    Releases the claims of an earlier incarnation of `worker`, creates the ledger
    `task_dir/units`, keeps `processes(...)` units in flight in a pool of processes
    which run the setup of the task type once, completes a unit after its function
    returned and appends a `unit_done` line to `task_dir/progress.jsonl`. On SIGTERM
    (or when `stop` is set, also before the first claim) it stops claiming, waits for
    the running units up to `grace_s` and releases the rest. The report
    `task_dir/workers/<worker>-<attempt>.json` is written in every case.

    Args:
        task_type: The task type, splittable.
        settings: The settings of the task.
        paths: The locations of the task.
        worker: The worker id, the same for every incarnation of a pod.
        cores: The number of cores.
        memory_bytes: The memory limit in bytes, `None` without a limit.
        base_gb: The memory of the task besides its processes in GiB.
        per_worker_gb: The memory per process in GiB.
        grace_s: Seconds the running units get after SIGTERM.
        attempt: The attempt of the pod, which names its report.
        stop: Set on SIGTERM by a caller which handles it already, e.g. `main`.
        heartbeat_s: Seconds between two heartbeats.
        coordinator: Whether the worker continues until the ledger is complete.
        stale_s: Seconds after which the heartbeat of another worker is stale,
            `grace_s + 60` by default.
        poll_s: Seconds between two releases of stale claims by a coordinator.

    Returns:
        The report.

    Raises:
        ValueError: The task type is not splittable, the memory is too small for one
            process, the worker id is invalid or the ledger has another size.
        UnitError: A unit failed; it was released.
        BrokenProcessPool: A process of the pool died, e.g. killed for its memory.
        Terminated: The worker stopped on SIGTERM.
    """
    start = time.monotonic()
    if not task_type.splittable:
        raise ValueError(f"task type {task_type.name!r} is not splittable")
    assert task_type.count_units is not None
    if stop is None:
        stop = threading.Event()
    with _stop_on_sigterm(stop):
        n_units = resolve(task_type.count_units)(settings)
        ledger = Ledger.create(paths.task_dir / UNITS_DIR, n_units)
        released = ledger.release_all(worker)
        if released:
            logger.info("released %d units of an earlier incarnation", len(released))
        n_processes = processes(cores, memory_bytes, base_gb, per_worker_gb)
        logger.info("worker %s: %d processes, %d units", worker, n_processes, n_units)
        units = _Units(ledger, worker, paths.task_dir / PROGRESS_FILE)
        sampler = MemorySampler()
        coordination = None
        if coordinator:
            stale = grace_s + STALE_MARGIN_S if stale_s is None else stale_s
            coordination = _Coordination(stale_s=stale, poll_s=poll_s)
        try:
            with _heartbeats(ledger, worker, heartbeat_s, sampler):
                _compute_units(
                    task_type,
                    settings,
                    paths,
                    units,
                    n_processes,
                    stop,
                    grace_s,
                    coordination,
                )
        finally:
            report = WorkerReport(
                worker=worker,
                node=os.environ.get("SIMRUN_NODE") or socket.gethostname(),
                processes=n_processes,
                units_done=sorted(units.done),
                seconds=time.monotonic() - start,
                peak_memory_bytes=sampler.peak,
                memory_limit_bytes=cgroup.memory_limit(),
                per_worker_gb=per_worker_gb,
            )
            write_report(report, paths.task_dir, attempt)
    if units.failed is not None:
        raise UnitError(units.failed)
    if stop.is_set():
        # also when the pool broke after SIGTERM: an eviction, not a lack of memory
        raise Terminated(f"worker {worker} stopped on SIGTERM")
    if units.broken is not None:
        raise units.broken
    return report


def _missing_text(missing: list[int], limit: int = 20) -> str:
    text = ", ".join(str(i) for i in missing[:limit])
    if len(missing) > limit:
        text += f", ... ({len(missing)} in all)"
    return text


def run_merge(task_type: TaskType, settings: BaseModel, paths: TaskPaths) -> None:
    """Merge the units of a task once its ledger is complete.

    Args:
        task_type: The task type, splittable.
        settings: The settings of the task.
        paths: The locations of the task.

    Raises:
        ValueError: The task type is not splittable.
        RuntimeError: Units are not done; the message names them.
    """
    if task_type.count_units is None or task_type.merge is None:
        raise ValueError(f"task type {task_type.name!r} is not splittable")
    n_units = resolve(task_type.count_units)(settings)
    root = paths.task_dir / UNITS_DIR
    missing = Ledger(root, n_units).missing() if root.exists() else list(range(n_units))
    if missing:
        raise RuntimeError(
            f"the ledger of {paths.task_dir} is incomplete, missing units: "
            f"{_missing_text(missing)}"
        )
    resolve(task_type.merge)(settings, paths)


def _pairs(value: str) -> list[tuple[str, str]]:
    """Parse comma separated `a=b` pairs."""
    pairs = []
    for item in filter(None, (part.strip() for part in value.split(","))):
        key, sep, target = item.partition("=")
        if not sep or not key or not target:
            raise ConfigurationError(f"not a pair a=b: {item!r}")
        pairs.append((key, target))
    return pairs


def paths_from_env(env: Mapping[str, str]) -> TaskPaths:
    """The locations of a task from the environment of its pod.

    The task directory is `<store>/runs/<run>/tasks/<task>` (`SIMRUN_TASK_DIR`), the
    inputs are `port=path` pairs (`SIMRUN_INPUTS`, a relative path is relative to the
    store) and the scratch directory is `SIMRUN_SCRATCH` (default `/scratch`).

    Args:
        env: The environment.

    Returns:
        The locations.

    Raises:
        ConfigurationError: `SIMRUN_TASK_DIR` is not set or a pair is malformed.
    """
    if not env.get("SIMRUN_TASK_DIR"):
        raise ConfigurationError("SIMRUN_TASK_DIR is not set")
    task_dir = Path(env["SIMRUN_TASK_DIR"])
    run_dir = task_dir.parent.parent
    store = run_dir.parent.parent
    inputs = {port: store / path for port, path in _pairs(env.get("SIMRUN_INPUTS", ""))}
    return TaskPaths(
        store=store,
        run_dir=run_dir,
        task_dir=task_dir,
        out=task_dir / "out",
        inputs=inputs,
        scratch=Path(env.get("SIMRUN_SCRATCH") or DEFAULT_SCRATCH),
    )


def shadow_tree(source: Path, target: Path) -> Path:
    """A writable tree of a read-only one: its directories, links to its files.

    A build backend may write into the tree it builds (a version file, metadata, a
    build directory); it writes into the writable directories of the shadow, and
    never into the source. A link (to a file or a directory) of the source is a link
    to it in the shadow.

    Args:
        source: The read-only tree.
        target: The shadow to create, replaced when it exists.

    Returns:
        `target`.
    """
    if target.exists():
        shutil.rmtree(target)
    for dirpath, dirnames, filenames in os.walk(source):
        directory = Path(dirpath)
        shadow = target / directory.relative_to(source)
        shadow.mkdir(parents=True, exist_ok=True)
        for name in list(dirnames):
            if (directory / name).is_symlink():
                dirnames.remove(name)
                filenames.append(name)
        for name in filenames:
            (shadow / name).symlink_to(directory / name)
    return target


def install_editable(
    snapshots: list[str], python: str = sys.executable, root: Path | None = None
) -> None:
    """Install snapshots of sources as editable packages, without dependencies.

    The image has the dependencies and the build backends of the sources (see
    `simrun.environment`), so the snapshots are built without isolation and without
    an index. A snapshot is mounted read-only: each is installed from a writable
    shadow tree (`shadow_tree`) below `root`, whose files are links to the snapshot.

    Args:
        snapshots: The paths of the snapshots; nothing is installed without one.
        python: The interpreter of the environment to install into.
        root: The directory of the shadow trees, `SOURCES_DIR` below the temporary
            directory by default.

    Raises:
        subprocess.CalledProcessError: The installation failed.
        OSError: A shadow tree cannot be created.
    """
    if not snapshots:
        return
    if root is None:
        root = Path(tempfile.gettempdir()) / SOURCES_DIR
    command = [
        "uv",
        "pip",
        "install",
        "--python",
        python,
        "--no-deps",
        "--no-index",
        "--no-build-isolation",
    ]
    for index, snapshot in enumerate(snapshots):
        source = Path(snapshot)
        shadow = shadow_tree(source, root / f"{index}-{source.name}")
        command += ["-e", str(shadow)]
    logger.info("installing %s", " ".join(snapshots))
    subprocess.run(command, check=True)
    # the path files of the packages are read at the start of an interpreter only
    for directory in site.getsitepackages():
        site.addsitedir(directory)
    importlib.invalidate_caches()


def stage(pairs: list[tuple[str, str]], store: Path, scratch: Path) -> None:
    """Copy inputs to the scratch directory.

    Args:
        pairs: `(source, destination)`; a relative source is relative to the store, a
            relative destination to the scratch directory.
        store: The root of the run store.
        scratch: The scratch directory.
    """
    for source, destination in pairs:
        src = store / source
        dst = scratch / destination
        logger.info("staging %s to %s", src, dst)
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)


def _task(env: Mapping[str, str]) -> tuple[TaskType, BaseModel, TaskPaths]:
    """The task type, the settings and the paths of the task of the environment."""
    paths = paths_from_env(env)
    name = env.get("SIMRUN_TASK_TYPE")
    if not name:
        raise ConfigurationError("SIMRUN_TASK_TYPE is not set")
    types = load_task_types()
    if name not in types:
        raise ConfigurationError(f"unknown task type {name!r}")
    task_type = types[name]
    settings = task_type.settings.model_validate_json(
        (paths.task_dir / SETTINGS_FILE).read_text()
    )
    return task_type, settings, paths


def _attempt(env: Mapping[str, str]) -> int:
    value = env.get("SIMRUN_ATTEMPT") or "0"
    try:
        return int(value)
    except ValueError:
        raise ConfigurationError(
            f"SIMRUN_ATTEMPT is not an integer: {value!r}"
        ) from None


def _coordinator(env: Mapping[str, str]) -> bool:
    role = env.get("SIMRUN_ROLE") or "extra"
    if role not in ROLES:
        raise ConfigurationError(f"SIMRUN_ROLE is not one of {sorted(ROLES)}: {role!r}")
    return ROLES[role]


def _stale_s(env: Mapping[str, str]) -> float | None:
    value = env.get("SIMRUN_STALE_S")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        raise ConfigurationError(f"SIMRUN_STALE_S is not a number: {value!r}") from None


def _editable(env: Mapping[str, str]) -> list[str]:
    value = env.get("SIMRUN_EDITABLE", "")
    return [path.strip() for path in value.split(",") if path.strip()]


def _gb(env: Mapping[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        raise ConfigurationError(f"{name} is not a number: {value!r}") from None


CONFIGURATION_ERRORS = (
    ConfigurationError,
    ValueError,
    OSError,
    subprocess.CalledProcessError,
)


def _logging() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


def main() -> None:
    """Run `simrun-worker` in a pod of a split task; exits with its code.

    SIGTERM is handled from the start, so a pod evicted while it prepares (as PID 1
    without a handler it would ignore SIGTERM) stops before its first claim. The
    worker of `SIMRUN_ROLE=base` is the coordinator, which returns once the ledger is
    complete (stale claims released after `SIMRUN_STALE_S`); `extra` (the default)
    returns when nothing is pending.

    Exit codes: 0 done, 3 a unit failed (logged and released), 4 a configuration
    error, 137 a process of the pool died (retried with more memory), 143 stopped on
    SIGTERM (retried).
    """
    stop = threading.Event()
    with _stop_on_sigterm(stop):
        sys.exit(_worker(stop))


def _worker(stop: threading.Event) -> int:
    """Prepare and run the worker of `main`; returns its exit code."""
    _logging()
    env = os.environ
    try:
        install_editable(_editable(env))
        task_type, settings, paths = _task(env)
        worker = env.get("SIMRUN_WORKER")
        if not worker:
            raise ConfigurationError("SIMRUN_WORKER is not set")
        attempt = _attempt(env)
        coordinator = _coordinator(env)
        stale_s = _stale_s(env)
        stage(_pairs(env.get("SIMRUN_STAGE", "")), paths.store, paths.scratch)
        base_gb = _gb(env, "SIMRUN_MEMORY_BASE_GB", task_type.hint.memory_base_gb)
        per_worker_gb = _gb(
            env, "SIMRUN_MEMORY_PER_WORKER_GB", task_type.hint.memory_per_worker_gb
        )
    except CONFIGURATION_ERRORS as error:
        logger.error("configuration error: %s", error)
        return EXIT_CONFIGURATION
    if stop.is_set():
        logger.info("stopped on SIGTERM before the first claim")
        return EXIT_TERMINATED
    for name in THREAD_VARIABLES:
        os.environ[name] = "1"
    try:
        run_worker(
            task_type,
            settings,
            paths,
            worker,
            cores=cgroup.cpu_quota() or os.process_cpu_count() or 1,
            memory_bytes=cgroup.memory_limit(),
            base_gb=base_gb,
            per_worker_gb=per_worker_gb,
            attempt=attempt,
            stop=stop,
            coordinator=coordinator,
            stale_s=stale_s,
        )
    except UnitError:
        return EXIT_ANALYSIS
    except BrokenProcessPool:
        return EXIT_POOL_BROKEN
    except Terminated as error:
        logger.info("%s", error)
        return EXIT_TERMINATED
    except ValueError as error:
        logger.error("configuration error: %s", error)
        return EXIT_CONFIGURATION
    return EXIT_DONE


def merge_main() -> None:
    """Run `simrun-merge` after the workers of a split task; exits with its code.

    SIGTERM is handled from the start: a merge stopped before it began exits 143.
    A successful merge writes `out/.simrun-merged` (`MERGED_MARKER`).

    Exit codes: 0 merged, 3 the ledger is incomplete or the merge failed, 4 a
    configuration error, 143 stopped on SIGTERM before the merge.
    """
    stop = threading.Event()
    with _stop_on_sigterm(stop):
        sys.exit(_merge(stop))


def _merge(stop: threading.Event) -> int:
    """Prepare and run the merge of `merge_main`; returns its exit code."""
    _logging()
    env = os.environ
    try:
        install_editable(_editable(env))
        task_type, settings, paths = _task(env)
    except CONFIGURATION_ERRORS as error:
        logger.error("configuration error: %s", error)
        return EXIT_CONFIGURATION
    if stop.is_set():
        logger.info("stopped on SIGTERM before the merge")
        return EXIT_TERMINATED
    try:
        run_merge(task_type, settings, paths)
        paths.out.mkdir(parents=True, exist_ok=True)
        (paths.out / MERGED_MARKER).touch()
    except Exception:
        logger.exception("the merge of %s failed", paths.task_dir)
        return EXIT_ANALYSIS
    return EXIT_DONE


def exec_main() -> None:
    """Run `simrun-exec <command...>`, the command of an unsplit plugin task.

    The harness installs the snapshots (`SIMRUN_EDITABLE`), stages the inputs, sets
    the thread variables of BLAS and OpenMP to 1 and runs the command as its child,
    while its heartbeat thread samples the memory. A SIGTERM is passed on to the
    command, which is killed after `GRACE_S`. The report
    `task_dir/workers/<task>-<attempt>.json` (the worker is the task id, or
    `SIMRUN_WORKER`) is written in every case after the command started, so the
    controller learns the memory of the task.

    Exit codes: 0 done, 3 the command failed, 4 a configuration error (also a
    command which cannot be started), 137 the command was killed (by the out of
    memory killer, retried with more memory) or exited 137, 143 stopped on SIGTERM
    (retried).
    """
    stop = threading.Event()
    with _stop_on_sigterm(stop):
        sys.exit(_exec(sys.argv[1:], stop))


def _exec(argv: list[str], stop: threading.Event) -> int:
    """Prepare and run the command of `exec_main`; returns its exit code."""
    _logging()
    env = os.environ
    if argv[:1] == ["--"]:
        argv = argv[1:]
    try:
        if not argv:
            raise ConfigurationError("simrun-exec needs a command")
        install_editable(_editable(env))
        paths = paths_from_env(env)
        attempt = _attempt(env)
        stage(_pairs(env.get("SIMRUN_STAGE", "")), paths.store, paths.scratch)
        per_worker_gb = _gb(env, "SIMRUN_MEMORY_PER_WORKER_GB", 0.0)
    except CONFIGURATION_ERRORS as error:
        logger.error("configuration error: %s", error)
        return EXIT_CONFIGURATION
    if stop.is_set():
        logger.info("stopped on SIGTERM before the command")
        return EXIT_TERMINATED
    for name in THREAD_VARIABLES:
        os.environ[name] = "1"
    worker = env.get("SIMRUN_WORKER") or paths.task_dir.name
    start = time.monotonic()
    try:
        process = subprocess.Popen(argv)
    except OSError as error:
        logger.error("configuration error: the command cannot start: %s", error)
        return EXIT_CONFIGURATION
    sampler = MemorySampler()
    try:
        with _heartbeats(None, worker, HEARTBEAT_S, sampler):
            returncode = _wait_for(process, stop, GRACE_S)
    finally:
        report = WorkerReport(
            worker=worker,
            node=os.environ.get("SIMRUN_NODE") or socket.gethostname(),
            processes=cgroup.cpu_quota() or os.process_cpu_count() or 1,
            seconds=time.monotonic() - start,
            peak_memory_bytes=sampler.peak,
            memory_limit_bytes=cgroup.memory_limit(),
            per_worker_gb=per_worker_gb,
        )
        write_report(report, paths.task_dir, attempt)
    if stop.is_set():
        logger.info("stopped on SIGTERM, the command exited %d", returncode)
        return EXIT_TERMINATED
    if returncode == 0:
        return EXIT_DONE
    if returncode in (-signal.SIGKILL, EXIT_POOL_BROKEN):
        logger.error("the command was killed (exit %d)", returncode)
        return EXIT_POOL_BROKEN
    logger.error("the command failed with exit code %d", returncode)
    return EXIT_ANALYSIS


def _wait_for(
    process: subprocess.Popen[bytes], stop: threading.Event, grace_s: float
) -> int:
    """Wait for a command; on `stop` send it SIGTERM, after `grace_s` SIGKILL."""
    deadline: float | None = None
    while True:
        try:
            return process.wait(timeout=POLL_S)
        except subprocess.TimeoutExpired:
            pass
        if stop.is_set() and deadline is None:
            logger.info("SIGTERM: passed on to the command, waiting %s s", grace_s)
            process.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + grace_s
        if deadline is not None and time.monotonic() >= deadline:
            logger.warning("the command did not stop within %s s: killed", grace_s)
            process.kill()
