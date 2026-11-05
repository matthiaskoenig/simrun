"""The worker harness: a pod of a split task computes units of the ledger in a pool.

`simrun-worker` (`main`) claims units of the ledger of its task, computes them in a
local process pool with the functions of the task type and writes a report;
`simrun-merge` (`merge_main`) merges the units once the ledger is complete. A pod
is evicted with SIGTERM and killed after its grace period, and is restarted with the
same worker id, which releases the claims of the incarnation before.
"""

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
import threading
import time
import uuid
from collections.abc import Generator, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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
DEFAULT_SCRATCH = "/scratch"
THREAD_VARIABLES = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS")
POLL_S = 0.2

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
        peak_memory_bytes: The peak memory of the cgroup.
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


def _init_process(setup: str | None, settings: BaseModel, paths: TaskPaths) -> None:
    """Start a process of the pool: ignore the signals, then run the setup."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if setup is not None:
        resolve(setup)(settings, paths)


def _compute(ref: str, settings: BaseModel, index: int, paths: TaskPaths) -> None:
    """Compute one unit in a process of the pool."""
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


def write_report(report: WorkerReport, task_dir: Path) -> Path:
    """Write the report as `task_dir/workers/<worker>.json` (temporary file, rename).

    Returns:
        The path of the report.
    """
    directory = task_dir / WORKERS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{report.worker}.json"
    tmp = directory / f".{report.worker}.json.{uuid.uuid4().hex}.tmp"
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


def _compute_units(
    task_type: TaskType,
    settings: BaseModel,
    paths: TaskPaths,
    units: _Units,
    n_processes: int,
    stop: threading.Event,
    grace_s: float,
) -> None:
    """Keep `n_processes` units in flight until none is pending or the worker stops."""
    assert task_type.compute_unit is not None
    pool = ProcessPoolExecutor(
        max_workers=n_processes,
        mp_context=multiprocessing.get_context("forkserver"),
        initializer=_init_process,
        initargs=(task_type.setup, settings, paths),
    )
    in_flight: dict[Future[None], int] = {}
    deadline: float | None = None
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
            while claiming and units.broken is None and len(in_flight) < n_processes:
                index = units.ledger.claim(units.worker)
                if index is None:
                    break
                try:
                    future = pool.submit(
                        _compute, task_type.compute_unit, settings, index, paths
                    )
                except BrokenProcessPool as error:
                    units.broken = error
                    units.release(index)
                    break
                in_flight[future] = index
            if not in_flight or units.failed is not None or units.broken is not None:
                break
            timeout = POLL_S
            if deadline is not None:
                timeout = min(timeout, deadline - time.monotonic())
                if timeout <= 0:
                    break
            finished, _ = wait(in_flight, timeout=timeout, return_when=FIRST_COMPLETED)
            for future in finished:
                units.finished(future, in_flight.pop(future))
    finally:
        if in_flight:
            # the processes ignore SIGTERM: kill them, then free their units
            pool.kill_workers()
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
    grace_s: float = 60.0,
) -> WorkerReport:
    """Compute units of the ledger of a task until none is pending.

    Releases the claims of an earlier incarnation of `worker`, creates the ledger
    `task_dir/units`, keeps `processes(...)` units in flight in a pool of processes
    which run the setup of the task type once, completes a unit after its function
    returned and appends a `unit_done` line to `task_dir/progress.jsonl`. On SIGTERM
    it stops claiming, waits for the running units up to `grace_s` and releases the
    rest. The report is written in every case.

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
        try:
            _compute_units(
                task_type, settings, paths, units, n_processes, stop, grace_s
            )
        finally:
            report = WorkerReport(
                worker=worker,
                node=os.environ.get("SIMRUN_NODE") or socket.gethostname(),
                processes=n_processes,
                units_done=sorted(units.done),
                seconds=time.monotonic() - start,
                peak_memory_bytes=cgroup.memory_peak(),
                memory_limit_bytes=cgroup.memory_limit(),
                per_worker_gb=per_worker_gb,
            )
            write_report(report, paths.task_dir)
    if units.failed is not None:
        raise UnitError(units.failed)
    if units.broken is not None:
        raise units.broken
    if stop.is_set():
        raise Terminated(f"worker {worker} stopped on SIGTERM")
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

    The task directory is `<store>/runs/<run>/<task>` (`SIMRUN_TASK_DIR`), the
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
    run_dir = task_dir.parent
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


def install_editable(snapshots: list[str]) -> None:
    """Install snapshots of sources as editable packages, without dependencies.

    Args:
        snapshots: The paths of the snapshots; nothing is installed without one.

    Raises:
        subprocess.CalledProcessError: The installation failed.
    """
    if not snapshots:
        return
    command = [
        "uv",
        "pip",
        "install",
        "--python",
        sys.executable,
        "--no-deps",
        "--no-index",
    ]
    for snapshot in snapshots:
        command += ["-e", snapshot]
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

    Exit codes: 0 done, 3 a unit failed (logged and released), 4 a configuration
    error, 137 a process of the pool died (retried with more memory), 143 stopped on
    SIGTERM (retried).
    """
    _logging()
    env = os.environ
    try:
        install_editable(
            [p.strip() for p in env.get("SIMRUN_EDITABLE", "").split(",") if p.strip()]
        )
        task_type, settings, paths = _task(env)
        worker = env.get("SIMRUN_WORKER")
        if not worker:
            raise ConfigurationError("SIMRUN_WORKER is not set")
        stage(_pairs(env.get("SIMRUN_STAGE", "")), paths.store, paths.scratch)
        base_gb = _gb(env, "SIMRUN_MEMORY_BASE_GB", task_type.hint.memory_base_gb)
        per_worker_gb = _gb(
            env, "SIMRUN_MEMORY_PER_WORKER_GB", task_type.hint.memory_per_worker_gb
        )
    except CONFIGURATION_ERRORS as error:
        logger.error("configuration error: %s", error)
        sys.exit(EXIT_CONFIGURATION)
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
        )
    except UnitError:
        sys.exit(EXIT_ANALYSIS)
    except BrokenProcessPool:
        sys.exit(EXIT_POOL_BROKEN)
    except Terminated as error:
        logger.info("%s", error)
        sys.exit(EXIT_TERMINATED)
    except ValueError as error:
        logger.error("configuration error: %s", error)
        sys.exit(EXIT_CONFIGURATION)
    sys.exit(EXIT_DONE)


def merge_main() -> None:
    """Run `simrun-merge` after the workers of a split task; exits with its code.

    Exit codes: 0 merged, 3 the ledger is incomplete or the merge failed, 4 a
    configuration error.
    """
    _logging()
    env = os.environ
    try:
        install_editable(
            [p.strip() for p in env.get("SIMRUN_EDITABLE", "").split(",") if p.strip()]
        )
        task_type, settings, paths = _task(env)
    except CONFIGURATION_ERRORS as error:
        logger.error("configuration error: %s", error)
        sys.exit(EXIT_CONFIGURATION)
    try:
        run_merge(task_type, settings, paths)
    except Exception:
        logger.exception("the merge of %s failed", paths.task_dir)
        sys.exit(EXIT_ANALYSIS)
    sys.exit(EXIT_DONE)
