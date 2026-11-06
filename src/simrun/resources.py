"""Resources learned from the reports of workers.

An observation is the memory per worker process and the seconds of a unit in one
process of one report of a worker, keyed by the task type, the model and a variant. A request is
resolved from the last `WINDOW` observations of its key: the 95th percentile of the
measured memory per worker times `MARGIN`, and at least `RETRY_FACTOR` times the
memory per worker of a pod killed for memory, until a later run of the key
succeeded (its measurement then says what the key needs). The memory measured is
anonymous memory (see `simrun.worker.MemorySampler`), and a worker counts as out of
memory only when the backend reports its pod killed for memory.
"""

import json
import logging
import math
import sqlite3
import time
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from simrun.plugins import ResourceHint, TaskType
from simrun.worker import WorkerReport
from simrun.workflow import ResourceOverride

logger = logging.getLogger(__name__)

RETRY_FACTOR = 1.5
MARGIN = 1.10
DEFAULT_PER_WORKER_GB = 6.0
GIB = 2**30
PERCENTILE = 0.95
MIN_PER_WORKER_GB = 0.1
# the observations of a key a request is resolved from, the most recent ones
WINDOW = 20


@dataclass(frozen=True)
class ResourceKey:
    """What observations of a task are grouped by.

    Attributes:
        task_type: Name of the task type.
        model: Name of the first source, empty without.
        variant: A setting the plugin names as cost relevant, empty by default.
    """

    task_type: str
    model: str
    variant: str = ""

    def text(self) -> str:
        """Returns the key as stored."""
        return "\x1f".join((self.task_type, self.model, self.variant))


def resource_key(
    task_type: TaskType, settings: Mapping[str, Any], model: str
) -> ResourceKey:
    """The key of the observations of a task, the one place it is derived.

    Args:
        task_type: The type of the task.
        settings: The settings of the task as submitted.
        model: The name of the model, the first source of the run, empty without.

    Returns:
        The key; its variant is the value of the `cost_setting` of the type (the
        default of the settings when the task does not set it), text as is and other
        values as JSON, empty for a type without one.
    """
    variant = ""
    if task_type.cost_setting is not None:
        try:
            values = task_type.settings.model_validate(settings).model_dump(mode="json")
        except ValidationError:
            values = dict(settings)
        value = values.get(task_type.cost_setting)
        variant = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
    return ResourceKey(task_type.name, model, variant)


@dataclass(frozen=True)
class ResolvedResources:
    """The resources requested for a task.

    Attributes:
        memory_base_gb: Base memory in GiB.
        memory_per_worker_gb: Memory per worker in GiB.
        scratch_gb: Local scratch in GiB.
        gpus: Number of GPUs.
        seconds_per_unit: Median seconds of a unit in one process, None without
            records.
        samples: Number of memory observations the memory was learned from.
        source: `learned from N runs`, `plugin hint`, `default` or `override`.
    """

    memory_base_gb: float
    memory_per_worker_gb: float
    scratch_gb: float
    gpus: int
    seconds_per_unit: float | None
    samples: int
    source: str


def _percentile(values: list[float], q: float) -> float:
    """Returns the percentile with linear interpolation of sorted values."""
    ordered = sorted(values)
    pos = q * (len(ordered) - 1)
    low = math.floor(pos)
    high = math.ceil(pos)
    return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)


def _round_up(value: float) -> float:
    """Rounds up to 0.1, ignoring floating point noise."""
    return math.ceil(round(value * 10, 9)) / 10


def _median(values: list[float]) -> float:
    return _percentile(values, 0.5)


class ResourceStore:
    """Observations of the workers of past tasks in a SQLite file.

    The file must be on a local volume. A connection is opened per call, so the
    store is safe to use from several threads of one process.
    """

    def __init__(self, db: Path) -> None:
        """Creates the database if needed.

        Args:
            db: Path of the SQLite file.
        """
        self.db = db
        db.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(
                "CREATE TABLE IF NOT EXISTS observations ("
                "key TEXT NOT NULL, per_worker_gb REAL, seconds_per_unit REAL, "
                "oom INTEGER NOT NULL, created REAL NOT NULL, "
                "run TEXT NOT NULL DEFAULT '')"
            )
            columns = {row[1] for row in con.execute("PRAGMA table_info(observations)")}
            if "run" not in columns:  # a store of an earlier version
                con.execute(
                    "ALTER TABLE observations ADD COLUMN run TEXT NOT NULL DEFAULT ''"
                )
            con.execute(
                "CREATE INDEX IF NOT EXISTS observations_key ON observations (key)"
            )
            con.commit()

    def _connect(self) -> closing[sqlite3.Connection]:
        con = sqlite3.connect(self.db, timeout=30)
        return closing(con)

    def record(
        self,
        key: ResourceKey,
        report: WorkerReport,
        base_gb: float,
        oom: bool = False,
        run: str = "",
    ) -> None:
        """Records the report of a worker.

        Args:
            key: The key of the task.
            report: The report.
            base_gb: The base memory of the task in GiB.
            oom: Whether the backend reports the worker killed for memory; the limit
                is used then.
            run: The id of the run of the report.
        """
        measured = report.memory_limit_bytes if oom else report.peak_memory_bytes
        per_worker = None
        if measured is None:
            if oom:
                logger.warning("Out of memory report of %s without a memory limit", key)
        else:
            value = (measured / GIB - base_gb) / max(report.processes, 1)
            if value > 0:
                per_worker = value
        # the time of a unit in one process, which an estimate multiplies by the
        # rounds of the units over the processes of the pods
        seconds = None
        if report.units_done:
            seconds = report.unit_seconds / len(report.units_done)
        if per_worker is None and seconds is None:
            return
        with self._connect() as con:
            con.execute(
                "INSERT INTO observations "
                "(key, per_worker_gb, seconds_per_unit, oom, created, run) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (key.text(), per_worker, seconds, int(oom), time.time(), run),
            )
            con.commit()

    def resolve(
        self, key: ResourceKey, hint: ResourceHint, override: ResourceOverride | None
    ) -> ResolvedResources:
        """Returns the resources to request.

        Args:
            key: The key of the task.
            hint: The hint of the plugin.
            override: The override of the workflow.

        Returns:
            The resolved resources; an override wins over learned values.
        """
        with self._connect() as con:
            rows = con.execute(
                "SELECT per_worker_gb, seconds_per_unit, oom, run FROM observations "
                "WHERE key = ? ORDER BY rowid DESC LIMIT ?",
                (key.text(), WINDOW),
            ).fetchall()
        rows.reverse()  # oldest first
        measured = [r[0] for r in rows if r[0] is not None and not r[2]]
        seconds = [r[1] for r in rows if r[1] is not None]
        # a kill counts until a later run of the key succeeded
        ooms = [
            r[0]
            for i, r in enumerate(rows)
            if r[0] is not None
            and r[2]
            and not any(not later[2] and later[3] != r[3] for later in rows[i + 1 :])
        ]
        if measured or ooms:
            wanted = _percentile(measured, PERCENTILE) * MARGIN if measured else 0.0
            if ooms:
                wanted = max(wanted, max(ooms) * RETRY_FACTOR)
            per_worker = max(MIN_PER_WORKER_GB, _round_up(wanted))
            source = f"learned from {len(measured) + len(ooms)} runs"
        elif hint.memory_per_worker_gb != DEFAULT_PER_WORKER_GB:
            per_worker = hint.memory_per_worker_gb
            source = "plugin hint"
        else:
            per_worker = DEFAULT_PER_WORKER_GB
            source = "default"
        if override is not None and override.memory_per_worker_gb is not None:
            per_worker = override.memory_per_worker_gb
            source = "override"
        scratch = hint.scratch_gb
        gpus = hint.gpus
        if override is not None:
            if override.scratch_gb is not None:
                scratch = override.scratch_gb
            if override.gpus is not None:
                gpus = override.gpus
        return ResolvedResources(
            memory_base_gb=hint.memory_base_gb,
            memory_per_worker_gb=per_worker,
            scratch_gb=scratch,
            gpus=gpus,
            seconds_per_unit=_median(seconds) if seconds else None,
            samples=len(measured) + len(ooms),
            source=source,
        )
