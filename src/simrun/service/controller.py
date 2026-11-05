"""The controller: follows the workflows of the runs and records their end.

The state of a run is the service's, not the one of Argo (Ruling 17b): a split task is
done when its `<task>-merge` node succeeded and failed when its `<task>-w0` or its
merge failed (after the retries of Argo), an extra worker never fails a task; an
unsplit task is its node `<task>`. When every task is done the run succeeded, when a
task failed the run failed; in both cases the controller stops the workflow, so extra
workers still waiting for admission go away, releases the reserved node and records
the reports of the workers as resource observations.

The controller keeps no state in memory: every `reconcile` reads the records of the
store, so a restarted service continues where the one before stopped.
"""

import json
import logging
import os
import re
import secrets
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from simrun.plugins import TaskType
from simrun.resources import ResourceKey, ResourceStore
from simrun.service.backend import FINAL_PHASES, Backend, BackendStatus
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.worker import WORKERS_DIR, WorkerReport
from simrun.workflow import Workflow

logger = logging.getLogger(__name__)

TERMINAL_RUN_STATES = frozenset(
    {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}
)
FINAL_TASK_STATES = frozenset({TaskState.DONE, TaskState.FAILED, TaskState.SKIPPED})
FAILED_PHASES = frozenset({"Failed", "Error"})
SKIPPED_PHASES = frozenset({"Skipped", "Omitted"})
# the names of the reports of a task recorded as observations
RECORDED_FILE = "recorded.json"
# a report whose peak memory reached this fraction of its limit was out of memory
OOM_FRACTION = 0.95


def _split_state(task_id: str, nodes: dict[str, str]) -> TaskState:
    w0 = nodes.get(f"{task_id}-w0")
    merge = nodes.get(f"{task_id}-merge")
    extra = re.compile(rf"{re.escape(task_id)}-extra(\(\d+:\d+\))?")
    extras = [phase for node, phase in nodes.items() if extra.fullmatch(node)]
    if merge == "Succeeded":
        return TaskState.DONE
    if w0 in FAILED_PHASES or merge in FAILED_PHASES:
        return TaskState.FAILED
    if w0 in SKIPPED_PHASES:
        return TaskState.SKIPPED
    if "Running" in (w0, merge, *extras) or w0 == "Succeeded":
        return TaskState.RUNNING
    if w0 is not None or merge is not None or extras:
        return TaskState.QUEUED
    return TaskState.WAITING


def _unsplit_state(phase: str | None) -> TaskState:
    if phase is None:
        return TaskState.WAITING
    if phase == "Succeeded":
        return TaskState.DONE
    if phase in FAILED_PHASES:
        return TaskState.FAILED
    if phase in SKIPPED_PHASES:
        return TaskState.SKIPPED
    if phase == "Running":
        return TaskState.RUNNING
    return TaskState.QUEUED


def task_states(
    workflow: Workflow, types: dict[str, TaskType], status: BackendStatus
) -> dict[str, TaskState]:
    """The state of every task of a workflow from the phases of its DAG nodes.

    A task is running when one of its pods runs (or its coordinator is done and the
    merge waits), queued when it has pods of which none runs, waiting without pods.

    Args:
        workflow: The workflow of the run.
        types: The task types by name.
        status: The status of the workflow in the backend.

    Returns:
        The state of every task by id.
    """
    states: dict[str, TaskState] = {}
    for task in workflow.tasks:
        if types[task.type].splittable:
            states[task.id] = _split_state(task.id, status.tasks)
        else:
            states[task.id] = _unsplit_state(status.tasks.get(task.id))
    return states


def _attempt(name: str) -> int:
    """The attempt of a report file `<worker>-<attempt>.json`."""
    try:
        return int(name.removesuffix(".json").rsplit("-", 1)[1])
    except IndexError, ValueError:
        return 0


def is_oom(report: WorkerReport, attempt: int, kills: dict[str, int]) -> bool:
    """Whether a report is of a worker which ran out of memory.

    Args:
        report: The report.
        attempt: Its attempt.
        kills: The pods killed for memory per worker, over all attempts.

    Returns:
        True when the peak memory reached `OOM_FRACTION` of the limit, or when the
        backend counts more kills of the worker than the attempt (the first attempts
        of a worker are the killed ones, the retries have more memory).
    """
    peak = report.peak_memory_bytes
    limit = report.memory_limit_bytes
    if peak is not None and limit is not None and peak >= OOM_FRACTION * limit:
        return True
    return attempt < kills.get(report.worker, 0)


def _write_json(path: Path, data: Any) -> None:
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    tmp.write_text(json.dumps(data))
    os.replace(tmp, path)


class Controller:
    """Maps the status of the backend onto the records of the runs."""

    def __init__(
        self,
        store: RunStore,
        resources: ResourceStore,
        backend: Backend,
        types: dict[str, TaskType],
    ) -> None:
        """Create the controller.

        Args:
            store: The run store.
            resources: The store of the resource observations.
            backend: The backend the workflows run on.
            types: The task types by name.
        """
        self.store = store
        self.resources = resources
        self.backend = backend
        self.types = types
        # held by every read-modify-write of a record, by the controller and the API
        self.lock = threading.RLock()

    def recover(self) -> None:
        """Fail the runs whose submission a stopped service left unfinished.

        Called once at the start of the service, which runs as one process: a record
        which is not terminal and has no backend workflow was never submitted.
        """
        with self.lock:
            for record in self.store.iter_records():
                if record.state in TERMINAL_RUN_STATES or record.backend_name:
                    continue
                logger.warning("run %s was not submitted, it fails", record.id)
                self._close(
                    record,
                    RunState.FAILED,
                    record.tasks,
                    "the service stopped while the run was submitted",
                    stop=False,
                )

    def reconcile(self) -> None:
        """Bring every record which is not terminal up to date with the backend."""
        for record in self.store.iter_records():
            if record.state in TERMINAL_RUN_STATES or record.backend_name is None:
                continue
            try:
                self._reconcile(record.id, record.backend_name)
            except Exception:
                logger.exception("reconciling run %s failed", record.id)

    def _reconcile(self, run_id: str, name: str) -> None:
        status: BackendStatus | None
        try:
            status = self.backend.status(name)
        except KeyError:
            status = None
        with self.lock:
            try:
                record = self.store.read(run_id)
            except KeyError:
                return  # deleted meanwhile
            if record.state in TERMINAL_RUN_STATES:
                return
            if status is None:
                self._record_reports(record, {})
                self._close(
                    record,
                    RunState.FAILED,
                    record.tasks,
                    f"the workflow {name} is gone",
                    stop=False,
                )
                return
            self._apply(record, status)

    def _apply(self, record: RunRecord, status: BackendStatus) -> None:
        tasks = task_states(record.workflow, self.types, status)
        self._record_reports(record, status.oom)
        stop = status.phase not in FINAL_PHASES
        failed = [t for t, s in tasks.items() if s == TaskState.FAILED]
        if failed:
            message = f"task {', '.join(failed)} failed"
            self._close(record, RunState.FAILED, tasks, message, stop=stop)
        elif all(s == TaskState.DONE for s in tasks.values()):
            self._close(record, RunState.SUCCEEDED, tasks, None, stop=stop)
        elif not stop:
            message = f"the workflow ended with phase {status.phase}"
            self._close(record, RunState.FAILED, tasks, message, stop=False)
        else:
            started = {TaskState.RUNNING, TaskState.PREEMPTED, TaskState.DONE}
            state = (
                RunState.RUNNING
                if any(s in started for s in tasks.values())
                else RunState.QUEUED
            )
            if state != record.state or tasks != record.tasks:
                self.store.write(
                    record.model_copy(update={"state": state, "tasks": tasks})
                )

    def cancel(self, run_id: str) -> RunRecord:
        """Cancel a run: terminate its workflow and release its node.

        Raises:
            KeyError: The run does not exist.
            ValueError: The run has ended.
        """
        with self.lock:
            record = self.store.read(run_id)
            if record.state in TERMINAL_RUN_STATES:
                raise ValueError(f"run {run_id} has ended: {record.state}")
            if record.backend_name is not None:
                self.backend.cancel(record.backend_name)
            return self._close(
                record, RunState.CANCELLED, record.tasks, "cancelled", stop=False
            )

    def _close(
        self,
        record: RunRecord,
        state: RunState,
        tasks: dict[str, TaskState],
        message: str | None,
        stop: bool,
    ) -> RunRecord:
        """End a run: stop its workflow, release its node and write the record.

        A failed release raises before the record is written, so the next
        reconciliation tries again.
        """
        if stop and record.backend_name is not None:
            try:
                self.backend.stop(record.backend_name)
            except Exception:
                logger.exception("stopping the workflow %s failed", record.backend_name)
        if record.reserve is not None:
            self.backend.unreserve(record.reserve)
        final = {
            task: s if s in FINAL_TASK_STATES else TaskState.SKIPPED
            for task, s in tasks.items()
        }
        closed = record.model_copy(
            update={
                "state": state,
                "tasks": final,
                "message": message,
                "finished": datetime.now(UTC),
            }
        )
        self.store.write(closed)
        logger.info("run %s %s", record.id, state.value)
        return closed

    def _record_reports(self, record: RunRecord, kills: dict[str, int]) -> None:
        """Record every report of a worker not recorded before as an observation."""
        model = record.sources[0].name if record.sources else ""
        for task in record.workflow.tasks:
            task_dir = self.store.run_dir(record.id) / "tasks" / task.id
            reports = task_dir / WORKERS_DIR
            if not reports.is_dir():
                continue
            done_file = task_dir / RECORDED_FILE
            recorded = (
                set(json.loads(done_file.read_text())) if done_file.exists() else set()
            )
            new = sorted(
                p.name
                for p in reports.iterdir()
                if p.suffix == ".json"
                and not p.name.startswith(".")
                and p.name not in recorded
            )
            if not new:
                continue
            key = ResourceKey(task.type, model, "")
            base = self.types[task.type].hint.memory_base_gb
            for name in new:
                try:
                    report = WorkerReport(**json.loads((reports / name).read_text()))
                except (ValueError, TypeError) as error:
                    logger.warning("report %s of run %s: %s", name, record.id, error)
                    continue
                oom = is_oom(report, _attempt(name), kills)
                self.resources.record(key, report, base, oom=oom)
            _write_json(done_file, sorted(recorded | set(new)))
