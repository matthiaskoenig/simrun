"""The controller: follows the workflows of the runs and records their end.

The state of a run is the service's, not the one of Argo (Ruling 17b): a split task is
done when its `<task>-merge` node succeeded and failed when its `<task>-w0` or its
merge failed (after the retries of Argo), an extra worker never fails a task; an
unsplit task is its node `<task>`. When every task is done the run succeeded, when a
task failed the run failed; in both cases the controller stops the workflow, so extra
workers still waiting for admission go away, and releases the reserved node.

A pod is retried once with the retry memory after a kill for memory: a worker, a
merge or an unsplit task the backend reports killed for memory twice fails the run
(Ruling 35), whose message names the retry memory.

The states of the tasks and the out of memory kills are stored in the record at every
poll. When the workflow is gone (deleted by its time to live before the controller saw
its end) the tasks are resolved from these states and from the store: a split task
whose merge wrote `out/.simrun-merged` is done (Ruling 23); only the tasks still
undetermined fail.

The reports of the workers of a task (of `simrun-exec` for an unsplit plugin task,
by the task id) are recorded as resource observations once the state of the task is
final, each marked in `tasks/<task>/recorded/` right after its
observation; a kill for memory the backend reports for an attempt without a report is
an observation of the pod shape at its limit (Ruling 24).

A terminal record is written first; a release of the node or a cancellation of the
workflow which fails stays in the record (`pending_release`, `pending_cancel`) and is
retried at the next polls, which do nothing else for a terminal run (Ruling 25). No
call of the backend is made while the record lock is held.

The controller keeps no state in memory: every `reconcile` reads the records of the
store, so a restarted service continues where the one before stopped; a record which
cannot be read is logged and skipped.
"""

import json
import logging
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from simrun import podspec
from simrun.planner import PodShape
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND
from simrun.resources import GIB, ResourceStore, resource_key
from simrun.service.backend import (
    FINAL_PHASES,
    PREEMPTED_PHASE,
    Backend,
    BackendStatus,
)
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.worker import MERGED_MARKER, WORKERS_DIR, WorkerReport
from simrun.workflow import TaskSpec, Workflow

logger = logging.getLogger(__name__)

TERMINAL_RUN_STATES = frozenset(
    {RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED}
)
FINAL_TASK_STATES = frozenset({TaskState.DONE, TaskState.FAILED, TaskState.SKIPPED})
FAILED_PHASES = frozenset({"Failed", "Error"})
SKIPPED_PHASES = frozenset({"Skipped", "Omitted"})
# seconds a run may stay submitting after its image was ready (or after its creation
# when it has none yet) and while no submission of it runs in this process, before
# the controller takes the submission as lost
SUBMIT_GRACE_S = 120.0
# submissions which resolve, build and submit at the same time
SUBMIT_WORKERS = 8
# a marker per report of a task recorded as an observation
RECORDED_DIR = "recorded"


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
    if PREEMPTED_PHASE in (w0, merge, *extras):
        return TaskState.PREEMPTED
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
    if phase == PREEMPTED_PHASE:
        return TaskState.PREEMPTED
    return TaskState.QUEUED


def task_states(
    workflow: Workflow, types: dict[str, TaskType], status: BackendStatus
) -> dict[str, TaskState]:
    """The state of every task of a workflow from the phases of its DAG nodes.

    A task is running when one of its pods runs (or its coordinator is done and the
    merge waits), preempted when none runs and a pod evicted waits for its retry,
    queued when it has pods of which none runs, waiting without pods.

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
    """Whether a report is of a worker which was killed for memory.

    Only the kills the backend reports count (Ruling 34): a peak near the limit is
    no kill, the kernel reclaims the page cache there.

    Args:
        report: The report.
        attempt: Its attempt.
        kills: The pods killed for memory per worker, over all attempts.

    Returns:
        True when the backend counts more kills of the worker than the attempt (the
        first attempts of a worker are the killed ones, the retries have more
        memory).
    """
    return attempt < kills.get(report.worker, 0)


def run_ids(store: RunStore) -> list[str]:
    """The ids of the runs of a store, sorted; hidden directories are no runs."""
    runs = store.root / "runs"
    if not runs.is_dir():
        return []
    return sorted(p.name for p in runs.iterdir() if not p.name.startswith("."))


def read_records(store: RunStore) -> Iterator[RunRecord]:
    """The records of a store by id; a record which cannot be read is logged."""
    for run_id in run_ids(store):
        try:
            yield store.read(run_id)
        except KeyError:
            continue
        except (ValueError, OSError) as error:
            logger.warning("the record of run %s cannot be read: %s", run_id, error)


def closed(
    record: RunRecord,
    state: RunState,
    tasks: dict[str, TaskState],
    message: str | None,
    **update: object,
) -> RunRecord:
    """The terminal record: tasks not final are skipped, the node is to release.

    Args:
        record: The record.
        state: The terminal state.
        tasks: The states of the tasks.
        message: The message, e.g. why the run failed.
        **update: Further fields of the record.

    Returns:
        The record, ended now.
    """
    final = {
        task: s if s in FINAL_TASK_STATES else TaskState.SKIPPED
        for task, s in tasks.items()
    }
    return record.model_copy(
        update={
            "state": state,
            "tasks": final,
            "message": message,
            "finished": datetime.now(UTC),
            "pending_release": record.reserve,
            **update,
        }
    )


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
        # held by every read-modify-write of a record, by the controller and the API;
        # never held during a call of the backend or a walk of the store
        self.lock = threading.RLock()
        self._reports_lock = threading.Lock()
        # a submission created before the controller started is not in flight
        self.started = datetime.now(UTC)
        self._submissions = ThreadPoolExecutor(
            max_workers=SUBMIT_WORKERS, thread_name_prefix="simrun-submit"
        )
        self._in_flight: set[str] = set()

    def start_submission(self, run_id: str, job: Callable[[str], None]) -> None:
        """Run the submission of a run in the background, see `in_flight`.

        A job which raises fails a run which is still submitting; its workflow may
        exist, so it is cancelled.

        Args:
            run_id: The run, written `submitting`.
            job: Resolves, builds and submits the run; called with its id.
        """
        with self.lock:
            self._in_flight.add(run_id)

        def run() -> None:
            try:
                job(run_id)
            except Exception as error:
                logger.exception("the submission of run %s failed", run_id)
                self._submission_failed(run_id, f"the submission failed: {error}")
            finally:
                with self.lock:
                    self._in_flight.discard(run_id)

        self._submissions.submit(run)

    def in_flight(self, run_id: str) -> bool:
        """Whether the submission of a run runs in this process."""
        with self.lock:
            return run_id in self._in_flight

    def close(self) -> None:
        """Wait for the running submissions; the ones not started are dropped.

        A dropped run stays submitting and fails at the next start of the service
        (`recover`).
        """
        self._submissions.shutdown(wait=True, cancel_futures=True)

    def _submission_failed(self, run_id: str, message: str) -> None:
        """Fail a run which is still submitting."""
        try:
            record = self.store.read(run_id)
        except KeyError:
            return
        if record.state == RunState.SUBMITTING:
            self._fail_submission(record, message)

    def _fail_submission(self, record: RunRecord, message: str) -> None:
        """Fail a submitting run unless it changed, cancel its workflow, settle it."""
        failed = closed(
            record,
            RunState.FAILED,
            record.tasks,
            message,
            pending_cancel=record.backend_name is not None,
        )
        if self.write_if_unchanged(record, failed):
            self.settle(record.id)

    def recover(self) -> None:
        """Settle what a stopped service left: called at the start of the service.

        The service is one process, so a run submitting since before the controller
        started has lost its submission: it fails, its workflow (named before the
        submission) is cancelled and its node released. A submission created later
        may be in flight (the API accepts runs while this runs) and is left to
        `reconcile`, which never fails a submission running in this process. A record which is not terminal and has no workflow name
        was written before names were stored, and fails too. The pending releases and
        cancellations of terminal runs are retried.
        """
        for run_id in run_ids(self.store):
            try:
                self._recover(run_id)
            except Exception:
                logger.exception("recovering run %s failed", run_id)

    def _recover(self, run_id: str) -> None:
        record = self.store.read(run_id)
        lost = record.state == RunState.SUBMITTING and record.created < self.started
        if lost or (
            record.state not in TERMINAL_RUN_STATES and record.backend_name is None
        ):
            self._submission_lost(record)
        self.settle(run_id)

    def _submission_lost(self, record: RunRecord) -> None:
        """Fail a run whose submission did not complete, and settle it."""
        logger.warning("the submission of run %s did not complete, it fails", record.id)
        self._fail_submission(record, "the submission of the run did not complete")

    def reconcile(self) -> None:
        """Bring every record up to date with the backend, one run at a time."""
        for run_id in run_ids(self.store):
            try:
                self._reconcile(run_id)
            except Exception:
                logger.exception("reconciling run %s failed", run_id)

    def _reconcile(self, run_id: str) -> None:
        record = self.store.read(run_id)
        if record.state in TERMINAL_RUN_STATES:
            self.settle(run_id)
            return
        if record.state == RunState.SUBMITTING:
            if self.in_flight(run_id):
                return  # resolved, built or submitted: its workflow may not exist yet
            since = record.built or record.created
            if (datetime.now(UTC) - since).total_seconds() >= SUBMIT_GRACE_S:
                self._submission_lost(record)
            return
        name = record.backend_name
        if name is None:
            return
        status: BackendStatus | None
        try:
            status = self.backend.status(name)
        except KeyError:
            status = None
        if status is None:
            new = self._gone(record, name)
            stop = False
        else:
            new = self._next(record, status)
            stop = status.phase not in FINAL_PHASES
        # recorded before the record is written: a failure is retried at the next poll
        self._record_reports(new)
        if new == record or not self.write_if_unchanged(record, new):
            return
        if new.state in TERMINAL_RUN_STATES:
            logger.info("run %s %s", run_id, new.state.value)
            if stop:
                try:
                    self.backend.stop(name)
                except Exception:
                    logger.exception("stopping the workflow %s failed", name)
            self.settle(run_id)

    def write_if_unchanged(self, before: RunRecord, after: RunRecord) -> bool:
        """Write `after` when the record on disk is still `before`.

        Returns:
            Whether `after` was written; not when the run changed or is gone.
        """
        with self.lock:
            try:
                current = self.store.read(before.id)
            except KeyError:
                return False
            if current != before:
                return False
            self.store.write(after)
            return True

    def _killed_twice(
        self, record: RunRecord, oom: dict[str, int]
    ) -> tuple[str, str] | None:
        """The task and the message of a pod killed for memory more than once.

        The retry after a kill has the retry memory (`RETRY_FACTOR` per process), so
        a second kill of the same worker, merge or unsplit task is a kill with it:
        the run fails instead of retrying again (Ruling 35).
        """
        for task in record.workflow.tasks:
            shape_values = record.plans.get(task.id, {}).get("shape")
            if shape_values is None:
                continue
            shape = PodShape(**shape_values)
            task_type = self.types[task.type]
            if task_type.splittable:
                merge = podspec.merge_shape(shape, task_type.hint.memory_base_gb)
                pods = [(rf"{re.escape(task.id)}-w\d+", shape)]
                pods.append((rf"{re.escape(task.id)}-merge", merge))
            else:
                pods = [(re.escape(task.id), shape)]
            for key, count in sorted(oom.items()):
                if count < 2:
                    continue
                for pattern, pod in pods:
                    if re.fullmatch(pattern, key):
                        return task.id, (
                            f"task {task.id}: {key} was killed for memory {count} "
                            f"times, also with the retry memory of "
                            f"{pod.retry_memory_gb} GiB ({pod.retry_per_worker_gb} "
                            "GiB per process); give the task more memory"
                        )
        return None

    def _next(self, record: RunRecord, status: BackendStatus) -> RunRecord:
        """The record after a status of its workflow."""
        tasks = task_states(record.workflow, self.types, status)
        record = record.model_copy(update={"oom_kills": dict(status.oom)})
        killed = self._killed_twice(record, status.oom)
        if killed is not None:
            task_id, message = killed
            return closed(
                record, RunState.FAILED, {**tasks, task_id: TaskState.FAILED}, message
            )
        failed = [t for t, s in tasks.items() if s == TaskState.FAILED]
        if failed:
            message = f"task {', '.join(failed)} failed"
            return closed(record, RunState.FAILED, tasks, message)
        if all(s == TaskState.DONE for s in tasks.values()):
            return closed(record, RunState.SUCCEEDED, tasks, None)
        if status.phase in FINAL_PHASES:
            message = f"the workflow ended with phase {status.phase}"
            return closed(record, RunState.FAILED, tasks, message)
        started = {TaskState.RUNNING, TaskState.PREEMPTED, TaskState.DONE}
        state = (
            RunState.RUNNING
            if any(s in started for s in tasks.values())
            else RunState.QUEUED
        )
        return record.model_copy(update={"state": state, "tasks": tasks})

    def _gone(self, record: RunRecord, name: str) -> RunRecord:
        """The record of a run whose workflow is gone, resolved from the store."""
        tasks: dict[str, TaskState] = {}
        undetermined: list[str] = []
        for task in record.workflow.tasks:
            last = record.tasks.get(task.id, TaskState.WAITING)
            out = self.store.run_dir(record.id) / "tasks" / task.id / "out"
            if last in FINAL_TASK_STATES:
                tasks[task.id] = last
            elif self.types[task.type].splittable and (out / MERGED_MARKER).is_file():
                tasks[task.id] = TaskState.DONE
            else:
                tasks[task.id] = TaskState.FAILED
                undetermined.append(task.id)
        failed = [t for t, s in tasks.items() if s == TaskState.FAILED]
        if undetermined:
            message = f"the workflow {name} is gone: {', '.join(undetermined)} unknown"
        elif failed:
            message = f"task {', '.join(failed)} failed"
        elif all(s == TaskState.DONE for s in tasks.values()):
            return closed(record, RunState.SUCCEEDED, tasks, None)
        else:
            message = f"the workflow {name} is gone"
        return closed(record, RunState.FAILED, tasks, message)

    def cancel(self, run_id: str) -> RunRecord:
        """Cancel a run: write it cancelled, then cancel its workflow, release its node.

        Raises:
            KeyError: The run does not exist.
            ValueError: The run has ended.
        """
        with self.lock:
            record = self.store.read(run_id)
            if record.state in TERMINAL_RUN_STATES:
                raise ValueError(f"run {run_id} has ended: {record.state}")
            cancelled = closed(
                record,
                RunState.CANCELLED,
                record.tasks,
                "cancelled",
                pending_cancel=record.backend_name is not None,
            )
            self.store.write(cancelled)
        self.settle(run_id)
        return self.store.read(run_id)

    def settle(self, run_id: str) -> None:
        """Retry the pending release and cancellation of a terminal run.

        An unknown node counts as released and an unknown workflow as cancelled; a
        node is released only from the reservation of this run. A run which was
        deleted meanwhile is ignored.
        """
        try:
            record = self.store.read(run_id)
        except KeyError:
            return
        if record.state not in TERMINAL_RUN_STATES:
            return
        cancelled = not record.pending_cancel or record.backend_name is None
        if not cancelled and record.backend_name is not None:
            try:
                self.backend.cancel(record.backend_name)
                cancelled = True
            except KeyError:
                cancelled = True
            except Exception:
                logger.exception(
                    "cancelling the workflow %s failed", record.backend_name
                )
        released = record.pending_release is None
        if record.pending_release is not None:
            try:
                self.backend.unreserve(record.pending_release, record.id)
                released = True
            except KeyError:
                logger.info(
                    "node %s is unknown, taken as released", record.pending_release
                )
                released = True
            except Exception:
                logger.exception("releasing node %s failed", record.pending_release)
        update: dict[str, object] = {}
        if cancelled and record.pending_cancel:
            update["pending_cancel"] = False
        if released and record.pending_release is not None:
            update["pending_release"] = None
        if update:
            self.write_if_unchanged(record, record.model_copy(update=update))

    def _record_reports(self, record: RunRecord) -> None:
        """Record the reports of the tasks in a final state not recorded before."""
        with self._reports_lock:
            for task in record.workflow.tasks:
                # a command task has the memory of its settings, nothing to learn
                if (
                    record.tasks.get(task.id) in FINAL_TASK_STATES
                    and task.type != COMMAND.name
                ):
                    self._record_task(record, task)

    def _record_task(self, record: RunRecord, task: TaskSpec) -> None:
        task_id = task.id
        task_dir = self.store.run_dir(record.id) / "tasks" / task_id
        reports_dir = task_dir / WORKERS_DIR
        marks = task_dir / RECORDED_DIR
        reports = (
            sorted(
                p.name
                for p in reports_dir.iterdir()
                if p.suffix == ".json" and not p.name.startswith(".")
            )
            if reports_dir.is_dir()
            else []
        )
        # the workers of a split task, an unsplit task by its id (`simrun-exec`)
        worker = re.compile(rf"{re.escape(task_id)}(-w\d+)?")
        kills = {
            (w, attempt)
            for w, count in record.oom_kills.items()
            if worker.fullmatch(w)
            for attempt in range(count)
        }
        if not reports and not kills:
            return
        task_type = self.types[task.type]
        model = record.sources[0].name if record.sources else ""
        key = resource_key(task_type, task.settings, model)
        base = task_type.hint.memory_base_gb
        marks.mkdir(exist_ok=True)
        for name in reports:
            if (marks / name).exists():
                continue
            try:
                report = WorkerReport(**json.loads((reports_dir / name).read_text()))
            except (ValueError, TypeError) as error:
                logger.warning("report %s of run %s: %s", name, record.id, error)
                (marks / name).touch()
                continue
            oom = is_oom(report, _attempt(name), record.oom_kills)
            self.resources.record(key, report, base, oom=oom, run=record.id)
            (marks / name).touch()
        shape = record.plans.get(task_id, {}).get("shape")
        for w, attempt in sorted(kills):
            mark = marks / f"{w}-{attempt}.oom"
            if f"{w}-{attempt}.json" in reports or mark.exists() or shape is None:
                continue
            report = WorkerReport(
                worker=w,
                node="",
                processes=shape["processes"],
                memory_limit_bytes=int(shape["memory_gb"] * GIB),
            )
            self.resources.record(key, report, base, oom=True, run=record.id)
            mark.touch()
