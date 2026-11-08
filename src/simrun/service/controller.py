"""The controller: follows the workflows of the runs and records their end.

The state of a run is the service's, not the one of Argo (Ruling 17b): a split task is
done when its `<task>-merge` node succeeded (when its coordinator `<task>-w0` merges,
`TaskPlan.coordinator_merges`, there is no such node and it is done when `<task>-w0`
succeeded) and failed when its `<task>-w0` or its merge failed (after the retries of
Argo), an extra worker never fails a task; an
unsplit task is its node `<task>`. When every task is done the run succeeded, when a
task failed the run failed; in both cases the controller stops the workflow, so extra
workers still waiting for admission go away, and releases the reserved node.

A pod is retried once with the retry memory after a kill for memory: a worker, a
merge or an unsplit task killed for memory again with the retry memory fails the run
(Ruling 35), whose message names the retry memory. A kill for memory needs the
evidence of the out of memory killer (Ruling 39): the reason `OOMKilled` of the
backend, or the report of an attempt which exited 137 counting kills of the out of
memory killer in its cgroup; a pod killed after its grace period was evicted.

The states of the tasks and the out of memory kills are stored in the record at every
poll. When the workflow is gone (deleted by its time to live before the controller saw
its end) the tasks are resolved from these states and from the store: a split task
whose merge wrote `out/.simrun-merged` is done (Ruling 23); only the tasks still
undetermined fail.

The reports of the workers of a task (of `simrun-exec` for an unsplit plugin task,
by the task id) are recorded as resource observations once the state of the task is
final, each marked in `tasks/<task>/recorded/` right after its
observation; a kill for memory of an attempt without a report is an observation of
the pod shape at its limit (Ruling 24).

The logs of the pods of a task are kept in the run store once the task is final and
its pods terminated, the ones which ended before the workflow is stopped or
cancelled (`simrun.service.logs`); an ended run stays followed (`pending_logs`) until
its logs are kept or its workflow is gone.

A terminal record is written first; a release of the node or a cancellation of the
workflow which fails stays in the record (`pending_release`, `pending_cancel`) and is
retried at the next polls, which do nothing else for a terminal run (Ruling 25). No
call of the backend is made while the record lock is held.

The controller keeps no state in memory: every `reconcile` reads the records of the
live runs of the index of the store (`simrun.service.index`), which the service
rebuilds from the records at its start, so a restarted service continues where the
one before stopped; a record which cannot be read is logged and skipped.
"""

import dataclasses
import json
import logging
import re
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from simrun import podspec
from simrun.planner import PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND
from simrun.resources import GIB, ResourceStore, resource_key
from simrun.service import logs
from simrun.service.backend import (
    FINAL_PHASES,
    PREEMPTED_PHASE,
    RETRYING_PHASE,
    Backend,
    BackendStatus,
    FailedAttempt,
)
from simrun.service.index import IndexedRunStore
from simrun.store import RunRecord, RunState, TaskState
from simrun.worker import (
    EXIT_ANALYSIS,
    EXIT_CONFIGURATION,
    EXIT_POOL_BROKEN,
    MERGED_MARKER,
    WORKERS_DIR,
    WorkerReport,
)
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
# what an exit of the harness means, in the message of a failed run
EXIT_MEANINGS = {
    EXIT_ANALYSIS: "the analysis failed",
    EXIT_CONFIGURATION: "a configuration error",
}
# seconds after the end of a run after which the logs of its pods are kept even
# when a pod has not terminated
LOGS_WAIT_S = 600.0


def _split_state(
    task_id: str, nodes: dict[str, str], coordinator_merges: bool = False
) -> TaskState:
    w0 = nodes.get(f"{task_id}-w0")
    merge = nodes.get(f"{task_id}-merge")
    if coordinator_merges and w0 == "Succeeded":
        return TaskState.DONE
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
    if RETRYING_PHASE in (w0, merge, *extras):
        return TaskState.RETRYING
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
    if phase == RETRYING_PHASE:
        return TaskState.RETRYING
    return TaskState.QUEUED


def task_plans(record: RunRecord) -> dict[str, TaskPlan]:
    """The plans of the tasks of a run from its record.

    A plan written before `coordinator_merges` existed reads it as False.
    """
    return {
        task_id: dataclasses.replace(TaskPlan(**plan), shape=PodShape(**plan["shape"]))
        for task_id, plan in record.plans.items()
    }


def task_states(
    workflow: Workflow,
    types: dict[str, TaskType],
    status: BackendStatus,
    plans: dict[str, TaskPlan],
) -> dict[str, TaskState]:
    """The state of every task of a workflow from the phases of its DAG nodes.

    A task is running when one of its pods runs (or its coordinator is done and the
    merge waits), preempted when none runs and a pod evicted waits for its retry,
    retrying when none runs and a pod which failed otherwise waits for its retry,
    queued when it has pods of which none runs, waiting without pods.

    Args:
        workflow: The workflow of the run.
        types: The task types by name.
        status: The status of the workflow in the backend.
        plans: The plans of the tasks, by id; a split task whose plan merges in the
            coordinator is done when `<task>-w0` succeeded.

    Returns:
        The state of every task by id.
    """
    states: dict[str, TaskState] = {}
    for task in workflow.tasks:
        if types[task.type].splittable:
            plan = plans.get(task.id)
            states[task.id] = _split_state(
                task.id, status.tasks, plan is not None and plan.coordinator_merges
            )
        else:
            states[task.id] = _unsplit_state(status.tasks.get(task.id))
    return states


def attempt_of(name: str) -> int:
    """The attempt of a report file `<worker>-<attempt>.json`, 0 without one."""
    try:
        return int(name.removesuffix(".json").rsplit("-", 1)[1])
    except IndexError, ValueError:
        return 0


def is_oom(report: WorkerReport, attempt: int, kills: dict[str, list[int]]) -> bool:
    """Whether a report is of a worker which was killed for memory.

    Only the evidence of the out of memory killer counts (Rulings 34, 39): a peak
    near the limit is no kill, the kernel reclaims the page cache there, and an
    exit 137 may be a kill after the grace period.

    Args:
        report: The report.
        attempt: Its attempt.
        kills: The attempts killed for memory per worker (`RunRecord.oom_kills`).

    Returns:
        True when the attempt is one killed for memory, or the report counts kills
        of the out of memory killer in its cgroup.
    """
    return report.oom_kills > 0 or attempt in kills.get(report.worker, ())


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
            # the logs of its pods, kept once they ended (or the workflow is gone)
            "pending_logs": record.backend_name is not None,
            **update,
        }
    )


class Controller:
    """Maps the status of the backend onto the records of the runs."""

    def __init__(
        self,
        store: IndexedRunStore,
        resources: ResourceStore,
        backend: Backend,
        types: dict[str, TaskType],
    ) -> None:
        """Create the controller.

        Args:
            store: The run store with its index.
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
        # resolves, builds and submits a run from its record (`Submitter.run`); a
        # submission a stopped service left is resumed with it
        self.resume: Callable[[str], None] | None = None
        self._woken = threading.Event()

    def wake(self) -> None:
        """Make the control loop reconcile now instead of at its next poll."""
        self._woken.set()

    def wait_for_wake(self, timeout_s: float) -> None:
        """Block until woken or `timeout_s` passed; clears the wake.

        Args:
            timeout_s: The longest time to wait, in seconds.
        """
        self._woken.wait(timeout_s)
        self._woken.clear()

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
        """Stop taking submissions, without waiting for the running ones.

        A build may take an hour; the pod of the service is killed long before.
        A run left submitting is resumed at the next start of the service
        (`recover`), from its record.
        """
        self._submissions.shutdown(wait=False, cancel_futures=True)

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
        started has lost its submission: it is resumed from its record
        (`resume`, idempotent: an image which was built is found, a node reserved
        for the run is kept, a workflow which was created is taken as submitted).
        A submission created later may be in flight (the API accepts runs while this
        runs) and is left to `reconcile`, which never fails a submission running in
        this process. A record which is not terminal and has no workflow name was
        written before names were stored, and fails. The pending logs, releases and
        cancellations of terminal runs are retried.
        """
        for run_id in self.store.index.live_ids():
            try:
                self._recover(run_id)
            except Exception:
                logger.exception("recovering run %s failed", run_id)

    def _recover(self, run_id: str) -> None:
        record = self.store.read(run_id)
        if record.state not in TERMINAL_RUN_STATES and record.backend_name is None:
            self._submission_lost(record)
        elif (
            record.state == RunState.SUBMITTING
            and record.created < self.started
            and not self.in_flight(run_id)
        ):
            if self.resume is None:
                self._submission_lost(record)
            else:
                logger.info("resuming the submission of run %s", run_id)
                self.start_submission(run_id, self.resume)
                return
        self.settle(run_id)

    def _submission_lost(self, record: RunRecord) -> None:
        """Fail a run whose submission did not complete, and settle it."""
        logger.warning("the submission of run %s did not complete, it fails", record.id)
        self._fail_submission(record, "the submission of the run did not complete")

    def reconcile(self) -> None:
        """Bring the records of the live runs up to date with the backend.

        The live runs are the ones of the index which have not ended or have a
        release or a cancellation pending; an ended run is not read again. Runs
        missing from the index are indexed first (`IndexedRunStore.reindex`) and
        followed also while their index rows cannot be written.
        """
        unindexed: list[str] = []
        try:
            with self.lock:
                unindexed = self.store.reindex()
        except Exception:
            logger.exception("indexing the runs missing from the index failed")
        for run_id in sorted({*self.store.index.live_ids(), *unindexed}):
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
        if status is not None:
            kept = self._keep_logs(new)
            if new.state in TERMINAL_RUN_STATES and kept:
                new = new.model_copy(update={"pending_logs": False})
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

    def _pods(
        self,
        task: TaskSpec,
        shape: PodShape | None,
        coordinator_merges: bool = False,
    ) -> list[tuple[str, PodShape | None]]:
        """The patterns of the pod keys of a task with the shapes of their pods.

        A split task whose coordinator merges has no merge pod.
        """
        task_type = self.types[task.type]
        if not task_type.splittable:
            return [(re.escape(task.id), shape)]
        if coordinator_merges:
            return [(rf"{re.escape(task.id)}-w\d+", shape)]
        merge = (
            None
            if shape is None
            else podspec.merge_shape(shape, task_type.hint.memory_base_gb)
        )
        return [
            (rf"{re.escape(task.id)}-w\d+", shape),
            (rf"{re.escape(task.id)}-merge", merge),
        ]

    def _task_of(self, record: RunRecord, key: str) -> TaskSpec | None:
        """The task of a pod key: a worker, a merge or an unsplit task."""
        for task in record.workflow.tasks:
            merges = bool(record.plans.get(task.id, {}).get("coordinator_merges"))
            if any(re.fullmatch(p, key) for p, _ in self._pods(task, None, merges)):
                return task
        return None

    def _reported_kill(self, record: RunRecord, key: str, attempt: int) -> bool:
        """Whether the report of an attempt counts kills of the out of memory killer.

        A worker (or `simrun-exec`) which exits 137 itself because a process of its
        pool (its command) died writes its report first.
        """
        task = self._task_of(record, key)
        if task is None:
            return False
        path = (
            self.store.run_dir(record.id)
            / "tasks"
            / task.id
            / WORKERS_DIR
            / f"{key}-{attempt}.json"
        )
        try:
            report = WorkerReport(**json.loads(path.read_text()))
        except FileNotFoundError:
            return False
        except (OSError, ValueError, TypeError) as error:
            logger.warning("report %s of run %s: %s", path.name, record.id, error)
            return False
        return report.oom_kills > 0

    def _memory_kills(
        self, record: RunRecord, failed: dict[str, list[FailedAttempt]]
    ) -> dict[str, list[int]]:
        """The attempts killed for memory per pod key, with the ones known before.

        An attempt was killed for memory with the evidence of the out of memory
        killer (Ruling 39): its container `OOMKilled`, or an exit 137 whose report
        counts kills of the out of memory killer. A pod killed after its grace
        period (exit 137 without either) was evicted.
        """
        kills = {key: set(attempts) for key, attempts in record.oom_kills.items()}
        for key, attempts in failed.items():
            for a in attempts:
                if a.attempt in kills.get(key, ()):
                    continue
                if a.oom_killed or (
                    a.exit_code == EXIT_POOL_BROKEN
                    and self._reported_kill(record, key, a.attempt)
                ):
                    kills.setdefault(key, set()).add(a.attempt)
        return {key: sorted(attempts) for key, attempts in sorted(kills.items())}

    def _killed_twice(
        self,
        record: RunRecord,
        failed: dict[str, list[FailedAttempt]],
        kills: dict[str, list[int]],
    ) -> tuple[str, str] | None:
        """The task and the message of a pod killed for memory with the retry memory.

        The attempt after an exit 137 has the retry memory (`RETRY_FACTOR` per
        process, the `podSpecPatch` of `simrun.argo`), so a kill for memory of
        such an attempt after a first kill fails the run instead of retrying again
        (Ruling 35). Argo knows only the last attempt: an attempt after one which
        was preempted has the first memory again, and its kill does not count
        (Ruling 39), the attempt after it has the retry memory again.
        """
        for task in record.workflow.tasks:
            shape_values = record.plans.get(task.id, {}).get("shape")
            if shape_values is None:
                continue
            merges = bool(record.plans[task.id].get("coordinator_merges"))
            pods = self._pods(task, PodShape(**shape_values), merges)
            for key, killed in kills.items():
                exits = {a.attempt: a.exit_code for a in failed.get(key, [])}
                counted = sum(
                    1
                    for i, attempt in enumerate(killed)
                    if i == 0 or exits.get(attempt - 1) == EXIT_POOL_BROKEN
                )
                if counted < 2:
                    continue
                for pattern, pod in pods:
                    if pod is not None and re.fullmatch(pattern, key):
                        return task.id, (
                            f"task {task.id}: {key} was killed for memory "
                            f"{len(killed)} times, also with the retry memory of "
                            f"{pod.retry_memory_gb} GiB ({pod.retry_per_worker_gb} "
                            "GiB per process); give the task more memory"
                        )
        return None

    def _failure(
        self,
        record: RunRecord,
        task_id: str,
        failed: dict[str, list[FailedAttempt]],
    ) -> str:
        """Why a task failed: the exit of its last failed pod, for the run message.

        Exit 3 and 4 are the analysis and a configuration error of the harness
        (`simrun.worker`); a `command` task exits as its command does.
        """
        task = next(t for t in record.workflow.tasks if t.id == task_id)
        last: tuple[str, FailedAttempt] | None = None
        for key, attempts in sorted(failed.items()):
            if not attempts or self._task_of(record, key) is not task:
                continue
            attempt = max(attempts, key=lambda a: a.attempt)
            if last is None or attempt.attempt >= last[1].attempt:
                last = (key, attempt)
        if last is None or last[1].exit_code is None:
            return f"task {task_id} failed"
        key, attempt = last
        reason = f"{key} exited {attempt.exit_code}"
        if task.type != COMMAND.name and attempt.exit_code in EXIT_MEANINGS:
            reason += f", {EXIT_MEANINGS[attempt.exit_code]}"
        return f"task {task_id} failed: {reason} (see its logs)"

    def _next(self, record: RunRecord, status: BackendStatus) -> RunRecord:
        """The record after a status of its workflow."""
        tasks = task_states(record.workflow, self.types, status, task_plans(record))
        kills = self._memory_kills(record, status.failed)
        record = record.model_copy(update={"oom_kills": kills})
        killed = self._killed_twice(record, status.failed, kills)
        if killed is not None:
            task_id, message = killed
            return closed(
                record, RunState.FAILED, {**tasks, task_id: TaskState.FAILED}, message
            )
        failed = [t for t, s in tasks.items() if s == TaskState.FAILED]
        if failed:
            message = "; ".join(
                self._failure(record, task_id, status.failed) for task_id in failed
            )
            return closed(record, RunState.FAILED, tasks, message)
        if all(s == TaskState.DONE for s in tasks.values()):
            return closed(record, RunState.SUCCEEDED, tasks, None)
        if status.phase in FINAL_PHASES:
            message = f"the workflow ended with phase {status.phase}"
            return closed(record, RunState.FAILED, tasks, message)
        started = {
            TaskState.RUNNING,
            TaskState.PREEMPTED,
            TaskState.RETRYING,
            TaskState.DONE,
        }
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
        """Retry the pending logs, release and cancellation of a terminal run.

        The logs are kept before the workflow is cancelled; a workflow which does
        not exist (any more) has no logs to keep. An unknown node counts as released
        and an unknown workflow as cancelled; a node is released only from the
        reservation of this run. A run which was deleted meanwhile is ignored.
        """
        try:
            record = self.store.read(run_id)
        except KeyError:
            return
        if record.state not in TERMINAL_RUN_STATES:
            return
        logs_kept = not record.pending_logs or record.backend_name is None
        if not logs_kept and record.backend_name is not None:
            try:
                self.backend.status(record.backend_name)
                logs_kept = self._keep_logs(record)
            except KeyError:
                logs_kept = True  # no workflow: no pods, no logs
            except Exception:
                logger.exception(
                    "the status of the workflow %s cannot be read", record.backend_name
                )
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
        if logs_kept and record.pending_logs:
            update["pending_logs"] = False
        if cancelled and record.pending_cancel:
            update["pending_cancel"] = False
        if released and record.pending_release is not None:
            update["pending_release"] = None
        if update:
            self.write_if_unchanged(record, record.model_copy(update=update))

    def _keep_logs(self, record: RunRecord) -> bool:
        """Keep the logs of the tasks in a final state whose logs are not kept yet.

        A task's logs are kept once all its pods terminated (a skipped task's pods
        stop after the workflow was stopped), or `LOGS_WAIT_S` after the end of the
        run whatever they are. Called before the record is written, so the logs of
        the pods which ended are read before the workflow is stopped or cancelled; a
        failure is logged and retried at the next poll (`pending_logs`).

        Returns:
            Whether the logs of every final task are kept.
        """
        name = record.backend_name
        if name is None:
            return True
        waited = record.finished is not None and (
            (datetime.now(UTC) - record.finished).total_seconds() >= LOGS_WAIT_S
        )
        complete = True
        for task_id, state in record.tasks.items():
            task_dir = self.store.run_dir(record.id) / "tasks" / task_id
            if state not in FINAL_TASK_STATES or logs.kept(task_dir):
                continue
            try:
                pods = self.backend.logs(name, task_id, logs.KEPT_TAIL_LINES)
                if not waited and not all(p.done for p in pods):
                    complete = False
                    continue
                logs.keep(task_dir, pods)
            except Exception:
                complete = False
                logger.exception(
                    "keeping the logs of task %s of run %s failed", task_id, record.id
                )
        return complete

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
            for w, attempts in record.oom_kills.items()
            if worker.fullmatch(w)
            for attempt in attempts
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
            oom = is_oom(report, attempt_of(name), record.oom_kills)
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
