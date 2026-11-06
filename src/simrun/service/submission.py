"""The submission of a run in the background: its environment, image and workflow.

`POST /runs` validates and plans a run, writes its record `submitting` (with the
deterministic name of its workflow and the settings of its tasks) and returns its id
at once. The controller then runs `Submitter.run` in its pool of submissions
(`Controller.start_submission`): it resolves the environment of the run, builds its
image when the registry does not have it (once per image: a second submission of the
same new environment waits for the first build and finds the image), records the
image and the time it was ready (`built`, from which the grace of a submission
counts), reserves the node of the run and submits its workflow; the run is queued
then.

A failure lands in the record: the run fails with a message, its node is released and
a workflow a failed submission may have created is cancelled. A run cancelled while it
is resolved or built is not submitted; one cancelled while its workflow is submitted
has the workflow cancelled; the workflow of one cancelled and deleted meanwhile is
cancelled as an orphan.
"""

import dataclasses
import logging
import shutil
import tempfile
import threading
from collections.abc import Generator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path

import simrun
from simrun.argo import to_argo
from simrun.config import ClusterConfig
from simrun.environment import Environment, dockerfile, image_ref, resolve_environment
from simrun.planner import PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.service.backend import Backend
from simrun.service.controller import TERMINAL_RUN_STATES, Controller, closed
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, RunStore

logger = logging.getLogger(__name__)

ENVIRONMENT_LOCK = "environment.lock"


class SubmissionError(Exception):
    """A submission which cannot go on; the message is the one of the record."""


def has_model_project(store: Path, sources: list[SourceProvenance]) -> bool:
    """Whether the model of a run (its first source) has a `pyproject.toml`."""
    return bool(sources) and (store / sources[0].snapshot / "pyproject.toml").is_file()


def task_plans(record: RunRecord) -> dict[str, TaskPlan]:
    """The plans of the tasks of a run from its record."""
    return {
        task_id: dataclasses.replace(TaskPlan(**plan), shape=PodShape(**plan["shape"]))
        for task_id, plan in record.plans.items()
    }


def _undo(current: RunRecord, record: RunRecord) -> dict[str, object]:
    """What a run which ended during its submission still has to undo.

    The workflow is cancelled, and the node released: the submission may have
    reserved it after the end of the run released it (a release removes only the
    reservation of its run, so it may be repeated).
    """
    return {
        "pending_cancel": True,
        "pending_release": current.pending_release or record.reserve,
        "pending_logs": True,
    }


class Submitter:
    """Resolves, builds and submits the runs written `submitting`."""

    def __init__(
        self,
        store: RunStore,
        backend: Backend,
        types: dict[str, TaskType],
        controller: Controller,
        cluster: ClusterConfig,
        python: str,
        simrun_wheel: Path | None,
    ) -> None:
        """Create the submitter.

        Args:
            store: The run store.
            backend: The backend the workflows run on.
            types: The task types by name.
            controller: The controller, whose lock guards the records.
            cluster: The cluster.
            python: Python version of the images.
            simrun_wheel: The simrun wheel the images install.
        """
        self.store = store
        self.backend = backend
        self.types = types
        self.controller = controller
        self.cluster = cluster
        self.python = python
        self.simrun_wheel = simrun_wheel
        self._reserve_lock = threading.Lock()
        self._builds_lock = threading.Lock()
        self._builds: dict[str, threading.Lock] = {}

    @contextmanager
    def _building(self, ref: str) -> Generator[None]:
        """Hold the lock of an image, so it is built once at a time."""
        with self._builds_lock:
            lock = self._builds.setdefault(ref, threading.Lock())
        with lock:
            yield

    def environment(self, record: RunRecord) -> tuple[str | None, str | None]:
        """The image of a run and the lock of its environment, built when missing.

        A run whose model has no `pyproject.toml` keeps the image it was written
        with (the default image of the service).

        Raises:
            SubmissionError: The environment cannot be resolved, the service has no
                simrun wheel to build with, or the build fails.
        """
        if not has_model_project(self.store.root, record.sources):
            return record.image, None
        model, *libraries = record.sources
        try:
            env = resolve_environment(
                self.store.root,
                model,
                libraries,
                python=self.python,
                simrun_version=simrun.__version__,
            )
        except (ValueError, RuntimeError) as error:
            raise SubmissionError(
                f"the environment cannot be resolved: {error}"
            ) from error
        ref = image_ref(env, self.cluster.registry)
        with self._building(ref):
            try:
                missing = not self.backend.has_image(ref)
            except Exception as error:
                raise SubmissionError(
                    f"the registry cannot tell whether it has {ref}: {error}"
                ) from error
            if missing:
                self._build(env, ref)
        return ref, env.lock

    def _build(self, env: Environment, ref: str) -> None:
        wheel = self.simrun_wheel
        if wheel is None:
            raise SubmissionError("the service has no simrun_wheel to build with")
        with tempfile.TemporaryDirectory(prefix="simrun-build-") as tmp:
            context = Path(tmp)
            (context / "Dockerfile").write_text(
                dockerfile(env, self.cluster.base_image_ref(), wheel.name)
            )
            shutil.copy2(wheel, context / wheel.name)
            logger.info("building the image %s", ref)
            try:
                self.backend.build_image(context, ref)
            except Exception as error:
                raise SubmissionError(
                    f"building the image {ref} failed: {error}"
                ) from error

    def run(self, run_id: str) -> None:
        """Resolve, build and submit a run written `submitting`.

        Args:
            run_id: The run.
        """
        try:
            record = self.store.read(run_id)
        except KeyError:
            return
        if record.state != RunState.SUBMITTING:
            return
        try:
            image, lock = self.environment(record)
            built = record.model_copy(
                update={"image": image, "built": datetime.now(UTC)}
            )
            if not self.controller.write_if_unchanged(record, built):
                logger.info("run %s changed while it was built: not submitted", run_id)
                return
            record = built
            if lock is not None:
                (self.store.run_dir(run_id) / ENVIRONMENT_LOCK).write_text(lock)
            try:
                manifest = to_argo(record, task_plans(record), self.types, self.cluster)
            except ValueError as error:
                raise SubmissionError(str(error)) from error
            if record.reserve is not None:
                self._reserve(record.reserve, run_id)
        except SubmissionError as error:
            self._fail(record, str(error))
            return
        try:
            self.backend.submit(manifest)
        except Exception as error:
            self._abort(record, error)
            return
        self._submitted(record)

    def _reserve(self, node: str, run_id: str) -> None:
        """Reserve the node of a run, unless another run took it meanwhile.

        Raises:
            SubmissionError: The node is unknown, reserved by another run, or its
                reservation failed.
        """
        with self._reserve_lock:
            try:
                found = next((n for n in self.backend.nodes() if n.name == node), None)
                if found is None:
                    raise SubmissionError(f"unknown node {node!r} to reserve")
                if found.reserved_by not in (None, run_id):
                    raise SubmissionError(
                        f"node {node!r} is reserved by run {found.reserved_by}"
                    )
                self.backend.reserve(node, run_id)
            except SubmissionError:
                raise
            except Exception as error:
                raise SubmissionError(
                    f"reserving node {node!r} failed: {error}"
                ) from error

    def _fail(self, record: RunRecord, message: str) -> None:
        """Fail a run whose submission cannot go on; nothing was submitted."""
        logger.error("the submission of run %s failed: %s", record.id, message)
        failed = closed(
            record, RunState.FAILED, record.tasks, message, pending_logs=False
        )
        if self.controller.write_if_unchanged(record, failed):
            self.controller.settle(record.id)

    def _abort(self, record: RunRecord, error: Exception) -> None:
        """Fail a run whose submission of the workflow failed, then settle it.

        The workflow may have been created although the submission raised, so it is
        cancelled like the node is released, retried by the controller on failure.
        """
        logger.error("submitting run %s failed: %s", record.id, error)
        failed = closed(
            record,
            RunState.FAILED,
            record.tasks,
            f"submitting failed: {error}",
            pending_cancel=True,
        )
        try:
            if not self.controller.write_if_unchanged(record, failed):
                # cancelled meanwhile: the cancellation is retried by the controller
                self._cancel_if_pending(record)
                return
        except OSError:
            logger.exception("writing the failed record of run %s failed", record.id)
            if record.reserve is not None:
                with suppress(Exception):
                    self.backend.unreserve(record.reserve, record.id)
            return
        self.controller.settle(record.id)

    def _cancel_if_pending(self, record: RunRecord) -> None:
        """Cancel the workflow of a run which ended or was deleted meanwhile."""
        with self.controller.lock:
            try:
                current: RunRecord | None = self.store.read(record.id)
            except KeyError:
                current = None
            if current is not None and current.state in TERMINAL_RUN_STATES:
                self.store.write(current.model_copy(update=_undo(current, record)))
        if current is None:
            self._cancel_orphan(record)
        else:
            self.controller.settle(record.id)

    def _submitted(self, record: RunRecord) -> None:
        """Queue a submitted run, or cancel its workflow when it ended meanwhile."""
        ended = False
        with self.controller.lock:
            try:
                current: RunRecord | None = self.store.read(record.id)
            except KeyError:
                current = None
            if current is not None:
                ended = current.state in TERMINAL_RUN_STATES
                if ended:  # cancelled while it was submitted: cancel the workflow
                    current = current.model_copy(update=_undo(current, record))
                else:
                    current = current.model_copy(update={"state": RunState.QUEUED})
                self.store.write(current)
        if current is None:
            self._cancel_orphan(record)
            return
        if ended:
            self.controller.settle(record.id)
        logger.info("run %s submitted by %s", record.id, record.submitted_by)

    def _cancel_orphan(self, record: RunRecord) -> None:
        """Undo the submission of a run cancelled and deleted while it was submitted.

        Its workflow is cancelled and its node released (the job may have reserved
        it after the run was released and deleted); no record is left to retry
        either, so both are best effort and logged.
        """
        name = record.backend_name
        logger.warning(
            "run %s was cancelled and deleted while it was submitted", record.id
        )
        if name is not None:
            try:
                self.backend.cancel(name)
            except KeyError:
                pass
            except Exception:
                logger.exception("cancelling the orphan workflow %s failed", name)
        if record.reserve is not None:
            try:
                self.backend.unreserve(record.reserve, record.id)
            except KeyError:
                pass
            except Exception:
                logger.exception(
                    "releasing node %s of the orphan run %s failed",
                    record.reserve,
                    record.id,
                )
