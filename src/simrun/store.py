"""The run store: run records and the layout of a run on the shared drive.

Layout: `<store>/runs/<id>/tasks/<task>`, the record in `<store>/runs/<id>/run.json`.
"""

import logging
import os
import secrets
from collections.abc import Iterator
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from simrun.plugins import TaskPaths
from simrun.snapshot import SourceProvenance
from simrun.workflow import Priority, Workflow

logger = logging.getLogger(__name__)

RECORD_FILE = "run.json"


class RunState(StrEnum):
    """State of a run."""

    SUBMITTING = "submitting"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskState(StrEnum):
    """State of a task of a run."""

    WAITING = "waiting"
    QUEUED = "queued"
    RUNNING = "running"
    PREEMPTED = "preempted"
    RETRYING = "retrying"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunRecord(BaseModel):
    """The record of a run.

    Attributes:
        id: Run id, `YYYYMMDD-HHMMSS-<6 hex>`.
        submitted_by: Submitting user.
        owner: Owner of the run.
        agent: Agent which submitted the run.
        priority: Priority in the queue.
        group: Group of the run.
        workflow: The workflow.
        sources: Provenance of the snapshots of the sources.
        image: Container image.
        reserve: Reservation.
        state: State of the run.
        tasks: State of every task.
        plans: The task plans as dicts.
        backend_name: Name of the backend workflow.
        created: Time of creation.
        built: Time the image of the run was ready and the submission of its workflow
            began, None while its environment is resolved and built.
        finished: Time of the end.
        message: A message, e.g. the failure.
        oom_kills: The attempts of the pods killed for memory per worker (the worker
            id, the task id of an unsplit task, `<task>-merge` of a merge).
        pending_release: A reserved node whose release failed, retried by the service.
        pending_cancel: Whether the workflow must still be cancelled, e.g. one which
            may have been created by a submission that failed.
        pending_logs: Whether the logs of the pods of an ended run must still be
            kept in the store (`simrun.service.logs`).
    """

    id: str
    submitted_by: str
    owner: str
    agent: str | None
    priority: Priority
    group: str | None
    workflow: Workflow
    sources: list[SourceProvenance]
    image: str | None
    reserve: str | None
    state: RunState
    tasks: dict[str, TaskState]
    plans: dict[str, Any]
    backend_name: str | None
    created: datetime
    finished: datetime | None
    message: str | None
    built: datetime | None = None
    oom_kills: dict[str, list[int]] = Field(default_factory=dict)
    pending_release: str | None = None
    pending_cancel: bool = False
    pending_logs: bool = False


class RunSummary(BaseModel):
    """The state of a run without its workflow and plans."""

    id: str
    state: RunState
    group: str | None
    submitted_by: str
    agent: str | None
    priority: Priority
    tasks: dict[str, TaskState]
    created: datetime
    finished: datetime | None
    message: str | None

    @classmethod
    def of(cls, record: RunRecord) -> RunSummary:
        """The summary of a record."""
        return cls.model_validate(record.model_dump(include=set(cls.model_fields)))


class RunStore:
    """The run store on the shared drive."""

    def __init__(self, root: Path) -> None:
        """Create a store.

        Args:
            root: Root directory of the store.
        """
        self.root = root

    def new_run_id(self) -> str:
        """Return a new run id, `YYYYMMDD-HHMMSS-<6 hex>`."""
        return f"{datetime.now(UTC):%Y%m%d-%H%M%S}-{secrets.token_hex(3)}"

    def run_dir(self, run_id: str) -> Path:
        """Return the directory of a run."""
        return self.root / "runs" / run_id

    def task_paths(
        self, run_id: str, task_id: str, inputs: dict[str, Path], scratch: Path
    ) -> TaskPaths:
        """Return the locations of a task of a run."""
        run_dir = self.run_dir(run_id)
        task_dir = run_dir / "tasks" / task_id
        return TaskPaths(
            store=self.root,
            run_dir=run_dir,
            task_dir=task_dir,
            out=task_dir / "out",
            inputs=inputs,
            scratch=scratch,
        )

    def write(self, record: RunRecord) -> None:
        """Write a record atomically (temporary file and rename)."""
        run_dir = self.run_dir(record.id)
        run_dir.mkdir(parents=True, exist_ok=True)
        tmp = run_dir / f".{RECORD_FILE}.{secrets.token_hex(4)}.tmp"
        tmp.write_text(record.model_dump_json(indent=2))
        os.replace(tmp, run_dir / RECORD_FILE)

    def read(self, run_id: str) -> RunRecord:
        """Read a record.

        Raises:
            KeyError: When the run does not exist.
        """
        path = self.run_dir(run_id) / RECORD_FILE
        try:
            text = path.read_text()
        except FileNotFoundError:
            raise KeyError(run_id) from None
        return RunRecord.model_validate_json(text)

    def iter_records(self) -> Iterator[RunRecord]:
        """Iterate over the records of all runs, ordered by id."""
        runs = self.root / "runs"
        if not runs.is_dir():
            return
        for path in sorted(runs.iterdir()):
            try:
                yield self.read(path.name)
            except KeyError:
                continue
