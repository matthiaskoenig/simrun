"""The client of the service: snapshots working trees, then calls the API.

The CLI (`simrun.cli`) and the MCP server (`simrun.mcp`) both go through `Client`.
"""

import json
import logging
import os
import tarfile
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import ValidationError

from simrun.local import LOCAL_DIR, ensure_local
from simrun.plugins import TaskType, load_task_types, resolve
from simrun.service.app import (
    CleanupResult,
    OutputFile,
    RunSummary,
    TaskEstimate,
    TaskProgress,
    Usage,
    ValidationResult,
)
from simrun.service.compare import CompareTable
from simrun.service.controller import TERMINAL_RUN_STATES
from simrun.service.edits import (
    EditRequest,
    EditResult,
    EditSession,
    EditState,
    OpenEdit,
)
from simrun.service.logs import DEFAULT_TAIL_LINES
from simrun.snapshot import SourceProvenance, provenance, snapshot
from simrun.store import RunRecord, RunState
from simrun.workflow import SplitMode, Workflow, load_workflow

logger = logging.getLogger(__name__)

URL_ENV = "SIMRUN_URL"
TOKEN_ENV = "SIMRUN_TOKEN"
STORE_ENV = "SIMRUN_STORE"
# one long poll of the service, below its maximum of 300 s
POLL_S = 30.0
REQUEST_TIMEOUT_S = 60.0
BACKOFF_START_S = 1.0
REFOLLOW_S = 0.2
BACKOFF_MAX_S = 10.0
# answers of a proxy or a restarting service, retried by `wait` and `simrun edit`
RETRY_STATUS = (502, 503, 504)


def _apply_event(current: dict[str, Any], event: str, data: str) -> bool:
    """Put an event of the stream of a run into `current`; whether it changed."""
    try:
        if event == "run":
            current["run"] = RunRecord.model_validate_json(data)
        elif event == "progress":
            raw = json.loads(data)
            current["progress"] = {
                k: TaskProgress.model_validate(v) for k, v in raw.items()
            }
        else:
            return False
    except ValueError:
        return False
    return "run" in current


def plan_tasks(workflow: Workflow, types: dict[str, TaskType]) -> Workflow:
    """The workflow with the settings of its tasks planned by their types.

    A task whose type is in `types` and has a `plan` gets the settings the plan
    returns for its validated settings, the fields which were given or which the
    plan set, dumped as JSON: the defaults stay with the plugin of the snapshot,
    which validates the settings in the pods. Every other task is kept as written,
    e.g. a task of a type which is not installed locally.

    Args:
        workflow: The workflow, which is not changed.
        types: The task types installed locally, by name.

    Returns:
        A copy of the workflow with the planned settings.

    Raises:
        ValueError: The settings of a task are invalid for its type.
    """
    tasks = []
    for task in workflow.tasks:
        task_type = types.get(task.type)
        if task_type is None or task_type.plan is None:
            tasks.append(task)
            continue
        try:
            settings = task_type.settings.model_validate(task.settings)
        except ValidationError as error:
            raise ValueError(
                f"task {task.id!r} of type {task.type!r}: {error}"
            ) from error
        planned = resolve(task_type.plan)(settings)
        logger.debug("task %s planned by %s", task.id, task_type.plan)
        tasks.append(
            task.model_copy(
                update={"settings": planned.model_dump(mode="json", exclude_unset=True)}
            )
        )
    return workflow.model_copy(update={"tasks": tasks})


class ClientError(Exception):
    """The service refused a request.

    Attributes:
        status: The HTTP status.
        detail: The message of the service.
    """

    def __init__(self, status: int, detail: str) -> None:
        """Create the error."""
        super().__init__(f"{status}: {detail}")
        self.status = status
        self.detail = detail


def _detail(response: httpx.Response) -> str:
    try:
        detail = response.json()["detail"]
    except ValueError, KeyError, TypeError:
        return response.text or response.reason_phrase
    if isinstance(detail, list):
        return "; ".join(
            f"{'.'.join(str(p) for p in d.get('loc', []))}: {d.get('msg', d)}"
            if isinstance(d, dict)
            else str(d)
            for d in detail
        )
    return str(detail)


class Client:
    """A client of the service of one user or agent.

    Args:
        url: Base URL of the service.
        token: Bearer token.
        store: The run store as mounted locally, where snapshots are written; None
            for a client which sends no snapshots.
        sources: How a source is sent: `store` copies a snapshot into `store`,
            `provenance` sends its provenance only (the local service runs the
            tasks with the environment of the command), `upload` is not
            implemented.
        transport: An HTTP transport, e.g. for tests.
    """

    def __init__(
        self,
        url: str,
        token: str,
        store: Path | None,
        sources: Literal["store", "upload", "provenance"] = "store",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Create the client."""
        self.url = url.rstrip("/")
        self.store = store
        self.sources = sources
        self._http = httpx.Client(
            base_url=url,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
            timeout=REQUEST_TIMEOUT_S,
        )

    @classmethod
    def from_env(cls) -> Client:
        """The client of `SIMRUN_URL`, `SIMRUN_TOKEN` and `SIMRUN_STORE`.

        Raises:
            RuntimeError: When one of them is not set.
        """
        values: dict[str, str] = {}
        for name in (URL_ENV, TOKEN_ENV, STORE_ENV):
            value = os.environ.get(name)
            if not value:
                raise RuntimeError(f"{name} is not set")
            values[name] = value
        return cls(values[URL_ENV], values[TOKEN_ENV], Path(values[STORE_ENV]))

    @classmethod
    def local(cls, root: Path, snapshot: bool = False) -> Client:
        """The client of the local service of a project directory.

        The service is started in the background when none runs.

        Args:
            root: The project directory, where `.simrun/` is.
            snapshot: Copy the sources into the local store and install them as on
                the cluster; else only their provenance is recorded.

        Raises:
            RuntimeError: The service did not start.
        """
        service = ensure_local(root)
        if snapshot:
            store = root.absolute() / LOCAL_DIR / "store"
            return cls(service.url, service.token, store, "store")
        return cls(service.url, service.token, None, "provenance")

    def source(self, repo: Path, name: str) -> SourceProvenance:
        """The provenance of a source, its snapshot written to the store first.

        Raises:
            RuntimeError: When the client sends no snapshots and records no
                provenance, or git fails.
        """
        if self.sources == "provenance":
            return provenance(repo, name)
        if self.sources == "store" and self.store is not None:
            return snapshot(repo, self.store, name)
        raise RuntimeError(f"sources {self.sources!r} are not available")

    def close(self) -> None:
        """Close the connections."""
        self._http.close()

    def _call(self, method: str, path: str, **kw: Any) -> httpx.Response:
        response = self._http.request(method, path, **kw)
        if response.is_error:
            raise ClientError(response.status_code, _detail(response))
        return response

    def _request(
        self,
        workflow: Path | Workflow,
        sources: dict[str, Path],
        group: str | None,
        split: SplitMode | None,
        reserve: str | None,
        local_commands: str | None = None,
    ) -> dict[str, Any]:
        if isinstance(workflow, Path):
            workflow = load_workflow(workflow)
        workflow = plan_tasks(workflow, load_task_types())
        recorded = [self.source(path, name) for name, path in sources.items()]
        return {
            "workflow": workflow.model_dump(mode="json"),
            "sources": [p.model_dump(mode="json") for p in recorded],
            "group": group,
            "split": split.value if split else None,
            "reserve": reserve,
            "local_commands": local_commands,
        }

    def submit(
        self,
        workflow: Path | Workflow,
        sources: dict[str, Path],
        group: str | None = None,
        split: SplitMode | None = None,
        reserve: str | None = None,
        local_commands: str | None = None,
    ) -> RunSummary:
        """Plan the tasks, snapshot the sources and submit a workflow.

        The settings of a task whose type is installed locally and has a `plan`
        are replaced by the planned ones, see `plan_tasks`.

        Args:
            workflow: The workflow or its YAML file.
            sources: Working trees by source name, the model first.
            group: Group of the run.
            split: Split mode of every task.
            reserve: A node to reserve.
            local_commands: With the local service, `docker` runs a `command` task
                in its image, `direct` (the default) as a process.

        Returns:
            The summary of the run, `submitting` while the service resolves its
            environment, builds its image and submits its workflow.

        Raises:
            ClientError: When the service refuses the submission.
        """
        body = self._request(workflow, sources, group, split, reserve, local_commands)
        return RunSummary.model_validate(self._call("POST", "/runs", json=body).json())

    def estimate(
        self,
        workflow: Path | Workflow,
        sources: dict[str, Path],
        group: str | None = None,
        split: SplitMode | None = None,
        reserve: str | None = None,
    ) -> dict[str, TaskEstimate]:
        """The resources, plans and durations of the tasks of a submission."""
        body = self._request(workflow, sources, group, split, reserve)
        data = self._call("POST", "/estimate", json=body).json()
        return {k: TaskEstimate.model_validate(v) for k, v in data.items()}

    def validate(self, workflow: Path | Workflow) -> ValidationResult:
        """Validate a workflow with its tasks planned, as `submit` submits it.

        Raises:
            ValueError: The settings of a task are invalid for its local type.
        """
        if isinstance(workflow, Path):
            workflow = load_workflow(workflow)
        body = plan_tasks(workflow, load_task_types()).model_dump(mode="json")
        return ValidationResult.model_validate(
            self._call("POST", "/validate", json=body).json()
        )

    def open_edit(self, body: OpenEdit) -> EditSession:
        """Open an edit session for the workflow file of this machine."""
        data = self._call("POST", "/edits", json=body.model_dump(mode="json")).json()
        return EditSession.model_validate(data)

    def edit_state(self, session_id: str, state: EditState) -> EditSession:
        """Send what was read from disk to an edit session."""
        response = self._call(
            "PUT",
            f"/edits/{session_id}/state",
            json=state.model_dump(mode="json", exclude_none=True),
        )
        return EditSession.model_validate(response.json())

    def edit_requests(self, session_id: str, timeout: float) -> list[EditRequest]:
        """Long poll the open requests of the browser, for up to `timeout` s."""
        response = self._call(
            "GET", f"/edits/{session_id}/requests", params={"timeout": timeout}
        )
        return [EditRequest.model_validate(item) for item in response.json()]

    def edit_result(self, session_id: str, request_id: str, result: EditResult) -> None:
        """Answer a request of the browser."""
        self._call(
            "POST",
            f"/edits/{session_id}/requests/{request_id}/result",
            json=result.model_dump(mode="json"),
        )

    def close_edit(self, session_id: str, timeout: float = REQUEST_TIMEOUT_S) -> None:
        """Close an edit session, the command stopped, waiting up to `timeout` s."""
        self._call("POST", f"/edits/{session_id}/close", timeout=timeout)

    def wait(self, run_id: str, timeout_s: float | None = None) -> RunSummary:
        """Wait for a run to end, or for `timeout_s` seconds (None: forever).

        A dropped connection or a restarting service is retried with a backoff
        of 1 s doubling to 10 s, until the timeout.

        Returns:
            The summary of the run, not terminal when the timeout passed; when the
            timeout passes during an outage, the last summary obtained.

        Raises:
            ClientError: For an unknown run or another refusal.
            httpx.TransportError: When the service was never reached before the
                timeout.
        """
        last: RunSummary | None = None
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        backoff = BACKOFF_START_S
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            poll = POLL_S if remaining is None else min(POLL_S, max(remaining, 0.0))
            try:
                response = self._http.get(
                    f"/runs/{run_id}/wait", params={"timeout": poll}
                )
                if response.status_code in RETRY_STATUS:
                    raise httpx.TransportError(f"status {response.status_code}")
            except httpx.TransportError as error:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    if last is None:
                        raise
                    return last
                logger.warning(
                    "wait for %s: %s, retrying in %.0f s", run_id, error, backoff
                )
                time.sleep(backoff if remaining is None else min(backoff, remaining))
                backoff = min(backoff * 2, BACKOFF_MAX_S)
                continue
            backoff = BACKOFF_START_S
            if response.is_error:
                raise ClientError(response.status_code, _detail(response))
            summary = last = RunSummary.model_validate(response.json())
            if summary.state in TERMINAL_RUN_STATES:
                return summary
            if deadline is not None and time.monotonic() >= deadline:
                return summary

    def wait_live(
        self,
        run_id: str,
        on_update: Callable[[dict[str, Any]], None],
        timeout_s: float | None = None,
    ) -> dict[str, Any]:
        """Follow a run through its event stream until it is final.

        The stream ends when the run is final and also every `events_max_s` of
        the service; it is opened again then. A dropped connection or a restarting
        service is retried with a backoff of 1 s doubling to 10 s.

        Args:
            run_id: The run.
            on_update: Called with the latest `{"run": RunRecord, "progress":
                dict[str, TaskProgress]}` whenever one of them changed.
            timeout_s: Seconds to follow (None: until the run is final).

        Returns:
            The last update; its run is not terminal when the timeout passed.

        Raises:
            ClientError: For an unknown run or another refusal.
            httpx.TransportError: When the service was never reached before the
                timeout.
        """
        current: dict[str, Any] = {}
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        backoff = BACKOFF_START_S
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return current or self._update(run_id)
            try:
                self._follow(run_id, current, on_update, remaining)
                backoff = BACKOFF_START_S
            except httpx.TransportError as error:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    if not current:
                        raise
                    return current
                logger.warning(
                    "follow %s: %s, retrying in %.0f s", run_id, error, backoff
                )
                time.sleep(backoff if remaining is None else min(backoff, remaining))
                backoff = min(backoff * 2, BACKOFF_MAX_S)
                continue
            run = current.get("run")
            if isinstance(run, RunRecord) and run.state in TERMINAL_RUN_STATES:
                return current
            time.sleep(REFOLLOW_S)  # a stream which ends at once does not spin

    def _update(self, run_id: str) -> dict[str, Any]:
        return {"run": self.status(run_id), "progress": self.progress(run_id)}

    def _follow(
        self,
        run_id: str,
        current: dict[str, Any],
        on_update: Callable[[dict[str, Any]], None],
        remaining: float | None,
    ) -> None:
        """Read one event stream into `current` until it ends."""
        read = POLL_S * 2 if remaining is None else min(POLL_S * 2, remaining + 0.5)
        timeout = httpx.Timeout(REQUEST_TIMEOUT_S, read=read)
        event = ""
        data: list[str] = []
        try:
            with self._http.stream(
                "GET", f"/runs/{run_id}/events", timeout=timeout
            ) as response:
                if response.status_code in RETRY_STATUS:
                    raise httpx.TransportError(f"status {response.status_code}")
                if response.is_error:
                    response.read()
                    raise ClientError(response.status_code, _detail(response))
                for line in response.iter_lines():
                    if line:
                        field, _, value = line.partition(":")
                        value = value.removeprefix(" ")
                        if field == "event":
                            event = value
                        elif field == "data":
                            data.append(value)
                        continue
                    if data and _apply_event(current, event, "\n".join(data)):
                        on_update(current)
                    event, data = "", []
        except httpx.ReadTimeout:
            if remaining is None:
                raise

    def status(self, run_id: str) -> RunRecord:
        """The record of a run."""
        return RunRecord.model_validate(self._call("GET", f"/runs/{run_id}").json())

    def progress(self, run_id: str) -> dict[str, TaskProgress]:
        """The progress of the tasks of a run."""
        data = self._call("GET", f"/runs/{run_id}/progress").json()
        return {k: TaskProgress.model_validate(v) for k, v in data.items()}

    def list_runs(
        self,
        group: str | None = None,
        agent: str | None = None,
        state: RunState | None = None,
        owner: str | None = None,
    ) -> list[RunSummary]:
        """The runs, filtered by group, agent, state and owner."""
        filters = {"group": group, "agent": agent, "state": state, "owner": owner}
        params = {k: v for k, v in filters.items() if v is not None}
        data = self._call("GET", "/runs", params=params).json()
        return [RunSummary.model_validate(r) for r in data]

    def compare(self, run_ids: list[str]) -> CompareTable:
        """Provenance and summaries of runs side by side."""
        data = self._call("POST", "/compare", json={"runs": run_ids}).json()
        return CompareTable.model_validate(data)

    def cancel(self, run_id: str) -> RunSummary:
        """Cancel a run."""
        return RunSummary.model_validate(
            self._call("POST", f"/runs/{run_id}/cancel").json()
        )

    def logs(
        self, run_id: str, task_id: str, tail_lines: int = DEFAULT_TAIL_LINES
    ) -> str:
        """The logs of the pods of a task, the last `tail_lines` of each (0: all)."""
        return self._call(
            "GET", f"/runs/{run_id}/logs/{task_id}", params={"tail_lines": tail_lines}
        ).text

    def outputs(self, run_id: str, task_id: str) -> list[OutputFile]:
        """The output files of a task, paths relative to the store."""
        data = self._call("GET", f"/runs/{run_id}/outputs/{task_id}").json()
        return [OutputFile.model_validate(f) for f in data]

    def output_file(self, run_id: str, task_id: str, path: str) -> bytes:
        """The bytes of a file of the output of a task, `path` relative to it."""
        return self._call(
            "GET", f"/runs/{run_id}/outputs/{task_id}/file", params={"path": path}
        ).content

    def output_archive(self, run_id: str, task_id: str, to: Path) -> list[Path]:
        """Extract the output of a task into the directory `to`.

        Returns:
            The extracted files, sorted.
        """
        to.mkdir(parents=True, exist_ok=True)
        with (
            tempfile.TemporaryFile() as spool,
            self._http.stream(
                "GET", f"/runs/{run_id}/outputs/{task_id}/archive"
            ) as response,
        ):
            if response.is_error:
                response.read()
                raise ClientError(response.status_code, _detail(response))
            for chunk in response.iter_bytes():
                spool.write(chunk)
            spool.seek(0)
            with tarfile.open(fileobj=spool, mode="r:") as tar:
                names = [m.name for m in tar.getmembers() if m.isfile()]
                tar.extractall(to, filter="data")
        return sorted(to / name for name in names)

    def delete(self, run_id: str) -> CleanupResult:
        """Delete an ended run."""
        return CleanupResult.model_validate(
            self._call("DELETE", f"/runs/{run_id}").json()
        )

    def cleanup(
        self,
        older_than_days: float,
        group: str | None = None,
        failed_only: bool = False,
        dry_run: bool = False,
    ) -> CleanupResult:
        """Remove ended runs and the snapshots nothing refers to any more."""
        body = {
            "older_than_days": older_than_days,
            "group": group,
            "failed_only": failed_only,
            "dry_run": dry_run,
        }
        return CleanupResult.model_validate(
            self._call("POST", "/cleanup", json=body).json()
        )

    def usage(self) -> Usage:
        """Bytes of the store per run and group."""
        return Usage.model_validate(self._call("GET", "/usage").json())
