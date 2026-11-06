"""The client of the service: snapshots working trees, then calls the API.

The CLI (`simrun.cli`) and the MCP server (`simrun.mcp`) both go through `Client`.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx

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
from simrun.snapshot import snapshot
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
BACKOFF_MAX_S = 10.0
# answers of a proxy or a restarting service, retried by `wait`
RETRY_STATUS = (502, 503, 504)


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
        store: The run store as mounted locally, where snapshots are written.
        transport: An HTTP transport, e.g. for tests.
    """

    def __init__(
        self,
        url: str,
        token: str,
        store: Path,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Create the client."""
        self.store = store
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
    ) -> dict[str, Any]:
        if isinstance(workflow, Path):
            workflow = load_workflow(workflow)
        provenance = [
            snapshot(path, self.store, name) for name, path in sources.items()
        ]
        return {
            "workflow": workflow.model_dump(mode="json"),
            "sources": [p.model_dump(mode="json") for p in provenance],
            "group": group,
            "split": split.value if split else None,
            "reserve": reserve,
        }

    def submit(
        self,
        workflow: Path | Workflow,
        sources: dict[str, Path],
        group: str | None = None,
        split: SplitMode | None = None,
        reserve: str | None = None,
    ) -> RunSummary:
        """Snapshot the sources and submit a workflow.

        Args:
            workflow: The workflow or its YAML file.
            sources: Working trees by source name, the model first.
            group: Group of the run.
            split: Split mode of every task.
            reserve: A node to reserve.

        Raises:
            ClientError: When the service refuses the submission.
        """
        body = self._request(workflow, sources, group, split, reserve)
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
        """Validate a workflow."""
        if isinstance(workflow, Path):
            workflow = load_workflow(workflow)
        body = workflow.model_dump(mode="json")
        return ValidationResult.model_validate(
            self._call("POST", "/validate", json=body).json()
        )

    def wait(self, run_id: str, timeout_s: float | None = None) -> RunSummary:
        """Wait for a run to end, or for `timeout_s` seconds (None: forever).

        A dropped connection or a restarting service is retried with a backoff
        of 1 s doubling to 10 s, until the timeout.

        Returns:
            The summary of the run, not terminal when the timeout passed.

        Raises:
            ClientError: For an unknown run or another refusal.
        """
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
                    return self.status_summary(run_id)
                logger.warning(
                    "wait for %s: %s, retrying in %.0f s", run_id, error, backoff
                )
                time.sleep(backoff if remaining is None else min(backoff, remaining))
                backoff = min(backoff * 2, BACKOFF_MAX_S)
                continue
            backoff = BACKOFF_START_S
            if response.is_error:
                raise ClientError(response.status_code, _detail(response))
            summary = RunSummary.model_validate(response.json())
            if summary.state in TERMINAL_RUN_STATES:
                return summary
            if deadline is not None and time.monotonic() >= deadline:
                return summary

    def status_summary(self, run_id: str) -> RunSummary:
        """The summary of a run."""
        return RunSummary.of(self.status(run_id))

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
    ) -> list[RunSummary]:
        """The runs, filtered."""
        params = {
            k: v
            for k, v in {"group": group, "agent": agent, "state": state}.items()
            if v is not None
        }
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

    def logs(self, run_id: str, task_id: str) -> str:
        """The log of a task."""
        return self._call("GET", f"/runs/{run_id}/logs/{task_id}").text

    def outputs(self, run_id: str, task_id: str) -> list[OutputFile]:
        """The output files of a task, paths relative to the store."""
        data = self._call("GET", f"/runs/{run_id}/outputs/{task_id}").json()
        return [OutputFile.model_validate(f) for f in data]

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
