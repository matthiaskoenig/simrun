"""The MCP server of simrun, a local stdio process of an agent.

The tools go through the `Client`; they return the JSON of the API and `store`, the
absolute local prefix of the paths in it, which are relative to the store, so an
agent opens a result with `store / path`. A refusal is returned as
`{"error": detail, "status": code}` instead of raised, so an agent can read it.
"""

import logging
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
from fastmcp import FastMCP
from pydantic import BaseModel

from simrun.client import Client, ClientError
from simrun.service.logs import DEFAULT_TAIL_LINES
from simrun.store import RunState
from simrun.workflow import SplitMode

logger = logging.getLogger(__name__)

DEFAULT_WAIT_S = 600.0
# status of an error which did not come from the service
CLIENT_SIDE = 400
UNREACHABLE = 503


def _plain(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_plain(v) for v in value]
    return value


def build_server(client: Client) -> FastMCP:
    """The MCP server whose tools call the service through `client`."""
    server = FastMCP("simrun")
    store = None if client.store is None else str(client.store.resolve())

    def answer(call: Callable[[], dict[str, Any]]) -> dict[str, Any]:
        try:
            return {**_plain(call()), "store": store}
        except ClientError as error:
            return {"error": error.detail, "status": error.status}
        except httpx.TransportError as error:
            return {
                "error": f"the service is not reachable: {error}",
                "status": UNREACHABLE,
            }
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
            return {"error": str(error), "status": CLIENT_SIDE}

    @server.tool
    def submit_workflow(
        workflow_path: str,
        sources: dict[str, str] | None = None,
        group: str | None = None,
        single_server: bool = False,
    ) -> dict[str, Any]:
        """Snapshot the working trees and submit a workflow.

        `sources` maps a source name to the path of its git working tree, the model
        first. Returns the summary of the run at once, in the state `submitting`
        while its environment is resolved and built; follow it with `wait_for_run`.
        """
        paths = {name: Path(p).expanduser() for name, p in (sources or {}).items()}
        split = SplitMode.SINGLE_SERVER if single_server else None
        return answer(
            lambda: {
                "run": client.submit(
                    Path(workflow_path).expanduser(), paths, group, split
                )
            }
        )

    @server.tool
    def wait_for_run(run_id: str, timeout_s: float = DEFAULT_WAIT_S) -> dict[str, Any]:
        """Wait for a run to end or for `timeout_s` seconds; returns its summary."""
        return answer(lambda: {"run": client.wait(run_id, timeout_s)})

    @server.tool
    def run_status(run_id: str) -> dict[str, Any]:
        """The record of a run: state, tasks, plans and the provenance of its sources."""
        return answer(lambda: {"run": client.status(run_id)})

    @server.tool
    def run_progress(run_id: str) -> dict[str, Any]:
        """The progress of every task of a run: ledger counts, first and last progress lines."""
        return answer(lambda: {"progress": client.progress(run_id)})

    @server.tool
    def list_runs(
        group: str | None = None,
        agent: str | None = None,
        state: str | None = None,
        owner: str | None = None,
    ) -> dict[str, Any]:
        """The runs, filtered by group, agent, state and owner.

        The runs of an owner include the ones of their agents.
        """
        return answer(
            lambda: {
                "runs": client.list_runs(
                    group, agent, RunState(state) if state else None, owner
                )
            }
        )

    @server.tool
    def compare_runs(run_ids: list[str]) -> dict[str, Any]:
        """Provenance and task summaries of runs side by side."""
        return answer(lambda: {"table": client.compare(run_ids)})

    @server.tool
    def cancel_run(run_id: str) -> dict[str, Any]:
        """Cancel a run."""
        return answer(lambda: {"run": client.cancel(run_id)})

    @server.tool
    def run_logs(
        run_id: str, task_id: str, tail_lines: int = DEFAULT_TAIL_LINES
    ) -> dict[str, Any]:
        """The logs of the pods of a task, the last `tail_lines` of each (0: all)."""
        return answer(lambda: {"logs": client.logs(run_id, task_id, tail_lines)})

    @server.tool
    def run_outputs(run_id: str, task_id: str) -> dict[str, Any]:
        """The output files of a task, paths relative to `store`."""
        return answer(lambda: {"files": client.outputs(run_id, task_id)})

    @server.tool
    def estimate_workflow(
        workflow_path: str,
        sources: dict[str, str] | None = None,
        single_server: bool = False,
    ) -> dict[str, Any]:
        """The plan and the expected duration of every task of a submission."""
        paths = {name: Path(p).expanduser() for name, p in (sources or {}).items()}
        split = SplitMode.SINGLE_SERVER if single_server else None
        return answer(
            lambda: {
                "tasks": client.estimate(
                    Path(workflow_path).expanduser(), paths, None, split
                )
            }
        )

    return server


def main() -> None:
    """Run the server on stdio with the client of the environment."""
    build_server(Client.from_env()).run()
