from pathlib import Path
from typing import Any

import pytest
import yaml
from fastmcp import Client as McpClient

from simrun.mcp import build_server
from simrun.testing import FakeBackend

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


TOOLS = {
    "submit_workflow",
    "wait_for_run",
    "run_status",
    "run_progress",
    "list_runs",
    "compare_runs",
    "cancel_run",
    "run_logs",
    "run_outputs",
    "estimate_workflow",
}


async def call(mcp: McpClient, name: str, **arguments: Any) -> dict[str, Any]:
    result = await mcp.call_tool(name, arguments)
    assert result.structured_content is not None
    return result.structured_content


def _workflow(path: Path) -> Path:
    path.write_text(
        yaml.safe_dump(
            {
                "name": "demo",
                "tasks": [
                    {"id": "squares", "type": "demo.squares", "settings": {"n": 4}}
                ],
            }
        )
    )
    return path


async def test_mcp_tools_listed(service, make_client) -> None:
    async with McpClient(build_server(make_client(service))) as mcp:
        assert {t.name for t in await mcp.list_tools()} == TOOLS


async def test_mcp_submit_and_wait(
    service_factory, make_client, repo: Path, tmp_path: Path
) -> None:
    with service_factory(FakeBackend(finish_after=0.3), poll_s=0.05) as service:
        client = make_client(service)
        async with McpClient(build_server(client)) as mcp:
            wf = _workflow(tmp_path / "wf.yaml")
            submitted = await call(
                mcp,
                "submit_workflow",
                workflow_path=str(wf),
                sources={"model": str(repo)},
            )
            run_id = submitted["run"]["id"]
            assert submitted["store"] == str(service.settings.store.resolve())
            waited = await call(mcp, "wait_for_run", run_id=run_id, timeout_s=30)
            assert waited["run"]["state"] == "succeeded"
            status = await call(mcp, "run_status", run_id=run_id)
            snapshot = status["run"]["sources"][0]["snapshot"]
            assert not Path(snapshot).is_absolute()
            assert (Path(status["store"]) / snapshot / "model.txt").is_file()
            listed = await call(mcp, "list_runs", state="succeeded")
            assert [r["id"] for r in listed["runs"]] == [run_id]
            assert (
                "squares"
                in (await call(mcp, "run_progress", run_id=run_id))["progress"]
            )
            assert (await call(mcp, "run_outputs", run_id=run_id, task_id="squares"))[
                "files"
            ] == []
            table = await call(mcp, "compare_runs", run_ids=[run_id])
            assert table["table"]["rows"][0]["run"] == run_id
            estimate = await call(
                mcp,
                "estimate_workflow",
                workflow_path=str(wf),
                sources={"model": str(repo)},
            )
            assert "squares" in estimate["tasks"]


async def test_mcp_error_is_returned(service, make_client) -> None:
    async with McpClient(build_server(make_client(service))) as mcp:
        result = await call(mcp, "cancel_run", run_id="nope")
        assert result["status"] == 404
        assert "nope" in result["error"]
        missing = await call(mcp, "submit_workflow", workflow_path="/no/such.yaml")
        assert "error" in missing
