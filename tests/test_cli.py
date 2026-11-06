import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

import simrun.cli
from simrun.testing import FakeBackend

runner = CliRunner()


@pytest.fixture
def cli(service, make_client, monkeypatch):
    monkeypatch.setattr(simrun.cli, "make_client", lambda: make_client(service))

    def invoke(*args: str):
        return runner.invoke(simrun.cli.app, list(args))

    return invoke


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


def test_cli_json(cli) -> None:
    result = cli("list", "--json")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == []


def test_cli_submit_list_status_compare(cli, repo: Path, tmp_path: Path) -> None:
    wf = _workflow(tmp_path / "wf.yaml")
    result = cli(
        "submit", str(wf), "--source", f"model={repo}", "--group", "g", "--json"
    )
    assert result.exit_code == 0, result.output
    run_id = json.loads(result.output)["id"]
    listed = json.loads(cli("list", "--group", "g", "--json").output)
    assert [r["id"] for r in listed] == [run_id]
    assert run_id in cli("list").output
    mine = json.loads(cli("list", "--owner", "mk", "--json").output)
    assert [r["id"] for r in mine] == [run_id]
    assert json.loads(cli("list", "--owner", "alice", "--json").output) == []
    status = cli("status", run_id)
    assert status.exit_code == 0 and "squares" in status.output
    assert (
        json.loads(cli("compare", run_id, "--json").output)["rows"][0]["run"] == run_id
    )
    assert cli("estimate", str(wf), "--source", f"model={repo}").exit_code == 0
    assert cli("usage").exit_code == 0
    assert cli("cleanup", "--dry-run").exit_code == 0
    cancelled = cli("cancel", run_id)
    assert cancelled.exit_code == 0 and "cancelled" in cancelled.output
    waited = cli("wait", run_id, "--timeout", "1")
    assert waited.exit_code == 1


def test_cli_refusal_prints_detail(cli) -> None:
    result = cli("cancel", "nope")
    assert result.exit_code == 1
    assert "nope" in result.output


def test_cli_bad_source(cli, tmp_path: Path) -> None:
    result = cli("submit", str(_workflow(tmp_path / "wf.yaml")), "--source", "model")
    assert result.exit_code == 1
    assert "NAME=PATH" in result.output


def test_cli_wait_succeeds(service_factory, make_client, monkeypatch) -> None:
    with service_factory(FakeBackend(finish_after=0.2), poll_s=0.05) as service:
        monkeypatch.setattr(simrun.cli, "make_client", lambda: make_client(service))
        run_id = service.submit()["id"]
        result = runner.invoke(simrun.cli.app, ["wait", run_id, "--timeout", "30"])
        assert result.exit_code == 0, result.output
        assert "succeeded" in result.output


def test_cli_wait_timeout_exits_2(cli, service) -> None:
    run_id = service.submit()["id"]
    result = cli("wait", run_id, "--timeout", "1")
    assert result.exit_code == 2
    assert result.output.strip().splitlines()[-1] != "2"
    assert result.output.count("\n") == 1


def test_cli_wait_failed_prints_one_line(cli, service) -> None:
    run_id = service.submit()["id"]
    cli("cancel", run_id)
    result = cli("wait", run_id, "--timeout", "1")
    assert result.exit_code == 1
    assert result.output == f"{run_id}: cancelled\ncancelled\n"  # state, message


def test_cli_bad_source_prints_only_message(cli, tmp_path: Path) -> None:
    result = cli("submit", str(_workflow(tmp_path / "wf.yaml")), "--source", "model")
    assert result.exit_code == 1
    assert result.output == "--source must be NAME=PATH, got 'model'\n"
