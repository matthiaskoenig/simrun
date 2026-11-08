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


def test_cli_submit_list_status_compare(
    cli, service, repo: Path, tmp_path: Path
) -> None:
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
    service.backend.add_log(f"simrun-{run_id}", "squares", "squares-w0(0)", "a\nb\n")
    assert cli("logs", run_id, "squares", "--tail", "1").output.endswith("<==\nb\n")
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


def test_cli_submit_wait_follows_to_the_end(
    service_factory, make_client, monkeypatch, tmp_path: Path
) -> None:
    with service_factory(FakeBackend(finish_after=0.3), poll_s=0.05) as service:
        monkeypatch.setattr(simrun.cli, "make_client", lambda: make_client(service))
        wf = _workflow(tmp_path / "wf.yaml")
        result = runner.invoke(simrun.cli.app, ["submit", str(wf), "--wait"])
        assert result.exit_code == 0, result.output
        assert "succeeded" in result.output
        assert "squares" in result.output
        as_json = runner.invoke(simrun.cli.app, ["submit", str(wf), "--wait", "--json"])
        assert as_json.exit_code == 0, as_json.output
        last = json.loads(as_json.output.strip().splitlines()[-1])
        assert last["state"] == "succeeded"


def test_cli_submit_wait_timeout_exits_2_over_ended_streams(
    service_factory, make_client, monkeypatch, tmp_path: Path
) -> None:
    # the streams end every 0.3 s (events_max_s), the waiting opens them again
    with service_factory(poll_s=0.05, events_max_s=0.3) as service:
        monkeypatch.setattr(simrun.cli, "make_client", lambda: make_client(service))
        wf = _workflow(tmp_path / "wf.yaml")
        result = runner.invoke(
            simrun.cli.app, ["submit", str(wf), "--wait", "--timeout", "2"]
        )
        assert result.exit_code == 2, result.output


def test_cli_submit_no_wait_by_default(cli, tmp_path: Path) -> None:
    result = cli("submit", str(_workflow(tmp_path / "wf.yaml")))
    assert result.exit_code == 0
    assert "submitted" in result.output


def test_cli_outputs_lists_and_extracts(cli, service, tmp_path: Path) -> None:
    run_id = service.submit()["id"]
    out = service.run_dir(run_id) / "tasks/squares/out"
    (out / "squares").mkdir(parents=True)
    (out / "summary.json").write_text("{}")
    (out / "squares/0.json").write_text('{"i": 0}')
    listed = json.loads(cli("outputs", run_id, "squares", "--json").output)
    assert listed == [
        {"path": "squares/0.json", "bytes": 8},
        {"path": "summary.json", "bytes": 2},
    ]
    assert "summary.json" in cli("outputs", run_id, "squares").output
    to = tmp_path / "got"
    result = cli("outputs", run_id, "squares", "--to", str(to))
    assert result.exit_code == 0, result.output
    assert (to / "squares/0.json").read_text() == '{"i": 0}'
    assert cli("outputs", run_id, "nope").exit_code == 1


def test_the_live_table_ticks_between_events(service, monkeypatch) -> None:
    import io
    import time

    from rich.console import Console

    from simrun.store import TaskState

    run_id = service.submit()["id"]
    record = service.store.read(run_id)
    running = record.model_copy(update={"tasks": {"squares": TaskState.RUNNING}})
    screen = io.StringIO()
    monkeypatch.setattr(
        simrun.cli, "console", Console(file=screen, force_terminal=True, width=80)
    )
    seen: list[str] = []

    class Client:
        def wait_live(self, run_id, callback, timeout) -> None:
            callback({"run": running, "progress": {}})
            time.sleep(1.4)  # no event: the table is drawn again by itself
            seen.append(screen.getvalue())

    simrun.cli._follow(Client(), run_id, None, False)  # ty: ignore[invalid-argument-type]
    assert "1 s" in seen[0]


def test_timeout_without_waiting_is_refused(cli, service, tmp_path: Path) -> None:
    result = cli("submit", str(_workflow(tmp_path / "wf.yaml")), "--timeout", "5")
    assert result.exit_code == 1
    assert "--timeout needs --wait" in result.output
    assert service.get("/runs").json() == []


def _screen(monkeypatch, width: int = 80):
    import io

    from rich.console import Console

    screen = io.StringIO()
    monkeypatch.setattr(simrun.cli, "console", Console(file=screen, width=width))
    return screen


def test_list_fits_80_columns_without_cutting_a_value(cli, service, monkeypatch):
    # an agent's run in a group: on a terminal of 80 columns a long value wraps in
    # its column, the run and its state stay whole
    run_id = service.submit("t-mk-bot", group="hctz-variants-of-the-pk-fit")["id"]
    screen = _screen(monkeypatch)
    result = cli("list")
    assert result.exit_code == 0, result.output
    text = screen.getvalue()
    assert "…" not in text
    lines = text.splitlines()
    assert max(len(line) for line in lines) <= 80
    header = next(line for line in lines if "state" in line)
    at = lines.index(header)
    row, *rest = lines[at + 2 :]  # below the rule
    assert run_id in row
    for value in ("mk-bot", "owner", "queued"):
        assert value in row
    # the group wraps in its column: its lines give it whole
    start, end = header.index("group"), header.index("by")
    group = "".join(line[start:end].strip() for line in [row, *rest])
    assert group == "hctz-variants-of-the-pk-fit"
    # wide enough, one line per run
    screen = _screen(monkeypatch, width=120)
    assert cli("list").exit_code == 0
    assert "hctz-variants-of-the-pk-fit" in screen.getvalue()


def test_list_leaves_out_a_column_without_values(cli, service, monkeypatch):
    service.submit()  # by a person, without a group
    screen = _screen(monkeypatch)
    assert cli("list").exit_code == 0
    header = next(line for line in screen.getvalue().splitlines() if "state" in line)
    assert "agent" not in header and "group" not in header
    assert "state" in header and "priority" in header
