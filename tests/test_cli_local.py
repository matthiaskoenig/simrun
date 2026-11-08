"""The `--local` switch: the commands against the local service of a directory."""

import json
import os
import shutil
import signal
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from simrun.cli import app
from simrun.local import LOCAL_DIR, read_service

EXAMPLES = Path(__file__).parent.parent / "examples"


def _alive(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return False
    return stat.rsplit(")", 1)[1].split()[0] != "Z"


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A project directory as the current one; its local service stops at the end."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SIMRUN_URL", raising=False)
    yield tmp_path
    service = read_service(tmp_path)
    if service is not None:
        try:
            os.kill(service.pid, signal.SIGTERM)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + 90
        while _alive(service.pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not _alive(service.pid)


def test_submit_local_runs_the_example_and_status_shows_it(project: Path) -> None:
    shutil.copy(EXAMPLES / "squares.yaml", "squares.yaml")
    runner = CliRunner()
    result = runner.invoke(
        app, ["submit", "squares.yaml", "--local", "--timeout", "120", "--json"]
    )
    assert result.exit_code == 0, result.output
    run_id = json.loads(result.output.strip().splitlines()[-1])["id"]
    status = runner.invoke(app, ["status", run_id, "--local", "--json"])
    assert json.loads(status.output)["run"]["state"] == "succeeded"
    listed = runner.invoke(app, ["list", "--local", "--json"])
    assert [r["id"] for r in json.loads(listed.output)] == [run_id]
    assert (project / LOCAL_DIR / "store" / "runs" / run_id / "run.json").is_file()


@pytest.mark.parametrize("flag", ["--docker", "--snapshot"])
def test_flags_which_need_local_are_refused_without_it(
    project: Path, flag: str
) -> None:
    shutil.copy(EXAMPLES / "squares.yaml", "squares.yaml")
    result = CliRunner().invoke(app, ["submit", "squares.yaml", flag])
    assert result.exit_code == 1
    assert f"{flag} needs --local" in result.output
    assert not (project / LOCAL_DIR).exists()


def test_edit_snapshot_needs_local(project: Path) -> None:
    result = CliRunner().invoke(app, ["edit", "w.yaml", "--snapshot"])
    assert result.exit_code == 1
    assert "--snapshot needs --local" in result.output
