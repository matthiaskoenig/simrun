"""The `--local` switch: the commands against the local service of a directory."""

import json
import os
import shutil
import signal
import site
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
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


def test_submit_local_records_the_working_tree_of_a_source(project: Path) -> None:
    model = project / "model"
    model.mkdir()
    (model / "a.txt").write_text("committed\n")
    _git(model, "init", "-q", "-b", "main")
    _git(model, "-c", "user.email=a@b.c", "-c", "user.name=T", "add", "-A")
    _git(model, "-c", "user.email=a@b.c", "-c", "user.name=T", "commit", "-qm", "i")
    (model / "a.txt").write_text("changed\n")  # uncommitted
    shutil.copy(EXAMPLES / "squares.yaml", "squares.yaml")
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            *("submit", "squares.yaml", "--source", "model=model"),
            *("--local", "--timeout", "120", "--json"),
        ],
    )
    assert result.exit_code == 0, result.output
    run_id = json.loads(result.output.strip().splitlines()[-1])["id"]
    status = runner.invoke(app, ["status", run_id, "--local", "--json"])
    record = json.loads(status.output)["run"]
    assert record["state"] == "succeeded"
    (source,) = record["sources"]
    assert (source["name"], source["branch"], source["dirty"]) == (
        "model",
        "main",
        True,
    )
    assert source["snapshot"] == ""


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


# an in-tree build backend without requirements: the editable install of the
# package needs neither the index nor the cache of uv
BACKEND = """
import base64, hashlib, os, zipfile

NAME, VERSION = "snapmodel", "0.1"


def _line(name, data):
    digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return f"{name},sha256={digest.decode()},{len(data)}"


def get_requires_for_build_editable(config_settings=None):
    return []


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    dist = f"{NAME}-{VERSION}.dist-info"
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
    files = {
        f"{NAME}.pth": (src + "\\n").encode(),
        f"{dist}/METADATA": f"Metadata-Version: 2.1\\nName: {NAME}\\nVersion: {VERSION}\\n".encode(),
        f"{dist}/WHEEL": b"Wheel-Version: 1.0\\nGenerator: t\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n",
    }
    wheel = f"{NAME}-{VERSION}-py3-none-any.whl"
    with zipfile.ZipFile(os.path.join(wheel_directory, wheel), "w") as archive:
        lines = []
        for name, data in files.items():
            archive.writestr(name, data)
            lines.append(_line(name, data))
        archive.writestr(f"{dist}/RECORD", "\\n".join([*lines, f"{dist}/RECORD,,"]) + "\\n")
    return wheel


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    raise NotImplementedError


def build_sdist(sdist_directory, config_settings=None):
    raise NotImplementedError
"""

PYPROJECT = """
[build-system]
requires = []
build-backend = "backend"
backend-path = ["."]

[project]
name = "snapmodel"
version = "0.1"
"""


def _git(path: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True)


def _site_packages() -> dict[str, list[str]]:
    """The entries of the site directories of the interpreter of the tests."""
    return {
        d: sorted(os.listdir(d)) for d in site.getsitepackages() if Path(d).is_dir()
    }


def test_submit_local_snapshot_installs_into_a_venv_of_the_run(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    model = project / "model"
    (model / "src" / "snapmodel").mkdir(parents=True)
    (model / "backend.py").write_text(BACKEND)
    (model / "pyproject.toml").write_text(PYPROJECT)
    package = model / "src" / "snapmodel" / "__init__.py"
    package.write_text('VALUE = "committed"\n')
    _git(model, "init", "-q", "-b", "main")
    _git(model, "-c", "user.email=a@b.c", "-c", "user.name=T", "add", "-A")
    _git(model, "-c", "user.email=a@b.c", "-c", "user.name=T", "commit", "-qm", "i")
    package.write_text('VALUE = "snapshot"\n')  # uncommitted, part of the snapshot
    code = "import snapmodel; print('VALUE', snapmodel.VALUE, snapmodel.__file__)"
    workflow = {
        "name": "snap",
        "tasks": [
            {"id": "squares", "type": "demo.squares", "settings": {"n": 4}},
            {
                "id": "show",
                "type": "command",
                "settings": {
                    "image": "unused:1",
                    "args": ["python", "-c", code],
                    "cores": 1,
                    "memory_gb": 0.5,
                },
            },
        ],
    }
    Path("w.yaml").write_text(yaml.safe_dump(workflow))
    monkeypatch.setenv("UV_OFFLINE", "1")
    before = _site_packages()
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            *("submit", "w.yaml", "--source", "model=model"),
            *("--local", "--snapshot", "--timeout", "120", "--json"),
        ],
    )
    assert result.exit_code == 0, result.output
    run_id = json.loads(result.output.strip().splitlines()[-1])["id"]
    logs = runner.invoke(app, ["logs", run_id, "show", "--local"])
    envs = project / LOCAL_DIR / "envs"
    assert f"VALUE snapshot {envs / run_id}" in logs.output, logs.output
    # the interpreter of the service, the one of the tests, is unchanged
    assert _site_packages() == before
    check = subprocess.run(
        [sys.executable, "-c", "import snapmodel"], capture_output=True, check=False
    )
    assert check.returncode == 1
    # the venv of the run is removed at its end
    assert not (envs / run_id).exists()


def test_serve_local_stop_stops_the_service_of_the_directory(project: Path) -> None:
    runner = CliRunner()
    assert runner.invoke(app, ["list", "--local"]).exit_code == 0
    service = read_service(project)
    assert service is not None
    result = runner.invoke(app, ["serve", "--local", "--stop"])
    assert result.exit_code == 0, result.output
    assert f"stopping the local service (pid {service.pid})" in result.output
    assert result.output.endswith("stopped\n")
    assert not _alive(service.pid)
    assert read_service(project) is None
    again = runner.invoke(app, ["serve", "--local", "--stop"])
    assert again.output == f"no local service runs in {project}\n"
    refused = runner.invoke(app, ["serve", "--stop"])
    assert refused.exit_code == 1
    assert "--stop needs --local" in refused.output
