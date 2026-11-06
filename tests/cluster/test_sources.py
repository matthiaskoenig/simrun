"""A run with sources on a real cluster: the path of the first milestone end to end.

A model repository (hatchling, `uv.lock`) with two task types of its own, `e2e.cubes`
(split) and `e2e.report` (an unsplit command), and a library (setuptools, depending
on `six`) with an uncommitted change are snapshotted into the store. The service
resolves their environment, builds its image from the generated Dockerfile with
BuildKit in the background, the pods install the snapshots read-only at their start,
the workers compute the cubes with the library, the merge sums them and the report
runs through `simrun-exec`. The test checks that `POST /runs` returned before the
image was built, the merged result, the report and the learned resources.
"""

import importlib
import json
import os
import subprocess
import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from k3d_cluster import REGISTRY, STORE, Service

from simrun.plugins import TaskType
from simrun.snapshot import snapshot

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(
        os.environ.get("SIMRUN_K3D") != "1",
        reason="the cluster tests run with SIMRUN_K3D=1",
    ),
]

N = 30

LIB_PYPROJECT = """\
[project]
name = "e2e-lib"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["six>=1.16"]

[build-system]
requires = ["setuptools>=61"]
build-backend = "setuptools.build_meta"
"""

LIB_INIT = '''\
"""The library of the end-to-end test."""

import six


def cube(i: int) -> int:
    return i**2  # the working tree changes it to the cube


def describe(total: int) -> str:
    return six.ensure_str(f"the sum of the cubes is {total}")
'''

MODEL_PYPROJECT = """\
[project]
name = "e2e-model"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["e2e-lib"]

[project.entry-points."simrun.task_types"]
"e2e.cubes" = "e2e_model.plugin:CUBES"
"e2e.report" = "e2e_model.plugin:REPORT"

[tool.uv.sources]
e2e-lib = { path = "../e2e-lib" }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
"""

PLUGIN = '''\
"""The task types of the end-to-end test model; simrun is the one of the image."""

import json
import os
import time
import uuid

from pydantic import BaseModel

from simrun.plugins import ResourceHint, TaskType


class CubesSettings(BaseModel):
    n: int
    sleep_s: float = 0.0
    memory_mb: int = 0


def count_units(settings):
    return settings.n


def setup(settings, paths):
    (paths.out / "units").mkdir(parents=True, exist_ok=True)


def compute_unit(settings, index, paths):
    from e2e_lib import cube

    ballast = bytearray(settings.memory_mb * 2**20)
    ballast[::4096] = b"\\x01" * len(ballast[::4096])
    time.sleep(settings.sleep_s)
    units = paths.out / "units"
    tmp = units / f".{index}.{uuid.uuid4().hex}.tmp"
    tmp.write_text(json.dumps({"i": index, "cube": cube(index)}))
    os.replace(tmp, units / f"{index}.json")


def merge(settings, paths):
    total = sum(
        json.loads((paths.out / "units" / f"{i}.json").read_text())["cube"]
        for i in range(settings.n)
    )
    sums = paths.out / "sums"
    sums.mkdir(parents=True, exist_ok=True)
    (sums / "summary.json").write_text(json.dumps({"sum": total}))


class ReportSettings(BaseModel):
    title: str = "cubes"
    memory_mb: int = 0
    hold_s: float = 0.0


CUBES = TaskType(
    name="e2e.cubes",
    settings=CubesSettings,
    outputs=("sums",),
    hint=ResourceHint(memory_base_gb=0.1, memory_per_worker_gb=0.5),
    count_units="e2e_model.plugin:count_units",
    setup="e2e_model.plugin:setup",
    compute_unit="e2e_model.plugin:compute_unit",
    merge="e2e_model.plugin:merge",
)
REPORT = TaskType(
    name="e2e.report",
    settings=ReportSettings,
    inputs=("cubes",),
    hint=ResourceHint(memory_base_gb=0.1, memory_per_worker_gb=0.5),
    command="python -m e2e_model.report {settings} {out}",
)
'''

REPORT = '''\
"""The report of the end-to-end test: an unsplit command run by `simrun-exec`."""

import json
import os
import sys
import time
from pathlib import Path

from e2e_lib import describe

from simrun.worker import paths_from_env


def main() -> None:
    settings = json.loads(Path(sys.argv[1]).read_text())
    out = Path(sys.argv[2])
    ballast = bytearray(settings["memory_mb"] * 2**20)
    ballast[::4096] = b"\\x01" * len(ballast[::4096])
    time.sleep(settings["hold_s"])
    paths = paths_from_env(os.environ)
    total = json.loads((paths.inputs["cubes"] / "summary.json").read_text())["sum"]
    out.mkdir(parents=True, exist_ok=True)
    report = {
        "title": settings["title"],
        "sum": total,
        "text": describe(total),
        "threads": os.environ.get("OMP_NUM_THREADS"),
    }
    (out / "report.json").write_text(json.dumps(report))


if __name__ == "__main__":
    main()
'''


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def _repository(path: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        (path / name).parent.mkdir(parents=True, exist_ok=True)
        (path / name).write_text(text)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "config", "user.email", "e2e@simrun.test")
    _git(path, "config", "user.name", "e2e")
    return path


def _commit(repo: Path) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "e2e")


def repositories(root: Path) -> tuple[Path, Path]:
    """The model and the library, the model locked, the library with a change."""
    lib = _repository(
        root / "e2e-lib",
        {"pyproject.toml": LIB_PYPROJECT, "e2e_lib/__init__.py": LIB_INIT},
    )
    (lib / ".gitignore").write_text("*.egg-info/\n")
    _commit(lib)
    # uncommitted: the snapshot is of the working tree
    init = lib / "e2e_lib/__init__.py"
    init.write_text(init.read_text().replace("i**2  # the working tree", "i**3  #"))
    model = _repository(
        root / "e2e-model",
        {
            "pyproject.toml": MODEL_PYPROJECT,
            "src/e2e_model/__init__.py": "",
            "src/e2e_model/plugin.py": PLUGIN,
            "src/e2e_model/report.py": REPORT,
        },
    )
    subprocess.run(["uv", "lock", "-q"], cwd=model, check=True)
    _commit(model)
    return model, lib


def plugin_types(model: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, TaskType]:
    """The task types of the model for the service, from its working tree.

    The service imports the plugins it knows (it counts the units of a split task),
    as one with the plugin installed does; the pods import them from the snapshot.
    """
    monkeypatch.syspath_prepend(str(model / "src"))
    for name in [m for m in sys.modules if m.split(".")[0] == "e2e_model"]:
        monkeypatch.delitem(sys.modules, name)
    module = importlib.import_module("e2e_model.plugin")
    return {t.name: t for t in (module.CUBES, module.REPORT)}


def _workflow() -> dict:
    return {
        "name": "e2e",
        "tasks": [
            {
                "id": "cubes",
                "type": "e2e.cubes",
                "settings": {"n": N, "sleep_s": 0.5, "memory_mb": 100},
            },
            {
                "id": "report",
                "type": "e2e.report",
                "settings": {"title": "cubes", "memory_mb": 300, "hold_s": 3.0},
                "inputs": {"cubes": "cubes.sums"},
            },
        ],
    }


def test_model_sources_end_to_end(
    service_factory: Callable[..., AbstractContextManager[Service]],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model, lib = repositories(tmp_path)
    sources = [
        snapshot(model, STORE, "e2e-model").model_dump(),
        snapshot(lib, STORE, "e2e-lib").model_dump(),
    ]
    with service_factory(types=plugin_types(model, monkeypatch)) as service:
        workflow = _workflow()
        run = service.submit("t-mk", workflow, sources=sources)
        returned = datetime.now(UTC)
        assert run["state"] == "submitting"

        record = service.wait(run["id"], timeout_s=1200)
        assert record["state"] == "succeeded", record["message"]
        assert record["tasks"] == {"cubes": "done", "report": "done"}
        # the image was built in the background, after the submission returned
        assert record["image"].startswith(f"{REGISTRY}/simrun-env:")
        assert datetime.fromisoformat(record["built"]) > returned

        expected = sum(i**3 for i in range(N))
        tasks = STORE / "runs" / run["id"] / "tasks"
        summary = json.loads((tasks / "cubes/out/sums/summary.json").read_text())
        assert summary == {"sum": expected}
        report = json.loads((tasks / "report/out/report.json").read_text())
        assert report == {
            "title": "cubes",
            "sum": expected,
            "text": f"the sum of the cubes is {expected}",
            "threads": "1",
        }
        assert (
            "e2e-lib" in (STORE / "runs" / run["id"] / "environment.lock").read_text()
        )
        # the snapshots are read-only: the pods built nothing into them
        for source in sources:
            assert not list((STORE / source["snapshot"]).rglob("*.egg-info"))

        # the report of `simrun-exec` and the reports of the workers were learned
        (exec_report,) = (tasks / "report/workers").glob("*.json")
        assert json.loads(exec_report.read_text())["peak_memory_bytes"] > 300 * 2**20
        assert (tasks / "report/recorded" / exec_report.name).is_file()
        estimate = service.estimate("t-mk", workflow, sources=sources)
        for task in ("cubes", "report"):
            resources = estimate[task]["resources"]
            assert resources["source"].startswith("learned from"), (task, resources)
        assert estimate["report"]["resources"]["samples"] == 1
        assert estimate["report"]["plan"]["shape"]["processes"] == 1
