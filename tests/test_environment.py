import subprocess
import sys
from pathlib import Path

import pytest

from simrun.environment import Environment, dockerfile, image_ref, resolve_environment
from simrun.snapshot import SourceProvenance


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UV_OFFLINE", "1")


LOCK = 'version = 1\nrevision = 5\nrequires-python = ">=3.14"\n'


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def _provenance(name: str, snapshot: str) -> SourceProvenance:
    return SourceProvenance(
        name=name,
        branch=None,
        commit="0" * 40,
        dirty=False,
        changed_files=[],
        diff_sha256="0" * 64,
        snapshot=snapshot,
        patch=None,
    )


def _project(root: Path, name: str, deps: list[str], sources: str = "") -> None:
    dependencies = ", ".join(f'"{d}"' for d in deps)
    _write(
        root / "pyproject.toml",
        f'[project]\nname = "{name}"\nversion = "0.1"\n'
        f'requires-python = ">=3.14"\ndependencies = [{dependencies}]\n{sources}',
    )


def _make(
    tmp_path: Path, lib_deps: list[str], name: str = "store"
) -> tuple[Path, SourceProvenance, list[SourceProvenance]]:
    store = tmp_path / name
    _project(store / "snap/extra", "extra", [])
    _write(store / "snap/model/uv.lock", LOCK)
    _project(store / "snap/model", "model", ["lib"])
    _project(store / "snap/lib", "lib", lib_deps)
    return (
        store,
        _provenance("model", "snap/model"),
        [_provenance("lib", "snap/lib"), _provenance("extra", "snap/extra")],
    )


def _resolve(
    store: Path, model: SourceProvenance, libs: list[SourceProvenance]
) -> Environment:
    return resolve_environment(store, model, libs, simrun_version="0.1.0")


def test_snapshot_dependency_in_lock(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    env = _resolve(store, model, libs)
    assert 'name = "extra"' in env.lock
    assert 'name = "pydantic"' not in env.lock or "pydantic" in env.requirements
    assert env.editable == ["snap/model", "snap/lib", "snap/extra"]
    assert env.python == "3.14"


def test_lock_independent_of_store_location(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path / "a", ["extra"])
    other, model2, libs2 = _make(tmp_path / "b", ["extra"], name="mnt")
    a = _resolve(store, model, libs)
    b = _resolve(other, model2, libs2)
    assert a.lock_sha256 == b.lock_sha256


def test_hash_stable(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    assert (
        _resolve(store, model, libs).lock_sha256
        == _resolve(store, model, libs).lock_sha256
    )


def test_hash_changes_with_dependencies(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    before = _resolve(store, model, libs)
    _project(store / "snap/lib", "lib", ["extra"])
    assert _resolve(store, model, libs).lock_sha256 != before.lock_sha256


def test_hash_changes_with_simrun_version(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    a = _resolve(store, model, libs)
    b = resolve_environment(store, model, libs, simrun_version="0.2.0")
    assert a.lock_sha256 != b.lock_sha256


def test_existing_sources_overridden_by_name(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    _project(
        store / "snap/model",
        "model",
        ["lib"],
        '[tool.uv.sources]\nlib = { path = "/does/not/exist" }\n',
    )
    env = _resolve(store, model, libs)
    assert "/does/not/exist" not in env.lock


def test_unsnapshotted_path_dependency_rejected(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    other = tmp_path / "other"
    _project(other, "other", [])
    _project(
        store / "snap/model",
        "model",
        ["lib", "other"],
        f'[tool.uv.sources]\nother = {{ path = "{other}" }}\n',
    )
    with pytest.raises(ValueError, match="'other'.*source"):
        _resolve(store, model, libs)


def test_dev_group_not_in_requirements(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    with (store / "snap/model/pyproject.toml").open("a") as handle:
        handle.write('[dependency-groups]\ndev = ["pytest"]\n')
    env = _resolve(store, model, libs)
    assert 'name = "pytest"' in env.lock
    assert "pytest==" not in env.requirements and "\npytest" not in env.requirements
    assert "pydantic" in env.requirements


def test_missing_lock(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    (store / "snap/model/uv.lock").unlink()
    with pytest.raises(ValueError, match=r"uv\.lock"):
        _resolve(store, model, libs)


def test_image_ref(tmp_path: Path) -> None:
    env = Environment(
        lock="l", lock_sha256="ab" * 32, python="3.14", simrun_version="1", editable=[]
    )
    assert image_ref(env, "reg:5000") == f"reg:5000/simrun-env:{'ab' * 8}"


def test_dockerfile_has_no_source(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    text = dockerfile(_resolve(store, model, libs), "reg:5000", "simrun-0.1.0.whl")
    assert text.startswith("FROM reg:5000/uv:python3.14-bookworm-slim")
    assert text.count("COPY") == 1
    assert "COPY simrun-0.1.0.whl " in text
    assert "--no-deps /tmp/simrun-0.1.0.whl" in text
    assert "uv pip install simrun" not in text
    assert "snap/" not in text
    assert "file://" not in text
    assert "pydantic" in text and "extra==" not in text


def test_worker_imports_light() -> None:
    code = (
        "import simrun.worker, simrun.demo, sys; "
        "assert not {'fastapi','kubernetes','hera','httpx','fastmcp'} "
        "& set(sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_missing_pyproject(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    (store / "snap/lib/pyproject.toml").unlink()
    with pytest.raises(ValueError, match=r"pyproject\.toml"):
        _resolve(store, model, libs)
