from pathlib import Path

import pytest

from simrun.environment import Environment, dockerfile, image_ref, resolve_environment
from simrun.snapshot import SourceProvenance


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UV_OFFLINE", "1")


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
    tmp_path: Path, lib_deps: list[str], name: str = "store", shared: Path | None = None
) -> tuple[Path, SourceProvenance, list[SourceProvenance]]:
    store = tmp_path / name
    extra = shared or tmp_path / "extra"
    _project(extra, "extra", [])
    _write(extra / "extra.py", "")
    _project(
        store / "snap/model",
        "model",
        ["lib", "extra"],
        f'[tool.uv.sources]\nextra = {{ path = "{extra}" }}\n',
    )
    _project(store / "snap/lib", "lib", lib_deps)
    return (
        store,
        _provenance("model", "snap/model"),
        [_provenance("lib", "snap/lib")],
    )


def test_snapshot_dependency_in_lock(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    env = resolve_environment(store, model, libs)
    assert 'name = "extra"' in env.lock
    assert env.editable == ["snap/model", "snap/lib"]
    assert env.python == "3.14"


def test_lock_independent_of_store_location(tmp_path: Path) -> None:
    extra = tmp_path / "extra"
    store, model, libs = _make(tmp_path / "a", ["extra"], shared=extra)
    other, model2, libs2 = _make(tmp_path / "b", ["extra"], name="mnt", shared=extra)
    a = resolve_environment(store, model, libs)
    b = resolve_environment(other, model2, libs2)
    assert a.lock_sha256 == b.lock_sha256


def test_hash_stable(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    assert (
        resolve_environment(store, model, libs).lock_sha256
        == resolve_environment(store, model, libs).lock_sha256
    )


def test_hash_changes_with_dependencies(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    before = resolve_environment(store, model, libs)
    _project(store / "snap/lib", "lib", ["extra"])
    assert resolve_environment(store, model, libs).lock_sha256 != before.lock_sha256


def test_existing_sources_overridden_by_name(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    _project(
        store / "snap/model",
        "model",
        ["lib"],
        '[tool.uv.sources]\nlib = { path = "/does/not/exist" }\n',
    )
    env = resolve_environment(store, model, libs)
    assert "/does/not/exist" not in env.lock


def test_image_ref(tmp_path: Path) -> None:
    env = Environment(lock="l", lock_sha256="ab" * 32, python="3.14", editable=[])
    assert image_ref(env, "reg:5000") == f"reg:5000/simrun-env:{'ab' * 8}"


def test_dockerfile_has_no_source(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    text = dockerfile(resolve_environment(store, model, libs), "reg:5000")
    assert text.startswith("FROM reg:5000/uv:python3.14-bookworm-slim")
    assert "COPY" not in text
    assert "snap/" not in text
    assert "uv pip install" in text and "simrun" in text
    assert "extra" in text
    assert "lib==" not in text and "model==" not in text


def test_missing_pyproject(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    (store / "snap/lib/pyproject.toml").unlink()
    with pytest.raises(ValueError, match=r"pyproject\.toml"):
        resolve_environment(store, model, libs)
