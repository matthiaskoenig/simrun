import dataclasses
import subprocess
import sys
from pathlib import Path

import pytest

from simrun.environment import (
    BUILD_CONSTRAINTS,
    Environment,
    dockerfile,
    image_ref,
    load_constraints,
    resolve_environment,
)
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
    with pytest.raises(ValueError, match=r"'other'.*source"):
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
    ref = image_ref(env, "reg:5000", "FROM a\n")
    assert ref.startswith("reg:5000/simrun-env:")
    assert len(ref.rpartition(":")[2]) == 16
    assert ref == image_ref(env, "reg:5000", "FROM a\n")
    # the image of another Dockerfile, e.g. another base image or other system
    # packages, is another image, not the one built before
    assert ref != image_ref(env, "reg:5000", "FROM b\n")
    other = Environment(
        lock="l", lock_sha256="cd" * 32, python="3.14", simrun_version="1", editable=[]
    )
    assert ref != image_ref(other, "reg:5000", "FROM a\n")


def test_dockerfile_installs_the_system_packages(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    env = _resolve(store, model, libs)
    text = dockerfile(env, "reg:5000/uv:1", "simrun-0.1.0.whl", ["libexpat1", "zlib1g"])
    install = next(line for line in text.splitlines() if "apt-get" in line)
    assert "apt-get install -y --no-install-recommends libexpat1 zlib1g" in install
    assert "rm -rf /var/lib/apt/lists/*" in install
    # before the requirements, whose wheels link the libraries
    assert text.index("apt-get") < text.index("uv pip install")
    assert "apt-get" not in dockerfile(env, "reg:5000/uv:1", "simrun-0.1.0.whl")


def test_dockerfile_has_no_source(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, ["extra"])
    base = "reg:5000/astral-sh/uv:0.13.7-python3.14-trixie-slim@sha256:00"
    text = dockerfile(_resolve(store, model, libs), base, "simrun-0.1.0.whl")
    assert text.startswith(f"FROM {base}\n")
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
        "forbidden = {'fastapi', 'kubernetes', 'hera', 'httpx', 'fastmcp', "
        "'packaging', 'simrun.environment'}; "
        "assert not forbidden & set(sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_missing_pyproject(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    (store / "snap/lib/pyproject.toml").unlink()
    with pytest.raises(ValueError, match=r"pyproject\.toml"):
        _resolve(store, model, libs)


def test_normalised_names(tmp_path: Path) -> None:
    store = tmp_path / "store"
    _write(store / "snap/model/uv.lock", LOCK)
    _project(store / "snap/model", "My_Model", ["my.lib", "pydantic[email]>=2.5"])
    _project(store / "snap/lib", "my_lib", [])
    env = _resolve(
        store, _provenance("model", "snap/model"), [_provenance("lib", "snap/lib")]
    )
    assert 'name = "my-lib"' in env.lock
    assert '{ name = "pydantic", specifier = ">=2.14" }' in env.lock


def _build_system(path: Path, requires: list[str], backend: str) -> None:
    with path.open("a") as handle:
        handle.write(
            f"[build-system]\nrequires = {requires!r}\nbuild-backend = {backend!r}\n"
        )


def _lines(requirements: str) -> set[str]:
    return {line.partition("==")[0] for line in requirements.splitlines()}


def test_build_requirements_in_the_image(tmp_path: Path) -> None:
    # the snapshots are installed without build isolation and without an index at
    # pod start: their build backends, also the requirements of an editable build of
    # the backend (`editables` of hatchling), must be in the image
    store, model, libs = _make(tmp_path, ["extra"])
    _build_system(store / "snap/model/pyproject.toml", ["hatchling"], "hatchling.build")
    _build_system(
        store / "snap/lib/pyproject.toml", ["setuptools>=61"], "setuptools.build_meta"
    )
    env = _resolve(store, model, libs)
    assert "hatchling" in env.build_requirements
    assert "setuptools>=61" in env.build_requirements
    assert any(r.startswith("editables") for r in env.build_requirements)
    assert {"hatchling", "editables", "setuptools"} <= _lines(env.requirements)
    text = dockerfile(env, "reg:5000/uv:1", "simrun-0.1.0.whl")
    assert "hatchling==" in text and "editables==" in text


def test_default_build_system(tmp_path: Path) -> None:
    # without [build-system] a project is built with the legacy backend of setuptools
    store, model, libs = _make(tmp_path, [])
    env = _resolve(store, model, libs)
    assert "setuptools>=40.8.0" in env.build_requirements
    assert "setuptools" in _lines(env.requirements)


def test_failing_build_backend(tmp_path: Path) -> None:
    store, model, libs = _make(tmp_path, [])
    _build_system(store / "snap/lib/pyproject.toml", [], "no_such_backend")
    with pytest.raises(RuntimeError, match=r"(?s)'lib'.*no_such_backend"):
        _resolve(store, model, libs)


def _tree(root: Path) -> dict[str, tuple[int, bytes]]:
    """Every entry below `root` with its mode and its content or link target."""
    found: dict[str, tuple[int, bytes]] = {}
    for path in sorted(root.rglob("*")):
        st = path.lstat()
        if path.is_symlink():
            content = str(path.readlink()).encode()
        elif path.is_file():
            content = path.read_bytes()
        else:
            content = b""
        found[path.relative_to(root).as_posix()] = (st.st_mode, content)
    return found


def test_resolve_never_writes_a_snapshot(tmp_path: Path) -> None:
    # a library of setuptools with dynamic metadata: locking builds its metadata,
    # which setuptools writes next to its sources (`*.egg-info`)
    store, model, libs = _make(tmp_path, [])
    lib = store / "snap/lib"
    _write(
        lib / "pyproject.toml",
        '[build-system]\nrequires = ["setuptools>=61"]\n'
        'build-backend = "setuptools.build_meta"\n'
        '[project]\nname = "lib"\ndynamic = ["version", "readme"]\n'
        'requires-python = ">=3.14"\ndependencies = []\n'
        "[tool.setuptools.dynamic]\n"
        'version = {attr = "lib.__version__"}\nreadme = {file = "README.txt"}\n',
    )
    _write(lib / "src/lib/__init__.py", '__version__ = "0.3"\n')
    _write(lib / "README.txt", "lib\n")
    (store / "snap/model/main.py").write_text("print(1)\n")
    before = {d: _tree(store / "snap" / d) for d in ("model", "lib", "extra")}
    env = _resolve(store, model, libs)
    assert 'name = "lib"' in env.lock
    assert {d: _tree(store / "snap" / d) for d in ("model", "lib", "extra")} == before


def _versions(lock: str) -> dict[str, str]:
    import tomllib

    return {p["name"]: p.get("version", "") for p in tomllib.loads(lock)["package"]}


def test_build_backends_are_pinned_by_the_constraints_of_the_service(
    tmp_path: Path,
) -> None:
    store, model, libs = _make(tmp_path, [])
    _build_system(store / "snap/model/pyproject.toml", ["hatchling"], "hatchling.build")
    env = _resolve(store, model, libs)
    pins = load_constraints(BUILD_CONSTRAINTS)
    versions = _versions(env.lock)
    # the backend and what it asks for are the versions of the constraints, not
    # the newest of the index, so a release of a backend changes no lock
    for name in ("hatchling", "editables"):
        assert pins[name] == f"{name}=={versions[name]}"
    # a constraints file of the service with another pin wins
    other = tmp_path / "constraints.txt"
    other.write_text("# raised by hand\nhatchling==1.33.0\neditables == 0.6\n")
    assert load_constraints(other) == {
        "hatchling": "hatchling==1.33.0",
        "editables": "editables==0.6",
    }
    pinned = resolve_environment(
        store, model, libs, simrun_version="0.1.0", build_constraints=other
    )
    assert _versions(pinned.lock)["hatchling"] == "1.33.0"
    # a pin is applied: a version which is not available fails the resolve
    other.write_text("hatchling==1.0.0\n")
    with pytest.raises(RuntimeError, match=r"hatchling==1\.0\.0"):
        resolve_environment(
            store, model, libs, simrun_version="0.1.0", build_constraints=other
        )


def test_the_constraints_file_names_exact_versions(tmp_path: Path) -> None:
    bad = tmp_path / "constraints.txt"
    bad.write_text("hatchling>=1\n")
    with pytest.raises(ValueError, match="hatchling>=1"):
        load_constraints(bad)


def test_dockerfile_installs_git_for_a_git_dependency(tmp_path: Path) -> None:
    # uv needs git to install a dependency locked from a git repository, e.g.
    # `pkpdsim @ git+https://...` of a branch of a model; the slim base has none
    store, model, libs = _make(tmp_path, [])
    env = _resolve(store, model, libs)
    assert "git" not in dockerfile(env, "reg:5000/uv:1", "w.whl", ["libexpat1"])
    pinned = "pkpdsim @ git+https://github.com/matthiaskoenig/pkpdsim@9065f38\n"
    with_git = dataclasses.replace(env, requirements=env.requirements + pinned)
    text = dockerfile(with_git, "reg:5000/uv:1", "w.whl", ["libexpat1"])
    install = next(line for line in text.splitlines() if "apt-get" in line)
    assert "--no-install-recommends libexpat1 git " in install
    assert dockerfile(with_git, "reg:5000/uv:1", "w.whl").count("apt-get install") == 1
