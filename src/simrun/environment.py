"""The environment of a run: a lock from the model and its snapshotted libraries.

The image of a run holds only the third party dependencies. The code of the model
repository and of the libraries comes from snapshots which the pod mounts read-only
and installs editable at start (`SIMRUN_EDITABLE`, see `simrun.worker`), so a change
of the code never rebuilds an image and a change of a dependency does.

The lock is resolved in a temporary project which holds a copy of the `pyproject.toml`
and `uv.lock` of the model and a symlink `store` to the store. The sources of the
libraries are the relative paths `store/<snapshot>`, so the lock carries these paths
and not the place the store is mounted at (`/mnt/cluster` on the development server,
`/store` in the service): the same snapshots give the same lock and hash anywhere.
"""

import hashlib
import logging
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import tomlkit

from simrun.snapshot import SourceProvenance

logger = logging.getLogger(__name__)

STORE_LINK = "store"


@dataclass(frozen=True)
class Environment:
    """The resolved environment of a run.

    Attributes:
        lock: Text of the `uv.lock`.
        lock_sha256: Hash of the lock and the python version, the identity of the image.
        python: Python version of the image.
        editable: Snapshot directories of the model and the libraries, relative to
            the store in the order of the sources, installed at pod start.
        requirements: The third party dependencies of the lock as pinned requirements,
            without the model and the snapshotted libraries.
    """

    lock: str
    lock_sha256: str
    python: str
    editable: list[str]
    requirements: str = field(default="", compare=False)


def _uv(args: list[str], cwd: Path) -> str:
    result = subprocess.run(["uv", *args], cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"uv {args[0]} failed with exit code {result.returncode}: "
            f"{result.stderr.strip()}"
        )
    return result.stdout


def _pyproject(store: Path, source: SourceProvenance) -> Path:
    path = store / source.snapshot / "pyproject.toml"
    if not path.is_file():
        raise ValueError(f"The source {source.name!r} has no pyproject.toml: {path}")
    return path


def _package_name(path: Path) -> str:
    with path.open("rb") as handle:
        name = tomllib.load(handle).get("project", {}).get("name")
    if not isinstance(name, str):
        raise ValueError(f"{path} has no [project].name")
    return name


def resolve_environment(
    store: Path,
    model: SourceProvenance,
    libraries: list[SourceProvenance],
    python: str = "3.14",
) -> Environment:
    """Lock the dependencies of the model with its libraries as snapshot paths.

    Args:
        store: Root of the run store.
        model: Snapshot of the model repository, which has the `pyproject.toml`.
        libraries: Snapshots of the libraries; each replaces the entry of
            `[tool.uv.sources]` of its package name, the other entries are kept.
        python: Python version of the image.

    Returns:
        The environment.

    Raises:
        ValueError: A snapshot has no `pyproject.toml`.
        RuntimeError: `uv` fails to lock.
    """
    pyproject = _pyproject(store, model)
    names = [_package_name(_pyproject(store, library)) for library in libraries]
    document = tomlkit.parse(pyproject.read_text())
    tool = document.setdefault("tool", tomlkit.table())
    uv = tool.setdefault("uv", tomlkit.table())
    sources = uv.setdefault("sources", tomlkit.table())
    for name, library in zip(names, libraries, strict=True):
        entry = tomlkit.inline_table()
        entry["path"] = f"{STORE_LINK}/{library.snapshot}"
        sources[name] = entry
    with tempfile.TemporaryDirectory(prefix="simrun-env-") as tmp:
        work = Path(tmp)
        (work / "pyproject.toml").write_text(tomlkit.dumps(document))
        model_lock = store / model.snapshot / "uv.lock"
        if model_lock.is_file():
            (work / "uv.lock").write_bytes(model_lock.read_bytes())
        (work / STORE_LINK).symlink_to(store.resolve(), target_is_directory=True)
        _uv(["lock", "--python", python], work)
        lock = (work / "uv.lock").read_text()
        skipped = [arg for name in names for arg in ("--no-emit-package", name)]
        requirements = _uv(
            [
                "export",
                "--frozen",
                "--no-emit-project",
                "--no-header",
                "--no-annotate",
                "--python",
                python,
                *skipped,
            ],
            work,
        )
    digest = hashlib.sha256(f"{python}\n{lock}".encode()).hexdigest()
    return Environment(
        lock=lock,
        lock_sha256=digest,
        python=python,
        editable=[model.snapshot, *(library.snapshot for library in libraries)],
        requirements=requirements,
    )


def image_ref(env: Environment, registry: str) -> str:
    """Reference of the image of an environment in the registry."""
    return f"{registry}/simrun-env:{env.lock_sha256[:16]}"


def dockerfile(env: Environment, registry: str) -> str:
    """Dockerfile of the image: the third party dependencies and `simrun`, no source.

    The pinned requirements are written with a heredoc, so nothing is copied from the
    build context. The model and the libraries are installed at pod start.

    Args:
        env: The environment.
        registry: Registry which mirrors the base image of uv.
    """
    return (
        f"FROM {registry}/uv:python{env.python}-bookworm-slim\n"
        "ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy VIRTUAL_ENV=/opt/venv "
        'PATH="/opt/venv/bin:$PATH"\n'
        f"RUN uv venv /opt/venv --python {env.python}\n"
        "RUN cat > /tmp/requirements.txt <<'SIMRUN_REQUIREMENTS'\n"
        f"{env.requirements.rstrip()}\n"
        "SIMRUN_REQUIREMENTS\n"
        "RUN uv pip install --no-deps -r /tmp/requirements.txt "
        "&& uv pip install simrun && rm /tmp/requirements.txt\n"
    )
