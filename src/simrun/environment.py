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
# what the worker of simrun imports besides the standard library
WORKER_REQUIREMENTS = ("pydantic>=2.14", "pyyaml>=6.0.3")
LOCAL_SOURCES = ("directory", "editable", "path", "virtual")


def _reject_host_paths(lock: str, allowed: list[str]) -> None:
    """Raise when the lock has a local package which is not a snapshotted source."""
    for package in tomllib.loads(lock).get("package", []):
        source = package.get("source", {})
        url = str(source.get("url", ""))
        local = any(key in source for key in LOCAL_SOURCES) or url.startswith("file:")
        if local and package["name"] not in allowed:
            raise ValueError(
                f"The model depends on the local package {package['name']!r}, which "
                "is not a snapshotted source: pass its working tree as a source"
            )


@dataclass(frozen=True)
class Environment:
    """The resolved environment of a run.

    Attributes:
        lock: Text of the `uv.lock`.
        lock_sha256: Hash of the lock and the python version, the identity of the image.
        python: Python version of the image.
        simrun_version: Version of the simrun wheel the image installs.
        editable: Snapshot directories of the model and the libraries, relative to
            the store in the order of the sources, installed at pod start.
        requirements: The third party dependencies of the lock as pinned requirements,
            without the model and the snapshotted libraries.
    """

    lock: str
    lock_sha256: str
    python: str
    simrun_version: str
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
    *,
    simrun_version: str,
) -> Environment:
    """Lock the dependencies of the model with its libraries as snapshot paths.

    Args:
        store: Root of the run store.
        model: Snapshot of the model repository, which has the `pyproject.toml`.
        libraries: Snapshots of the libraries; each replaces the entry of
            `[tool.uv.sources]` of its package name, the other entries are kept.
        python: Python version of the image.
        simrun_version: Version of the simrun wheel of the image, part of the hash.

    Returns:
        The environment.

    Raises:
        ValueError: A snapshot has no `pyproject.toml`, the model has no `uv.lock`
            (it would resolve against the live index and the hash would drift), or
            the model depends on a local package which is not a snapshotted source.
        RuntimeError: `uv` fails to lock.
    """
    pyproject = _pyproject(store, model)
    names = [_package_name(_pyproject(store, library)) for library in libraries]
    model_lock = store / model.snapshot / "uv.lock"
    if not model_lock.is_file():
        raise ValueError(
            f"The model {model.name!r} has no uv.lock: run `uv lock` and commit it"
        )
    document = tomlkit.parse(pyproject.read_text())
    project = document.setdefault("project", tomlkit.table())
    dependencies = project.setdefault("dependencies", tomlkit.array())
    # uv applies a source only to a dependency of the root, so the libraries are
    # dependencies of the temporary project, which locks their dependencies as well
    for requirement in (*WORKER_REQUIREMENTS, *names):
        stem = requirement.split(">")[0].lower()
        if not any(
            str(d).lower().replace("_", "-").split(">")[0].strip() == stem
            for d in dependencies
        ):
            dependencies.append(requirement)
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
        (work / "uv.lock").write_bytes(model_lock.read_bytes())
        (work / STORE_LINK).symlink_to(store.resolve(), target_is_directory=True)
        _uv(["lock", "--python", python], work)
        lock = (work / "uv.lock").read_text()
        _reject_host_paths(lock, [_package_name(pyproject), *names])
        skipped = [arg for name in names for arg in ("--no-emit-package", name)]
        requirements = _uv(
            [
                "export",
                "--frozen",
                "--no-emit-project",
                "--no-dev",
                "--no-default-groups",
                "--no-header",
                "--no-annotate",
                "--python",
                python,
                *skipped,
            ],
            work,
        )
    digest = hashlib.sha256(f"{python}\n{simrun_version}\n{lock}".encode()).hexdigest()
    return Environment(
        lock=lock,
        lock_sha256=digest,
        python=python,
        simrun_version=simrun_version,
        editable=[model.snapshot, *(library.snapshot for library in libraries)],
        requirements=requirements,
    )


def image_ref(env: Environment, registry: str) -> str:
    """Reference of the image of an environment in the registry."""
    return f"{registry}/simrun-env:{env.lock_sha256[:16]}"


def dockerfile(env: Environment, registry: str, wheel: str) -> str:
    """Dockerfile of the image: the third party dependencies and `simrun`, no source.

    The pinned requirements are written with a heredoc, so no source is copied from the
    build context. The model and the libraries are installed at pod start.

    Args:
        env: The environment.
        registry: Registry which mirrors the base image of uv.
        wheel: File name of the simrun wheel in the build context, the only file
            copied; simrun is unpublished, so it is never installed by name.
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
        "&& rm /tmp/requirements.txt\n"
        f"COPY {wheel} /tmp/{wheel}\n"
        f"RUN uv pip install --no-deps /tmp/{wheel} && rm /tmp/{wheel}\n"
    )
