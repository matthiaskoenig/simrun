"""The environment of a run: a lock from the model and its snapshotted libraries.

The image of a run holds only the third party dependencies. The code of the model
repository and of the libraries comes from snapshots which the pod mounts read-only
and installs editable at start (`SIMRUN_EDITABLE`, see `simrun.worker`), so a change
of the code never rebuilds an image and a change of a dependency does.

A pod installs the snapshots without build isolation and without an index, so the
image holds their build backends: the `[build-system].requires` of the model and of
every library (the legacy backend of setuptools without a `[build-system]`, PEP 517)
and what the backend asks for an editable build (`get_requires_for_build_editable`,
PEP 660, e.g. `editables` of hatchling), which a hook asks at resolve time in an
environment of `uv run` with the static requirements, in a shadow tree of the
snapshot. They are dependencies of the temporary project, so the lock pins them with
the third party dependencies and the image installs them. Their versions are the pins
of a constraints file of the service (`build_constraints.txt`, `BUILD_CONSTRAINTS`),
for the packages the `uv.lock` of the model does not have, so a release of a backend
changes no lock.

The lock is resolved in a temporary project, a writable shadow tree of the model
(`simrun.worker.shadow_tree`: its directories, links to its files) with its
`pyproject.toml` changed and a copy of its `uv.lock`, and a directory
`.simrun-store` with a shadow tree of every library at `.simrun-store/<snapshot>`.
`uv lock` builds the metadata of a source with dynamic metadata, which a backend
writes next to the sources (e.g. the `*.egg-info` of setuptools): into the shadows,
never into the snapshots, which are named by the hash of their content. The sources
of the libraries are the relative paths `.simrun-store/<snapshot>`, so the lock
carries these paths and not the place the store is mounted at (`/mnt/cluster` on the
development server, `/store` in the service): the same snapshots give the same lock
and hash anywhere.
"""

import hashlib
import json
import logging
import re
import subprocess
import tempfile
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import tomlkit

from simrun.snapshot import SourceProvenance
from simrun.worker import shadow_tree

logger = logging.getLogger(__name__)

# the directory of the shadows of the libraries in the project which is locked, a
# name which a model repository does not use
SHADOWS_DIR = ".simrun-store"
# the pinned versions of the build backends, see the file
BUILD_CONSTRAINTS = Path(__file__).with_name("build_constraints.txt")
NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
PIN = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*==\s*([^\s;#]+)$")
# what the worker of simrun imports besides the standard library
WORKER_REQUIREMENTS = ("pydantic>=2.14", "pyyaml>=6.0.3")
LOCAL_SOURCES = ("directory", "editable", "path", "virtual")
# the build system of a project without `[build-system]` (PEP 517)
DEFAULT_REQUIRES = ("setuptools>=40.8.0",)
DEFAULT_BACKEND = "setuptools.build_meta:__legacy__"
# asks a build backend in the source tree what it needs for an editable build
EDITABLE_HOOK = """
import importlib, json, os, sys
backend, source, out, *paths = sys.argv[1:]
os.chdir(source)
sys.path[:0] = [os.path.join(source, path) for path in paths]
module, _, attributes = backend.partition(":")
target = importlib.import_module(module)
for attribute in filter(None, attributes.split(".")):
    target = getattr(target, attribute)
hook = getattr(target, "get_requires_for_build_editable", None)
with open(out, "w") as handle:
    json.dump([] if hook is None else list(hook()), handle)
"""


def normalise(name: str) -> str:
    """Normalised name of a package (PEP 503), as `uv.lock` writes it."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _reject_host_paths(lock: str, allowed: list[str]) -> None:
    """Raise when the lock has a local package which is not a snapshotted source."""
    for package in tomllib.loads(lock).get("package", []):
        source = package.get("source", {})
        url = str(source.get("url", ""))
        local = any(key in source for key in LOCAL_SOURCES) or url.startswith("file:")
        if local and normalise(package["name"]) not in {normalise(a) for a in allowed}:
            raise ValueError(
                f"The model depends on the local package {package['name']!r}, which "
                "is not a snapshotted source: pass its working tree as a source"
            )


def load_constraints(path: Path) -> dict[str, str]:
    """The pins of a constraints file by normalised name, e.g. `hatchling==1.33.0`.

    Raises:
        ValueError: A line is no exact pin `<name>==<version>`.
    """
    pins: dict[str, str] = {}
    for line in path.read_text().splitlines():
        text = line.partition("#")[0].strip()
        if not text:
            continue
        found = PIN.match(text)
        if found is None:
            raise ValueError(f"{path}: {text!r} is no pin <name>==<version>")
        pins[normalise(found[1])] = f"{found[1]}=={found[2]}"
    return pins


def _pinned(requirements: list[str], pins: dict[str, str]) -> list[str]:
    """The pins of the packages of requirements, for `uv run --with`."""
    names = [NAME.match(r) for r in requirements]
    return [pins[normalise(n[1])] for n in names if n and normalise(n[1]) in pins]


@dataclass(frozen=True)
class Environment:
    """The resolved environment of a run.

    Attributes:
        lock: Text of the `uv.lock`.
        lock_sha256: Hash of the lock and the python version, the identity of the image.
        python: Python version of the image.
        simrun_version: Version of the simrun wheel the image installs, with the
            hash of the wheel when the service has one.
        editable: Snapshot directories of the model and the libraries, relative to
            the store in the order of the sources, installed at pod start.
        requirements: The third party dependencies of the lock as pinned requirements,
            without the model and the snapshotted libraries.
        build_requirements: The requirements of the build backends of the sources,
            which the lock pins with the dependencies.
    """

    lock: str
    lock_sha256: str
    python: str
    simrun_version: str
    editable: list[str]
    requirements: str = field(default="", compare=False)
    build_requirements: tuple[str, ...] = field(default=(), compare=False)


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


def build_requirements(
    directory: Path, name: str, python: str, pins: dict[str, str] | None = None
) -> list[str]:
    """What building a source editable needs: its build system and its backend's hook.

    Args:
        directory: The snapshot of the source, with its `pyproject.toml`.
        name: The name of the source, for messages.
        python: The python version the hook runs with.
        pins: The pinned versions of build backends (`load_constraints`), applied
            to the environment the hook runs in.

    Returns:
        `[build-system].requires` and the result of the backend's
        `get_requires_for_build_editable`.

    Raises:
        RuntimeError: The backend cannot be loaded or its hook fails.
    """
    with (directory / "pyproject.toml").open("rb") as handle:
        system = tomllib.load(handle).get("build-system")
    if system is None:
        requires, backend, paths = list(DEFAULT_REQUIRES), DEFAULT_BACKEND, []
    else:
        requires = [str(r) for r in system.get("requires", [])]
        backend = str(system.get("build-backend", DEFAULT_BACKEND))
        paths = [str(p) for p in system.get("backend-path", [])]
    with tempfile.TemporaryDirectory(prefix="simrun-build-") as tmp:
        work = Path(tmp)
        # the hook may write into the tree, never into the snapshot
        shadow = shadow_tree(directory, work / "source")
        out = work / "requires.json"
        pinned = _pinned(requires, pins or {})
        with_args = [arg for r in (*requires, *pinned) for arg in ("--with", r)]
        command = ["run", "--no-project", "--isolated", "--python", python]
        command += [*with_args, "--", "python", "-P", "-c", EDITABLE_HOOK]
        try:
            _uv([*command, backend, str(shadow), str(out), *paths], work)
            dynamic = json.loads(out.read_text())
        except (RuntimeError, OSError, ValueError) as error:
            raise RuntimeError(
                f"the build backend {backend!r} of the source {name!r} cannot tell "
                f"what an editable build needs: {error}"
            ) from error
    return [*requires, *(str(r) for r in dynamic)]


def resolve_environment(
    store: Path,
    model: SourceProvenance,
    libraries: list[SourceProvenance],
    python: str = "3.14",
    *,
    simrun_version: str,
    build_constraints: Path = BUILD_CONSTRAINTS,
) -> Environment:
    """Lock the dependencies of the model with its libraries as snapshot paths.

    Args:
        store: Root of the run store.
        model: Snapshot of the model repository, which has the `pyproject.toml`.
        libraries: Snapshots of the libraries; each replaces the entry of
            `[tool.uv.sources]` of its package name, the other entries are kept.
        python: Python version of the image.
        simrun_version: Version of the simrun wheel of the image, part of the hash
            (the service adds the hash of the wheel, `Submitter._harness`).
        build_constraints: The pinned versions of the build backends; a pin of a
            package the `uv.lock` of the model does not have constrains the lock.

    Returns:
        The environment.

    Raises:
        ValueError: A snapshot has no `pyproject.toml`, the model has no `uv.lock`
            (it would resolve against the live index and the hash would drift), or
            the model depends on a local package which is not a snapshotted source.
        RuntimeError: `uv` fails to lock, or a build backend cannot tell what an
            editable build needs.
    """
    pyproject = _pyproject(store, model)
    names = [_package_name(_pyproject(store, library)) for library in libraries]
    model_lock = store / model.snapshot / "uv.lock"
    if not model_lock.is_file():
        raise ValueError(
            f"The model {model.name!r} has no uv.lock: run `uv lock` and commit it"
        )
    pins = load_constraints(build_constraints)
    locked = {
        normalise(p["name"])
        for p in tomllib.loads(model_lock.read_text()).get("package", [])
    }
    builds: list[str] = []
    for source in (model, *libraries):
        for requirement in build_requirements(
            store / source.snapshot, source.name, python, pins
        ):
            if requirement not in builds:
                builds.append(requirement)
    document = tomlkit.parse(pyproject.read_text())
    project = document.setdefault("project", tomlkit.table())
    dependencies = project.setdefault("dependencies", tomlkit.array())
    # uv applies a source only to a dependency of the root, so the libraries are
    # dependencies of the temporary project, which locks their dependencies as well
    # the worker requirements are always added: uv intersects the requirements of a
    # package, so the lock never resolves one below the floor of the worker; the
    # build requirements too, the pods build the sources in the image
    for requirement in (*WORKER_REQUIREMENTS, *builds, *names):
        dependencies.append(requirement)
    tool = document.setdefault("tool", tomlkit.table())
    uv = tool.setdefault("uv", tomlkit.table())
    # the backends at their pins, unless the lock of the model has them already
    constraints = uv.setdefault("constraint-dependencies", tomlkit.array())
    for key, pin in pins.items():
        if key not in locked:
            constraints.append(pin)
    sources = uv.setdefault("sources", tomlkit.table())
    for name, library in zip(names, libraries, strict=True):
        for key in [k for k in sources if normalise(k) == normalise(name)]:
            del sources[key]  # the same package spelled otherwise
        entry = tomlkit.inline_table()
        entry["path"] = f"{SHADOWS_DIR}/{library.snapshot}"
        sources[name] = entry
    with tempfile.TemporaryDirectory(prefix="simrun-env-") as tmp:
        # uv writes the metadata it builds into the shadows, never the snapshots
        work = shadow_tree(store / model.snapshot, Path(tmp) / "project")
        for name in ("pyproject.toml", "uv.lock"):
            (work / name).unlink()
        (work / "pyproject.toml").write_text(tomlkit.dumps(document))
        (work / "uv.lock").write_bytes(model_lock.read_bytes())
        for library in libraries:
            shadow_tree(store / library.snapshot, work / SHADOWS_DIR / library.snapshot)
        try:
            _uv(["lock", "--python", python], work)
        except RuntimeError as error:
            raise RuntimeError(
                "The dependencies of the model cannot be locked with the "
                f"requirements of the worker {', '.join(WORKER_REQUIREMENTS)}, the "
                f"build requirements {', '.join(builds)} and the libraries: {error}"
            ) from error
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
        build_requirements=tuple(builds),
    )


def image_ref(env: Environment, registry: str, dockerfile_text: str) -> str:
    """Reference of the image of an environment in the registry.

    Args:
        env: The environment.
        registry: The registry of the cluster.
        dockerfile_text: The Dockerfile of the image (`dockerfile`); with the lock it
            is the identity of the image, so another base image or other system
            packages give another image instead of the one built before.
    """
    digest = hashlib.sha256(f"{env.lock_sha256}\n{dockerfile_text}".encode())
    return f"{registry}/simrun-env:{digest.hexdigest()[:16]}"


# the cache of uv in a build: a cache of BuildKit, kept between the builds and outside
# of the image, so a build downloads only the wheels an earlier one did not
UV_CACHE = "/root/.cache/uv"
_UV_RUN = (
    f"RUN --mount=type=cache,id=simrun-uv,target={UV_CACHE} UV_CACHE_DIR={UV_CACHE}"
)
# a requirement locked from a git repository, e.g. `pkpdsim @ git+https://...`
GIT_REQUIREMENT = re.compile(r"@ git\+", re.MULTILINE)


def dockerfile(
    env: Environment,
    base_image: str,
    wheel: str,
    system_packages: Sequence[str] = (),
) -> str:
    """Dockerfile of the image: the dependencies, build backends and `simrun`, no source.

    The packages are installed with their bytecode compiled, so a pod does not
    compile the modules it imports, and with the downloads of uv in a cache of
    BuildKit (`UV_CACHE`), so a build after a change of a dependency downloads only
    what changed. `simrun` is the last layer: a new harness rebuilds only it.

    The pinned requirements are written with a heredoc, so no source is copied from the
    build context. The model and the libraries are installed at pod start.

    Args:
        env: The environment.
        base_image: The base image, with uv and the python of the environment, in
            the registry of the cluster (`ClusterConfig.base_image_ref`).
        wheel: File name of the simrun wheel in the build context, the only file
            copied; the harness of the service, never installed by name.
        system_packages: Debian packages installed before the requirements, the
            shared libraries their wheels link (`ClusterConfig.system_packages`);
            `git` as well when a requirement is locked from a git repository.
    """
    packages = list(system_packages)
    if GIT_REQUIREMENT.search(env.requirements) and "git" not in packages:
        packages.append("git")  # uv clones a dependency locked from git
    system = ""
    if packages:
        system = (
            "RUN apt-get update && apt-get install -y --no-install-recommends "
            f"{' '.join(packages)} && rm -rf /var/lib/apt/lists/*\n"
        )
    return (
        f"FROM {base_image}\n"
        f"{system}"
        "ENV UV_PYTHON_DOWNLOADS=never UV_LINK_MODE=copy VIRTUAL_ENV=/opt/venv "
        'PATH="/opt/venv/bin:$PATH"\n'
        f"RUN uv venv /opt/venv --python {env.python}\n"
        "RUN cat > /tmp/requirements.txt <<'SIMRUN_REQUIREMENTS'\n"
        f"{env.requirements.rstrip()}\n"
        "SIMRUN_REQUIREMENTS\n"
        f"{_UV_RUN} uv pip install --no-deps --compile-bytecode "
        "-r /tmp/requirements.txt && rm /tmp/requirements.txt\n"
        f"COPY {wheel} /tmp/{wheel}\n"
        f"{_UV_RUN} uv pip install --no-deps --compile-bytecode /tmp/{wheel} "
        f"&& rm /tmp/{wheel}\n"
    )
