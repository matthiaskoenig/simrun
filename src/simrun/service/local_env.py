"""The environment of a local run with snapshots: a venv of the run.

A run of the local backend with snapshots (`--snapshot`) must not install them into
the environment of the service, the one of the user. The backend creates a venv per
run in `.simrun/envs/<run>/venv`, which sees the packages of the service through a
path file (`BASE_PTH`), installs the snapshots into it as editable packages from
shadow trees in `.simrun/envs/<run>/sources/` (`simrun.worker.shadow_tree`), and
starts the harness with its python; a `command` task finds it first on `PATH`. The
venv is removed at the end of the run.

The snapshots are installed without their dependencies, which the environment of
the service has, with `uv` (on `PATH` or the `uv` package), else with `pip`; their
build backends come from the index or the cache of the installer, as for any build.
"""

import contextlib
import importlib.util
import logging
import os
import shutil
import signal
import site
import subprocess
import sys
import sysconfig
import venv
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from simrun.worker import shadow_tree

logger = logging.getLogger(__name__)

BASE_PTH = "~simrun-base.pth"
"""The path file of the packages of the service in the venv of a run; named to sort
after the path files of the snapshots (`site` reads them in the order of their
names), so a snapshot comes before the same package of the service."""
VENV_DIR = "venv"
SOURCES_DIR = "sources"
_POLL_S = 0.1


class RunEnvironmentError(RuntimeError):
    """The venv of a run cannot be created or the snapshots cannot be installed."""


def base_site_dirs() -> list[str]:
    """The site directories of this interpreter, the user site when enabled."""
    dirs = list(site.getsitepackages())
    if site.ENABLE_USER_SITE:
        dirs.append(site.getusersitepackages())
    return [d for d in dict.fromkeys(dirs) if Path(d).is_dir()]


def installer(python: Path) -> list[str]:
    """The command which installs into the venv of `python`, before its arguments.

    Raises:
        RunEnvironmentError: Neither uv nor pip is installed.
    """
    uv = shutil.which("uv")
    if uv is None and importlib.util.find_spec("uv") is not None:
        from uv import find_uv_bin

        with contextlib.suppress(FileNotFoundError):
            uv = find_uv_bin()
    if uv is not None:
        return [uv, "pip", "install", "--python", str(python), "--no-deps"]
    if importlib.util.find_spec("pip") is not None:
        # the pip of the service, through the path file, installs into the venv
        return [str(python), "-m", "pip", "install", "--no-deps", "--no-input"]
    raise RunEnvironmentError(
        "the snapshots of a run cannot be installed: neither uv nor pip is "
        f"installed for {sys.executable}; install uv (https://docs.astral.sh/uv/)"
    )


def create(
    directory: Path,
    snapshots: list[str],
    log: BinaryIO,
    cancelled: Callable[[], bool] = lambda: False,
) -> Path:
    """Create the venv of a run and install its snapshots into it.

    Args:
        directory: The directory of the environment of the run, replaced.
        snapshots: The paths of the snapshots.
        log: Where the output of the installer goes.
        cancelled: Polled while the installer runs; when it holds, the installer is
            killed.

    Returns:
        The python of the venv.

    Raises:
        RunEnvironmentError: The venv cannot be created, there is no installer, or
            the installation failed or was cancelled.
    """
    if directory.exists():
        shutil.rmtree(directory)
    target = directory / VENV_DIR
    try:
        venv.EnvBuilder(symlinks=True, with_pip=False).create(target)
        purelib = Path(
            sysconfig.get_path(
                "purelib", "venv", {"base": str(target), "platbase": str(target)}
            )
        )
        purelib.mkdir(parents=True, exist_ok=True)
        lines = [f"import site; site.addsitedir({d!r})\n" for d in base_site_dirs()]
        (purelib / BASE_PTH).write_text("".join(lines))
        python = target / "bin" / "python"
        command = installer(python)
        for index, snapshot in enumerate(snapshots):
            source = Path(snapshot)
            shadow = shadow_tree(
                source, directory / SOURCES_DIR / f"{index}-{source.name}"
            )
            command += ["-e", str(shadow)]
    except OSError as error:
        raise RunEnvironmentError(
            f"the venv of the run cannot be created: {error}"
        ) from error
    log.write(f"installing {', '.join(snapshots)} into {target}\n".encode())
    log.flush()
    code = _run(command, log, cancelled)
    if code != 0:
        raise RunEnvironmentError(
            f"installing the snapshots into {target} failed with exit {code}"
        )
    return python


def _run(command: list[str], log: BinaryIO, cancelled: Callable[[], bool]) -> int:
    """Run the installer in its own process group; killed when `cancelled` holds."""
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as error:
        raise RunEnvironmentError(f"the installer cannot start: {error}") from error
    while True:
        try:
            return process.wait(_POLL_S)
        except subprocess.TimeoutExpired:
            pass
        if cancelled():
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise RunEnvironmentError("the installation was cancelled")
