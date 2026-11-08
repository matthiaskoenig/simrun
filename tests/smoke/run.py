"""Run the smoke workflows: the analyses of sbmlsim and pkdb_models through simrun.

`sbmlsim.yaml` runs every kind of analysis of the HCTZ example of sbmlsim (simulate,
fit, identifiability, fisher, report, petab, scan, sensitivity, uncertainty) with
the checkout of sbmlsim as its source; `pkdb_models.yaml` the simulations, a fit
with what reads it and the build of the models of the HCTZ model of pkdb_models,
with the checkouts of pkdb_models and sbmlsim as its sources. Both are small enough
to run in minutes and find what breaks a real model before the migration of the
models does.

The commands run in an environment of their own, `.tools/smoke/venv`: this
checkout of simrun and the checkouts of sbmlsim and pkdb_models installed editable,
so the client plans with the plugin of sbmlsim and the local service knows its task
types.

    uv run python tests/smoke/run.py                 # --local, on this machine
    uv run python tests/smoke/run.py --k3d           # on the local test system
    uv run python tests/smoke/run.py --local --k3d   # both, one after the other
    uv run python tests/smoke/run.py --only sbmlsim  # one of the workflows

`--k3d` runs a service from the environment against the cluster of the local test
system (`tests/cluster/local.py up` first, which also builds the wheel of the pods
from this checkout), on a free port, and stops it at the end. After every run the
timeline of its tasks shows where the time went (`timeline.py`), with the load of
the machine, which other work on it changes. The exit code is 0 when every run
succeeded.
"""

import argparse
import contextlib
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Generator
from pathlib import Path

import httpx
import timeline

REPO = Path(__file__).resolve().parents[2]
SMOKE = Path(__file__).resolve().parent
TOOLS = REPO / ".tools" / "smoke"
VENV = TOOLS / "venv"
# the directory of the local service of the smoke runs (`.simrun/` in it)
LOCAL = TOOLS / "local"
K3D = REPO / ".tools" / "k3d" / "local" / "settings.yaml"
K3D_STORE = REPO / ".tools" / "k3d" / "store"
LOCAL_STORE = LOCAL / ".simrun" / "store"
KUBECONFIG = REPO / ".tools" / "kubeconfig"
TOKEN = "t-mk"
TIMEOUT_S = 3600.0


def workflows(sbmlsim: Path, pkdb_models: Path) -> dict[str, list[str]]:
    """The `--source` arguments of every workflow, the model first."""
    return {
        "sbmlsim": [f"sbmlsim={sbmlsim}"],
        "pkdb_models": [f"model={pkdb_models}", f"sbmlsim={sbmlsim}"],
    }


def environment(sbmlsim: Path, pkdb_models: Path) -> Path:
    """The environment of the commands, installed anew; returns its `simrun`."""
    python = VENV / "bin" / "python"
    if not python.exists():
        subprocess.run(["uv", "venv", "-q", "--python", "3.14", str(VENV)], check=True)
    subprocess.run(
        [
            *("uv", "pip", "install", "-q", "--python", str(python)),
            *("-e", str(REPO), "-e", str(sbmlsim), "-e", str(pkdb_models)),
        ],
        check=True,
    )
    return VENV / "bin" / "simrun"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def k3d_service() -> Generator[dict[str, str]]:
    """A service of the environment against the local test system; its variables."""
    if not K3D.is_file():
        sys.exit(f"{K3D} is missing: run `uv run python tests/cluster/local.py up`")
    port = _free_port()
    env = os.environ | {"KUBECONFIG": str(KUBECONFIG), "SIMRUN_CONFIG": str(K3D)}
    process = subprocess.Popen(
        [
            str(VENV / "bin" / "uvicorn"),
            *("--factory", "simrun.service.app:serve"),
            *("--host", "127.0.0.1", "--port", str(port)),
            *("--log-level", "warning", "--timeout-graceful-shutdown", "5"),
        ],
        env=env,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 60
        while True:
            with contextlib.suppress(httpx.HTTPError):
                headers = {"Authorization": f"Bearer {TOKEN}"}
                if httpx.get(f"{url}/me", headers=headers).is_success:
                    break
            if process.poll() is not None or time.monotonic() > deadline:
                sys.exit("the service of the local test system did not start")
            time.sleep(0.5)
        yield {"SIMRUN_URL": url, "SIMRUN_TOKEN": TOKEN}
    finally:
        process.terminate()
        process.wait(30)


def _simrun(simrun: Path, args: list[str], env: dict[str, str]) -> dict:
    result = subprocess.run(
        [str(simrun), *args, "--json"],
        cwd=LOCAL,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode not in (0, 1):  # 1: a run which did not succeed
        sys.exit(f"simrun {' '.join(args)} failed:\n{result.stderr}")
    return json.loads(result.stdout)


def run(simrun: Path, name: str, sources: list[str], env: dict[str, str]) -> bool:
    """Submit a workflow, wait for its end and show its timeline; True if it succeeded."""
    k3d = "SIMRUN_URL" in env
    target = [] if k3d else ["--local"]
    source_args = [a for s in sources for a in ("--source", s)]
    load = os.getloadavg()[0]
    submitted = _simrun(
        simrun,
        ["submit", str(SMOKE / f"{name}.yaml"), *source_args, *target, "--no-wait"],
        env,
    )
    run_id = submitted["id"]
    print(f"{name} on {'k3d' if k3d else 'this machine'}: {run_id}", flush=True)
    _simrun(simrun, ["wait", run_id, *target, "--timeout", str(TIMEOUT_S)], env)
    record = _simrun(simrun, ["status", run_id, *target], env)["run"]
    if record.get("message"):
        print(f"  {record['message']}")
    line = timeline.read(K3D_STORE if k3d else LOCAL_STORE, run_id)
    if k3d:
        timeline.add_k3d(line)
    timeline.show(line)
    print(
        f"  load of the machine {load:.0f} at the start, {os.getloadavg()[0]:.0f} at the end"
    )
    return record["state"] == "succeeded"


def main() -> None:
    """Run the smoke workflows; exit 1 when a run did not succeed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sbmlsim", type=Path, default=Path("~/git/sbmlsim"))
    parser.add_argument("--pkdb-models", type=Path, default=Path("~/git/pkdb_models"))
    parser.add_argument("--local", action="store_true", help="on this machine")
    parser.add_argument("--k3d", action="store_true", help="on the local test system")
    parser.add_argument("--only", choices=["sbmlsim", "pkdb_models"])
    args = parser.parse_args()
    sbmlsim = args.sbmlsim.expanduser().resolve()
    pkdb_models = args.pkdb_models.expanduser().resolve()
    simrun = environment(sbmlsim, pkdb_models)
    LOCAL.mkdir(parents=True, exist_ok=True)
    selected = {
        name: sources
        for name, sources in workflows(sbmlsim, pkdb_models).items()
        if args.only in (None, name)
    }
    # the variables of a service of the caller are not the ones of the smoke runs
    env = {k: v for k, v in os.environ.items() if not k.startswith("SIMRUN_")}
    results: dict[str, bool] = {}
    if args.local or not args.k3d:
        for name, sources in selected.items():
            results[f"{name} locally"] = run(simrun, name, sources, env)
        subprocess.run(
            [str(simrun), "serve", "--local", "--stop"], cwd=LOCAL, env=env, check=False
        )
    if args.k3d:
        with k3d_service() as service:
            for name, sources in selected.items():
                results[f"{name} on k3d"] = run(simrun, name, sources, env | service)
    failed = [name for name, ok in results.items() if not ok]
    print("smoke: " + (f"failed: {', '.join(failed)}" if failed else "succeeded"))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
