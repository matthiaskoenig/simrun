"""The k3d cluster of the integration tests and the helpers of the tests, see `conftest`."""

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from kubernetes import client

from simrun.config import DEFAULT_BASE_IMAGE, ClusterConfig
from simrun.service.app import ServiceSettings

REPO = Path(__file__).resolve().parents[2]
TOOLS = REPO / ".tools"
BIN = TOOLS / "bin"
K3D = BIN / "k3d"
KUBECTL = BIN / "kubectl"
KUBECONFIG = TOOLS / "kubeconfig"
STORE = TOOLS / "k3d" / "store"
# the simrun wheel of the image of the runs, which the service builds images with
WHEELS = TOOLS / "k3d" / "wheels"
K3D_VERSION = "v5.9.0"
KUBECTL_VERSION = "v1.37.2"
CLUSTER = "simrun-test"
REGISTRY_NAME = "k3d-simrun-registry"
REGISTRY = f"{REGISTRY_NAME}:5000"
REGISTRY_HOST = "localhost:5000"
NAMESPACE = "simrun"
AGENTS = [f"k3d-{CLUSTER}-agent-{i}" for i in range(3)]
# images of k3s, Kueue, Argo, BuildKit and the runs
MIN_FREE_GB = 20
SETUP_TIMEOUT_S = 600.0
TERMINAL = ("succeeded", "failed", "cancelled")
# the base image of the images of the environments (`ClusterConfig.base_image`, a
# repository of the registry), mirrored from its source with its digest
UV_SOURCE = f"ghcr.io/{DEFAULT_BASE_IMAGE}"
UV_MIRROR = DEFAULT_BASE_IMAGE.partition("@")[0]
SERVER = f"k3d-{CLUSTER}-server-0"


def enabled() -> bool:
    return os.environ.get("SIMRUN_K3D") == "1"


def _run(*args: str | Path, check: bool = True, timeout: float = 600) -> str:
    env = {**os.environ, "KUBECONFIG": str(KUBECONFIG)}
    result = subprocess.run(
        [str(a) for a in args],
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"{' '.join(map(str, args))} failed:\n{result.stdout}\n{result.stderr}"
        )
    return result.stdout


def _download(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".part")
    with urllib.request.urlopen(url, timeout=300) as response:
        tmp.write_bytes(response.read())
    tmp.chmod(0o755)
    tmp.replace(path)


def download_tools() -> None:
    """Download k3d and kubectl into `.tools/bin` when missing."""
    if not K3D.exists():
        _download(
            "https://github.com/k3d-io/k3d/releases/download/"
            f"{K3D_VERSION}/k3d-linux-amd64",
            K3D,
        )
    if not KUBECTL.exists():
        _download(
            f"https://dl.k8s.io/release/{KUBECTL_VERSION}/bin/linux/amd64/kubectl",
            KUBECTL,
        )


def check_disk() -> None:
    root = _run("docker", "info", "--format", "{{.DockerRootDir}}").strip()
    free_gb = shutil.disk_usage(root if Path(root).exists() else "/").free / 2**30
    if free_gb < MIN_FREE_GB:
        pytest.fail(f"only {free_gb:.0f} GiB free for docker, {MIN_FREE_GB} needed")


def _exists(kind: str, name: str) -> bool:
    listed = json.loads(_run(K3D, kind, "list", "-o", "json") or "[]")
    return any(item.get("name") == name for item in listed)


def create_cluster() -> None:
    if not _exists("registry", REGISTRY_NAME):
        _run(K3D, "registry", "create", "simrun-registry", "--port", REGISTRY_HOST)
    if not _exists("cluster", CLUSTER):
        STORE.mkdir(parents=True, exist_ok=True)
        _run(
            K3D,
            "cluster",
            "create",
            CLUSTER,
            "--servers",
            "1",
            "--agents",
            "3",
            "--agents-memory",
            "6g",
            "--registry-use",
            REGISTRY,
            "--volume",
            f"{STORE}:/var/simrun/store@all",
            "--k3s-arg",
            "--disable=traefik@server:0",
            "--k3s-arg",
            "--node-label=simrun/worker=true@agent:*",
            "--kubeconfig-update-default=false",
            "--kubeconfig-switch-context=false",
            "--wait",
            "--timeout",
            "300s",
        )
    KUBECONFIG.write_text(_run(K3D, "kubeconfig", "get", CLUSTER))
    KUBECONFIG.chmod(0o600)
    (STORE / "snapshots").mkdir(parents=True, exist_ok=True)


def apply_manifests() -> None:
    """Apply `deploy/k3d` until Kueue's CRDs and webhook accept its resources."""
    deadline = time.monotonic() + SETUP_TIMEOUT_S
    while True:
        try:
            _run(
                KUBECTL,
                "apply",
                "--server-side",
                "--force-conflicts",
                "-k",
                REPO / "deploy" / "k3d",
            )
            break
        except RuntimeError:
            if time.monotonic() > deadline:
                raise
            time.sleep(10)
    for namespace, deployment in (
        ("kueue-system", "kueue-controller-manager"),
        (NAMESPACE, "workflow-controller"),
        (NAMESPACE, "buildkitd"),
    ):
        _run(
            KUBECTL,
            "-n",
            namespace,
            "rollout",
            "status",
            f"deploy/{deployment}",
            "--timeout=300s",
        )


def build_image() -> tuple[str, Path]:
    """Build the image of the runs from the working tree and push it.

    Returns:
        The image as the nodes name it, and the simrun wheel it installs, kept in
        `WHEELS` for the builds of the images of environments.
    """
    with tempfile.TemporaryDirectory(prefix="simrun-image-") as tmp:
        context = Path(tmp)
        _run("uv", "build", "--wheel", "--out-dir", context, REPO)
        (wheel,) = context.glob("simrun-*.whl")
        shutil.rmtree(WHEELS, ignore_errors=True)
        WHEELS.mkdir(parents=True)
        kept = WHEELS / wheel.name
        shutil.copy2(wheel, kept)
        dockerfile = Path(__file__).with_name("Dockerfile")
        shutil.copy2(dockerfile, context / "Dockerfile")
        digest = hashlib.sha256(
            wheel.read_bytes() + dockerfile.read_bytes() + str(os.getuid()).encode()
        ).hexdigest()[:12]
        tag = f"simrun-test:{digest}"
        _run(
            "docker",
            "build",
            "--quiet",
            "--build-arg",
            f"WHEEL={wheel.name}",
            "--build-arg",
            f"UID={os.getuid()}",
            "--build-arg",
            f"GID={os.getgid()}",
            "-t",
            f"{REGISTRY_HOST}/{tag}",
            context,
        )
    _run("docker", "push", "--quiet", f"{REGISTRY_HOST}/{tag}")
    return f"{REGISTRY}/{tag}", kept


def build_service_image(wheel: Path, plugins: list[Path]) -> str:
    """Build the image of the service from `deploy/service/Dockerfile` with docker.

    The context as the README describes it: the requirements of the lock of simrun
    (with hashes, and without as the constraints of the plugins), its wheel and the
    wheels of the plugins; the base is the mirror of the base image
    of the environments.

    Returns:
        The image, as the host names it.
    """
    with tempfile.TemporaryDirectory(prefix="simrun-service-") as tmp:
        context = Path(tmp)
        export = [
            "uv",
            "export",
            "--frozen",
            "--no-dev",
            "--no-emit-project",
            "--no-header",
            "--no-annotate",
            "--project",
            REPO,
        ]
        (context / "requirements.txt").write_text(_run(*export))
        (context / "constraints.txt").write_text(_run(*export, "--no-hashes"))
        shutil.copy2(wheel, context / wheel.name)
        (context / "plugins").mkdir()
        for plugin in plugins:
            shutil.copy2(plugin, context / "plugins" / plugin.name)
        dockerfile = REPO / "deploy" / "service" / "Dockerfile"
        shutil.copy2(dockerfile, context / "Dockerfile")
        digest = hashlib.sha256(
            b"".join(p.read_bytes() for p in sorted(context.rglob("*")) if p.is_file())
        ).hexdigest()[:12]
        tag = f"{REGISTRY_HOST}/simrun-service:{digest}"
        _run(
            "docker",
            "build",
            "--quiet",
            "--build-arg",
            f"BASE={REGISTRY_HOST}/{DEFAULT_BASE_IMAGE}",
            "--build-arg",
            f"WHEEL={wheel.name}",
            "-t",
            tag,
            context,
        )
    return tag


def mirror_uv_image() -> None:
    """Copy the base image of the environments into the registry of the cluster.

    The index is copied as it is, so the generated Dockerfiles find it by its
    digest.
    """
    target = f"{REGISTRY_HOST}/{UV_MIRROR}"
    _run("docker", "buildx", "imagetools", "create", "--tag", target, UV_SOURCE)
    digest = DEFAULT_BASE_IMAGE.partition("@")[2]
    found = _run(
        "docker",
        "buildx",
        "imagetools",
        "inspect",
        target,
        "--format",
        "{{.Manifest.Digest}}",
    ).strip()
    if found != digest:
        raise RuntimeError(f"the mirror of {UV_SOURCE} has the digest {found}")


def delete_cluster() -> None:
    """Delete the cluster, the registry, the store and the images of the runs.

    The pods of an image of an environment run as root and leave files of root in
    the store, which the server node gives back to the user first.
    """
    _run(
        "docker",
        "exec",
        SERVER,
        "chown",
        "-R",
        f"{os.getuid()}:{os.getgid()}",
        "/var/simrun/store",
        check=False,
    )
    _run(K3D, "cluster", "delete", CLUSTER, check=False)
    images = [
        image
        for repository in ("simrun-test", "simrun-service")
        for image in _run(
            "docker", "images", "--quiet", f"{REGISTRY_HOST}/{repository}", check=False
        ).split()
    ]
    if images:
        _run("docker", "image", "rm", "--force", *sorted(set(images)), check=False)
    _run(K3D, "registry", "delete", REGISTRY_NAME, check=False)
    shutil.rmtree(STORE, ignore_errors=True)
    shutil.rmtree(WHEELS, ignore_errors=True)
    KUBECONFIG.unlink(missing_ok=True)


@dataclass
class Cluster:
    """The cluster of the session.

    Attributes:
        api: A client of its API.
        image: The image of the runs, as the nodes name it.
        wheel: The simrun wheel of the image, which the service builds images with.
        config: The cluster configuration of the service.
    """

    api: client.ApiClient
    image: str
    wheel: Path
    config: ClusterConfig

    @property
    def core(self) -> client.CoreV1Api:
        return client.CoreV1Api(self.api)

    def pods(self, run_id: str) -> list[dict[str, Any]]:
        """The pods of the workflow of a run, serialised."""
        pods = self.core.list_namespaced_pod(
            NAMESPACE, label_selector=f"workflows.argoproj.io/workflow=simrun-{run_id}"
        )
        return [self.api.sanitize_for_serialization(p) for p in pods.items]

    def running_pods(self, run_id: str) -> list[dict[str, Any]]:
        """The pods of a run whose main container runs."""
        running = []
        for pod in self.pods(run_id):
            statuses = (pod.get("status") or {}).get("containerStatuses") or []
            if any(
                s["name"] == "main" and "running" in (s.get("state") or {})
                for s in statuses
            ):
                running.append(pod)
        return running

    def preempted(self, run_id: str) -> bool:
        """Whether Kueue preempted a workload of a pod of a run."""
        events = self.core.list_namespaced_event(
            NAMESPACE, field_selector="reason=PreemptedWorkload"
        )
        victim = f"workload {NAMESPACE}/pod-simrun-{run_id}-"
        return any(victim in (e.message or "") for e in events.items)

    def node(self, name: str) -> dict[str, Any]:
        return self.api.sanitize_for_serialization(self.core.read_node(name))

    def clean(self) -> None:
        """Delete every workflow and its pods, and remove every reservation."""
        custom = client.CustomObjectsApi(self.api)
        custom.delete_collection_namespaced_custom_object(
            "argoproj.io", "v1alpha1", NAMESPACE, "workflows"
        )
        deadline = time.monotonic() + 180
        selector = "workflows.argoproj.io/workflow"
        while self.core.list_namespaced_pod(NAMESPACE, label_selector=selector).items:
            if time.monotonic() > deadline:
                raise TimeoutError("the pods of the workflows were not deleted")
            time.sleep(2)
        for name in AGENTS:
            node = self.node(name)
            taints = [
                t
                for t in (node["spec"].get("taints") or [])
                if t["key"] != "simrun/reserved"
            ]
            self.core.patch_node(
                name,
                {
                    "metadata": {"labels": {"simrun/reserved": None}},
                    "spec": {"taints": taints},
                },
            )


class Service:
    """The service of a test, in its process, and helpers of the tests."""

    def __init__(
        self, http: TestClient, cluster: Cluster, settings: ServiceSettings
    ) -> None:
        self.http = http
        self.cluster = cluster
        self.settings = settings

    @staticmethod
    def workflow(**settings: Any) -> dict[str, Any]:
        """The squares demo; `resources` is the override of the task."""
        resources = settings.pop("resources", None)
        task: dict[str, Any] = {
            "id": "squares",
            "type": "demo.squares",
            "settings": settings,
        }
        if resources is not None:
            task["resources"] = resources
        return {"name": "demo", "tasks": [task]}

    def submit(
        self, token: str, workflow: dict[str, Any], **request: Any
    ) -> dict[str, Any]:
        body = {"workflow": workflow, "sources": [], **request}
        response = self.http.post(
            "/runs", json=body, headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 201, response.text
        return response.json()

    def estimate(
        self, token: str, workflow: dict[str, Any], **request: Any
    ) -> dict[str, Any]:
        response = self.http.post(
            "/estimate",
            json={"workflow": workflow, "sources": [], **request},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200, response.text
        return response.json()

    def record(self, run_id: str) -> dict[str, Any]:
        response = self.http.get(
            f"/runs/{run_id}", headers={"Authorization": "Bearer t-mk"}
        )
        assert response.status_code == 200, response.text
        return response.json()

    def wait(self, run_id: str, timeout_s: float = 300.0) -> dict[str, Any]:
        """The record of a run once it ended."""

        def ended() -> dict[str, Any] | None:
            record = self.record(run_id)
            return record if record["state"] in TERMINAL else None

        return wait_for(ended, timeout_s, f"run {run_id} did not end")

    def task_dir(self, run_id: str) -> Path:
        return self.settings.store / "runs" / run_id / "tasks" / "squares"

    def total(self, run_id: str) -> int:
        summary = json.loads(
            (self.task_dir(run_id) / "out" / "summary.json").read_text()
        )
        return summary["sum"]


def wait_for[T](condition: Callable[[], T | None], timeout_s: float, what: str) -> T:
    """The first value of `condition` which is neither None nor false, polled every
    second."""
    deadline = time.monotonic() + timeout_s
    while True:
        value = condition()
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"{what} within {timeout_s} s")
        time.sleep(1.0)
