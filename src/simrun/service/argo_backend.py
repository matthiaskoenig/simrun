"""The backend of the service on a cluster: Argo Workflows, Kueue and BuildKit.

A run is an Argo `Workflow` (`simrun.argo.to_argo`) in the namespace of the cluster;
Kueue admits its pods (plain pods with a queue label) and preempts lower priorities.
The status of a workflow is read from the nodes of its DAG (`workflow_status`): a node
retried by Argo (a `Retry` node) is `Pending` while its attempt waits for admission,
`Preempted` while the attempt after a failure which was not a kill for memory waits
(an eviction by Kueue, a lost pod, a pod killed after its grace period), `Running`
while its attempt runs; the failed attempts are listed per worker with their exit
codes and whether the kernel killed them for memory (`OOMKilled`).

The nodes are the ones labelled `simrun/worker=true` (the nodes of the ResourceFlavor
of Kueue) without a taint which keeps pods off, a reservation of simrun aside; their
cores and memory are the allocatable ones minus the wait container of the Argo
executor, rounded down to whole cores and GiB, so a pod the planner sizes for a node
fits it with the executor beside it.

A reservation is the taint `simrun/reserved=<run id>:NoExecute`, which evicts the
pods of other runs from the node, and the label `simrun/reserved=<run id>`, which the
pods of the run select.

An image is built by a Job of `buildctl`, which sends the build context (a ConfigMap
of its files, owned by the Job) to the BuildKit daemon of the cluster and pushes the
image to the registry with the credentials of the optional Secret
`simrun-registry-auth`; whether the registry has an image is asked its API.
"""

import base64
import hashlib
import logging
import math
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
from kubernetes import client
from kubernetes import config as kube_config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException
from kubernetes.utils import parse_quantity

from simrun.argo import CONTAINER, GPU_RESOURCE, RESERVED_KEY
from simrun.config import ClusterConfig
from simrun.planner import Node
from simrun.service.app import CONFIG_ENV, load_settings
from simrun.service.backend import (
    FINAL_PHASES,
    PREEMPTED_PHASE,
    BackendStatus,
    FailedAttempt,
)

logger = logging.getLogger(__name__)

GROUP = "argoproj.io"
VERSION = "v1alpha1"
PLURAL = "workflows"
WORKFLOW_LABEL = "workflows.argoproj.io/workflow"
NODE_ANNOTATION = "workflows.argoproj.io/node-name"
# the label of the nodes the pods of the runs run on, the nodes of the ResourceFlavor
WORKER_LABEL = "simrun/worker"
# the affinity of the pods of the infrastructure: off the nodes of the runs
OFF_WORKERS = {
    "nodeAffinity": {
        "requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [
                {
                    "matchExpressions": [
                        {"key": WORKER_LABEL, "operator": "DoesNotExist"}
                    ]
                }
            ]
        }
    }
}
# the resources of a container of the Argo executor, see
# deploy/cluster/argo/controller-config.yaml
EXECUTOR_CPU = 0.1
EXECUTOR_MEMORY_GB = 0.25
GIB = 2**30
# the name of an attempt of a retried node ends in `(<attempt>)`
ATTEMPT = re.compile(r"\((\d+)\)$")
EXIT_CODE = re.compile(r"\(exit code (-?\d+)\)")
# node types which are no task of the DAG
GROUP_TYPES = frozenset({"DAG", "TaskGroup", "Steps", "StepGroup"})
FAILED_PHASES = frozenset({"Failed", "Error"})
OOM_REASON = "OOMKilled"
# the message of an attempt whose container of the executor failed starts so
EXECUTOR_CONTAINERS = ("init: ", "wait: ")
REGISTRY_AUTH_SECRET = "simrun-registry-auth"
# a ConfigMap holds at most 1 MiB
MAX_CONTEXT_BYTES = 1_000_000
BUILD_TIMEOUT_S = 3600.0
BUILD_POLL_S = 2.0
BUILD_TTL_S = 600
MANIFEST_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)


def _killed_for_memory(node: dict[str, Any]) -> bool:
    """Whether the main container of an attempt was killed by the out of memory killer.

    Argo v4 names the container in the message of the node, e.g.
    `main: OOMKilled (exit code 137)`; a kill of a container of the executor
    (`init: ...`, `wait: ...`) is no kill of the worker. An exit 137 alone is no
    evidence (Ruling 39): a pod which did not stop within its grace period is killed
    with SIGKILL, and a worker which exits 137 itself (Ruling 7) reports in its
    report whether the out of memory killer killed a process of its pool.
    """
    message = node.get("message") or ""
    if message.startswith(EXECUTOR_CONTAINERS):
        return False
    return message.removeprefix(f"{CONTAINER}: ").startswith(OOM_REASON)


def _exit_code(node: dict[str, Any]) -> int | None:
    """The exit code of the main container of an attempt, None without one.

    The output of Argo, else the one its message names, e.g. `Error (exit code
    137)`; an attempt whose container of the executor failed has none of the main
    container, and a pod deleted none at all (`-1`).
    """
    message = node.get("message") or ""
    if message.startswith(EXECUTOR_CONTAINERS):
        return None
    text = str((node.get("outputs") or {}).get("exitCode") or "")
    if not text:
        found = EXIT_CODE.search(message)
        text = found.group(1) if found else ""
    try:
        code = int(text)
    except ValueError:
        return None
    return code if code >= 0 else None


def _parameter(node: dict[str, Any], name: str) -> str | None:
    """The value of an input parameter of a node, None without it."""
    for parameter in (node.get("inputs") or {}).get("parameters") or []:
        if parameter.get("name") == name:
            return parameter.get("value")
    return None


def _phase(node: dict[str, Any], nodes: dict[str, dict[str, Any]]) -> str:
    """The phase of a task node, from its last attempt when Argo retries it."""
    phase = node.get("phase") or "Pending"
    if node.get("type") != "Retry" or phase in FINAL_PHASES:
        return phase
    attempts = [nodes[c] for c in node.get("children") or [] if c in nodes]
    if not attempts:
        return "Pending"
    last = attempts[-1]
    last_phase = last.get("phase") or "Pending"
    if last_phase in ("Running", "Succeeded"):
        return "Running"
    failed = last if last_phase in FAILED_PHASES else None
    if failed is None and len(attempts) > 1:
        before = attempts[-2]
        failed = before if before.get("phase") in FAILED_PHASES else None
    if failed is not None and not _killed_for_memory(failed):
        return PREEMPTED_PHASE
    return "Pending"


def workflow_status(workflow: dict[str, Any]) -> BackendStatus:
    """The status of a workflow from its Argo object.

    Args:
        workflow: The workflow as the API returns it.

    Returns:
        The phase of the workflow, of every task node of its DAG by its name relative
        to the workflow, and the failed attempts of the pods per worker id, per task
        id of an unsplit task and per `<task>-merge` of a merge.
    """
    name = workflow["metadata"]["name"]
    status = workflow.get("status") or {}
    nodes: dict[str, dict[str, Any]] = status.get("nodes") or {}
    prefix = f"{name}."
    tasks: dict[str, str] = {}
    failed: dict[str, list[FailedAttempt]] = {}
    for node in nodes.values():
        full = node.get("name") or ""
        if not full.startswith(prefix):
            continue
        relative = full.removeprefix(prefix)
        kind = node.get("type")
        attempt = ATTEMPT.search(relative)
        if kind == "Pod" and node.get("phase") in FAILED_PHASES:
            # a worker by its id, an unsplit task or a merge by its DAG task
            key = _parameter(node, "worker") or ATTEMPT.sub("", relative)
            failed.setdefault(key, []).append(
                FailedAttempt(
                    attempt=int(attempt.group(1)) if attempt else 0,
                    exit_code=_exit_code(node),
                    oom_killed=_killed_for_memory(node),
                )
            )
        if kind in GROUP_TYPES or attempt:
            continue
        tasks[relative] = _phase(node, nodes)
    for attempts in failed.values():
        attempts.sort(key=lambda a: a.attempt)
    return BackendStatus(
        phase=status.get("phase") or "Pending", tasks=tasks, failed=failed
    )


def node_from(node: dict[str, Any]) -> Node | None:
    """The node of the planner of a Kubernetes node, None when pods cannot run on it.

    Args:
        node: The node as the API serialises it.

    Returns:
        The node with its allocatable cores and memory minus one executor container,
        rounded down, and its reservation; None for a node which is unschedulable or
        has a taint other than a reservation which keeps pods off.
    """
    metadata = node.get("metadata") or {}
    spec = node.get("spec") or {}
    if spec.get("unschedulable"):
        return None
    reserved = (metadata.get("labels") or {}).get(RESERVED_KEY)
    for taint in spec.get("taints") or []:
        if taint.get("key") == RESERVED_KEY:
            reserved = reserved or taint.get("value")
        elif taint.get("effect") in ("NoSchedule", "NoExecute"):
            return None
    allocatable = (node.get("status") or {}).get("allocatable") or {}
    cpu = float(parse_quantity(allocatable.get("cpu", "0")))
    memory = float(parse_quantity(allocatable.get("memory", "0"))) / GIB
    gpus = int(parse_quantity(allocatable.get(GPU_RESOURCE, "0")))
    return Node(
        name=metadata["name"],
        cores=max(0, math.floor(cpu - EXECUTOR_CPU)),
        memory_gb=float(max(0, math.floor(memory - EXECUTOR_MEMORY_GB))),
        gpus=gpus,
        reserved_by=reserved,
    )


def manifest_url(ref: str, config: ClusterConfig) -> str:
    """The URL of the manifest of an image of the registry of the cluster.

    Args:
        ref: The image, `<registry>/<repository>[:<tag>|@<digest>]`.
        config: The cluster.

    Returns:
        The URL of the manifest in the API of the registry.

    Raises:
        ValueError: The image is not in the registry of the cluster.
    """
    host, _, rest = ref.partition("/")
    if host != config.registry or not rest:
        raise ValueError(f"the image {ref!r} is not in the registry {config.registry}")
    if "@" in rest:
        repository, _, reference = rest.partition("@")
    else:
        repository, colon, reference = rest.rpartition(":")
        if not colon:
            repository, reference = rest, "latest"
    base = (config.registry_url or f"http://{config.registry}").rstrip("/")
    return f"{base}/v2/{repository}/manifests/{reference}"


def _task_node(relative: str, task_id: str) -> bool:
    """Whether a node name relative to its workflow is a pod of a task."""
    pattern = rf"{re.escape(task_id)}(-w0|-extra\(\d+:\d+\)|-merge)?(\(\d+\))?"
    return re.fullmatch(pattern, relative) is not None


def _api_client(api: client.ApiClient | None) -> client.ApiClient:
    """The client of the API.

    The given one, else in a pod its service account, else the kubeconfig of
    `KUBECONFIG` or `~/.kube/config`.
    """
    if api is not None:
        return api
    configuration = client.Configuration()
    try:
        kube_config.load_incluster_config(client_configuration=configuration)
    except ConfigException:
        # read at the call: the client reads `KUBECONFIG` once, at its import
        kube_config.load_kube_config(
            config_file=os.environ.get("KUBECONFIG"),
            client_configuration=configuration,
        )
    return client.ApiClient(configuration)


class ArgoBackend:
    """The backend of the service on a cluster of Argo Workflows and Kueue."""

    def __init__(
        self, config: ClusterConfig, api: client.ApiClient | None = None
    ) -> None:
        """Create the backend.

        Args:
            config: The cluster.
            api: The client of the Kubernetes API; by default the service account of
                the pod, else the kubeconfig.
        """
        self.config = config
        self.api = _api_client(api)
        self.core = client.CoreV1Api(self.api)
        self.batch = client.BatchV1Api(self.api)
        self.custom = client.CustomObjectsApi(self.api)

    # workflows

    def submit(self, manifest: dict[str, Any]) -> str:
        """Create the workflow; returns its name."""
        created = self.custom.create_namespaced_custom_object(
            GROUP, VERSION, self.config.namespace, PLURAL, manifest
        )
        return created["metadata"]["name"]  # ty: ignore[not-subscriptable]

    def _workflow(self, name: str) -> dict[str, Any]:
        try:
            return self.custom.get_namespaced_custom_object(  # ty: ignore[invalid-return-type]
                GROUP, VERSION, self.config.namespace, PLURAL, name
            )
        except ApiException as error:
            if error.status == 404:
                raise KeyError(name) from None
            raise

    def status(self, name: str) -> BackendStatus:
        """The status of a workflow; KeyError when it does not exist."""
        return workflow_status(self._workflow(name))

    def _shutdown(self, name: str, strategy: str) -> None:
        """Shut a workflow down and delete its pods still pending.

        Argo stops the running pods; a pod waiting for admission by Kueue never runs,
        so it is deleted here.
        """
        try:
            self.custom.patch_namespaced_custom_object(
                GROUP,
                VERSION,
                self.config.namespace,
                PLURAL,
                name,
                {"spec": {"shutdown": strategy}},
            )
        except ApiException as error:
            if error.status == 404:
                raise KeyError(name) from None
            raise
        for pod in self._pods(f"{WORKFLOW_LABEL}={name}", "status.phase=Pending"):
            try:
                self.core.delete_namespaced_pod(
                    pod["metadata"]["name"], self.config.namespace
                )
            except ApiException as error:
                if error.status != 404:
                    raise

    def _pods(self, labels: str, fields: str | None = None) -> list[dict[str, Any]]:
        """The pods of the namespace with the labels and fields, serialised."""
        pods = self.core.list_namespaced_pod(
            self.config.namespace, label_selector=labels, field_selector=fields
        )
        return [self.api.sanitize_for_serialization(p) for p in pods.items]

    def cancel(self, name: str) -> None:
        """Terminate a workflow and its pods; KeyError when it does not exist."""
        self._shutdown(name, "Terminate")

    def stop(self, name: str) -> None:
        """Stop a workflow whose run is decided; KeyError when it does not exist."""
        self._shutdown(name, "Stop")

    def logs(self, name: str, task_id: str) -> str:
        """The logs of the pods of a task, oldest first, each under a header."""
        prefix = f"{name}."
        parts: list[str] = []
        for pod in sorted(
            self._pods(f"{WORKFLOW_LABEL}={name}"),
            key=lambda p: p["metadata"].get("creationTimestamp") or "",
        ):
            node = (pod["metadata"].get("annotations") or {}).get(NODE_ANNOTATION, "")
            if not _task_node(node.removeprefix(prefix), task_id):
                continue
            pod_name = pod["metadata"]["name"]
            try:
                text = self.core.read_namespaced_pod_log(
                    pod_name, self.config.namespace, container=CONTAINER
                )
            except ApiException as error:
                text = f"(no logs: {error.reason})\n"
            parts.append(f"==> {node} ({pod_name}) <==\n{text}")
        return "\n".join(parts)

    # nodes

    def nodes(self) -> list[Node]:
        """The worker nodes on which pods can run, with their reservations."""
        listed = self.core.list_node(label_selector=f"{WORKER_LABEL}=true")
        nodes = (
            node_from(self.api.sanitize_for_serialization(n)) for n in listed.items
        )
        return [n for n in nodes if n is not None]

    def _node(self, node: str) -> dict[str, Any]:
        try:
            return self.api.sanitize_for_serialization(self.core.read_node(node))
        except ApiException as error:
            if error.status == 404:
                raise KeyError(node) from None
            raise

    def _patch_node(self, node: str, body: dict[str, Any]) -> None:
        self.core.patch_node(node, body)

    def reserve(self, node: str, run_id: str) -> None:
        """Taint and label a node for a run; KeyError for an unknown node.

        The taint replaces a reservation the node had; the resource version of the
        node guards the list of taints against a concurrent change.
        """
        current = self._node(node)
        taints = [
            t
            for t in (current.get("spec") or {}).get("taints") or []
            if t.get("key") != RESERVED_KEY
        ]
        taints.append({"key": RESERVED_KEY, "value": run_id, "effect": "NoExecute"})
        self._patch_node(
            node,
            {
                "metadata": {
                    "labels": {RESERVED_KEY: run_id},
                    "resourceVersion": current["metadata"]["resourceVersion"],
                },
                "spec": {"taints": taints},
            },
        )
        logger.info("node %s reserved for run %s", node, run_id)

    def unreserve(self, node: str, run_id: str) -> None:
        """Remove the taint and the label of the reservation of a run.

        Raises:
            KeyError: The node does not exist.
        """
        current = self._node(node)
        labels = (current["metadata"].get("labels")) or {}
        all_taints = (current.get("spec") or {}).get("taints") or []
        taints = [
            t
            for t in all_taints
            if not (t.get("key") == RESERVED_KEY and t.get("value") == run_id)
        ]
        body: dict[str, Any] = {
            "metadata": {"resourceVersion": current["metadata"]["resourceVersion"]}
        }
        if labels.get(RESERVED_KEY) == run_id:
            body["metadata"]["labels"] = {RESERVED_KEY: None}
        if len(taints) != len(all_taints):
            body["spec"] = {"taints": taints}
        if len(body) == 1 and "labels" not in body["metadata"]:
            return
        self._patch_node(node, body)
        logger.info("node %s released by run %s", node, run_id)

    # images

    def build_image(self, context: Path, ref: str) -> None:
        """Build the image of a flat build context with BuildKit and push it.

        Raises:
            ValueError: The context has a directory or is too large for a ConfigMap.
            RuntimeError: The build failed.
            TimeoutError: The build took longer than `BUILD_TIMEOUT_S`.
        """
        files: dict[str, str] = {}
        for path in sorted(context.iterdir()):
            if not path.is_file():
                raise ValueError(f"the build context has a directory: {path.name}")
            files[path.name] = base64.b64encode(path.read_bytes()).decode()
        if sum(len(v) for v in files.values()) > MAX_CONTEXT_BYTES:
            raise ValueError(f"the build context {context} is larger than 1 MB")
        digest = hashlib.sha256(ref.encode()).hexdigest()[:10]
        name = f"simrun-build-{digest}-{uuid.uuid4().hex[:6]}"
        namespace = self.config.namespace
        job = self.batch.create_namespaced_job(
            namespace, client.V1Job.model_validate(self._build_job(name, ref))
        )
        # deleted in every case: its pod would wait for a context never stored
        try:
            owner = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "name": name,
                "uid": self.api.sanitize_for_serialization(job)["metadata"]["uid"],
            }
            context_map = {
                "metadata": {"name": name, "ownerReferences": [owner]},
                "binaryData": files,
            }
            self.core.create_namespaced_config_map(
                namespace, client.V1ConfigMap.model_validate(context_map)
            )
            logger.info("building %s in the job %s", ref, name)
            self._wait_for_build(name, ref)
        finally:
            try:
                self.batch.delete_namespaced_job(
                    name, namespace, propagation_policy="Background"
                )
            except ApiException as error:
                if error.status != 404:
                    logger.warning("deleting the build job %s failed: %s", name, error)

    def _build_job(self, name: str, ref: str) -> dict[str, Any]:
        resources = {"cpu": "200m", "memory": "256Mi"}
        return {
            "metadata": {"name": name, "labels": {"app": "simrun-build"}},
            "spec": {
                "backoffLimit": 0,
                "ttlSecondsAfterFinished": BUILD_TTL_S,
                "template": {
                    "metadata": {"labels": {"app": "simrun-build"}},
                    "spec": {
                        "restartPolicy": "Never",
                        "affinity": OFF_WORKERS,
                        "containers": [
                            {
                                "name": "buildctl",
                                "image": self.config.buildctl_image,
                                "command": [
                                    "buildctl",
                                    "--addr",
                                    self.config.buildkit,
                                    "build",
                                    "--frontend",
                                    "dockerfile.v0",
                                    "--local",
                                    "context=/context",
                                    "--local",
                                    "dockerfile=/context",
                                    "--output",
                                    f"type=image,name={ref},push=true",
                                ],
                                "env": [{"name": "DOCKER_CONFIG", "value": "/docker"}],
                                "resources": {
                                    "requests": resources,
                                    "limits": resources,
                                },
                                "volumeMounts": [
                                    {"name": "context", "mountPath": "/context"},
                                    {"name": "docker", "mountPath": "/docker"},
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "context", "configMap": {"name": name}},
                            {
                                "name": "docker",
                                "secret": {
                                    "secretName": REGISTRY_AUTH_SECRET,
                                    "optional": True,
                                },
                            },
                        ],
                    },
                },
            },
        }

    def _wait_for_build(self, name: str, ref: str) -> None:
        deadline = time.monotonic() + BUILD_TIMEOUT_S
        while time.monotonic() < deadline:
            # the Job with its status: `jobs/status` would need a right of its own
            job = self.api.sanitize_for_serialization(
                self.batch.read_namespaced_job(name, self.config.namespace)
            )
            status = job.get("status") or {}
            if status.get("succeeded"):
                logger.info("built %s", ref)
                return
            if status.get("failed"):
                raise RuntimeError(
                    f"the build of {ref} failed:\n{self._job_logs(name)}"
                )
            time.sleep(BUILD_POLL_S)
        raise TimeoutError(f"the build of {ref} took longer than {BUILD_TIMEOUT_S} s")

    def _job_logs(self, name: str) -> str:
        texts = []
        for pod in self._pods(f"job-name={name}"):
            pod_name = pod["metadata"]["name"]
            try:
                texts.append(
                    self.core.read_namespaced_pod_log(
                        pod_name, self.config.namespace, tail_lines=50
                    )
                )
            except ApiException as error:
                texts.append(f"(no logs of {pod_name}: {error.reason})")
        return "\n".join(texts)

    def has_image(self, ref: str) -> bool:
        """Whether the registry has the image; asks the API of the registry.

        Raises:
            ValueError: The image is not in the registry of the cluster.
            RuntimeError: The registry answered neither found nor not found.
        """
        url = manifest_url(ref, self.config)
        response = httpx.head(
            url, headers={"Accept": ", ".join(MANIFEST_TYPES)}, timeout=30.0
        )
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        raise RuntimeError(f"the registry answered {response.status_code} for {url}")


def backend() -> ArgoBackend:
    """The backend of the settings named by `SIMRUN_CONFIG`, for `serve`."""
    return ArgoBackend(load_settings(Path(os.environ[CONFIG_ENV])).cluster)
