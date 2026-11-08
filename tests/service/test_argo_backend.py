import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from simrun.argo import RUN_LABEL
from simrun.config import ClusterConfig
from simrun.planner import Node
from simrun.service import argo_backend
from simrun.service.argo_backend import (
    LOG_LIMIT_BYTES,
    WORKER_LABEL,
    ArgoBackend,
    _attempt_of,
    _task_node,
    backend,
    manifest_url,
    node_from,
    pod_requests,
    workflow_status,
)
from simrun.service.backend import FailedAttempt, Placement, PodLog

WF = "simrun-r1"


def _run(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, Any, Any, Any]:
    """The arguments of `submit` for a run whose manifest is a stub."""
    monkeypatch.setattr(
        "simrun.service.argo_backend.to_argo",
        lambda *_: {"metadata": {"name": WF}},
    )
    return None, {}, {}, ClusterConfig(registry="localhost:5000")


CONFIG = ClusterConfig(registry="k3d-simrun-registry:5000")


def _node(
    name: str,
    kind: str,
    phase: str,
    children: list[str] | None = None,
    worker: str | None = None,
    message: str | None = None,
    exit_code: str | None = None,
) -> dict[str, Any]:
    node: dict[str, Any] = {
        "id": name,
        "name": f"{WF}.{name}" if name else WF,
        "type": kind,
        "phase": phase,
        "children": children or [],
    }
    if worker is not None:
        node["inputs"] = {"parameters": [{"name": "worker", "value": worker}]}
    if message is not None:
        node["message"] = message
    if exit_code is not None:
        node["outputs"] = {"exitCode": exit_code}
    return node


def _workflow(*nodes: dict[str, Any], phase: str = "Running") -> dict[str, Any]:
    return {
        "metadata": {"name": WF},
        "status": {"phase": phase, "nodes": {n["id"]: n for n in nodes}},
    }


# the message of Argo v4.1.6 for a container killed for memory
OOM = "main: OOMKilled (exit code 137)"


def test_status_of_the_dag_nodes() -> None:
    status = workflow_status(
        _workflow(
            _node("", "DAG", "Running", ["a-w0", "a-extra"]),
            _node("a-w0", "Retry", "Running", ["a-w0(0)"], "a-w0"),
            _node("a-w0(0)", "Pod", "Running", worker="a-w0"),
            _node("a-extra", "TaskGroup", "Running", ["a-extra(0:1)"]),
            _node("a-extra(0:1)", "Retry", "Running", ["a-extra(0:1)(0)"], "a-w1"),
            _node("a-extra(0:1)(0)", "Pod", "Pending", worker="a-w1"),
            _node("b", "Retry", "Succeeded", ["b(0)"]),
            _node("b(0)", "Pod", "Succeeded"),
        )
    )
    assert status.phase == "Running"
    assert status.tasks == {
        "a-w0": "Running",
        "a-extra(0:1)": "Pending",
        "b": "Succeeded",
    }
    assert status.failed == {}


def test_a_new_workflow_is_pending() -> None:
    status = workflow_status({"metadata": {"name": WF}})
    assert status.phase == "Pending"
    assert status.tasks == {}


def test_a_retry_after_an_eviction_is_preempted_until_it_runs() -> None:
    evicted = _node("a-w0(0)", "Pod", "Error", worker="a-w0", message="pod deleted")
    waiting = _workflow(
        _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
        evicted,
        _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
    )
    assert workflow_status(waiting).tasks == {"a-w0": "Preempted"}
    not_yet_retried = _workflow(
        _node("a-w0", "Retry", "Running", ["a-w0(0)"], "a-w0"), evicted
    )
    assert workflow_status(not_yet_retried).tasks == {"a-w0": "Preempted"}
    running = _workflow(
        _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
        evicted,
        _node("a-w0(1)", "Pod", "Running", worker="a-w0"),
    )
    assert workflow_status(running).tasks == {"a-w0": "Running"}


def test_a_retry_after_a_kill_for_memory_is_queued_and_counted() -> None:
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
            _node("a-w0(0)", "Pod", "Failed", worker="a-w0", message=OOM),
            _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
            _node("a-extra(0:2)", "Retry", "Running", ["x(0)", "x(1)"], "a-w2"),
            _node("x(0)", "Pod", "Failed", worker="a-w2", message=OOM),
            _node("x(1)", "Pod", "Failed", worker="a-w2", message=OOM),
        )
    )
    assert status.tasks == {"a-w0": "Pending", "a-extra(0:2)": "Pending"}
    assert status.failed == {
        "a-w0": [FailedAttempt(attempt=0, exit_code=137, oom_killed=True)],
        "a-w2": [
            FailedAttempt(attempt=0, exit_code=137, oom_killed=True),
            FailedAttempt(attempt=1, exit_code=137, oom_killed=True),
        ],
    }


@pytest.mark.parametrize(
    ("message", "exit_code", "phase"),
    [
        ("pod deleted", None, "Preempted"),
        ("Error (exit code 143)", "143", "Preempted"),
        ("Pod was evicted", None, "Preempted"),
        ("Error (exit code 1)", "1", "Retrying"),
        ("Error (exit code 137)", "137", "Retrying"),
        ("main: OOMKilled (exit code 137)", "137", "Pending"),
    ],
)
def test_the_phase_of_a_retry_names_why(
    message: str, exit_code: str | None, phase: str
) -> None:
    # Preempted only with the evidence of an eviction: the pod was deleted (no exit
    # code) or the worker stopped on SIGTERM (143)
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
            _node(
                "a-w0(0)",
                "Pod",
                "Error",
                worker="a-w0",
                message=message,
                exit_code=exit_code,
            ),
            _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
        )
    )
    assert status.tasks == {"a-w0": phase}


def test_a_kill_of_the_executor_is_no_kill_of_the_worker() -> None:
    init = "init: OOMKilled (exit code 137)"
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
            _node("a-w0(0)", "Pod", "Error", worker="a-w0", message=init),
            _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
        )
    )
    assert status.failed == {"a-w0": [FailedAttempt(attempt=0, oom_killed=False)]}
    assert status.tasks == {"a-w0": "Preempted"}


def test_final_phases_of_a_retry_node_win() -> None:
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Failed", ["a-w0(0)"], "a-w0"),
            _node("a-w0(0)", "Pod", "Failed", worker="a-w0"),
            phase="Failed",
        )
    )
    assert status.phase == "Failed"
    assert status.tasks == {"a-w0": "Failed"}


def _k8s_node(
    taints: list[dict[str, str]] | None = None,
    labels: dict[str, str] | None = None,
    unschedulable: bool = False,
    **allocatable: str,
) -> dict[str, Any]:
    return {
        "metadata": {"name": "n1", "labels": labels or {}},
        "spec": {"taints": taints, "unschedulable": unschedulable},
        "status": {"allocatable": {"cpu": "20", "memory": "6442450Ki", **allocatable}},
    }


# 6442450Ki, the allocatable memory of `_k8s_node`, in GiB: 6.14
ALLOCATABLE_GB = 6442450 / 1024**2


def test_node_is_allocatable_minus_the_executor_rounded_down() -> None:
    # 6.14 GiB, 0.25 GiB of the executor beside it: 5 GiB, 19 cores for the
    # planner; the scheduler counts the requests of the pods against all of it
    node = node_from(_k8s_node())
    assert node == Node("n1", 19, 5.0, 0, None, 20.0, ALLOCATABLE_GB)
    assert node is not None
    assert (node.allocatable_cores, round(node.allocatable_memory_gb, 2)) == (
        20.0,
        6.14,
    )
    node = _k8s_node(cpu="19500m")
    node["status"]["allocatable"]["nvidia.com/gpu"] = "2"
    assert node_from(node) == Node("n1", 19, 5.0, 2, None, 19.5, ALLOCATABLE_GB)


def _k8s_pod(
    node: str,
    namespace: str = "kube-system",
    containers: list[dict[str, str]] | None = None,
    init: list[dict[str, str]] | None = None,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    def container(requests: dict[str, str]) -> dict[str, Any]:
        return {"name": "c", "resources": {"requests": requests}}

    return {
        "metadata": {"name": "p", "namespace": namespace, "labels": labels or {}},
        "spec": {
            "nodeName": node,
            "containers": [container(r) for r in containers or []],
            "initContainers": [container(r) for r in init or []],
        },
    }


def test_requests_of_a_pod() -> None:
    assert pod_requests(_k8s_pod("n1")) == (0.0, 0.0)
    pod = _k8s_pod(
        "n1", containers=[{"cpu": "250m", "memory": "512Mi"}, {"memory": "512Mi"}]
    )
    assert pod_requests(pod) == (0.25, 1.0)
    # an init container runs before the containers: the larger of the two counts
    pod = _k8s_pod(
        "n1", containers=[{"cpu": "1"}], init=[{"cpu": "2", "memory": "1Gi"}]
    )
    assert pod_requests(pod) == (2.0, 1.0)
    pod["spec"]["overhead"] = {"cpu": "100m", "memory": "128Mi"}
    assert pod_requests(pod) == (2.1, 1.125)


def test_nodes_without_the_requests_of_other_pods() -> None:
    # a DaemonSet of 1 GiB on n1 and the pod of a run, which Kueue counts itself
    daemon = _k8s_pod("n1", containers=[{"cpu": "500m", "memory": "1Gi"}])
    run = _k8s_pod(
        "n1",
        namespace="simrun",
        containers=[{"cpu": "4", "memory": "4Gi"}],
        labels={"workflows.argoproj.io/workflow": "simrun-r1"},
    )
    elsewhere = _k8s_pod("n2", containers=[{"cpu": "8", "memory": "8Gi"}])
    infrastructure = _k8s_pod(
        "n1", namespace="simrun", containers=[{"memory": "1Gi"}], labels={"app": "x"}
    )
    pods = SimpleNamespace(items=[daemon, run, elsewhere, infrastructure])
    created, calls = _fake_backend(list_pod_for_all_namespaces=pods)
    # 20 cores and 6.14 GiB less the executor, 0.5 cores and 2 GiB of other pods;
    # without the executor and not rounded, 19.5 cores and 4.14 GiB
    assert created.nodes() == [Node("n1", 19, 3.0, 0, None, 19.5, ALLOCATABLE_GB - 2)]
    (listing,) = [c for c in calls if c[0] == "list_pod_for_all_namespaces"]
    assert listing[2]["field_selector"] == (
        "status.phase!=Succeeded,status.phase!=Failed"
    )


def test_node_reservation_and_taints() -> None:
    reserved = {"key": "simrun/reserved", "value": "r1", "effect": "NoExecute"}
    assert node_from(_k8s_node([reserved])) == Node(
        "n1", 19, 5.0, 0, "r1", 20.0, ALLOCATABLE_GB
    )
    labelled = _k8s_node(labels={"simrun/reserved": "r2"})
    assert node_from(labelled) == Node("n1", 19, 5.0, 0, "r2", 20.0, ALLOCATABLE_GB)
    control = {"key": "node-role.kubernetes.io/control-plane", "effect": "NoSchedule"}
    assert node_from(_k8s_node([control])) is None
    prefer = {"key": "x", "effect": "PreferNoSchedule"}
    assert node_from(_k8s_node([prefer])) is not None
    assert node_from(_k8s_node(unschedulable=True)) is None


def test_manifest_url() -> None:
    base = "http://k3d-simrun-registry:5000/v2"
    ref = "k3d-simrun-registry:5000/simrun/env:abc"
    assert manifest_url(ref, CONFIG) == f"{base}/simrun/env/manifests/abc"
    assert manifest_url("k3d-simrun-registry:5000/env", CONFIG) == (
        f"{base}/env/manifests/latest"
    )
    assert manifest_url("k3d-simrun-registry:5000/env@sha256:00", CONFIG) == (
        f"{base}/env/manifests/sha256:00"
    )
    local = CONFIG.model_copy(update={"registry_url": "http://localhost:5000/"})
    assert manifest_url(ref, local) == (
        "http://localhost:5000/v2/simrun/env/manifests/abc"
    )
    with pytest.raises(ValueError, match="not in the registry"):
        manifest_url("docker.io/library/python:3", CONFIG)


def test_pods_of_a_task_for_the_logs() -> None:
    for name in ("a-w0(0)", "a-extra(1:2)(3)", "a-merge(0)", "a(0)", "a"):
        assert _task_node(name, "a")
    for name in ("ab(0)", "b-w0(0)", "a-extra(0)", "a-w1(0)"):
        assert not _task_node(name, "a")


def test_backend_of_the_settings(tmp_path, monkeypatch) -> None:
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "c", "cluster": {"server": "https://k:6443"}}],
                "users": [{"name": "u", "user": {"token": "t"}}],
                "contexts": [{"name": "x", "context": {"cluster": "c", "user": "u"}}],
                "current-context": "x",
            }
        )
    )
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        yaml.safe_dump(
            {
                "store": str(tmp_path / "store"),
                "db": str(tmp_path / "db.sqlite"),
                "tokens": str(tmp_path / "tokens.yaml"),
                "owner": "mk",
                "cluster": {"registry": "zot:5000", "namespace": "runs"},
            }
        )
    )
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    monkeypatch.setenv("SIMRUN_CONFIG", str(settings))
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    created = backend()
    assert isinstance(created, ArgoBackend)
    assert created.config.namespace == "runs"
    assert created.api.configuration.host == "https://k:6443"


def test_an_exit_137_is_no_evidence_of_a_kill_for_memory() -> None:
    # a worker which exits 137 itself (Ruling 7, its report says whether the out of
    # memory killer killed a process) or a pod killed after its grace period: only
    # `OOMKilled` is the evidence of the kernel (Ruling 39)
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
            _node(
                "a-w0(0)",
                "Pod",
                "Failed",
                worker="a-w0",
                message="Error (exit code 137)",
                exit_code="137",
            ),
            _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
        )
    )
    # neither a kill for memory nor an eviction: the pod is retrying
    assert status.tasks == {"a-w0": "Retrying"}
    assert status.failed == {"a-w0": [FailedAttempt(attempt=0, exit_code=137)]}


def test_kills_of_unsplit_tasks_and_merges_are_counted() -> None:
    status = workflow_status(
        _workflow(
            _node("b", "Retry", "Running", ["b(0)", "b(1)"]),
            _node("b(0)", "Pod", "Failed", message=OOM, exit_code="137"),
            _node("b(1)", "Pod", "Running"),
            _node("a-merge", "Retry", "Running", ["a-merge(0)"]),
            _node("a-merge(0)", "Pod", "Failed", message=OOM),
        )
    )
    assert status.failed == {
        "b": [FailedAttempt(attempt=0, exit_code=137, oom_killed=True)],
        "a-merge": [FailedAttempt(attempt=0, exit_code=137, oom_killed=True)],
    }
    assert status.tasks == {"b": "Running", "a-merge": "Pending"}


# the calls of the backend: the resource and verb of the API each one needs
ACCESS = {
    "create_namespaced_custom_object": ("argoproj.io", "workflows", "create"),
    "get_namespaced_custom_object": ("argoproj.io", "workflows", "get"),
    "patch_namespaced_custom_object": ("argoproj.io", "workflows", "patch"),
    "list_namespaced_pod": ("", "pods", "list"),
    "delete_namespaced_pod": ("", "pods", "delete"),
    "read_namespaced_pod_log": ("", "pods/log", "get"),
    "list_node": ("", "nodes", "list"),
    "list_pod_for_all_namespaces": ("", "pods", "list"),
    "read_node": ("", "nodes", "get"),
    "patch_node": ("", "nodes", "patch"),
    "create_namespaced_job": ("batch", "jobs", "create"),
    "read_namespaced_job": ("batch", "jobs", "get"),
    "read_namespaced_job_status": ("batch", "jobs/status", "get"),
    "delete_namespaced_job": ("batch", "jobs", "delete"),
    "create_namespaced_config_map": ("", "configmaps", "create"),
    # the list of `Watch.stream` with `watch=True`
    "watch_namespaced_custom_object": ("argoproj.io", "workflows", "watch"),
}


class FakeApi:
    """An API of the client: records every call, answers from `answers` by name."""

    def __init__(self, calls: list[tuple[str, tuple, dict]], answers: dict) -> None:
        self.calls = calls
        self.answers = answers

    def __getattr__(self, name: str) -> Any:
        def call(*args: Any, **kw: Any) -> Any:
            self.calls.append((name, args, kw))
            answer = self.answers.get(name)
            return answer(*args, **kw) if callable(answer) else answer

        return call


def _pod(
    name: str, node: str, created: str, phase: str = "Succeeded"
) -> dict[str, Any]:
    return {
        "metadata": {
            "name": name,
            "creationTimestamp": created,
            "annotations": {"workflows.argoproj.io/node-name": f"{WF}.{node}"},
        },
        "status": {"phase": phase},
    }


def _k8s_reserved(run_id: str | None) -> dict[str, Any]:
    node = _k8s_node(
        [{"key": "simrun/reserved", "value": run_id, "effect": "NoExecute"}]
        if run_id
        else [{"key": "other", "value": "x", "effect": "NoSchedule"}],
        labels={"simrun/reserved": run_id} if run_id else {},
    )
    node["metadata"]["resourceVersion"] = "7"
    return node


def _fake_backend(**answers: Any) -> tuple[ArgoBackend, list[tuple[str, tuple, dict]]]:
    calls: list[tuple[str, tuple, dict]] = []
    defaults: dict[str, Any] = {
        "create_namespaced_custom_object": {"metadata": {"name": WF}},
        "get_namespaced_custom_object": {"metadata": {"name": WF}},
        "list_namespaced_pod": SimpleNamespace(
            items=[
                _pod("p-merge", "a-merge(0)", "2026-01-01T00:00:03Z"),
                _pod("p-w0", "a-w0(0)", "2026-01-01T00:00:01Z", "Running"),
                _pod("p-b", "b(0)", "2026-01-01T00:00:02Z"),
            ]
        ),
        "read_namespaced_pod_log": lambda name, *a, **k: f"log of {name}\n",
        "list_node": SimpleNamespace(items=[_k8s_node()]),
        "list_pod_for_all_namespaces": SimpleNamespace(items=[]),
        "read_node": _k8s_reserved("r1"),
        "create_namespaced_job": {"metadata": {"uid": "u1"}},
        "read_namespaced_job": {"status": {"succeeded": 1}},
        "read_namespaced_job_status": {"status": {"succeeded": 1}},
    }
    defaults.update(answers)
    created = ArgoBackend(CONFIG, client.ApiClient())
    created.core = FakeApi(calls, defaults)  # ty: ignore[invalid-assignment]
    created.batch = FakeApi(calls, defaults)  # ty: ignore[invalid-assignment]
    created.custom = FakeApi(calls, defaults)  # ty: ignore[invalid-assignment]
    return created, calls


def _names(calls: list[tuple[str, tuple, dict]]) -> list[str]:
    return [name for name, _, _ in calls]


# the calls on resources of the cluster or on the pods of every namespace, which a
# ClusterRole has to grant
CLUSTER_CALLS = {"list_node", "read_node", "patch_node", "list_pod_for_all_namespaces"}


def _granted() -> dict[str, set[tuple[str, str, str]]]:
    """What the Role (`namespace`) and the ClusterRole (`cluster`) of the service
    grant its service account."""
    docs = yaml.safe_load_all(
        (Path(__file__).parents[2] / "deploy/cluster/simrun.yaml").read_text()
    )
    objects = {(d["kind"], d["metadata"]["name"]): d for d in docs if d}
    account = {"kind": "ServiceAccount", "name": "simrun", "namespace": "simrun"}
    granted: dict[str, set[tuple[str, str, str]]] = {
        "namespace": set(),
        "cluster": set(),
    }
    for (kind, _), binding in objects.items():
        if kind not in ("RoleBinding", "ClusterRoleBinding"):
            continue
        if account not in binding["subjects"]:
            continue
        ref = binding["roleRef"]
        scope = "cluster" if kind == "ClusterRoleBinding" else "namespace"
        for rule in objects[(ref["kind"], ref["name"])]["rules"]:
            for group in rule["apiGroups"]:
                for resource in rule["resources"]:
                    for verb in rule["verbs"]:
                        granted[scope].add((group, resource, verb))
    return granted


def test_the_service_account_may_do_what_the_backend_does(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created, calls = _fake_backend()
    created.submit(*_run(monkeypatch))
    created.status(WF)
    created.cancel(WF)
    created.stop(WF)
    created.logs(WF, "a")
    created.nodes()
    created.reserve("n1", "r2")
    created.unreserve("n1", "r1")
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    created.build_image(tmp_path, "k3d-simrun-registry:5000/x:1")
    used = set(_names(calls)) | {"watch_namespaced_custom_object"}
    assert used <= set(ACCESS), used - set(ACCESS)
    granted = _granted()
    cluster = {ACCESS[name] for name in used & CLUSTER_CALLS}
    assert not cluster - granted["cluster"]
    namespaced = {ACCESS[name] for name in used - CLUSTER_CALLS}
    assert not namespaced - granted["namespace"] - granted["cluster"]


def test_cancel_and_stop_delete_the_pending_pods() -> None:
    for method, strategy in (("cancel", "Terminate"), ("stop", "Stop")):
        created, calls = _fake_backend()
        getattr(created, method)(WF)
        patch, listing, *deletes = calls
        assert patch[0] == "patch_namespaced_custom_object"
        assert patch[1][-1] == {"spec": {"shutdown": strategy}}
        assert listing[0] == "list_namespaced_pod"
        assert listing[2]["field_selector"] == "status.phase=Pending"
        assert listing[2]["label_selector"] == f"workflows.argoproj.io/workflow={WF}"
        assert [d[1][0] for d in deletes] == ["p-merge", "p-w0", "p-b"]


def test_cancel_of_an_unknown_workflow() -> None:
    def gone(*args: Any, **kw: Any) -> None:
        raise ApiException(status=404)

    created, _ = _fake_backend(patch_namespaced_custom_object=gone)
    with pytest.raises(KeyError):
        created.cancel(WF)
    with pytest.raises(KeyError):
        created.stop(WF)


def test_logs_of_the_pods_of_a_task_in_their_order() -> None:
    created, calls = _fake_backend()
    assert created.logs(WF, "a") == [
        PodLog(node="a-w0(0)", pod="p-w0", text="log of p-w0\n", done=False),
        PodLog(node="a-merge(0)", pod="p-merge", text="log of p-merge\n"),
    ]
    reads = [c for c in calls if c[0] == "read_namespaced_pod_log"]
    assert all(c[2]["container"] == "main" for c in reads)
    assert all("tail_lines" not in c[2] for c in reads)
    # never more than a bounded part of a log into the memory of the service
    assert all(c[2]["limit_bytes"] == LOG_LIMIT_BYTES for c in reads)
    calls.clear()
    created.logs(WF, "a", tail_lines=50)
    reads = [c for c in calls if c[0] == "read_namespaced_pod_log"]
    assert [c[2]["tail_lines"] for c in reads] == [50, 50]


def test_logs_of_a_pod_without_a_log_yet() -> None:
    # the API answers an empty body (None) for a pod whose container has not
    # started, e.g. a pending worker of a running task
    created, _ = _fake_backend(read_namespaced_pod_log=lambda *a, **k: None)
    found = created.logs(WF, "a", tail_lines=300)
    assert [(log.pod, log.text) for log in found] == [("p-w0", ""), ("p-merge", "")]


def test_logs_of_a_pod_which_has_not_started() -> None:
    # the API answers 400 for a pod whose container waits, e.g. for its image
    def waiting(*args: Any, **kw: Any) -> None:
        raise ApiException(status=400, reason="Bad Request")

    def forbidden(*args: Any, **kw: Any) -> None:
        raise ApiException(status=403, reason="Forbidden")

    created, _ = _fake_backend(read_namespaced_pod_log=waiting)
    texts = [log.text for log in created.logs(WF, "a")]
    assert texts == ["(the pod has not started)\n"] * 2
    created, _ = _fake_backend(read_namespaced_pod_log=forbidden)
    texts = [log.text for log in created.logs(WF, "a")]
    assert texts == ["(no logs: Forbidden)\n"] * 2


def test_reserve_taints_and_labels_the_node() -> None:
    created, calls = _fake_backend(read_node=_k8s_reserved(None))
    created.reserve("n1", "r2")
    (patch,) = [c for c in calls if c[0] == "patch_node"]
    assert patch[1][0] == "n1"
    assert patch[1][1] == {
        "metadata": {"labels": {"simrun/reserved": "r2"}, "resourceVersion": "7"},
        "spec": {
            "taints": [
                {"key": "other", "value": "x", "effect": "NoSchedule"},
                {"key": "simrun/reserved", "value": "r2", "effect": "NoExecute"},
            ]
        },
    }


def test_unreserve_removes_only_the_reservation_of_the_run() -> None:
    created, calls = _fake_backend()
    created.unreserve("n1", "r9")
    assert "patch_node" not in _names(calls)
    created.unreserve("n1", "r1")
    (patch,) = [c for c in calls if c[0] == "patch_node"]
    assert patch[1][1] == {
        "metadata": {"labels": {"simrun/reserved": None}, "resourceVersion": "7"},
        "spec": {"taints": []},
    }


def test_unknown_node() -> None:
    def gone(*args: Any, **kw: Any) -> None:
        raise ApiException(status=404)

    created, _ = _fake_backend(read_node=gone)
    with pytest.raises(KeyError):
        created.reserve("n9", "r1")
    with pytest.raises(KeyError):
        created.unreserve("n9", "r1")


def test_the_build_job_is_deleted_when_its_context_cannot_be_stored(
    tmp_path: Path,
) -> None:
    def refused(*args: Any, **kw: Any) -> None:
        raise ApiException(status=422)

    created, calls = _fake_backend(create_namespaced_config_map=refused)
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    with pytest.raises(ApiException):
        created.build_image(tmp_path, "k3d-simrun-registry:5000/x:1")
    assert "delete_namespaced_job" in _names(calls)


def test_the_build_job_stays_off_the_worker_nodes(tmp_path: Path) -> None:
    created, calls = _fake_backend()
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    created.build_image(tmp_path, "k3d-simrun-registry:5000/x:1")
    (job,) = [c[1][1] for c in calls if c[0] == "create_namespaced_job"]
    affinity = job.spec.template.spec.affinity.node_affinity
    terms = affinity.required_during_scheduling_ignored_during_execution
    (term,) = terms.node_selector_terms
    (expression,) = term.match_expressions
    assert (expression.key, expression.operator) == (WORKER_LABEL, "DoesNotExist")


def test_the_infrastructure_stays_off_the_worker_nodes() -> None:
    root = Path(__file__).parents[2] / "deploy/cluster"
    deployments = [
        d
        for name in ("buildkit.yaml", "zot.yaml", "simrun.yaml")
        for d in yaml.safe_load_all((root / name).read_text())
        if d and d["kind"] == "Deployment"
    ]
    assert len(deployments) == 3
    for deployment in deployments:
        affinity = deployment["spec"]["template"]["spec"]["affinity"]
        terms = affinity["nodeAffinity"][
            "requiredDuringSchedulingIgnoredDuringExecution"
        ]["nodeSelectorTerms"]
        assert terms == [
            {"matchExpressions": [{"key": WORKER_LABEL, "operator": "DoesNotExist"}]}
        ]


def test_a_workflow_which_exists_is_taken_as_submitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def exists(*args: Any, **kw: Any) -> None:
        raise ApiException(status=409)

    created, _ = _fake_backend(create_namespaced_custom_object=exists)
    assert created.submit(*_run(monkeypatch)) == WF

    def forbidden(*args: Any, **kw: Any) -> None:
        raise ApiException(status=403)

    created, _ = _fake_backend(create_namespaced_custom_object=forbidden)
    with pytest.raises(ApiException):
        created.submit(*_run(monkeypatch))


def test_attempt_of_a_pod() -> None:
    def pod(*env: dict[str, str], name: str = "main") -> dict[str, Any]:
        return {"spec": {"containers": [{"name": name, "env": list(env)}]}}

    assert _attempt_of(pod({"name": "SIMRUN_ATTEMPT", "value": "1"})) == 1
    assert _attempt_of(pod({"name": "SIMRUN_ATTEMPT", "value": "0"})) == 0
    # not substituted, not a number, missing, or of another container
    assert _attempt_of(pod({"name": "SIMRUN_ATTEMPT", "value": "{{retries}}"})) is None
    assert _attempt_of(pod({"name": "SIMRUN_ATTEMPT"})) is None
    assert _attempt_of(pod({"name": "SIMRUN_WORKER", "value": "a-w0"})) is None
    assert (
        _attempt_of(pod({"name": "SIMRUN_ATTEMPT", "value": "1"}, name="wait")) is None
    )
    assert _attempt_of({}) is None


def test_placements_of_run_pods() -> None:
    run = _k8s_pod(
        "n2",
        namespace="simrun",
        containers=[{"cpu": "4", "memory": "6Gi"}],
        labels={"workflows.argoproj.io/workflow": WF},
    )
    run["metadata"]["name"] = "p-extra"
    run["metadata"]["annotations"] = {
        "workflows.argoproj.io/node-name": f"{WF}.squares-extra(0:1)"
    }
    run["status"] = {"phase": "Running"}
    run["spec"]["containers"][0]["name"] = "main"
    run["spec"]["containers"][0]["env"] = [
        {"name": "SIMRUN_WORKER", "value": "squares-w1"},
        {"name": "SIMRUN_ATTEMPT", "value": "2"},
    ]
    merge = _k8s_pod(
        "n2",
        namespace="simrun",
        containers=[{"cpu": "1"}],
        labels={"workflows.argoproj.io/workflow": WF},
    )
    merge["metadata"]["name"] = "p-merge"
    merge["metadata"]["annotations"] = {
        "workflows.argoproj.io/node-name": f"{WF}.squares-merge"
    }
    merge["status"] = {"phase": "Pending"}
    other = _k8s_pod("n2", namespace="simrun", containers=[{"cpu": "1"}])
    created, calls = _fake_backend(
        list_namespaced_pod=SimpleNamespace(items=[run, merge, other])
    )
    assert created.placements() == [
        Placement(
            workflow=WF,
            dag_task="squares-extra",
            pod="p-extra",
            node="n2",
            phase="Running",
            cores=4.0,
            memory_gb=6.0,
            worker="squares-w1",
            attempt=2,
        ),
        Placement(
            workflow=WF,
            dag_task="squares-merge",
            pod="p-merge",
            node="n2",
            phase="Pending",
            cores=1.0,
            memory_gb=0.0,
            worker=None,
            attempt=None,
        ),
    ]
    (listing,) = [c for c in calls if c[0] == "list_namespaced_pod"]
    assert listing[1] == (CONFIG.namespace,)
    assert listing[2]["field_selector"] == (
        "status.phase!=Succeeded,status.phase!=Failed"
    )


class FakeWatch:
    """A `Watch` whose streams are the given rounds: events, or an exception."""

    def __init__(self, rounds: list[Any], streams: list[tuple[tuple, dict]]) -> None:
        self.rounds = rounds
        self.streams = streams
        self.stopped = False

    def stream(self, func: Any, *args: Any, **kw: Any) -> Any:
        self.streams.append((args, kw))
        events = self.rounds.pop(0)
        if isinstance(events, Exception):
            raise events
        yield from events

    def stop(self) -> None:
        self.stopped = True


def test_subscribe_wakes_the_controller_on_every_change_of_a_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created, _ = _fake_backend()
    streams: list[tuple[tuple, dict]] = []
    done = threading.Event()

    def last_round() -> Any:
        yield {"type": "MODIFIED"}
        created.close()
        done.set()

    # a watch which ends, one which fails (e.g. the API is restarted), then one
    # whose event comes as the backend is closed
    rounds: list[Any] = [
        [{"type": "ADDED"}, {"type": "MODIFIED"}],
        ApiException(status=500),
        last_round(),
    ]
    monkeypatch.setattr(argo_backend, "Watch", lambda: FakeWatch(rounds, streams))
    monkeypatch.setattr(argo_backend, "WATCH_RETRY_S", 0.01)
    woken: list[int] = []
    created.subscribe(lambda: woken.append(1))
    assert done.wait(5)
    created.join_watch(5)
    assert len(woken) == 3
    assert rounds == []
    args, kw = streams[0]
    assert args == ("argoproj.io", "v1alpha1", "simrun", "workflows")
    # the workflows of the runs only, every one of them
    assert kw["label_selector"] == RUN_LABEL
    assert kw["timeout_seconds"] == argo_backend.WATCH_TIMEOUT_S


def test_a_second_subscription_shares_the_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created, _ = _fake_backend()
    streams: list[tuple[tuple, dict]] = []
    done = threading.Event()
    subscribed = threading.Event()

    def events() -> Any:
        assert subscribed.wait(5)  # both subscribed before the first event
        yield {"type": "MODIFIED"}
        created.close()
        done.set()

    rounds: list[Any] = [events()]
    monkeypatch.setattr(argo_backend, "Watch", lambda: FakeWatch(rounds, streams))
    first: list[int] = []
    second: list[int] = []
    created.subscribe(lambda: first.append(1))
    created.subscribe(lambda: second.append(1))
    subscribed.set()
    assert done.wait(5)
    created.join_watch(5)
    assert (first, second, len(streams)) == ([1], [1], 1)


def test_a_cut_watch_starts_again_at_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # a connection the API server ends (IncompleteRead under load) is no failure:
    # the watch starts again without the wait and without a traceback
    from urllib3.exceptions import ProtocolError

    created, _ = _fake_backend()
    streams: list[tuple[tuple, dict]] = []
    done = threading.Event()

    def last_round() -> Any:
        yield {"type": "MODIFIED"}
        created.close()
        done.set()

    rounds: list[Any] = [ProtocolError("Connection broken"), last_round()]
    monkeypatch.setattr(argo_backend, "Watch", lambda: FakeWatch(rounds, streams))
    monkeypatch.setattr(argo_backend, "WATCH_RETRY_S", 30.0)
    woken: list[int] = []
    with caplog.at_level("INFO", logger="simrun.service.argo_backend"):
        created.subscribe(lambda: woken.append(1))
        assert done.wait(5)
        created.join_watch(5)
    assert woken == [1]
    assert "was cut" in caplog.text
    assert "Traceback" not in caplog.text and not any(
        r.exc_info for r in caplog.records
    )
