from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from simrun.config import ClusterConfig
from simrun.planner import Node
from simrun.service.argo_backend import (
    WORKER_LABEL,
    ArgoBackend,
    _task_node,
    backend,
    manifest_url,
    node_from,
    workflow_status,
)

WF = "simrun-r1"
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
    assert status.oom == {}


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
    assert status.oom == {"a-w0": 1, "a-w2": 2}


def test_a_kill_of_the_executor_is_no_kill_of_the_worker() -> None:
    init = "init: OOMKilled (exit code 137)"
    status = workflow_status(
        _workflow(
            _node("a-w0", "Retry", "Running", ["a-w0(0)", "a-w0(1)"], "a-w0"),
            _node("a-w0(0)", "Pod", "Error", worker="a-w0", message=init),
            _node("a-w0(1)", "Pod", "Pending", worker="a-w0"),
        )
    )
    assert status.oom == {}
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


def test_node_is_allocatable_minus_the_executor_rounded_down() -> None:
    # 6442450Ki is 6.14 GiB, 0.25 GiB of the executor beside it: 5 GiB, 19 cores
    assert node_from(_k8s_node()) == Node("n1", 19, 5.0, 0, None)
    node = _k8s_node(cpu="19500m")
    node["status"]["allocatable"]["nvidia.com/gpu"] = "2"
    assert node_from(node) == Node("n1", 19, 5.0, 2, None)


def test_node_reservation_and_taints() -> None:
    reserved = {"key": "simrun/reserved", "value": "r1", "effect": "NoExecute"}
    assert node_from(_k8s_node([reserved])) == Node("n1", 19, 5.0, 0, "r1")
    labelled = _k8s_node(labels={"simrun/reserved": "r2"})
    assert node_from(labelled) == Node("n1", 19, 5.0, 0, "r2")
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


def test_a_dead_pool_process_is_a_kill_for_memory() -> None:
    # Ruling 7: the worker exits 137 itself when a process of its pool died
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
    assert status.tasks == {"a-w0": "Pending"}
    assert status.oom == {"a-w0": 1}


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
    assert status.oom == {"b": 1, "a-merge": 1}
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
    "read_node": ("", "nodes", "get"),
    "patch_node": ("", "nodes", "patch"),
    "create_namespaced_job": ("batch", "jobs", "create"),
    "read_namespaced_job": ("batch", "jobs", "get"),
    "read_namespaced_job_status": ("batch", "jobs/status", "get"),
    "delete_namespaced_job": ("batch", "jobs", "delete"),
    "create_namespaced_config_map": ("", "configmaps", "create"),
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


def _pod(name: str, node: str, created: str) -> dict[str, Any]:
    return {
        "metadata": {
            "name": name,
            "creationTimestamp": created,
            "annotations": {"workflows.argoproj.io/node-name": f"{WF}.{node}"},
        }
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
                _pod("p-w0", "a-w0(0)", "2026-01-01T00:00:01Z"),
                _pod("p-b", "b(0)", "2026-01-01T00:00:02Z"),
            ]
        ),
        "read_namespaced_pod_log": lambda name, *a, **k: f"log of {name}\n",
        "list_node": SimpleNamespace(items=[_k8s_node()]),
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


def _granted() -> set[tuple[str, str, str]]:
    """What the Role and the ClusterRole of the service grant its service account."""
    docs = yaml.safe_load_all(
        (Path(__file__).parents[2] / "deploy/cluster/simrun.yaml").read_text()
    )
    objects = {(d["kind"], d["metadata"]["name"]): d for d in docs if d}
    account = {"kind": "ServiceAccount", "name": "simrun", "namespace": "simrun"}
    granted: set[tuple[str, str, str]] = set()
    for (kind, _), binding in objects.items():
        if kind not in ("RoleBinding", "ClusterRoleBinding"):
            continue
        if account not in binding["subjects"]:
            continue
        ref = binding["roleRef"]
        for rule in objects[(ref["kind"], ref["name"])]["rules"]:
            for group in rule["apiGroups"]:
                for resource in rule["resources"]:
                    for verb in rule["verbs"]:
                        granted.add((group, resource, verb))
    return granted


def test_the_service_account_may_do_what_the_backend_does(tmp_path: Path) -> None:
    created, calls = _fake_backend()
    created.submit({"metadata": {"name": WF}})
    created.status(WF)
    created.cancel(WF)
    created.stop(WF)
    created.logs(WF, "a")
    created.nodes()
    created.reserve("n1", "r2")
    created.unreserve("n1", "r1")
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    created.build_image(tmp_path, "k3d-simrun-registry:5000/x:1")
    used = set(_names(calls))
    assert used <= set(ACCESS), used - set(ACCESS)
    missing = {ACCESS[name] for name in used} - _granted()
    assert not missing


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
    text = created.logs(WF, "a")
    assert text == (
        f"==> {WF}.a-w0(0) (p-w0) <==\nlog of p-w0\n\n"
        f"==> {WF}.a-merge(0) (p-merge) <==\nlog of p-merge\n"
    )
    reads = [c for c in calls if c[0] == "read_namespaced_pod_log"]
    assert all(c[2]["container"] == "main" for c in reads)


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
