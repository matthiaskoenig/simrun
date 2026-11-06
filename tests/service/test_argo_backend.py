from typing import Any

import pytest
import yaml

from simrun.config import ClusterConfig
from simrun.planner import Node
from simrun.service.argo_backend import (
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
