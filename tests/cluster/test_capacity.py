"""The capacity of a node is what the pods of other namespaces leave (Important 9).

A DaemonSet with requests on the agents (as the NFS driver, node-exporter and the
device plugin of a real cluster) takes 1 GiB of every agent: the backend reports the
nodes 1 GiB smaller, the planner sizes a pod for a whole node beside the DaemonSet,
and the pod is admitted and runs instead of waiting forever.
"""

import os
from collections.abc import Iterator

import pytest
from k3d_cluster import AGENTS, Service, wait_for
from kubernetes import client

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(
        os.environ.get("SIMRUN_K3D") != "1",
        reason="the cluster tests run with SIMRUN_K3D=1",
    ),
]

NAME = "simrun-test-daemon"
NAMESPACE = "default"


@pytest.fixture
def daemonset(service: Service) -> Iterator[None]:
    apps = client.AppsV1Api(service.cluster.api)
    resources = {"cpu": "100m", "memory": "1Gi"}
    body = {
        "metadata": {"name": NAME},
        "spec": {
            "selector": {"matchLabels": {"app": NAME}},
            "template": {
                "metadata": {"labels": {"app": NAME}},
                "spec": {
                    "nodeSelector": {"simrun/worker": "true"},
                    "terminationGracePeriodSeconds": 0,
                    "containers": [
                        {
                            "name": "sleep",
                            "image": service.cluster.image,
                            "command": ["sleep", "infinity"],
                            "resources": {"requests": resources, "limits": resources},
                        }
                    ],
                },
            },
        },
    }
    apps.create_namespaced_daemon_set(
        NAMESPACE, client.V1DaemonSet.model_validate(body)
    )
    cluster = service.cluster

    def pods() -> list[dict]:
        listed = cluster.core.list_namespaced_pod(
            NAMESPACE, label_selector=f"app={NAME}"
        )
        return [cluster.api.sanitize_for_serialization(p) for p in listed.items]

    try:
        wait_for(
            lambda: sorted(p["spec"].get("nodeName") or "" for p in pods()) == AGENTS,
            120,
            "the DaemonSet was not placed on the agents",
        )
        yield
    finally:
        apps.delete_namespaced_daemon_set(NAME, NAMESPACE)
        wait_for(lambda: not pods(), 120, "the pods of the DaemonSet were not deleted")


def _memory(service: Service) -> dict[str, float]:
    backend = service.http.app.state.backend  # ty: ignore[unresolved-attribute]
    return {n.name: n.memory_gb for n in backend.nodes()}


def test_pods_of_other_namespaces_are_subtracted(service: Service, request) -> None:
    before = _memory(service)
    request.getfixturevalue("daemonset")
    after = _memory(service)
    assert set(after) == set(AGENTS)
    assert all(after[a] == before[a] - 1 for a in AGENTS), (before, after)
    # one process takes all a node has left besides the base of 0.5 GiB, so memory
    # binds and not the cores of the node (a runner of continuous integration has 4)
    largest = max(after.values())
    workflow = service.workflow(
        n=60, sleep_s=0.2, resources={"memory_per_worker_gb": largest - 0.5}
    )
    run = service.submit("t-mk", workflow)
    record = service.wait(run["id"])
    assert record["state"] == "succeeded", record["message"]
    assert record["plans"]["squares"]["shape"]["memory_gb"] == largest
