"""The central assumptions of the design on a real cluster (k3d, Kueue, Argo).

Every run is the squares demo. Memory is the binding resource: an agent has 6 GiB
(5 GiB for a pod besides the executor, as the backend reports it), a pod of
`ONE_PER_NODE` (one process of 4 GiB besides the base of 0.5 GiB) takes 5 GiB, so one
pod fits a node and a run of `fastest` has three pods on three nodes.
"""

import json
import os
import time
import uuid
from pathlib import Path

import pytest
from k3d_cluster import AGENTS, REGISTRY, TERMINAL, Service, wait_for

pytestmark = [
    pytest.mark.cluster,
    pytest.mark.skipif(
        os.environ.get("SIMRUN_K3D") != "1",
        reason="the cluster tests run with SIMRUN_K3D=1",
    ),
]

ONE_PER_NODE = {"memory_per_worker_gb": 4.0}


def squares(n: int) -> int:
    return sum(i * i for i in range(n))


def _node_name(pod: dict) -> str:
    return pod["metadata"]["annotations"]["workflows.argoproj.io/node-name"]


def test_demo_run_end_to_end(service: Service) -> None:
    workflow = service.workflow(n=200, sleep_s=0.2, resources=ONE_PER_NODE)
    run = service.submit("t-mk", workflow)
    record = service.wait(run["id"])
    assert record["state"] == "succeeded", record["message"]
    assert record["tasks"] == {"squares": "done"}
    assert service.total(run["id"]) == squares(200)
    task_dir = service.task_dir(run["id"])
    reports = sorted(p.name for p in (task_dir / "workers").glob("*.json"))
    assert reports == ["squares-w0-0.json", "squares-w1-0.json", "squares-w2-0.json"]
    nodes = {
        json.loads((task_dir / "workers" / r).read_text())["node"] for r in reports
    }
    assert nodes == set(AGENTS)
    # the controller recorded the three reports as observations
    assert sorted(p.name for p in (task_dir / "recorded").iterdir()) == reports


def test_take_back_same_priority(service: Service) -> None:
    # A alone on three pods takes about 400 * 0.5 / 3 = 67 s, so its extra workers
    # cannot end of themselves within the 60 s in which a pod of B has to run
    a = service.submit(
        "t-mk-bot", service.workflow(n=400, sleep_s=0.5, resources=ONE_PER_NODE)
    )
    wait_for(
        lambda: len(service.cluster.running_pods(a["id"])) == 3,
        120,
        "run A did not run on all three nodes",
    )
    b = service.submit(
        "t-mk-bot", service.workflow(n=60, sleep_s=0.2, resources=ONE_PER_NODE)
    )
    a_units = service.task_dir(a["id"]) / "out" / "units"

    def b_runs_beside_a() -> bool:
        b_running = service.cluster.running_pods(b["id"])
        if b_running:
            assert service.cluster.running_pods(a["id"]), "A lost all its pods to B"
            # an extra worker ends of itself only once nothing is pending, when at
            # most one unit per pod of A is still in flight
            done = len(list(a_units.glob("*.json")))
            assert done < 390, f"A had computed {done} of 400 units: no take-back"
        return bool(b_running)

    wait_for(b_runs_beside_a, 60, "no pod of run B ran")
    # Kueue preempted a pod of A for B; an attempt Argo had not seen running yet is
    # recreated as the same attempt, so the status of A need not say `Preempted`
    assert service.cluster.preempted(a["id"]), "Kueue preempted no pod of run A"
    for run, n in ((a, 400), (b, 60)):
        record = service.wait(run["id"])
        assert record["state"] == "succeeded", record["message"]
        assert service.total(run["id"]) == squares(n)


def test_owner_preempts_agent(service: Service) -> None:
    agent = service.submit(
        "t-alice-bot", service.workflow(n=300, sleep_s=0.2, resources=ONE_PER_NODE)
    )
    wait_for(
        lambda: len(service.cluster.running_pods(agent["id"])) == 3,
        120,
        "the agent run did not run on all three nodes",
    )
    owner = service.submit(
        "t-mk", service.workflow(n=60, sleep_s=0.2, resources=ONE_PER_NODE)
    )
    wait_for(
        lambda: service.cluster.running_pods(owner["id"]),
        60,
        "the owner run did not start",
    )
    owner_record = service.wait(owner["id"])
    assert owner_record["state"] == "succeeded", owner_record["message"]
    agent_record = service.wait(agent["id"])
    assert agent_record["state"] == "succeeded", agent_record["message"]
    assert agent_record["finished"] > owner_record["finished"]
    assert service.total(owner["id"]) == squares(60)
    assert service.total(agent["id"]) == squares(300)


def test_oom_retry(service: Service) -> None:
    # the hint of the demo is 0.5 GiB per process: 9 processes in 5 GiB, which the
    # units of 600 MB exceed; the retry has 0.75 GiB per process, 6 processes
    workflow = service.workflow(n=33, sleep_s=1.0, memory_mb=600)
    before = service.estimate("t-mk", workflow)["squares"]["resources"]
    assert before["memory_per_worker_gb"] == 0.5
    run = service.submit("t-mk", workflow, split="single-server")
    record = service.wait(run["id"])
    assert record["state"] == "succeeded", record["message"]
    assert service.total(run["id"]) == squares(33)
    assert record["oom_kills"] == {"squares-w0": 1}
    after = service.estimate("t-mk", workflow)["squares"]["resources"]
    assert after["memory_per_worker_gb"] > before["memory_per_worker_gb"]


def test_reservation(service: Service) -> None:
    node = AGENTS[0]
    reserved = service.submit(
        "t-mk",
        service.workflow(n=100, sleep_s=0.2, resources=ONE_PER_NODE),
        reserve=node,
    )
    taints = service.cluster.node(node)["spec"].get("taints") or []
    assert {
        "key": "simrun/reserved",
        "value": reserved["id"],
        "effect": "NoExecute",
    } in [{k: t.get(k) for k in ("key", "value", "effect")} for t in taints]
    wait_for(
        lambda: service.cluster.running_pods(reserved["id"]),
        120,
        "the run on the reserved node did not start",
    )
    other = service.submit(
        "t-mk", service.workflow(n=60, sleep_s=0.2, resources=ONE_PER_NODE)
    )
    deadline = time.monotonic() + 300
    while service.record(reserved["id"])["state"] not in TERMINAL:
        assert time.monotonic() < deadline, "the run on the reserved node did not end"
        for pod in service.cluster.pods(reserved["id"]):
            assert pod["spec"].get("nodeName") in (None, node)
        for pod in service.cluster.pods(other["id"]):
            assert pod["spec"].get("nodeName") != node, _node_name(pod)
        time.sleep(1.0)
    for run in (reserved, other):
        record = service.wait(run["id"])
        assert record["state"] == "succeeded", record["message"]
    released = service.cluster.node(node)
    assert "simrun/reserved" not in (released["metadata"].get("labels") or {})
    assert not [
        t for t in released["spec"].get("taints") or [] if t["key"] == "simrun/reserved"
    ]


def test_lost_worker(service: Service) -> None:
    run = service.submit(
        "t-mk", service.workflow(n=150, sleep_s=0.2, resources=ONE_PER_NODE)
    )
    pod = wait_for(
        lambda: next(
            (
                p
                for p in service.cluster.running_pods(run["id"])
                if "squares-extra(" in _node_name(p)
            ),
            None,
        ),
        120,
        "no extra worker ran",
    )
    service.cluster.core.delete_namespaced_pod(
        pod["metadata"]["name"], "simrun", grace_period_seconds=0
    )
    record = service.wait(run["id"])
    assert record["state"] == "succeeded", record["message"]
    assert service.total(run["id"]) == squares(150)


def test_build_image(service: Service, tmp_path: Path) -> None:
    backend = service.http.app.state.backend  # ty: ignore[unresolved-attribute]
    context = tmp_path / "context"
    context.mkdir()
    (context / "Dockerfile").write_text(
        f"FROM {service.settings.default_image}\nCOPY hello.txt /tmp/hello.txt\n"
    )
    (context / "hello.txt").write_text("hello\n")
    ref = f"{REGISTRY}/simrun-built:{uuid.uuid4().hex[:8]}"
    assert not backend.has_image(ref)
    backend.build_image(context, ref)
    assert backend.has_image(ref)
