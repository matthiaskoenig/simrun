"""A backend in memory for tests of the service, advanced by the test."""

import dataclasses
import os
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

from simrun.argo import to_argo
from simrun.config import ClusterConfig
from simrun.planner import Node, TaskPlan
from simrun.plugins import TaskType
from simrun.service.backend import (
    FINAL_PHASES,
    BackendStatus,
    FailedAttempt,
    Placement,
    PodLog,
)
from simrun.store import RunRecord
from simrun.worker import MERGED_MARKER

# the tokens of the tests of the service: the owner, the owner's agent, another
# person and the agent of the other person
TOKENS: dict[str, dict[str, str]] = {
    "t-mk": {"name": "mk", "kind": "person"},
    "t-mk-bot": {"name": "mk-bot", "kind": "agent", "owner": "mk"},
    "t-alice": {"name": "alice", "kind": "person"},
    "t-alice-bot": {"name": "alice-bot", "kind": "agent", "owner": "alice"},
}

# the nodes of the tests of the planner: one large server and eleven small ones
TEST_NODES = [Node("srv-01", 104, 1000)] + [
    Node(f"srv-{i:02d}", 40, 188) for i in range(2, 13)
]


def dag_nodes(manifest: dict[str, Any]) -> list[str]:
    """The names of the DAG nodes of a workflow manifest, as Argo names them.

    A DAG task over a sequence is one node per item, `<task>(<index>:<item>)`.
    """
    spec = manifest["spec"]
    dag = next(t for t in spec["templates"] if t["name"] == spec["entrypoint"])["dag"]
    names: list[str] = []
    for task in dag["tasks"]:
        sequence = task.get("withSequence")
        if sequence is None:
            names.append(task["name"])
            continue
        start = int(sequence.get("start", 0))
        for index in range(int(sequence["count"])):
            names.append(f"{task['name']}({index}:{start + index})")
    return names


class FakeBackend:
    """A backend whose workflows change only when the test says so.

    A submitted workflow is `Pending` without nodes. `start` sets every node
    running, `finish` every node succeeded, `fail` and `set_node` one node,
    `fail_attempt` and `oom_kill` fail attempts of a pod; `finish_after` finishes a
    workflow that many seconds after its submission. The attempts of a pod are
    numbered from 0 in the order they fail. A coordinator which merges and succeeds
    writes the marker of a merge into the output of its task in `store`, as the
    harness does, unless `merges` is False (the harness of simrun 0.1.0).
    The calls which change the cluster are recorded in `calls`.

    Attributes:
        manifests: The submitted manifests by name.
        subscribed: The callbacks registered with `subscribe`.
        calls: `("submit", name)`, `("cancel", name)`, `("stop", name)`,
            `("logs", name, task_id)`, `("reserve", node, run_id)`,
            `("unreserve", node, run_id)` and `("build_image", ref, files of the
            context)`.
        images: The images of the registry.
        pod_logs: The logs of the pods by workflow name and task id, see `add_log`.
        store: The store of the service, where a coordinator which merges writes the
            marker of its merge; None writes none.
        merges: Whether a coordinator which merges writes the marker.
    """

    def __init__(
        self,
        nodes: list[Node] | None = None,
        finish_after: float | None = None,
        images: Iterable[str] = (),
    ) -> None:
        """Create the backend.

        Args:
            nodes: The nodes of the cluster, `TEST_NODES` by default.
            finish_after: Seconds after which a submitted workflow succeeds.
            images: The images the registry has.
        """
        self._lock = threading.Lock()
        self._nodes = {n.name: n for n in (nodes or TEST_NODES)}
        self.finish_after = finish_after
        self.images = set(images)
        self.manifests: dict[str, dict[str, Any]] = {}
        self.subscribed: list[Callable[[], None]] = []
        self.calls: list[tuple[Any, ...]] = []
        self.pod_logs: dict[tuple[str, str], list[PodLog]] = {}
        self._status: dict[str, BackendStatus] = {}
        self._submitted: dict[str, float] = {}
        self._placed: list[Placement] = []
        # the outputs of the tasks whose coordinator merges, by workflow and node
        self._merging: dict[str, dict[str, Path]] = {}
        self.store: Path | None = None
        self.merges = True

    runs_images = True
    extra_workers = True

    def subscribe(self, callback: Callable[[], None]) -> None:
        """Remember the callback in `subscribed`; the fake never calls it."""
        self.subscribed.append(callback)

    def submit(
        self,
        record: RunRecord,
        plans: dict[str, TaskPlan],
        types: dict[str, TaskType],
        cluster: ClusterConfig,
    ) -> str:
        """Accept the workflow of a run, pending without nodes; one which exists is kept.

        Raises:
            ValueError: The workflow cannot be built, e.g. a task has no image.
        """
        manifest = to_argo(record, plans, types, cluster)
        name = manifest["metadata"]["name"]
        with self._lock:
            if name in self.manifests:
                self.calls.append(("submit", name))
                return name
            self.manifests[name] = manifest
            self._merging[name] = {
                f"{task.id}-w0": Path("runs", record.id, "tasks", task.id, "out")
                for task in record.workflow.tasks
                if types[task.type].splittable and plans[task.id].coordinator_merges
            }
            self._status[name] = BackendStatus(phase="Pending", tasks={})
            self._submitted[name] = time.monotonic()
            self.calls.append(("submit", name))
        return name

    def status(self, name: str) -> BackendStatus:
        """The status of a workflow; KeyError for an unknown one."""
        with self._lock:
            status = self._status[name]
            elapsed = time.monotonic() - self._submitted[name]
            if (
                self.finish_after is not None
                and elapsed >= self.finish_after
                and status.phase not in FINAL_PHASES
            ):
                self._set_all(name, "Succeeded")
            return self._status[name].model_copy(deep=True)

    def _set_all(self, name: str, phase: str) -> None:
        status = self._status[name]
        status.phase = phase
        status.tasks = dict.fromkeys(dag_nodes(self.manifests[name]), phase)
        for node in status.tasks:
            self._merged(name, node, phase)

    def _merged(self, name: str, node: str, phase: str) -> None:
        """Write the marker of a merge of a coordinator which merges and succeeded."""
        out = self._merging.get(name, {}).get(node)
        if phase != "Succeeded" or out is None or self.store is None:
            return
        if self.merges:
            (self.store / out).mkdir(parents=True, exist_ok=True)
            (self.store / out / MERGED_MARKER).touch()

    def start(self, name: str) -> None:
        """Set every node of a workflow running."""
        with self._lock:
            self._set_all(name, "Running")

    def finish(self, name: str) -> None:
        """Let every node of a workflow succeed."""
        with self._lock:
            self._set_all(name, "Succeeded")

    def set_node(self, name: str, node: str, phase: str) -> None:
        """Set the phase of one node."""
        with self._lock:
            self._status[name].tasks[node] = phase
            self._merged(name, node, phase)

    def fail(self, name: str, node: str) -> None:
        """Fail a node, given by its name or by a task id (its coordinator `-w0`)."""
        with self._lock:
            nodes = dag_nodes(self.manifests[name])
            target = node if node in nodes else f"{node}-w0"
            self._status[name].tasks[target] = "Failed"

    def delete(self, name: str) -> None:
        """Remove a workflow, as its time to live does."""
        with self._lock:
            del self._status[name]

    def fail_attempt(
        self,
        name: str,
        worker: str,
        exit_code: int | None = None,
        oom_killed: bool = False,
    ) -> None:
        """Fail the next attempt of a pod of a worker, merge or unsplit task."""
        with self._lock:
            attempts = self._status[name].failed.setdefault(worker, [])
            attempts.append(
                FailedAttempt(
                    attempt=len(attempts), exit_code=exit_code, oom_killed=oom_killed
                )
            )

    def oom_kill(self, name: str, worker: str, count: int = 1) -> None:
        """Let the next attempts of a pod be killed by the out of memory killer."""
        for _ in range(count):
            self.fail_attempt(name, worker, exit_code=137, oom_killed=True)

    def cancel(self, name: str) -> None:
        """Terminate a workflow."""
        with self._lock:
            self.calls.append(("cancel", name))
            self._status[name].phase = "Failed"

    def stop(self, name: str) -> None:
        """Stop a workflow."""
        with self._lock:
            self.calls.append(("stop", name))
            self._status[name].phase = "Failed"

    def add_log(
        self, name: str, task_id: str, node: str, text: str, done: bool = True
    ) -> None:
        """Add the log of a pod of a task, the pod named `<workflow>-<node>`."""
        with self._lock:
            pod = PodLog(node=node, pod=f"{name}-{node}", text=text, done=done)
            self.pod_logs.setdefault((name, task_id), []).append(pod)

    def logs(
        self, name: str, task_id: str, tail_lines: int | None = None
    ) -> list[PodLog]:
        """The logs added with `add_log`, the last `tail_lines` lines of each."""
        with self._lock:
            self.calls.append(("logs", name, task_id))
            logs = list(self.pod_logs.get((name, task_id), []))
        if tail_lines is None:
            return logs
        return [
            log.model_copy(
                update={"text": "".join(log.text.splitlines(True)[-tail_lines:])}
            )
            for log in logs
        ]

    def nodes(self) -> list[Node]:
        """The nodes, with their reservations."""
        with self._lock:
            return list(self._nodes.values())

    def place(self, placement: Placement) -> None:
        """Place a pod, listed by `placements` until its workflow has ended.

        A placement of a pod placed before replaces it, e.g. once the pod is bound.
        """
        with self._lock:
            self._placed = [p for p in self._placed if p.pod != placement.pod]
            self._placed.append(placement)

    def placements(self) -> list[Placement]:
        """The pods placed for the workflows which have not ended."""
        with self._lock:
            return [
                p
                for p in self._placed
                if p.workflow in self._status
                and self._status[p.workflow].phase not in FINAL_PHASES
            ]

    def reserve(self, node: str, run_id: str) -> None:
        """Reserve a node; KeyError for an unknown one."""
        with self._lock:
            self._nodes[node] = dataclasses.replace(
                self._nodes[node], reserved_by=run_id
            )
            self.calls.append(("reserve", node, run_id))

    def unreserve(self, node: str, run_id: str) -> None:
        """Release a node reserved by the run; KeyError for an unknown node."""
        with self._lock:
            self.calls.append(("unreserve", node, run_id))
            if self._nodes[node].reserved_by == run_id:
                self._nodes[node] = dataclasses.replace(
                    self._nodes[node], reserved_by=None
                )

    def build_image(self, context: Path, ref: str) -> None:
        """Record the build and add the image to the registry."""
        with self._lock:
            self.calls.append(("build_image", ref, sorted(os.listdir(context))))
            self.images.add(ref)

    def has_image(self, ref: str) -> bool:
        """Whether the image was given or built."""
        with self._lock:
            return ref in self.images
