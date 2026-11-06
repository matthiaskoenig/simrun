"""A backend in memory for tests of the service, advanced by the test."""

import dataclasses
import os
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from simrun.planner import Node
from simrun.service.backend import FINAL_PHASES, BackendStatus

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
    running, `finish` every node succeeded, `fail` and `set_node` one node;
    `finish_after` finishes a workflow that many seconds after its submission.
    The calls which change the cluster are recorded in `calls`.

    Attributes:
        manifests: The submitted manifests by name.
        calls: `("submit", name)`, `("cancel", name)`, `("stop", name)`,
            `("reserve", node, run_id)`, `("unreserve", node, run_id)` and
            `("build_image", ref, files of the context)`.
        images: The images of the registry.
        logs_text: The logs returned by `logs`, by workflow name and task id.
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
        self.calls: list[tuple[Any, ...]] = []
        self.logs_text: dict[tuple[str, str], str] = {}
        self._status: dict[str, BackendStatus] = {}
        self._submitted: dict[str, float] = {}

    def submit(self, manifest: dict[str, Any]) -> str:
        """Accept a workflow, pending without nodes."""
        name = manifest["metadata"]["name"]
        with self._lock:
            self.manifests[name] = manifest
            self._status[name] = BackendStatus(phase="Pending", tasks={}, oom={})
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

    def oom_kill(self, name: str, worker: str, count: int = 1) -> None:
        """Count pods of a worker killed for memory."""
        with self._lock:
            oom = self._status[name].oom
            oom[worker] = oom.get(worker, 0) + count

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

    def logs(self, name: str, task_id: str) -> str:
        """The logs set in `logs_text`, empty by default."""
        return self.logs_text.get((name, task_id), "")

    def nodes(self) -> list[Node]:
        """The nodes, with their reservations."""
        with self._lock:
            return list(self._nodes.values())

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
