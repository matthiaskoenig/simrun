"""The backend the service runs workflows on: Argo Workflows, or a fake in tests."""

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

from simrun.planner import Node

# phases of an Argo workflow or node which do not change any more
FINAL_PHASES = frozenset({"Succeeded", "Failed", "Error"})


class BackendStatus(BaseModel):
    """The state of a workflow in the backend.

    The service, not the backend, decides the state of a run from it (see
    `simrun.service.controller`).

    Attributes:
        phase: Phase of the workflow, e.g. `Pending`, `Running`, `Succeeded`.
        tasks: Phase of every node of the DAG by its name: `<task>-w0`,
            `<task>-extra(<i>:<item>)` and `<task>-merge` of a split task, `<task>` of
            an unsplit one; a node not yet created is missing.
        oom: Pods killed for memory per worker id (`<task>-w<i>`), over all attempts.
    """

    phase: str
    tasks: dict[str, str]
    oom: dict[str, int]


class Backend(Protocol):
    """What the service needs of a cluster."""

    def submit(self, manifest: dict[str, Any]) -> str:
        """Submit a workflow manifest and return the name of the workflow."""
        ...

    def status(self, name: str) -> BackendStatus:
        """The status of a workflow.

        Raises:
            KeyError: The workflow does not exist (any more).
        """
        ...

    def cancel(self, name: str) -> None:
        """Terminate a workflow and its pods."""
        ...

    def stop(self, name: str) -> None:
        """Stop a workflow whose run is decided: pods still pending are removed."""
        ...

    def logs(self, name: str, task_id: str) -> str:
        """The logs of the pods of a task of a workflow."""
        ...

    def nodes(self) -> list[Node]:
        """The nodes of the cluster."""
        ...

    def reserve(self, node: str, run_id: str) -> None:
        """Reserve a node for a run: taint `simrun/reserved=<run id>` and label."""
        ...

    def unreserve(self, node: str) -> None:
        """Remove the taint and the label of a reservation."""
        ...

    def build_image(self, context: Path, ref: str) -> None:
        """Build the image of a build context and push it as `ref`."""
        ...

    def has_image(self, ref: str) -> bool:
        """Whether the registry has the image `ref`."""
        ...
