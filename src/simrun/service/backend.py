"""The backend the service runs workflows on: Argo Workflows, or a fake in tests."""

from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, Field

from simrun.planner import Node

# phases of an Argo workflow or node which do not change any more
FINAL_PHASES = frozenset({"Succeeded", "Failed", "Error"})
# the phase of a task node whose pod was evicted (or lost) and waits for its retry
PREEMPTED_PHASE = "Preempted"
# the phase of a task node whose pod failed of itself (no kill for memory, no
# eviction) and waits for its retry
RETRYING_PHASE = "Retrying"


class FailedAttempt(BaseModel):
    """An attempt of a pod which failed.

    Attributes:
        attempt: The number of the attempt, from 0, as the pod has it in
            `SIMRUN_ATTEMPT` (Argo's `{{retries}}`) and in the name of its report.
        exit_code: The exit code of its main container, None without one (e.g. a
            pod deleted by an eviction of Kueue).
        oom_killed: Whether the main container was killed by the out of memory
            killer (the reason `OOMKilled`); an exit 137 alone is no evidence of it.
    """

    attempt: int
    exit_code: int | None = None
    oom_killed: bool = False


class PodLog(BaseModel):
    """The log of the main container of one pod (one attempt) of a task.

    Attributes:
        node: The node of the pod in the workflow, e.g. `squares-w0(1)`.
        pod: The name of the pod.
        text: The log.
    """

    node: str
    pod: str
    text: str


class BackendStatus(BaseModel):
    """The state of a workflow in the backend.

    The service, not the backend, decides the state of a run from it (see
    `simrun.service.controller`).

    Attributes:
        phase: Phase of the workflow, e.g. `Pending`, `Running`, `Succeeded`.
        tasks: Phase of every node of the DAG by its name: `<task>-w0`,
            `<task>-extra(<i>:<item>)` and `<task>-merge` of a split task, `<task>` of
            an unsplit one; a node not yet created is missing. A node whose pod was
            evicted and whose retry waits is `Preempted` (`PREEMPTED_PHASE`), one
            whose pod failed otherwise `Retrying` (`RETRYING_PHASE`).
        failed: The attempts of pods which failed, in the order of their number: per
            worker id (`<task>-w<i>`), per task id of an unsplit task and per
            `<task>-merge` of a merge.
    """

    phase: str
    tasks: dict[str, str]
    failed: dict[str, list[FailedAttempt]] = Field(default_factory=dict)


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

    def logs(
        self, name: str, task_id: str, tail_lines: int | None = None
    ) -> list[PodLog]:
        """The logs of the pods of a task of a workflow, oldest first.

        Every attempt whose pod still exists; `tail_lines` limits each log to its
        last lines.
        """
        ...

    def nodes(self) -> list[Node]:
        """The nodes of the cluster."""
        ...

    def reserve(self, node: str, run_id: str) -> None:
        """Reserve a node for a run.

        The node gets the taint `simrun/reserved=<run id>:NoExecute` and the label
        `simrun/reserved=<run id>`.
        """
        ...

    def unreserve(self, node: str, run_id: str) -> None:
        """Remove the taint and the label of the reservation of a run.

        A node reserved by another run (or by none) is left as it is, so a late
        release never removes the reservation of a later run.

        Raises:
            KeyError: The node does not exist.
        """
        ...

    def build_image(self, context: Path, ref: str) -> None:
        """Build the image of a build context and push it as `ref`."""
        ...

    def has_image(self, ref: str) -> bool:
        """Whether the registry has the image `ref`."""
        ...
