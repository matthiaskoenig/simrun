"""The workflow model: tasks, the references between them and their order."""

from enum import StrEnum
from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator


class SplitMode(StrEnum):
    """How the pieces of a task are placed on the nodes."""

    FASTEST = "fastest"
    SINGLE_SERVER = "single-server"


class Priority(StrEnum):
    """The priority of a run in the queue."""

    OWNER = "owner"
    STANDARD = "standard"
    AGENT = "agent"


class WorkflowError(ValueError):
    """A workflow which is not valid."""


class ResourceOverride(BaseModel):
    """Resources of a task which replace the learned or default ones."""

    workers: int | None = None
    memory_per_worker_gb: float | None = None
    scratch_gb: float | None = None
    gpus: int | None = None


class TaskSpec(BaseModel):
    """A task of a workflow.

    Attributes:
        id: Identifier of the task within the workflow.
        type: Name of the task plugin.
        settings: Settings handed to the plugin.
        inputs: Input port to the output of another task, `<task>.<port>`.
        split: Split mode of the task, the one of the workflow by default.
        resources: Resources replacing the learned or default ones.
        stage: Input ports copied to local scratch.
    """

    id: str = Field(pattern=r"^[a-z][a-z0-9-]*$")
    type: str
    settings: dict[str, Any] = Field(default_factory=dict)
    inputs: dict[str, str] = Field(default_factory=dict)
    split: SplitMode | None = None
    resources: ResourceOverride | None = None
    stage: list[str] = Field(default_factory=list)

    def dependencies(self) -> list[str]:
        """Ids of the tasks this task reads from, in input order, without repeats."""
        return list(dict.fromkeys(ref.split(".", 1)[0] for ref in self.inputs.values()))


class Workflow(BaseModel):
    """A workflow: tasks which read the outputs of each other."""

    name: str
    tasks: list[TaskSpec]
    split: SplitMode = SplitMode.FASTEST
    reserve: str | None = None

    def __init__(self, **data: Any) -> None:
        """Create a workflow, a reference error is raised as a `WorkflowError`."""
        try:
            super().__init__(**data)
        except ValidationError as error:
            for detail in error.errors():
                cause = detail.get("ctx", {}).get("error")
                if isinstance(cause, WorkflowError):
                    raise cause from error
            raise

    @model_validator(mode="after")
    def _validate(self) -> Self:
        ids = [t.id for t in self.tasks]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise WorkflowError(f"duplicate task ids: {', '.join(duplicates)}")
        for task in self.tasks:
            for port, ref in task.inputs.items():
                source, dot, output = ref.partition(".")
                if not dot or not output:
                    raise WorkflowError(
                        f"input {port!r} of task {task.id!r} must be "
                        f"'<task>.<port>', got {ref!r}"
                    )
                if source not in ids:
                    raise WorkflowError(
                        f"input {port!r} of task {task.id!r} refers to "
                        f"unknown task {source!r}"
                    )
        self.order()
        return self

    def task(self, task_id: str) -> TaskSpec:
        """Return the task with the given id.

        Raises:
            WorkflowError: If there is no such task.
        """
        for task in self.tasks:
            if task.id == task_id:
                return task
        raise WorkflowError(f"unknown task {task_id!r}")

    def order(self) -> list[TaskSpec]:
        """Return the tasks in topological order, stable in file order.

        Raises:
            WorkflowError: If the references form a cycle.
        """
        remaining = {t.id: set(t.dependencies()) for t in self.tasks}
        done: set[str] = set()
        ordered: list[TaskSpec] = []
        while remaining:
            ready = next(
                (
                    t
                    for t in self.tasks
                    if t.id in remaining and remaining[t.id] <= done
                ),
                None,
            )
            if ready is None:
                raise WorkflowError(
                    f"cycle among tasks: {', '.join(sorted(remaining))}"
                )
            ordered.append(ready)
            done.add(ready.id)
            del remaining[ready.id]
        return ordered


def load_workflow(path: Path) -> Workflow:
    """Read a workflow from a YAML file.

    Raises:
        WorkflowError: If the file is not a valid workflow.
    """
    try:
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict):
            raise WorkflowError(f"{path}: not a mapping")
        return Workflow(**data)
    except (ValidationError, yaml.YAMLError, UnicodeDecodeError, TypeError) as error:
        raise WorkflowError(f"{path}: {error}") from error


def dump_workflow(workflow: Workflow, path: Path) -> None:
    """Write a workflow as a YAML file."""
    data = workflow.model_dump(mode="json")
    path.write_text(yaml.safe_dump(data, sort_keys=False))


class _IndentedDumper(yaml.SafeDumper):
    """The safe dumper which indents a sequence below its key, as `- id:` below `tasks:`."""

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        """Indent every block, also a sequence which PyYAML would leave indentless."""
        super().increase_indent(flow, False)


def workflow_yaml(workflow: Workflow) -> str:
    """The compact YAML of a workflow: the fields which differ from the defaults.

    `name` and `tasks` are always written, also when they are the defaults. A
    sequence is indented below its key, as the workflows of the docs are written.

    Args:
        workflow: The workflow.

    Returns:
        The YAML text.
    """
    data = workflow.model_dump(mode="json", exclude_defaults=True)
    data = {"name": workflow.name, "tasks": data.get("tasks", [])} | data
    return yaml.dump(data, Dumper=_IndentedDumper, sort_keys=False)
