"""The plugin interface: task types, their resource hints and their discovery."""

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any

from pydantic import BaseModel

ENTRY_POINT_GROUP = "simrun.task_types"


@dataclass(frozen=True)
class ResourceHint:
    """Default resources of a task type.

    Attributes:
        memory_base_gb: Base memory of a task in GiB.
        memory_per_worker_gb: Memory per worker in GiB.
        scratch_gb: Local scratch in GiB.
        gpus: Number of GPUs.
    """

    memory_base_gb: float = 2.0
    memory_per_worker_gb: float = 6.0
    scratch_gb: float = 0.0
    gpus: int = 0


@dataclass(frozen=True)
class TaskPaths:
    """The locations a task works in.

    Attributes:
        store: Root of the run store.
        run_dir: Directory of the run.
        task_dir: Directory of the task.
        out: Output directory of the task, `task_dir / "out"`.
        inputs: Input port to the directory of the output it reads.
        scratch: Local scratch directory.
    """

    store: Path
    run_dir: Path
    task_dir: Path
    out: Path
    inputs: dict[str, Path]
    scratch: Path


@dataclass(frozen=True)
class TaskType:
    """A type of task, registered as an entry point of `ENTRY_POINT_GROUP`.

    The callables are `"module:function"` strings, so a worker process resolves
    them by import, see `resolve`. Signatures: `count_units(settings) -> int`,
    `setup(settings, paths) -> None` (once per process),
    `compute_unit(settings, index, paths) -> None`, `merge(settings, paths) -> None`
    and `plan(settings) -> settings`, which the client calls before a submission
    when the type is installed locally (`simrun.client.plan_tasks`).

    A unit may be computed more than once: by a worker which is evicted before it
    completes the unit, or by a restarted incarnation of a worker whose earlier one
    still runs. So `compute_unit` writes outputs which depend on the settings and the
    index only (deterministic), each written atomically (a temporary file in the same
    directory, then renamed), so a reader never sees a partial unit.

    Attributes:
        name: Name of the type, as used in a workflow.
        settings: Pydantic model of the settings.
        inputs: Names of the input ports.
        outputs: Names of the output ports.
        hint: Default resources.
        checkpoints: Whether the task resumes from checkpoints.
        command: Argv template of unsplit tasks with `{settings}`, `{out}` and
            `{input}`: a part with `{input}` is repeated once per input port of the
            task, sorted by port, with `<port>=<path>`, and dropped without inputs.
            It runs through `simrun-exec` (see `simrun.worker.exec_main`).
        uses_cores: Whether the command of an unsplit task uses the cores of its pod
            (a pool of processes); else the pod is sized for one process.
        count_units: Reference to the function counting the units of work.
        setup: Reference to the function preparing a process.
        compute_unit: Reference to the function computing one unit.
        merge: Reference to the function merging the units.
        plan: Reference to the function which completes the settings of a task
            before it is submitted, e.g. with values derived from its model.
        image: Image of the task, None for the environment image of the run.
        cost_setting: Name of the setting which changes the cost of a task, e.g. the
            subset of the data of a fit; the resources are learned per value of it
            (`simrun.resources.resource_key`).
    """

    name: str
    settings: type[BaseModel]
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ("out",)
    hint: ResourceHint = ResourceHint()
    checkpoints: bool = False
    command: str | None = None
    uses_cores: bool = False
    count_units: str | None = None
    setup: str | None = None
    compute_unit: str | None = None
    merge: str | None = None
    plan: str | None = None
    image: str | None = None
    cost_setting: str | None = None

    @property
    def splittable(self) -> bool:
        """Whether the task can be split into pieces."""
        return None not in (self.count_units, self.compute_unit, self.merge)


def resolve(ref: str) -> Callable[..., Any]:
    """Import the function a `"module:function"` string names.

    Args:
        ref: Reference of the form `module:function`.

    Returns:
        The function.
    """
    module, _, name = ref.partition(":")
    return getattr(import_module(module), name)


def load_task_types() -> dict[str, TaskType]:
    """Load the task types of all entry points.

    Returns:
        The task types by name.

    Raises:
        ValueError: A name is registered twice.
    """
    types: dict[str, TaskType] = {}
    for entry in entry_points(group=ENTRY_POINT_GROUP):
        task_type = entry.load()
        if task_type.name in types:
            raise ValueError(f"duplicate task type {task_type.name!r}")
        types[task_type.name] = task_type
    return types
