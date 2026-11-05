"""The parts of a pod which every backend shares: image, shapes, command, environment.

`simrun.argo` and `simrun.cwl` run the same commands with the same environment; they
differ in where the run store and the scratch directory are (a mount, a path) and in
how a worker is named.
"""

import math
import shlex

from simrun.planner import PodShape
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND, CommandSettings
from simrun.store import RunRecord
from simrun.worker import SETTINGS_FILE
from simrun.workflow import TaskSpec

WORKER_COMMAND = "simrun-worker"
MERGE_COMMAND = "simrun-merge"
# seconds beyond the grace after which the coordinator takes a worker as gone
STALE_MARGIN_S = 60


def number(value: float) -> str:
    """A number of GiB for the environment, e.g. `0.5`."""
    return str(float(value))


def task_dir(record: RunRecord, task_id: str, store: str) -> str:
    """The directory of a task below the store root `store`."""
    return f"{store}/runs/{record.id}/tasks/{task_id}"


def output(record: RunRecord, ref: str) -> str:
    """The output a reference `<task>.<port>` names, relative to the store."""
    task_id, _, port = ref.partition(".")
    return f"runs/{record.id}/tasks/{task_id}/out/{port}"


def env_values(
    record: RunRecord,
    task: TaskSpec,
    task_type: TaskType,
    shape: PodShape,
    store: str,
    scratch: str,
) -> dict[str, str]:
    """The environment of the worker harness, see `simrun.worker`.

    Args:
        record: The run.
        task: The task.
        task_type: Its type.
        shape: The pod shape.
        store: The root of the store as the pod sees it.
        scratch: The scratch directory of the pod.

    Returns:
        The variables by name; the caller adds the ones of its backend.

    Raises:
        ValueError: The task stages a port which is no input.
    """
    inputs = {port: output(record, ref) for port, ref in task.inputs.items()}
    for port in task.stage:
        if port not in inputs:
            raise ValueError(f"task {task.id!r} stages {port!r}, which is no input")
    return {
        "SIMRUN_TASK_DIR": task_dir(record, task.id, store),
        "SIMRUN_TASK_TYPE": task_type.name,
        "SIMRUN_MEMORY_BASE_GB": number(task_type.hint.memory_base_gb),
        "SIMRUN_MEMORY_PER_WORKER_GB": number(shape.per_worker_gb),
        "SIMRUN_INPUTS": ",".join(f"{port}={path}" for port, path in inputs.items()),
        "SIMRUN_STAGE": ",".join(
            f"{inputs[port]}={scratch}/{port}" for port in task.stage
        ),
        "SIMRUN_EDITABLE": ",".join(f"{store}/{s.snapshot}" for s in record.sources),
        "SIMRUN_SCRATCH": scratch,
    }


def image(record: RunRecord, task: TaskSpec, task_type: TaskType) -> str:
    """The image of a task: the one of its type, else the one of the run.

    Raises:
        ValueError: The type has no image and the run has none.
    """
    if task_type.image is not None:
        return task_type.image
    if record.image is None:
        raise ValueError(
            f"task {task.id!r} of type {task_type.name!r} needs the image of the run, "
            "and the run has none"
        )
    return record.image


def merge_shape(shape: PodShape, base_gb: float) -> PodShape:
    """The merge: one core and the memory of one worker process besides the base."""
    return PodShape(
        cores=1,
        memory_gb=math.ceil(base_gb + shape.per_worker_gb),
        processes=1,
        per_worker_gb=shape.per_worker_gb,
        retry_memory_gb=math.ceil(base_gb + shape.retry_per_worker_gb),
        retry_per_worker_gb=shape.retry_per_worker_gb,
        scratch_gb=0.0,
        gpus=0,
    )


def unsplit_command(
    record: RunRecord, task: TaskSpec, task_type: TaskType, store: str
) -> tuple[str, list[str]]:
    """The image and argv of an unsplit task."""
    if task_type.name == COMMAND.name:
        settings = CommandSettings.model_validate(task.settings)
        return settings.image, settings.args
    if task_type.command is None:
        raise ValueError(
            f"task {task.id!r}: type {task_type.name!r} is neither splittable "
            "nor has a command"
        )
    directory = task_dir(record, task.id, store)
    argv = [
        part.replace("{settings}", f"{directory}/{SETTINGS_FILE}").replace(
            "{out}", f"{directory}/out"
        )
        for part in shlex.split(task_type.command)
    ]
    return image(record, task, task_type), argv
