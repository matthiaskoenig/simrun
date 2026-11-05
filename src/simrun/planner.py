"""The planner: the shape of a worker pod and the number of pieces of a task."""

import logging
import math
from collections import Counter
from dataclasses import dataclass

from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND, CommandSettings
from simrun.resources import GIB, RETRY_FACTOR, ResolvedResources
from simrun.worker import processes
from simrun.workflow import Priority, SplitMode, TaskSpec

logger = logging.getLogger(__name__)

MAX_PIECES = 8
PRIORITY_VALUES = {
    "simrun-owner-base": 1000,
    "simrun-owner-extra": 900,
    "simrun-standard-base": 500,
    "simrun-standard-extra": 400,
    "simrun-agent-base": 200,
    "simrun-agent-extra": 100,
}


class PlanError(ValueError):
    """A task which cannot be planned on the nodes."""


@dataclass(frozen=True)
class Node:
    """A node of the cluster.

    Attributes:
        name: Name of the node.
        cores: Allocatable cores.
        memory_gb: Allocatable memory in GiB.
        gpus: Number of GPUs.
        reserved_by: Name of the reservation holding the node, None if free.
    """

    name: str
    cores: int
    memory_gb: float
    gpus: int = 0
    reserved_by: str | None = None


@dataclass(frozen=True)
class PodShape:
    """The resources of a worker pod.

    Attributes:
        cores: Number of cores.
        memory_gb: Memory in GiB, rounded up.
        processes: Number of worker processes.
        per_worker_gb: Memory per worker process in GiB.
        retry_memory_gb: Memory of a retry in GiB.
        retry_per_worker_gb: Memory per worker process of a retry in GiB.
        scratch_gb: Local scratch in GiB.
        gpus: Number of GPUs.
    """

    cores: int
    memory_gb: int
    processes: int
    per_worker_gb: float
    retry_memory_gb: int
    retry_per_worker_gb: float
    scratch_gb: float
    gpus: int


@dataclass(frozen=True)
class TaskPlan:
    """The plan of a task.

    Attributes:
        task_id: Id of the task.
        pods: Number of worker pods.
        shape: Shape of a pod.
        base_priority: Priority class of the first pods.
        extra_priority: Priority class of the further pods.
        units: Number of units, None for an unsplit task.
    """

    task_id: str
    pods: int
    shape: PodShape
    base_priority: str
    extra_priority: str
    units: int | None


def _usable(nodes: list[Node], gpus: int) -> list[Node]:
    return [n for n in nodes if n.reserved_by is None and n.gpus >= gpus]


def _common_node(nodes: list[Node], gpus: int) -> Node:
    """Returns the most common node by cores and memory, the larger on ties."""
    usable = _usable(nodes, gpus)
    if not usable:
        raise PlanError(f"no node which is not reserved has {gpus} GPUs")
    counts = Counter((n.cores, n.memory_gb) for n in usable)
    cores, memory = max(counts, key=lambda k: (counts[k], k[1], k[0]))
    return next(n for n in usable if (n.cores, n.memory_gb) == (cores, memory))


def shape(resources: ResolvedResources, nodes: list[Node]) -> PodShape:
    """Sizes a pod for the most common node which is not reserved.

    Args:
        resources: The resources of the task.
        nodes: The nodes of the cluster.

    Returns:
        The shape of a pod.

    Raises:
        PlanError: One worker fits on no node.
    """
    node = _common_node(nodes, resources.gpus)
    base = resources.memory_base_gb
    per_worker = resources.memory_per_worker_gb
    try:
        n = processes(node.cores, int(node.memory_gb * GIB), base, per_worker)
    except ValueError:
        usable = _usable(nodes, resources.gpus)
        largest = max(usable, key=lambda x: x.memory_gb)
        raise PlanError(
            f"one worker needs {per_worker} GiB besides {base} GiB base, more than "
            f"the largest node {largest.name} with {largest.memory_gb} GiB"
        ) from None
    retry_per_worker = per_worker * RETRY_FACTOR
    try:
        retry_n = processes(
            node.cores, int(node.memory_gb * GIB), base, retry_per_worker
        )
        retry_memory = math.ceil(base + retry_per_worker * retry_n)
    except ValueError:
        logger.warning(
            "a retry with %s GiB per worker fits on no node", retry_per_worker
        )
        retry_per_worker = per_worker
        retry_memory = math.ceil(base + per_worker * n)
    return PodShape(
        cores=n,
        memory_gb=math.ceil(base + per_worker * n),
        processes=n,
        per_worker_gb=per_worker,
        retry_memory_gb=retry_memory,
        retry_per_worker_gb=retry_per_worker,
        scratch_gb=resources.scratch_gb,
        gpus=resources.gpus,
    )


def _command_shape(settings: CommandSettings, nodes: list[Node]) -> PodShape:
    memory = math.ceil(settings.memory_gb)
    usable = _usable(nodes, settings.gpus)
    if not usable:
        raise PlanError(f"no node which is not reserved has {settings.gpus} GPUs")
    node_memory = max(n.memory_gb for n in usable)
    retry = max(
        memory, min(math.ceil(settings.memory_gb * RETRY_FACTOR), int(node_memory))
    )
    return PodShape(
        cores=settings.cores,
        memory_gb=memory,
        processes=1,
        per_worker_gb=settings.memory_gb,
        retry_memory_gb=retry,
        retry_per_worker_gb=float(retry),
        scratch_gb=settings.scratch_gb,
        gpus=settings.gpus,
    )


def _slots(pod: PodShape, nodes: list[Node]) -> int:
    total = 0
    for n in _usable(nodes, pod.gpus):
        fit = min(int(n.cores // pod.cores), int(n.memory_gb // pod.memory_gb))
        if pod.gpus:
            fit = min(fit, n.gpus // pod.gpus)
        total += max(fit, 0)
    return total


def plan_task(
    task: TaskSpec,
    task_type: TaskType,
    resources: ResolvedResources,
    nodes: list[Node],
    mode: SplitMode,
    priority: Priority,
    units: int | None,
) -> TaskPlan:
    """Plans the pods of a task.

    Args:
        task: The task.
        task_type: Its type.
        resources: The resolved resources.
        nodes: The nodes of the cluster.
        mode: The split mode.
        priority: The priority of the run.
        units: The number of units, None for an unsplit task.

    Returns:
        The plan; extra pods are not limited by idle capacity, Kueue queues them.

    Raises:
        PlanError: The task fits on no node.
    """
    base = f"simrun-{priority.value}-base"
    extra = f"simrun-{priority.value}-extra"
    if not task_type.splittable:
        if task_type.name == COMMAND.name:
            pod = _command_shape(CommandSettings.model_validate(task.settings), nodes)
        else:
            pod = shape(resources, nodes)
        return TaskPlan(task.id, 1, pod, base, extra, None)
    pod = shape(resources, nodes)
    pods = 1
    if mode == SplitMode.FASTEST and units is not None:
        pods = min(MAX_PIECES, units, _slots(pod, nodes))
    return TaskPlan(task.id, max(1, pods), pod, base, extra, units)
