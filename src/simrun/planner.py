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


@dataclass(frozen=True, init=False)
class Node:
    """A node of the cluster.

    Attributes:
        name: Name of the node.
        cores: The whole cores a worker pod of a run can have on the node, what the
            planner plans with.
        memory_gb: The GiB of memory a worker pod of a run can have on the node,
            what the planner plans with.
        gpus: Number of GPUs.
        reserved_by: Name of the reservation holding the node, None if free.
        allocatable_cores: The cores the scheduler gives the pods of the runs
            (sidecars included), as it counts their requests.
        allocatable_memory_gb: The GiB of memory the scheduler gives the pods of
            the runs, as it counts their requests.
    """

    name: str
    cores: int
    memory_gb: float
    gpus: int
    reserved_by: str | None
    allocatable_cores: float
    allocatable_memory_gb: float

    def __init__(
        self,
        name: str,
        cores: int,
        memory_gb: float,
        gpus: int = 0,
        reserved_by: str | None = None,
        allocatable_cores: float | None = None,
        allocatable_memory_gb: float | None = None,
    ) -> None:
        """Create the node; the allocatable cores and memory default to the others."""
        values = {
            "name": name,
            "cores": cores,
            "memory_gb": memory_gb,
            "gpus": gpus,
            "reserved_by": reserved_by,
            "allocatable_cores": float(
                cores if allocatable_cores is None else allocatable_cores
            ),
            "allocatable_memory_gb": float(
                memory_gb if allocatable_memory_gb is None else allocatable_memory_gb
            ),
        }
        for key, value in values.items():
            # a frozen dataclass sets its fields past its own __setattr__
            object.__setattr__(self, key, value)


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


def _largest(nodes: list[Node], gpus: int) -> Node:
    usable = _usable(nodes, gpus)
    if not usable:
        raise PlanError(f"no node which is not reserved has {gpus} GPUs")
    return max(usable, key=lambda n: (n.memory_gb, n.cores))


def _fit(node: Node, base: float, per_worker: float) -> int | None:
    """Returns the number of processes on the node, None if one does not fit."""
    try:
        return processes(node.cores, int(node.memory_gb * GIB), base, per_worker)
    except ValueError:
        return None


def shape(
    resources: ResolvedResources, nodes: list[Node], max_processes: int | None = None
) -> PodShape:
    """Sizes a pod for the most common node which is not reserved.

    When one worker does not fit that node, the largest usable node is taken.

    Args:
        resources: The resources of the task.
        nodes: The nodes of the cluster.
        max_processes: At most this many processes, as many as fit by default.

    Returns:
        The shape of a pod.

    Raises:
        PlanError: One worker fits on no node.
    """
    base = resources.memory_base_gb
    per_worker = resources.memory_per_worker_gb
    node = _common_node(nodes, resources.gpus)
    n = _fit(node, base, per_worker)
    if n is None:
        node = _largest(nodes, resources.gpus)
        n = _fit(node, base, per_worker)
    if n is None:
        raise PlanError(
            f"one worker needs {per_worker} GiB besides {base} GiB base, more than "
            f"the largest node {node.name} with {node.memory_gb} GiB"
        )
    if max_processes is not None:
        n = min(n, max_processes)
    retry_per_worker = per_worker * RETRY_FACTOR
    retry_node = node
    retry_n = _fit(retry_node, base, retry_per_worker)
    if retry_n is None:
        retry_node = _largest(nodes, resources.gpus)
        retry_n = _fit(retry_node, base, retry_per_worker)
    if retry_n is not None:
        retry_n = min(retry_n, n)
        retry_memory = math.ceil(base + retry_per_worker * retry_n)
    else:
        logger.warning(
            "a retry with %s GiB per worker fits on no node, "
            "it takes the whole memory of %s with one process",
            retry_per_worker,
            retry_node.name,
        )
        retry_memory = int(retry_node.memory_gb)
        retry_per_worker = max(0.0, retry_memory - base)
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
    fitting = [
        n
        for n in _usable(nodes, settings.gpus)
        if n.cores >= settings.cores and n.memory_gb >= memory
    ]
    if not fitting:
        largest = _largest(nodes, settings.gpus)
        raise PlanError(
            f"the command needs {settings.cores} cores, {memory} GiB and "
            f"{settings.gpus} GPUs, more than the largest node {largest.name} "
            f"with {largest.cores} cores and {largest.memory_gb} GiB"
        )
    node_memory = max(n.memory_gb for n in fitting)
    retry = max(
        memory, min(math.ceil(settings.memory_gb * RETRY_FACTOR), int(node_memory))
    )
    return PodShape(
        cores=settings.cores,
        memory_gb=memory,
        processes=1,
        per_worker_gb=settings.memory_gb,
        retry_memory_gb=retry,
        # processes is 1, so the memory per worker of a retry is its memory
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
            # a single command is one process, unless it uses the cores of its pod
            pod = shape(resources, nodes, None if task_type.uses_cores else 1)
        return TaskPlan(task.id, 1, pod, base, extra, None)
    pod = shape(resources, nodes)
    pods = 1
    if mode == SplitMode.FASTEST and units is not None:
        pods = min(MAX_PIECES, units, _slots(pod, nodes))
    return TaskPlan(task.id, max(1, pods), pod, base, extra, units)
