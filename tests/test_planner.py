import math

import pytest
from pydantic import BaseModel

from simrun.planner import (
    MAX_PIECES,
    Node,
    PlanError,
    plan_task,
    shape,
)
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND
from simrun.resources import ResolvedResources
from simrun.workflow import Priority, SplitMode, TaskSpec


class _S(BaseModel):
    pass


def _type(command: str | None = None) -> TaskType:
    return TaskType(
        name="t",
        settings=_S,
        command=command,
        count_units=None if command else "m:count",
        compute_unit=None if command else "m:unit",
        merge=None if command else "m:merge",
    )


def _res(per_worker: float = 5.1, gpus: int = 0) -> ResolvedResources:
    return ResolvedResources(2.0, per_worker, 0.0, gpus, None, 0, "default")


NODES = [Node("srv-01", 104, 1000)] + [
    Node(f"srv-{i:02d}", 40, 188) for i in range(2, 13)
]


def _spec() -> TaskSpec:
    return TaskSpec(id="a", type="t")


def _plan(mode=SplitMode.FASTEST, units=400, priority=Priority.OWNER, nodes=NODES):
    return plan_task(_spec(), _type(), _res(), nodes, mode, priority, units)


def test_shape_memory_bound():
    s = shape(_res(), NODES)
    assert s.processes == 36
    assert s.cores == 36
    assert s.memory_gb == 186
    assert s.retry_per_worker_gb == pytest.approx(7.65)
    assert s.retry_memory_gb == 186


def test_fastest_pieces():
    assert _plan().pods == MAX_PIECES


def test_single_server():
    assert _plan(mode=SplitMode.SINGLE_SERVER).pods == 1


def test_priorities():
    p = _plan()
    assert (p.base_priority, p.extra_priority) == (
        "simrun-owner-base",
        "simrun-owner-extra",
    )
    assert _plan(priority=Priority.AGENT).base_priority == "simrun-agent-base"


def test_reserved_nodes_excluded():
    nodes = [Node("srv-01", 104, 1000)] + [
        Node(f"srv-{i:02d}", 40, 188, reserved_by="x") for i in range(2, 13)
    ]
    s = shape(_res(), nodes)
    assert s.processes == 104
    assert s.memory_gb == 533


def test_too_large_raises():
    with pytest.raises(PlanError) as e:
        shape(_res(2000), NODES)
    assert "2000" in str(e.value)
    assert "srv-01" in str(e.value)


def test_gpus_only_nodes_with_gpus():
    nodes = [Node("a", 40, 188), Node("g", 16, 128, gpus=2)]
    s = shape(_res(gpus=1), nodes)
    assert s.gpus == 1
    assert s.cores == 16


def test_slots_limit_pieces():
    nodes = [Node("a", 40, 188), Node("b", 40, 188)]
    assert _plan(nodes=nodes).pods == 2


def test_command_unsplit():
    spec = TaskSpec(
        id="c",
        type="command",
        settings={
            "image": "i",
            "args": ["x"],
            "cores": 4,
            "memory_gb": 10.5,
            "gpus": 1,
        },
    )
    nodes = [Node("g", 16, 12, gpus=1)]
    p = plan_task(
        spec, COMMAND, _res(), nodes, SplitMode.FASTEST, Priority.STANDARD, None
    )
    assert p.pods == 1
    assert p.units is None
    assert (p.shape.cores, p.shape.memory_gb, p.shape.gpus) == (4, 11, 1)
    assert p.shape.retry_memory_gb == 12


def test_larger_node_when_common_does_not_fit():
    s = shape(_res(200), NODES)
    assert s.processes == 4
    assert s.memory_gb == 802


def test_retry_on_larger_node():
    # 130 GiB fits 188; 1.5x = 195 does not, but fits srv-01 (1000)
    s = shape(_res(130), NODES)
    assert s.processes == 1
    assert s.retry_per_worker_gb == pytest.approx(195)
    assert s.retry_memory_gb == 197  # capped at the processes of the shape


def test_retry_fits_nowhere_takes_whole_node(caplog):
    s = shape(_res(800), NODES)
    assert s.processes == 1
    assert s.retry_memory_gb == 1000
    assert "fits on no node" in caplog.text


def _command(**kw):
    settings = {"image": "i", "args": ["x"], "cores": 4, "memory_gb": 10} | kw
    spec = TaskSpec(id="c", type="command", settings=settings)
    return plan_task(
        spec, COMMAND, _res(), NODES, SplitMode.FASTEST, Priority.STANDARD, None
    )


@pytest.mark.parametrize("kw", [{"cores": 500}, {"memory_gb": 2000}, {"gpus": 1}])
def test_command_too_large(kw):
    with pytest.raises(PlanError) as e:
        _command(**kw)
    assert "srv-01" in str(e.value) or "GPUs" in str(e.value)


def test_unsplit_plugin_task_is_one_process():
    # without records a single command takes one core and the memory of one process,
    # not the whole server
    p = plan_task(
        TaskSpec(id="r", type="t"),
        _type("report {settings}"),
        _res(),
        NODES,
        SplitMode.FASTEST,
        Priority.OWNER,
        None,
    )
    assert (p.pods, p.units) == (1, None)
    assert (p.shape.cores, p.shape.processes, p.shape.memory_gb) == (1, 1, 8)
    assert p.shape.retry_memory_gb == 10
    assert p.shape.retry_per_worker_gb == pytest.approx(7.65)


def test_unsplit_plugin_task_which_uses_the_cores_of_its_pod():
    task_type = TaskType(
        name="t", settings=_S, command="fit {settings}", uses_cores=True
    )
    p = plan_task(
        TaskSpec(id="r", type="t"),
        task_type,
        _res(),
        NODES,
        SplitMode.FASTEST,
        Priority.OWNER,
        None,
    )
    assert (p.shape.cores, p.shape.processes) == (36, 36)


def _learned(seconds_per_unit: float) -> ResolvedResources:
    return ResolvedResources(2.0, 5.1, 0.0, 0, seconds_per_unit, 3, "learned")


def _plan_learned(seconds_per_unit, units=400, **kw):
    return plan_task(
        _spec(),
        _type(),
        _learned(seconds_per_unit),
        NODES,
        SplitMode.FASTEST,
        Priority.OWNER,
        units,
        **kw,
    )


@pytest.mark.parametrize(
    ("seconds_per_unit", "pods"),
    # 400 units on 36 processes a pod: 11.1 s of work a pod per second of a unit
    [(0.1, 1), (0.5, 1), (1.0, 1), (2.0, 2), (3.0, 3), (10.0, MAX_PIECES)],
)
def test_fastest_adds_a_pod_only_for_work_longer_than_its_start(seconds_per_unit, pods):
    # every pod gets at least POD_START_S of work, else a pod would start when
    # the others are nearly done and only hold cores
    assert _plan_learned(seconds_per_unit).pods == pods


def test_fastest_pod_start_of_the_cluster():
    assert _plan_learned(1.0, pod_start_s=2.0).pods == 5
    assert _plan_learned(1.0, pod_start_s=20.0).pods == 1


def test_fastest_without_records_has_no_pod_without_units():
    # 40 units fill two pods of 36 processes, a third pod would get none
    assert _plan(units=40).pods == 2
    assert _plan(units=36).pods == 1
    assert _plan(units=1).pods == 1


def test_single_server_ignores_the_work():
    plan = _plan_learned(10.0, units=400)
    assert plan.pods == MAX_PIECES
    single = plan_task(
        _spec(),
        _type(),
        _learned(10.0),
        NODES,
        SplitMode.SINGLE_SERVER,
        Priority.OWNER,
        400,
    )
    assert single.pods == 1


def test_a_pod_has_no_more_processes_than_units():
    # 3 units on a server of 36 processes: a pod of 3 processes and their cores,
    # the other cores stay free for other tasks
    plan = _plan(units=3)
    assert (plan.pods, plan.shape.processes, plan.shape.cores) == (1, 3, 3)
    assert plan.shape.memory_gb == math.ceil(2.0 + 5.1 * 3)
    single = _plan(mode=SplitMode.SINGLE_SERVER, units=3)
    assert (single.shape.processes, single.shape.cores) == (3, 3)
    assert _plan(units=400).shape.processes == 36
