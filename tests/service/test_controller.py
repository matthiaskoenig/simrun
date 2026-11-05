from simrun.demo import SQUARES
from simrun.resources import GIB, ResourceKey, ResourceStore
from simrun.service.backend import BackendStatus
from simrun.service.controller import task_states
from simrun.store import RunStore, TaskState
from simrun.worker import WorkerReport, write_report
from simrun.workflow import Workflow

TYPES = {SQUARES.name: SQUARES}
KEY = ResourceKey("demo.squares", "", "")


def _report(worker: str, peak_gb: float, limit_gb: float = 8.0) -> WorkerReport:
    return WorkerReport(
        worker=worker,
        node="srv-02",
        processes=2,
        units_done=[0, 1],
        seconds=4.0,
        peak_memory_bytes=int(peak_gb * GIB),
        memory_limit_bytes=int(limit_gb * GIB),
    )


def _samples(service) -> int:
    resources = ResourceStore(service.settings.db)
    return resources.resolve(KEY, SQUARES.hint, None).samples


def test_controller_records_resources(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks/squares"
    write_report(_report("squares-w0", 2.0), task_dir, 0)
    write_report(_report("squares-w1", 2.5), task_dir, 0)
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    assert _samples(service) == 2
    service.controller.reconcile()
    assert _samples(service) == 2
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "succeeded"
    assert record.finished is not None
    assert ("stop", service.name(run_id)) not in service.backend.calls


def test_oom_reports(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks/squares"
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.oom_kill(name, "squares-w1")
    write_report(_report("squares-w0", 7.9), task_dir, 0)  # 99 % of the limit
    write_report(_report("squares-w1", 1.0), task_dir, 0)  # killed by the backend
    write_report(_report("squares-w1", 1.0), task_dir, 1)
    service.controller.reconcile()
    resolved = ResourceStore(service.settings.db).resolve(KEY, SQUARES.hint, None)
    assert resolved.samples == 3
    # two oom reports at the limit of 8 GiB: (8 - 0.5) / 2 = 3.75 GiB, times 1.5
    assert resolved.memory_per_worker_gb >= 5.6


def test_reservation_released(service):
    run_id = service.submit(reserve="srv-02")["id"]
    name = service.name(run_id)
    assert ("reserve", "srv-02", run_id) in service.backend.calls
    manifest = service.backend.manifests[name]
    assert manifest["spec"]["nodeSelector"] == {"simrun/reserved": run_id}
    service.backend.start(name)
    service.controller.reconcile()
    assert ("unreserve", "srv-02") not in service.backend.calls
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    assert ("unreserve", "srv-02") in service.backend.calls
    # the extra workers were still running: the workflow is stopped
    assert ("stop", name) in service.backend.calls
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "succeeded"


def test_failed_task_fails_run(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.fail(name, "squares")
    service.controller.reconcile()
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "failed"
    assert record.tasks == {"squares": TaskState.FAILED}
    assert "squares" in (record.message or "")
    assert ("stop", name) in service.backend.calls


def test_extra_worker_failure_ignored(service):
    run_id = service.submit(workflow=service.workflow(n=100))["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.set_node(name, "squares-extra(0:1)", "Failed")
    service.controller.reconcile()
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "running"
    assert record.tasks == {"squares": TaskState.RUNNING}


def test_workflow_gone_fails_run(service_factory):
    with service_factory() as service:
        run_id = service.submit()["id"]
    with service_factory() as service:  # a new backend which never saw the run
        service.controller.reconcile()
        record = RunStore(service.settings.store).read(run_id)
        assert record.state == "failed"
        assert "gone" in (record.message or "")


WORKFLOW = Workflow(
    name="w",
    tasks=[
        {"id": "a", "type": "demo.squares", "settings": {"n": 4}},
        {"id": "b", "type": "command", "inputs": {"x": "a.out"}},
    ],
)


def _states(**nodes: str) -> dict[str, TaskState]:
    from simrun.plugins_builtin import COMMAND

    status = BackendStatus(phase="Running", tasks=nodes, oom={})
    return task_states(WORKFLOW, {**TYPES, COMMAND.name: COMMAND}, status)


def test_task_states():
    assert _states() == {"a": TaskState.WAITING, "b": TaskState.WAITING}
    assert _states(**{"a-w0": "Pending"})["a"] == TaskState.QUEUED
    assert _states(**{"a-w0": "Pending", "a-extra(0:1)": "Running"})["a"] == (
        TaskState.RUNNING
    )
    assert _states(**{"a-w0": "Succeeded", "a-merge": "Pending"})["a"] == (
        TaskState.RUNNING
    )
    assert _states(**{"a-w0": "Error"})["a"] == TaskState.FAILED
    assert _states(**{"a-merge": "Failed"})["a"] == TaskState.FAILED
    done = _states(**{"a-merge": "Succeeded", "a-extra(0:1)": "Failed", "b": "Running"})
    assert done == {"a": TaskState.DONE, "b": TaskState.RUNNING}
    assert _states(b="Succeeded")["b"] == TaskState.DONE
    assert _states(b="Failed")["b"] == TaskState.FAILED
