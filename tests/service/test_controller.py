from simrun.demo import SQUARES
from simrun.resources import GIB, ResourceKey, ResourceStore
from simrun.service.backend import BackendStatus
from simrun.service.controller import Controller, task_states
from simrun.store import RunStore, TaskState
from simrun.testing import FakeBackend
from simrun.worker import MERGED_MARKER, WorkerReport, write_report
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
    service.backend.oom_kill(name, "squares-w1")  # attempt 0 killed without a report
    write_report(_report("squares-w0", 7.9), task_dir, 0)  # 99 % of the limit
    write_report(_report("squares-w1", 1.0), task_dir, 1)
    service.controller.reconcile()
    assert _samples(service) == 0  # the task runs: nothing recorded yet
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    service.controller.reconcile()
    resolved = ResourceStore(service.settings.db).resolve(KEY, SQUARES.hint, None)
    assert resolved.samples == 3
    # oom: w0 at its limit, (8 - 0.5) / 2 = 3.75 GiB, and w1 at the pod shape of 21
    # GiB with 40 processes, 0.5125 GiB; the larger times 1.5, rounded up
    assert resolved.memory_per_worker_gb == 5.7
    marks = sorted(p.name for p in (task_dir / "recorded").iterdir())
    assert marks == ["squares-w0-0.json", "squares-w1-0.oom", "squares-w1-1.json"]


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
        assert record.tasks == {"squares": TaskState.FAILED}
        assert "gone" in (record.message or "")


def _two_tasks(service) -> dict:
    command = {
        "image": "localhost:5000/tools:1",
        "args": ["true"],
        "cores": 1,
        "memory_gb": 1.0,
    }
    workflow = service.workflow()
    workflow["tasks"].append({"id": "cmd", "type": "command", "settings": command})
    return workflow


def test_finished_then_gone(service):
    run_id = service.submit(workflow=_two_tasks(service))["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.set_node(name, "cmd", "Succeeded")
    service.controller.reconcile()
    record = RunStore(service.settings.store).read(run_id)
    assert record.tasks == {"squares": TaskState.RUNNING, "cmd": TaskState.DONE}
    # the workflow finishes and its time to live deletes it before the next poll
    out = service.run_dir(run_id) / "tasks/squares/out"
    out.mkdir(parents=True)
    (out / MERGED_MARKER).touch()
    service.backend.delete(name)
    service.controller.reconcile()
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "succeeded"
    assert record.tasks == {"squares": TaskState.DONE, "cmd": TaskState.DONE}


def test_gone_with_merge_marker(service):
    run_id = service.submit(workflow=_two_tasks(service))["id"]
    name = service.name(run_id)
    out = service.run_dir(run_id) / "tasks/squares/out"
    out.mkdir(parents=True)
    (out / MERGED_MARKER).touch()
    service.backend.delete(name)
    service.controller.reconcile()
    record = RunStore(service.settings.store).read(run_id)
    assert record.state == "failed"
    assert record.tasks == {"squares": TaskState.DONE, "cmd": TaskState.FAILED}
    assert record.message == f"the workflow {name} is gone: cmd unknown"


class _BrokenRelease(FakeBackend):
    broken = True

    def unreserve(self, node):
        if self.broken:
            raise RuntimeError("api down")
        super().unreserve(node)


def test_failed_release_is_retried(service_factory):
    with service_factory(_BrokenRelease()) as service:
        run_id = service.submit(reserve="srv-02")["id"]
        service.backend.finish(service.name(run_id))
        service.controller.reconcile()
        store = RunStore(service.settings.store)
        record = store.read(run_id)
        assert record.state == "succeeded"
        assert record.pending_release == "srv-02"
        service.backend.broken = False
        service.controller.reconcile()
        assert store.read(run_id).pending_release is None
        assert ("unreserve", "srv-02") in service.backend.calls


def test_unknown_node_counts_as_released(service):
    run_id = service.submit()["id"]
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    store = RunStore(service.settings.store)
    store.write(store.read(run_id).model_copy(update={"pending_release": "ghost"}))
    service.controller.reconcile()
    assert store.read(run_id).pending_release is None


def test_corrupt_record_does_not_stop_the_others(service):
    bad = service.submit(group="g")["id"]
    good = service.submit()["id"]
    (service.run_dir(bad) / "run.json").write_text("{not json")
    service.backend.finish(service.name(good))
    service.controller.reconcile()
    assert RunStore(service.settings.store).read(good).state == "succeeded"
    assert [r["id"] for r in service.get("/runs").json()] == [good]
    assert service.get("/usage").status_code == 200
    cleanup = service.post("/cleanup", {"older_than_days": 0, "dry_run": True})
    assert cleanup.json()["runs"] == [good]
    service.controller.recover()


def test_failing_recover_keeps_the_loop(service_factory, monkeypatch):
    def broken(self):
        raise RuntimeError("recover failed")

    monkeypatch.setattr(Controller, "recover", broken)
    with service_factory(FakeBackend(finish_after=0.1), poll_s=0.05) as service:
        run_id = service.submit()["id"]
        response = service.get(f"/runs/{run_id}/wait", params={"timeout": 5})
        assert response.json()["state"] == "succeeded"


class _Orphaning(FakeBackend):
    """Creates the workflow, then fails; cancels fail while broken."""

    broken = True

    def submit(self, manifest):
        super().submit(manifest)
        raise RuntimeError("timeout")

    def cancel(self, name):
        if self.broken:
            raise RuntimeError("api down")
        super().cancel(name)


def test_recover_cancels_orphan(service_factory):
    backend = _Orphaning()
    with service_factory(backend) as service:
        response = service.post(
            "/runs", {"workflow": service.workflow(), "sources": []}
        )
        assert response.status_code == 502
        (summary,) = service.get("/runs").json()
        run_id = summary["id"]
        record = RunStore(service.settings.store).read(run_id)
        assert record.backend_name == service.name(run_id)
        assert record.pending_cancel
    backend.broken = False
    with service_factory(backend) as service:
        service.controller.recover()
        assert ("cancel", service.name(run_id)) in backend.calls
        assert not RunStore(service.settings.store).read(run_id).pending_cancel


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
