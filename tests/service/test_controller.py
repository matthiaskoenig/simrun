import shutil
from datetime import UTC, datetime, timedelta

import pytest

from simrun.demo import SQUARES
from simrun.plugins_builtin import COMMAND
from simrun.resources import GIB, ResourceKey, ResourceStore
from simrun.service.backend import BackendStatus
from simrun.service.controller import (
    SUBMIT_GRACE_S,
    Controller,
    is_oom,
    task_states,
)
from simrun.store import RunState, TaskState
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
    record = service.store.read(run_id)
    assert record.state == "succeeded"
    assert record.finished is not None
    assert ("stop", service.name(run_id)) not in service.backend.calls


def test_oom_reports(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks/squares"
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.oom_kill(name, "squares-w1")  # attempt 0 killed without a report
    # 99 % of the limit, but no kill: a healthy worker, not out of memory
    write_report(_report("squares-w0", 7.9), task_dir, 0)
    write_report(_report("squares-w1", 1.0), task_dir, 1)
    service.controller.reconcile()
    assert _samples(service) == 0  # the task runs: nothing recorded yet
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    service.controller.reconcile()
    resolved = ResourceStore(service.settings.db).resolve(KEY, SQUARES.hint, None)
    assert resolved.samples == 3
    # measured (7.9 - 0.5) / 2 = 3.7 and (1.0 - 0.5) / 2 = 0.25 GiB, p95 3.5275
    # times 1.1; the kill of w1 at the pod shape of 21 GiB with 40 processes,
    # 0.5125 GiB times 1.5, is smaller
    assert resolved.memory_per_worker_gb == 3.9
    marks = sorted(p.name for p in (task_dir / "recorded").iterdir())
    assert marks == ["squares-w0-0.json", "squares-w1-0.oom", "squares-w1-1.json"]


def test_is_oom_only_from_kills():
    report = _report("squares-w0", 8.0)  # at its limit
    assert not is_oom(report, 0, {})
    assert is_oom(report, 0, {"squares-w0": [0]})
    assert not is_oom(report, 1, {"squares-w0": [0]})
    assert is_oom(report, 2, {"squares-w0": [0, 2]})
    # the report of a worker whose pool lost a process to the out of memory killer
    killed = _report("squares-w0", 1.0)
    killed.oom_kills = 1
    assert is_oom(killed, 0, {})


def _running(service) -> tuple[str, str]:
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.start(name)
    return run_id, name


def _state(service, run_id: str) -> RunState:
    return service.store.read(run_id).state


def test_a_kill_after_the_grace_period_is_no_kill_for_memory(service):
    # exit 137 without OOMKilled and without a report of the out of memory killer:
    # the kubelet killed a pod which did not stop within its grace (Ruling 39)
    run_id, name = _running(service)
    for _ in range(3):
        service.backend.fail_attempt(name, "squares-w1", exit_code=137)
        service.controller.reconcile()
    assert _state(service, run_id) == "running"
    assert service.store.read(run_id).oom_kills == {}


def test_a_worker_reports_the_kill_of_a_process_of_its_pool(service):
    run_id, name = _running(service)
    task_dir = service.run_dir(run_id) / "tasks/squares"
    killed = _report("squares-w1", 1.0)
    killed.oom_kills = 1
    write_report(killed, task_dir, 0)
    service.backend.fail_attempt(name, "squares-w1", exit_code=137)
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert (record.state, record.oom_kills) == ("running", {"squares-w1": [0]})
    # the retry, with the retry memory, killed by the kernel: the second kill
    service.backend.oom_kill(name, "squares-w1")
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert record.state == "failed"
    assert "squares-w1 was killed for memory 2 times" in (record.message or "")


def test_a_kill_after_a_preempted_attempt_does_not_count(service):
    # attempt 0 killed for memory, attempt 1 preempted (no exit code): Argo gives
    # attempt 2 the first memory again, so its kill is no kill with the retry
    # memory; attempt 3 has the retry memory after the exit 137 of attempt 2
    run_id, name = _running(service)
    service.backend.oom_kill(name, "squares-w1")
    service.backend.fail_attempt(name, "squares-w1", exit_code=None)
    service.backend.oom_kill(name, "squares-w1")
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert (record.state, record.oom_kills) == ("running", {"squares-w1": [0, 2]})
    service.backend.oom_kill(name, "squares-w1")
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert record.state == "failed"
    assert "squares-w1 was killed for memory 3 times" in (record.message or "")


def test_reservation_released(service):
    run_id = service.submit(reserve="srv-02")["id"]
    name = service.name(run_id)
    assert ("reserve", "srv-02", run_id) in service.backend.calls
    manifest = service.backend.manifests[name]
    assert manifest["spec"]["nodeSelector"] == {"simrun/reserved": run_id}
    service.backend.start(name)
    service.controller.reconcile()
    assert ("unreserve", "srv-02", run_id) not in service.backend.calls
    service.backend.set_node(name, "squares-merge", "Succeeded")
    service.controller.reconcile()
    assert ("unreserve", "srv-02", run_id) in service.backend.calls
    # the extra workers were still running: the workflow is stopped
    assert ("stop", name) in service.backend.calls
    record = service.store.read(run_id)
    assert record.state == "succeeded"


def test_failed_task_fails_run(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.fail(name, "squares")
    service.controller.reconcile()
    record = service.store.read(run_id)
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
    record = service.store.read(run_id)
    assert record.state == "running"
    assert record.tasks == {"squares": TaskState.RUNNING}


def test_workflow_gone_fails_run(service_factory):
    with service_factory() as service:
        run_id = service.submit()["id"]
    with service_factory() as service:  # a new backend which never saw the run
        service.controller.reconcile()
        record = service.store.read(run_id)
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
    record = service.store.read(run_id)
    assert record.tasks == {"squares": TaskState.RUNNING, "cmd": TaskState.DONE}
    # the workflow finishes and its time to live deletes it before the next poll
    out = service.run_dir(run_id) / "tasks/squares/out"
    out.mkdir(parents=True)
    (out / MERGED_MARKER).touch()
    service.backend.delete(name)
    service.controller.reconcile()
    record = service.store.read(run_id)
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
    record = service.store.read(run_id)
    assert record.state == "failed"
    assert record.tasks == {"squares": TaskState.DONE, "cmd": TaskState.FAILED}
    assert record.message == f"the workflow {name} is gone: cmd unknown"


class _BrokenRelease(FakeBackend):
    broken = True

    def unreserve(self, node, run_id):
        if self.broken:
            raise RuntimeError("api down")
        super().unreserve(node, run_id)


def test_failed_release_is_retried(service_factory):
    with service_factory(_BrokenRelease()) as service:
        run_id = service.submit(reserve="srv-02")["id"]
        service.backend.finish(service.name(run_id))
        service.controller.reconcile()
        store = service.store
        record = store.read(run_id)
        assert record.state == "succeeded"
        assert record.pending_release == "srv-02"
        service.backend.broken = False
        service.controller.reconcile()
        assert store.read(run_id).pending_release is None
        assert ("unreserve", "srv-02", run_id) in service.backend.calls


def test_unknown_node_counts_as_released(service):
    run_id = service.submit()["id"]
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    store = service.store
    store.write(store.read(run_id).model_copy(update={"pending_release": "ghost"}))
    service.controller.reconcile()
    assert store.read(run_id).pending_release is None


def test_corrupt_record_does_not_stop_the_others(service):
    bad = service.submit(group="g")["id"]
    good = service.submit()["id"]
    (service.run_dir(bad) / "run.json").write_text("{not json")
    service.backend.finish(service.name(good))
    service.controller.reconcile()
    assert service.store.read(good).state == "succeeded"
    # the list comes from the index, which has the record as the service wrote it
    assert [r["id"] for r in service.get("/runs").json()] == sorted([bad, good])
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
        run_id = service.submit()["id"]
        record = service.store.read(run_id)
        assert record.state == "failed"
        assert record.backend_name == service.name(run_id)
        assert record.pending_cancel
    backend.broken = False
    with service_factory(backend) as service:
        service.controller.recover()
        assert ("cancel", service.name(run_id)) in backend.calls
        assert not service.store.read(run_id).pending_cancel


WORKFLOW = Workflow(
    name="w",
    tasks=[
        {"id": "a", "type": "demo.squares", "settings": {"n": 4}},
        {"id": "b", "type": "command", "inputs": {"x": "a.out"}},
    ],
)


def _states(**nodes: str) -> dict[str, TaskState]:
    status = BackendStatus(phase="Running", tasks=nodes)
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


def test_preempted_task_states():
    assert _states(**{"a-w0": "Preempted"})["a"] == TaskState.PREEMPTED
    running = _states(**{"a-w0": "Running", "a-extra(0:1)": "Preempted"})
    assert running["a"] == TaskState.RUNNING
    assert _states(b="Preempted")["b"] == TaskState.PREEMPTED


def test_retrying_task_states():
    # a pod which failed of itself and waits for its retry
    assert _states(**{"a-w0": "Retrying"})["a"] == TaskState.RETRYING
    assert _states(b="Retrying")["b"] == TaskState.RETRYING
    running = _states(**{"a-w0": "Running", "a-extra(0:1)": "Retrying"})
    assert running["a"] == TaskState.RUNNING


def test_late_release_keeps_a_later_reservation(service_factory):
    with service_factory(_BrokenRelease()) as service:
        run_id = service.submit(reserve="srv-02")["id"]
        service.backend.finish(service.name(run_id))
        service.controller.reconcile()  # the release fails, it is pending
        # meanwhile the node was freed by hand and reserved by a later run
        service.backend.broken = False
        service.backend.unreserve("srv-02", run_id)
        service.backend.reserve("srv-02", "later")
        service.controller.reconcile()
        store = service.store
        assert store.read(run_id).pending_release is None
        (node,) = [n for n in service.backend.nodes() if n.name == "srv-02"]
        assert node.reserved_by == "later"


def test_fake_unreserve_only_own_reservation():
    backend = FakeBackend()
    backend.reserve("srv-02", "a")
    backend.unreserve("srv-02", "b")
    assert next(n for n in backend.nodes() if n.name == "srv-02").reserved_by == "a"
    backend.unreserve("srv-02", "a")
    assert next(n for n in backend.nodes() if n.name == "srv-02").reserved_by is None


def test_settle_of_a_deleted_run(service):
    run_id = service.submit()["id"]
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    shutil.rmtree(service.run_dir(run_id))
    service.controller.settle(run_id)  # no KeyError


def test_poll_during_submit(service):
    backend = service.backend
    original = FakeBackend.submit

    def submit(self, manifest):
        service.controller.reconcile()  # a poll lands while the submission is in flight
        return original(self, manifest)

    backend.submit = submit.__get__(backend)
    summary = service.submit(reserve="srv-02")
    assert summary["state"] == "queued"
    assert summary["message"] is None
    assert not [c for c in backend.calls if c[0] in ("unreserve", "cancel")]


def test_cancel_during_submit_cancels_the_workflow(service):
    backend = service.backend
    original = FakeBackend.submit

    def submit(self, manifest):
        service.controller.cancel(manifest["metadata"]["labels"]["simrun/run"])
        return original(self, manifest)

    backend.submit = submit.__get__(backend)
    summary = service.submit()
    assert summary["state"] == "cancelled"
    name = service.name(summary["id"])
    # the cancel during the submission found no workflow; the one after it did
    calls = [c for c in backend.calls if c[0] != "logs"]
    assert calls[-2:] == [("submit", name), ("cancel", name)]
    assert backend.status(name).phase == "Failed"
    record = service.store.read(summary["id"])
    assert not record.pending_cancel


def _submitting(
    service, age_s: float, since=None, built_age_s=None, reserve="srv-02"
) -> str:
    """A run submitting since `age_s` seconds before `since` (default now), its
    image ready `built_age_s` seconds before, not yet without."""
    run_id = service.submit(reserve=reserve)["id"]
    store = service.store
    record = store.read(run_id)
    since = since or datetime.now(UTC)
    created = since - timedelta(seconds=age_s)
    built = None if built_age_s is None else since - timedelta(seconds=built_age_s)
    store.write(
        record.model_copy(
            update={"state": RunState.SUBMITTING, "created": created, "built": built}
        )
    )
    return run_id


def test_young_submission_is_left_alone(service):
    run_id = _submitting(service, 1.0)
    service.controller.reconcile()
    assert service.store.read(run_id).state == "submitting"


@pytest.mark.parametrize("age_s", [0.001, SUBMIT_GRACE_S + 1])
def test_recover_resumes_a_lost_submission(service, age_s):
    # created before the controller started: no submission of it is in flight in
    # this process, so the controller resumes it from its record
    run_id = _submitting(service, age_s, since=service.controller.started)
    service.controller.recover()
    record = service.submitted(run_id)
    assert record["state"] == "queued", record["message"]
    name = service.name(run_id)
    assert ("cancel", name) not in service.backend.calls
    assert service.backend.manifests[name]["metadata"]["name"] == name
    (node,) = [n for n in service.backend.nodes() if n.name == "srv-02"]
    assert node.reserved_by == run_id


def test_recover_fails_a_run_without_a_workflow_name(service):
    run_id = _submitting(service, 1.0, since=service.controller.started)
    record = service.store.read(run_id)
    service.store.write(record.model_copy(update={"backend_name": None}))
    service.controller.recover()
    record = service.store.read(run_id)
    assert record.state == "failed"
    assert "submission" in (record.message or "")
    assert ("unreserve", "srv-02", run_id) in service.backend.calls


def test_recover_keeps_submission_after_start(service):
    run_id = _submitting(service, 0.0)  # created after the controller started
    assert service.store.read(run_id).created > (service.controller.started)
    service.controller.recover()
    record = service.store.read(run_id)
    assert record.state == "submitting"
    assert ("cancel", service.name(run_id)) not in service.backend.calls


def test_cancel_and_delete_during_submit(service):
    backend = service.backend
    original = FakeBackend.submit

    def submit(self, manifest):
        run_id = manifest["metadata"]["labels"]["simrun/run"]
        service.controller.cancel(run_id)
        shutil.rmtree(service.run_dir(run_id))  # as DELETE does
        return original(self, manifest)

    backend.submit = submit.__get__(backend)
    run_id = service.submit(wait=False)["id"]
    service.submitted(run_id)
    assert not service.run_dir(run_id).exists()
    (name,) = backend.manifests
    assert backend.calls[-2:] == [("submit", name), ("cancel", name)]
    assert backend.status(name).phase == "Failed"


def test_reconcile_fails_old_submission(service):
    run_id = _submitting(service, SUBMIT_GRACE_S + 1)
    service.controller.reconcile()
    assert service.store.read(run_id).state == "failed"


def test_second_memory_kill_of_a_worker_fails_the_run(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.oom_kill(name, "squares-w1")
    service.controller.reconcile()
    assert service.store.read(run_id).state == "running"
    service.backend.oom_kill(name, "squares-w1")  # also with the retry memory
    service.controller.reconcile()
    record = service.store.read(run_id)
    assert record.state == "failed"
    assert record.tasks == {"squares": TaskState.FAILED}
    # the plan of the demo on the test nodes: 40 processes, a retry of 31 GiB
    assert record.message == (
        "task squares: squares-w1 was killed for memory 2 times, also with the "
        "retry memory of 31 GiB (0.75 GiB per process); give the task more memory"
    )
    assert ("stop", name) in service.backend.calls


def test_second_memory_kill_of_a_merge_or_unsplit_task_fails_the_run(service):
    for key, retry in (("squares-merge", "2 GiB (0.75"), ("cmd", "2 GiB (2.0")):
        run_id = service.submit(workflow=_two_tasks(service))["id"]
        name = service.name(run_id)
        service.backend.start(name)
        service.backend.oom_kill(name, key, count=2)
        service.controller.reconcile()
        record = service.store.read(run_id)
        assert record.state == "failed"
        assert f"{key} was killed for memory 2 times" in (record.message or "")
        assert f"retry memory of {retry} GiB per process)" in (record.message or "")


def test_unsplit_plugin_task_learns_its_memory(service):
    workflow = {"name": "r", "tasks": [{"id": "report", "type": "demo.report"}]}
    run_id = service.submit(workflow=workflow)["id"]
    name = service.name(run_id)
    task_dir = service.run_dir(run_id) / "tasks/report"
    service.backend.start(name)
    service.backend.oom_kill(name, "report")  # attempt 0, by the task id
    report = WorkerReport(
        worker="report",
        node="srv-02",
        processes=1,
        peak_memory_bytes=1 * GIB,
        memory_limit_bytes=3 * GIB,
    )
    write_report(report, task_dir, 1)
    service.backend.finish(name)
    service.controller.reconcile()
    key = ResourceKey("demo.report", "", "")
    hint = service.controller.types["demo.report"].hint
    resolved = ResourceStore(service.settings.db).resolve(key, hint, None)
    # measured (1 - 0.5) / 1 process; the kill of the pod of one process of
    # 2 GiB, 1.5 GiB per process times 1.5
    assert resolved.samples == 2
    assert resolved.memory_per_worker_gb == 2.3
    marks = sorted(p.name for p in (task_dir / "recorded").iterdir())
    assert marks == ["report-0.oom", "report-1.json"]


def test_command_tasks_are_not_learned(service):
    run_id = service.submit(workflow=_two_tasks(service))["id"]
    name = service.name(run_id)
    service.backend.start(name)
    service.backend.oom_kill(name, "cmd")
    service.backend.finish(name)
    service.controller.reconcile()
    key = ResourceKey("command", "", "")
    resources = ResourceStore(service.settings.db)
    assert resources.resolve(key, COMMAND.hint, None).samples == 0
    assert not (service.run_dir(run_id) / "tasks/cmd/recorded").exists()


def test_grace_counts_from_the_build(service):
    # created long ago, its image was ready a second ago: in the grace
    run_id = _submitting(service, SUBMIT_GRACE_S * 10, built_age_s=1.0)
    service.controller.reconcile()
    assert service.store.read(run_id).state == "submitting"
    old = _submitting(
        service, SUBMIT_GRACE_S * 10, built_age_s=SUBMIT_GRACE_S + 1, reserve="srv-03"
    )
    service.controller.reconcile()
    assert service.store.read(old).state == "failed"


def test_cancel_before_the_reservation_releases_the_node(service):
    backend = service.backend
    original = FakeBackend.reserve

    def reserve(self, node, run_id):
        # the run is cancelled (and settled) before its job reserves the node
        service.controller.cancel(run_id)
        return original(self, node, run_id)

    backend.reserve = reserve.__get__(backend)
    record = service.submit(reserve="srv-02")
    assert record["state"] == "cancelled"
    assert record["pending_release"] is None and not record["pending_cancel"]
    (node,) = [n for n in backend.nodes() if n.name == "srv-02"]
    assert node.reserved_by is None
    assert ("cancel", service.name(record["id"])) in backend.calls


def test_observations_are_kept_apart_by_the_cost_setting(service):
    workflow = {
        "name": "s",
        "tasks": [
            {"id": "fit", "type": "demo.subset", "settings": {"n": 4, "subset": "a"}}
        ],
    }
    run_id = service.submit(workflow=workflow)["id"]
    write_report(_report("fit-w0", 2.0), service.run_dir(run_id) / "tasks/fit", 0)
    service.backend.finish(service.name(run_id))
    service.controller.reconcile()
    hint = service.controller.types["demo.subset"].hint
    resources = ResourceStore(service.settings.db)
    assert resources.resolve(ResourceKey("demo.subset", "", "a"), hint, None).samples
    # the service resolves a submission of the same subset from it, not another one
    for subset, source in (("a", "learned from 1 runs"), ("b", "plugin hint")):
        workflow["tasks"][0]["settings"]["subset"] = subset
        estimate = service.post("/estimate", {"workflow": workflow, "sources": []})
        assert estimate.json()["fit"]["resources"]["source"] == source


def test_cancel_and_delete_before_the_reservation_releases_the_node(service):
    backend = service.backend
    original = FakeBackend.reserve

    def reserve(self, node, run_id):
        # cancelled and deleted before its job reserves the node: the job finds no
        # record after the submission and cancels the orphan workflow
        service.controller.cancel(run_id)
        deleted = service.client.delete(f"/runs/{run_id}", headers=service.auth())
        assert deleted.status_code == 200, deleted.text
        return original(self, node, run_id)

    backend.reserve = reserve.__get__(backend)
    run_id = service.submit(reserve="srv-02", wait=False)["id"]
    service.submitted(run_id)
    name = service.name(run_id)
    assert ("cancel", name) in backend.calls
    (node,) = [n for n in backend.nodes() if n.name == "srv-02"]
    assert node.reserved_by is None
