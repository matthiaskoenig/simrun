import json
from pathlib import Path
from typing import Any

import pytest

from simrun.ledger import Ledger, LedgerCounts
from simrun.planner import Node
from simrun.service.app import TaskProgress, openapi_document, run_progress
from simrun.service.backend import Placement
from simrun.store import TaskState
from simrun.worker import WorkerReport, write_report

GB = 1024**3


def _placement(service, run_id, **kw) -> Placement:
    values: dict[str, Any] = {
        "workflow": service.name(run_id),
        "dag_task": "squares-w0",
        "pod": "pod-w0",
        "node": "srv-02",
        "phase": "Running",
        "cores": 4.0,
        "memory_gb": 6.0,
        "worker": "squares-w0",
    }
    return Placement(**{**values, **kw})


def _named(service, token: str):
    workflow = service.workflow()
    workflow["name"] = "squares"
    return service.submit(token=token, workflow=workflow)


def test_run_items(service):
    first = _named(service, "t-mk")
    second = _named(service, "t-mk-bot")
    web = service.client.app.state.service
    got = web.run_items(None, None, None, None, 200)
    assert [i.id for i in got] == [second["id"], first["id"]]
    assert [i.owner for i in got] == ["mk", "mk"]
    assert [i.workflow for i in got] == ["squares", "squares"]
    assert [i.id for i in web.run_items(None, None, None, None, 1)] == [second["id"]]


def test_run_progress_counts_like_the_web_app():
    counts = {
        "a": LedgerCounts(pending=600, claimed=10, done=390, total=1000),
        "b": LedgerCounts(pending=0, claimed=0, done=0, total=250),
        "c": None,
    }
    tasks = {
        "a": TaskState.RUNNING,
        "b": TaskState.RUNNING,
        "c": TaskState.WAITING,
        "d": TaskState.DONE,
    }
    # the mean of 0.39, 0, 0 and 1 over the four tasks
    assert run_progress(tasks, counts) == pytest.approx(1.39 / 4)
    assert run_progress({"d": TaskState.DONE, "e": TaskState.SKIPPED}, {}) == 1.0
    assert run_progress({}, {}) == 0.0
    # every unit done, but the task is not over: below 1
    whole = LedgerCounts(pending=0, claimed=0, done=8, total=8)
    assert run_progress({"a": TaskState.RUNNING}, {"a": whole}) == 0.99
    empty = LedgerCounts(pending=0, claimed=0, done=0, total=0)
    assert run_progress({"a": TaskState.RUNNING}, {"a": empty}) == 0.0


def test_run_items_carry_the_progress_of_active_runs(service):
    ended = service.submit()["id"]
    service.backend.start(service.name(ended))
    service.backend.finish(service.name(ended))
    service.controller.reconcile()
    run_id = service.submit(workflow=service.workflow(n=40))["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    ledger = Ledger.create(task_dir / "units", 40)
    for _ in range(10):
        unit = ledger.claim("squares-w0")
        assert unit is not None
        ledger.complete(unit, "squares-w0")
    service.backend.start(service.name(run_id))
    service.controller.reconcile()
    web = service.client.app.state.service
    items = {i.id: i for i in web.run_items(None, None, None, None, 200)}
    assert items[run_id].state == "running"
    assert items[run_id].progress == 0.25
    assert items[ended].state == "succeeded"
    assert items[ended].progress is None


def test_task_progress_without_a_first_line_of_an_older_service():
    # a client validates the answers of a service which is not yet redeployed
    progress = TaskProgress.model_validate({"counts": None, "last": None})
    assert progress.first is None


def test_me(service):
    response = service.get("/me", token="t-alice-bot")
    assert response.json() == {"name": "alice-bot", "kind": "agent", "owner": "alice"}
    assert service.client.get("/me").status_code == 401


def test_servers(service):
    run_id = service.submit()["id"]
    name = service.name(run_id)
    service.backend.place(_placement(service, run_id))
    service.backend.place(
        _placement(service, run_id, workflow="unknown", pod="x", node="srv-03")
    )
    service.backend.start(name)
    service.backend.reserve("srv-04", "someone")
    servers = service.get("/servers").json()
    assert len(servers) == 12
    by_name = {s["name"]: s for s in servers}
    assert by_name["srv-02"]["pods"] == [
        {
            "run_id": run_id,
            "task_id": "squares",
            "pod": "pod-w0",
            "phase": "Running",
            "cores": 4.0,
            "memory_gb": 6.0,
        }
    ]
    assert by_name["srv-01"]["cores"] == 104
    assert by_name["srv-04"]["reserved_by"] == "someone"
    assert by_name["srv-01"]["pods"] == []
    assert by_name["srv-03"]["pods"] == []  # the workflow is not started


def test_servers_have_the_capacity_the_scheduler_counts(service):
    # the planner plans a worker pod with 39 cores and 187 GiB, the scheduler
    # counts the requests of the pods (their sidecars included) against 39.5 and
    # 187.6: the strip shows the pods against the latter
    service.backend._nodes["srv-02"] = Node(
        "srv-02", 39, 187.0, allocatable_cores=39.5, allocatable_memory_gb=187.6
    )
    by_name = {s["name"]: s for s in service.get("/servers").json()}
    assert (by_name["srv-02"]["cores"], by_name["srv-02"]["memory_gb"]) == (
        39.5,
        187.6,
    )
    # a node without them: the capacity of the planner
    assert (by_name["srv-03"]["cores"], by_name["srv-03"]["memory_gb"]) == (40, 188)


def _progress(task_dir: Path, *units: tuple[str, int]) -> None:
    lines = [
        json.dumps(
            {
                "time": "2026-11-07T00:00:00+00:00",
                "worker": w,
                "event": "unit_done",
                "unit": u,
            }
        )
        for w, u in units
    ]
    task_dir.mkdir(parents=True, exist_ok=True)
    with (task_dir / "progress.jsonl").open("a") as handle:
        handle.write("".join(f"{line}\n" for line in lines))


def test_workers_running_and_done(service):
    workflow = service.workflow(n=40)
    record = service.submit(workflow=workflow)
    run_id = record["id"]
    name = service.name(run_id)
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    write_report(
        WorkerReport(
            worker="squares-w0",
            node="srv-01",
            processes=8,
            units_done=[0, 1, 2],
            peak_memory_bytes=2 * GB,
            memory_limit_bytes=4 * GB,
        ),
        task_dir,
    )
    _progress(task_dir, ("squares-w0", 0), ("squares-w0", 1), ("squares-w0", 2))
    ledger = Ledger.create(task_dir / "units", 40)
    assert ledger.claim("squares-w1") == 0
    service.backend.place(
        _placement(
            service,
            run_id,
            dag_task="squares-extra",
            pod="pod-w1",
            node="srv-05",
            worker="squares-w1",
        )
    )
    service.backend.start(name)
    views = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert [v["worker"] for v in views] == ["squares-w0", "squares-w1"]
    done, running = views
    assert done["state"] == "done"
    assert done["node"] == "srv-01"
    assert done["units_done"] == 3
    assert done["peak_memory_gb"] == 2.0
    assert done["memory_limit_gb"] == 4.0
    assert done["processes"] == 8
    assert done["attempts"] == 1
    assert done["oom_killed"] is False
    assert running["state"] == "running"
    assert running["node"] == "srv-05"
    assert running["units_claimed"] == 1
    assert running["processes"] is None
    assert running["attempts"] == 1


def test_workers_count_their_units_from_the_progress_of_the_task(service):
    run_id = service.submit(workflow=service.workflow(n=40))["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    # a running worker without a report: its lines count, a unit done twice once
    _progress(task_dir, ("squares-w0", 4), ("squares-w0", 5), ("squares-w0", 4))
    _progress(task_dir, ("squares-w0", 6), ("squares-w1", 7))
    with (task_dir / "progress.jsonl").open("a") as handle:
        handle.write('{"time": "t", "loss": 0.5}\n{"worker": "squares-w0", "ev')
    service.backend.place(_placement(service, run_id))
    service.backend.start(service.name(run_id))
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert view["worker"] == "squares-w0"
    assert view["state"] == "running"
    assert view["units_done"] == 3


def test_workers_failed_by_oom(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    write_report(
        WorkerReport(worker="squares-w0", node="srv-01", processes=2, oom_kills=1),
        task_dir,
    )
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert view["state"] == "failed"
    assert view["oom_killed"] is True


def test_workers_retried_after_an_eviction(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    # attempt 0 was evicted (SIGTERM, no OOM): a worker reports in `finally`
    write_report(
        WorkerReport(worker="squares-w0", node="srv-01", processes=2), task_dir, 0
    )
    # its retry, attempt 1, waits for a node
    service.backend.place(
        _placement(
            service, run_id, pod="pod-w0-1", node=None, phase="Pending", attempt=1
        )
    )
    service.backend.start(service.name(run_id))
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert view["state"] == "running"
    assert view["node"] is None  # never the node of the evicted attempt
    assert view["attempts"] == 2
    assert view["oom_killed"] is False
    service.backend.place(
        _placement(service, run_id, pod="pod-w0-1", node="srv-02", attempt=1)
    )
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert (view["state"], view["node"], view["attempts"]) == ("running", "srv-02", 2)


def test_workers_retried_after_an_oom_kill(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    write_report(
        WorkerReport(worker="squares-w0", node="srv-01", processes=2, oom_kills=1),
        task_dir,
        0,
    )
    service.backend.place(_placement(service, run_id, pod="pod-w0-1", attempt=1))
    service.backend.start(service.name(run_id))
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert (view["state"], view["node"], view["attempts"]) == ("running", "srv-02", 2)
    # the attempt before was killed for memory
    assert view["oom_killed"] is True


def test_workers_done_while_the_pod_of_the_reported_attempt_ends(service):
    run_id = service.submit()["id"]
    task_dir = service.run_dir(run_id) / "tasks" / "squares"
    write_report(
        WorkerReport(worker="squares-w0", node="srv-01", processes=2), task_dir, 0
    )
    # the pod of attempt 0 is still listed while its container exits
    service.backend.place(_placement(service, run_id, node="srv-01", attempt=0))
    service.backend.start(service.name(run_id))
    (view,) = service.get(f"/runs/{run_id}/tasks/squares/workers").json()
    assert (view["state"], view["node"], view["attempts"]) == ("done", "srv-01", 1)


def test_the_workers_of_a_run_list_the_pods_once(service, monkeypatch):
    tasks = [{"id": t, "type": "demo.squares", "settings": {"n": 4}} for t in "abc"]
    run_id = service.submit(workflow={"name": "demo", "tasks": tasks})["id"]
    name = service.name(run_id)
    service.backend.place(
        _placement(service, run_id, dag_task="b-w0", pod="pod-b", worker="b-w0")
    )
    service.backend.start(name)
    calls: list[None] = []
    placements = service.backend.placements

    def counted() -> list[Placement]:
        calls.append(None)
        return placements()

    monkeypatch.setattr(service.backend, "placements", counted)
    web = service.client.app.state.service
    workers = web.task_workers(web.read(run_id))
    assert len(calls) == 1
    assert {t: [v.worker for v in views] for t, views in workers.items()} == {
        "a": [],
        "b": ["b-w0"],
        "c": [],
    }
    # a tick of the stream of the run: one listing for the workers of every task
    calls.clear()
    payloads, final = web.run_state(web.read(run_id))
    assert sorted(json.loads(payloads["workers"])) == ["a", "b", "c"]
    assert not final
    assert len(calls) == 1
    # a run which ended has no pods to list
    service.backend.finish(name)
    service.controller.reconcile()
    calls.clear()
    record = web.read(run_id)
    assert record.state == "succeeded"
    web.task_workers(record)
    web.run_state(record)
    assert calls == []


def test_workers_unknown_task(service):
    run_id = service.submit()["id"]
    assert service.get(f"/runs/{run_id}/tasks/nope/workers").status_code == 404
    assert service.get("/runs/nope/tasks/squares/workers").status_code == 404


def test_web_served_without_token(service_factory, tmp_path):
    web = tmp_path / "web"
    web.mkdir()
    (web / "index.html").write_text("<p>app</p>")
    with service_factory(web=web) as service:
        got = service.client.get("/ui/")
        assert got.status_code == 200
        assert got.text == "<p>app</p>"
        assert service.client.get("/runs").status_code == 401


def test_no_web_without_setting(service):
    assert service.client.get("/ui/").status_code == 404
    assert service.client.get("/runs").status_code == 401


def test_openapi_document_is_current():
    expected = json.loads(Path("frontend/src/api/openapi.json").read_text())
    assert openapi_document() == expected, (
        "frontend/src/api/openapi.json is out of date: run "
        "`uv run python scripts/openapi.py`"
    )


def test_openapi_document_has_the_schemas_of_the_frontend():
    names = set(openapi_document()["components"]["schemas"])
    assert {
        "RunItem",
        "RunSummary",
        "RunRecord",
        "TaskProgress",
        "ServerUsage",
        "PodUsage",
        "WorkerView",
        "Identity",
        "OutputFile",
    } <= names
