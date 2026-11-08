import json
import shutil
import time

import httpx

from simrun.service import app as app_module
from simrun.service.controller import TERMINAL_RUN_STATES

AUTH = {"Authorization": "Bearer t-mk"}


def test_events_first_snapshot(live):
    web = live(events_max_s=1.5, poll_s=0.2)
    run_id = web.submit()
    started = time.monotonic()
    got = list(web.stream("/events"))
    assert time.monotonic() - started < 10
    assert [name for name, _ in got[:2]] == ["runs", "servers"]

    runs = json.loads(got[0][1])
    assert [r["id"] for r in runs] == [run_id]
    assert runs[0]["owner"] == "mk"
    assert len(json.loads(got[1][1])) == 12


def test_events_send_changes_only(live):

    web = live(events_max_s=5, poll_s=0.2)
    run_id = web.submit()
    seen: list[str] = []
    first = None
    for name, data in web.stream("/events"):
        seen.append(name)
        if name != "runs":
            continue
        state = json.loads(data)[0]["state"]
        if first is None:
            first = state
            web.backend.start(web.name(run_id))
        else:
            assert state != first
            break
    assert first is not None
    assert seen.count("servers") == 1
    assert seen.count("runs") == 2


def test_events_follow_the_store_between_the_polls_of_the_controller(live):
    # the controller reconciles when woken (the watch of the workflows), so a
    # stream checks the store more often than the controller polls
    web = live(events_max_s=5, poll_s=30, events_poll_s=0.1)
    run_id = web.submit()
    states: list[str] = []
    started = time.monotonic()
    for name, data in web.stream("/events"):
        if name != "runs":
            continue
        states.append(json.loads(data)[0]["state"])
        if len(states) == 1:
            web.backend.start(web.name(run_id))
            web.app.state.controller.wake()
        else:
            break
    assert len(states) == 2 and states[1] != states[0]
    assert time.monotonic() - started < 3


def test_run_events_end_with_a_terminal_run(live):

    web = live(events_max_s=30, poll_s=0.2)
    run_id = web.submit()
    web.backend.start(web.name(run_id))
    web.backend.finish(web.name(run_id))
    deadline = time.monotonic() + 30
    while (record := web.http.get(f"/runs/{run_id}").json())[
        "state"
    ] not in TERMINAL_RUN_STATES:
        assert time.monotonic() < deadline, record["state"]
        time.sleep(0.05)
    started = time.monotonic()
    got = list(web.stream(f"/runs/{run_id}/events"))
    assert time.monotonic() - started < 10
    assert sorted(name for name, _ in got) == ["progress", "run", "workers"]
    by_name = dict(got)
    assert json.loads(by_name["run"])["id"] == run_id
    assert list(json.loads(by_name["progress"])) == ["squares"]
    assert list(json.loads(by_name["workers"])) == ["squares"]


def test_events_keep_the_connection_alive(live, monkeypatch):
    monkeypatch.setattr(app_module, "KEEP_ALIVE_S", 0.3)
    web = live(events_max_s=5, poll_s=0.1)
    web.submit()
    lines: list[str] = []
    with httpx.stream("GET", web.base + "/events", headers=AUTH, timeout=30) as r:
        for line in r.iter_lines():
            lines.append(line)
            if line == ": keep-alive":
                break
    # the events first, then a comment while nothing changed
    assert lines[0] == "event: runs"
    assert lines[-1] == ": keep-alive"


def test_events_need_a_token(live):
    web = live(events_max_s=1.5, poll_s=0.2)
    run_id = web.submit()
    assert httpx.get(web.base + "/events").status_code == 401
    assert httpx.get(web.base + f"/runs/{run_id}/events").status_code == 401
    assert httpx.get(web.base + "/runs/nope/events", headers=AUTH).status_code == 404


def test_run_events_end_when_the_run_is_gone(live):
    web = live(events_max_s=30, poll_s=0.2)
    run_id = web.submit()
    names = []
    for name, _ in web.stream(f"/runs/{run_id}/events"):
        names.append(name)
        if name == "workers":
            shutil.rmtree(web.settings.store / "runs" / run_id)
    assert names == ["run", "progress", "workers"]
