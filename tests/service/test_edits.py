import json
import threading
import time
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from simrun.service import app as app_module
from simrun.service.edits import EditResult, EditState
from simrun.store import RunSummary


def open_body(**kw):
    return {
        "host": "box",
        "path": "/w/flow.yaml",
        "workflow": {"name": "demo", "tasks": []},
        "file_sha256": "h1",
        "sources": [],
        "tree": [],
        **kw,
    }


def open_edit(service):
    response = service.post("/edits", open_body())
    assert response.status_code == 201, response.text
    return response.json()


class Command:
    """A thread which plays `simrun edit`: polls the requests and answers them."""

    def __init__(self, service, session_id, answer):
        self.service = service
        self.session_id = session_id
        self.answer = answer
        self.seen = []
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        edits = self.service.client.app.state.service.edits
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not self.seen:
            edits.seen(self.session_id)
            for request in edits.requests(self.session_id):
                self.seen.append(request)
                edits.answer(self.session_id, request.id, self.answer(request))
            time.sleep(0.02)

    def join(self):
        self.thread.join(10)


def request_files(service, session_id):
    return list((service.settings.store / "edits" / session_id / "requests").glob("*"))


def test_open_and_read_a_session(service):
    session = open_edit(service)
    assert session["owner"] == "mk"
    assert session["connected"] is True
    assert service.get(f"/edits/{session['id']}").json() == session
    assert service.get("/edits", "t-alice").json() == []
    listed = service.get("/edits", "t-mk-bot").json()
    assert [s["id"] for s in listed] == [session["id"]]
    assert service.get("/edits/nope").status_code == 404


def test_list_is_connected_first_then_newest(service):
    first, second, third = (open_edit(service)["id"] for _ in range(3))
    service.post(f"/edits/{second}/close")
    ids = [s["id"] for s in service.get("/edits").json()]
    assert ids[-1] == second
    assert set(ids[:2]) == {first, third}
    assert ids[0] == third


def test_connected_follows_the_polls(service):
    session = open_edit(service)
    path = service.settings.store / "edits" / session["id"] / "session.json"
    stored = json.loads(path.read_text())
    stored["seen"] = (datetime.now(UTC) - timedelta(seconds=31)).isoformat()
    stored["connected"] = True  # never trusted
    path.write_text(json.dumps(stored))
    assert service.get(f"/edits/{session['id']}").json()["connected"] is False
    assert service.get(f"/edits/{session['id']}/requests?timeout=0").json() == []
    assert service.get(f"/edits/{session['id']}").json()["connected"] is True
    closed = service.post(f"/edits/{session['id']}/close").json()
    assert closed["closed"] is True and closed["connected"] is False


def test_put_state_stores_what_was_read(service):
    session = open_edit(service)
    body = {
        "workflow": {"name": "other", "tasks": []},
        "file_sha256": "h9",
        "tree": [
            {"name": "m", "branch": "main", "commit": "c", "changed_files": ["a"]}
        ],
    }
    response = service.client.put(
        f"/edits/{session['id']}/state", json=body, headers=service.auth()
    )
    assert response.status_code == 200, response.text
    assert response.json()["file_sha256"] == "h9"
    assert response.json()["tree"][0]["changed_files"] == ["a"]
    assert response.json()["workflow"]["name"] == "other"


def test_save_round_trip(service):
    session = open_edit(service)
    command = Command(
        service, session["id"], lambda r: EditResult(ok=True, file_sha256="h2")
    )
    workflow = service.workflow()
    response = service.post(
        f"/edits/{session['id']}/save", {"workflow": workflow, "base": "h1"}
    )
    command.join()
    assert response.status_code == 200, response.text
    assert response.json()["file_sha256"] == "h2"
    assert response.json()["workflow"]["tasks"][0]["id"] == "squares"
    assert command.seen[0].kind == "save" and command.seen[0].base == "h1"
    assert request_files(service, session["id"]) == []


def test_save_conflict_is_409(service):
    session = open_edit(service)
    command = Command(
        service,
        session["id"],
        lambda r: EditResult(ok=False, status=409, detail="the file changed on disk"),
    )
    response = service.post(
        f"/edits/{session['id']}/save", {"workflow": service.workflow(), "base": "h0"}
    )
    command.join()
    assert response.status_code == 409
    assert response.json()["detail"] == "the file changed on disk"
    assert service.get(f"/edits/{session['id']}").json()["file_sha256"] == "h1"


def test_run_round_trip(service):
    submitted = service.submit()
    summary = RunSummary.model_validate(
        {k: submitted[k] for k in RunSummary.model_fields}
    )
    session = open_edit(service)
    command = Command(
        service, session["id"], lambda r: EditResult(ok=True, run=summary)
    )
    response = service.post(
        f"/edits/{session['id']}/run",
        {"workflow": service.workflow(), "group": "g", "split": "single-server"},
    )
    command.join()
    assert response.status_code == 200, response.text
    assert response.json()["id"] == submitted["id"]
    assert command.seen[0].kind == "run" and command.seen[0].group == "g"
    assert command.seen[0].split == "single-server"


def test_save_of_a_disconnected_session_is_503_at_once(service):
    session = open_edit(service)
    service.post(f"/edits/{session['id']}/close")
    started = time.monotonic()
    body = {"workflow": service.workflow(), "base": "h1"}
    assert service.post(f"/edits/{session['id']}/save", body).status_code == 503
    run = service.post(f"/edits/{session['id']}/run", {"workflow": service.workflow()})
    assert run.status_code == 503
    assert time.monotonic() - started < 1


def test_unanswered_save_is_503(service, monkeypatch):
    monkeypatch.setattr(app_module, "EDIT_WAIT_S", 0.5)
    session = open_edit(service)
    response = service.post(
        f"/edits/{session['id']}/save", {"workflow": service.workflow(), "base": "h1"}
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "the machine of the session did not answer"
    assert request_files(service, session["id"]) == []


def test_only_the_owner_saves(service):
    session = open_edit(service)
    sid = session["id"]
    alice = "t-alice"
    wf = service.workflow()
    assert (
        service.post(
            f"/edits/{sid}/save", {"workflow": wf, "base": ""}, alice
        ).status_code
        == 403
    )
    assert service.post(f"/edits/{sid}/run", {"workflow": wf}, alice).status_code == 403
    assert service.get(f"/edits/{sid}/requests?timeout=0", alice).status_code == 403
    state = {"workflow": wf, "file_sha256": "x", "tree": []}
    put = service.client.put(
        f"/edits/{sid}/state", json=state, headers=service.auth(alice)
    )
    assert put.status_code == 403
    assert service.post(f"/edits/{sid}/close", None, alice).status_code == 403
    answer = service.post(f"/edits/{sid}/requests/r/result", {"ok": True}, alice)
    assert answer.status_code == 403
    assert service.get(f"/edits/{sid}", alice).status_code == 200


def test_requests_long_poll_returns_when_a_request_arrives(live):
    web = live()
    session = web.http.post("/edits", json=open_body()).json()
    sid = session["id"]
    body = {"workflow": {"name": "demo", "tasks": []}, "base": "h1"}
    saved = []
    timer = threading.Timer(
        0.5, lambda: saved.append(web.http.post(f"/edits/{sid}/save", json=body))
    )
    timer.start()
    started = time.monotonic()
    response = web.http.get(f"/edits/{sid}/requests", params={"timeout": 10})
    assert time.monotonic() - started < 2.5
    found = response.json()
    assert [r["kind"] for r in found] == ["save"]
    answer = {"ok": True, "file_sha256": "h2"}
    web.http.post(f"/edits/{sid}/requests/{found[0]['id']}/result", json=answer)
    timer.join(10)
    assert saved[0].status_code == 200


def test_edit_stream_sends_changes(live):
    web = live(events_max_s=3, poll_s=0.2)
    session = web.http.post("/edits", json=open_body()).json()
    sid = session["id"]
    state = {"workflow": {"name": "demo", "tasks": []}, "file_sha256": "h7", "tree": []}
    threading.Timer(
        1.0, lambda: web.http.put(f"/edits/{sid}/state", json=state)
    ).start()
    got = list(web.stream(f"/edits/{sid}/events"))
    assert {name for name, _ in got} == {"edit"}
    hashes = [json.loads(data)["file_sha256"] for _, data in got]
    assert hashes[0] == "h1" and hashes[-1] == "h7"
    assert web.http.get("/edits/nope").status_code == 404
    unknown = httpx.get(web.base + "/edits/nope/events", headers=web.http.headers)
    assert unknown.status_code == 404


def test_save_without_a_hash_keeps_the_hash_the_command_put(service):
    session = open_edit(service)
    sid = session["id"]
    edits = service.client.app.state.service.edits

    def answer(request):
        state = {"workflow": request.workflow, "file_sha256": "h2", "tree": []}
        edits.update(sid, EditState.model_validate(state))
        return EditResult(ok=True)

    command = Command(service, sid, answer)
    response = service.post(
        f"/edits/{sid}/save", {"workflow": service.workflow(), "base": "h1"}
    )
    command.join()
    assert response.status_code == 200, response.text
    assert response.json()["file_sha256"] == "h2"


def test_a_poll_which_lost_its_client_does_not_mark_the_session_seen(live):
    web = live()
    sid = web.http.post("/edits", json=open_body()).json()["id"]
    with pytest.raises(httpx.ReadTimeout):
        web.http.get(f"/edits/{sid}/requests", params={"timeout": 2}, timeout=0.5)
    seen = web.http.get(f"/edits/{sid}").json()["seen"]
    time.sleep(2.5)
    assert web.http.get(f"/edits/{sid}").json()["seen"] == seen
