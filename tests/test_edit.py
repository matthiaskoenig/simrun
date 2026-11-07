import contextlib
import hashlib
import shutil
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from simrun import cli, edit
from simrun.cli import app
from simrun.client import Client, ClientError
from simrun.edit import Editor, EditResult, run_edit
from simrun.service import app as app_module
from simrun.service import edits as edits_module
from simrun.workflow import load_workflow

SQUARES = {"id": "squares", "type": "demo.squares", "settings": {"n": 4}}


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def make(live, tmp_path):
    def factory(sources=None, **kw):
        web = live(**kw)
        client = Client(web.base, "t-mk", web.settings.store)
        path = tmp_path / "wf.yaml"
        editor = Editor(client, path, sources or {}, None)
        return web, client, path, editor

    return factory


def browser(web, session_id, route, body, out):
    """The browser: posts to a route in a thread, so the command can answer."""

    def post():
        out.append(web.http.post(f"/edits/{session_id}/{route}", json=body))

    thread = threading.Thread(target=post, daemon=True)
    thread.start()
    return thread


def serve(editor, thread):
    while thread.is_alive():
        editor.step(0.2)
    thread.join()


def workflow_body(name="wf"):
    return {"name": name, "tasks": [SQUARES]}


def session(web, session_id):
    return web.http.get(f"/edits/{session_id}").json()


def test_save_writes_the_file(make):
    web, _, path, editor = make()
    opened = editor.open()
    assert opened.workflow.name == "wf" and opened.workflow.tasks == []
    out = []
    thread = browser(
        web, opened.id, "save", {"workflow": workflow_body(), "base": ""}, out
    )
    serve(editor, thread)
    assert out[0].status_code == 200, out[0].text
    assert load_workflow(path).tasks[0].id == "squares"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert out[0].json()["file_sha256"] == digest
    editor.step(0)
    assert session(web, opened.id)["file_sha256"] == digest
    assert not list(path.parent.glob("*.tmp"))


def test_save_conflict_keeps_the_file(make):
    web, _, path, editor = make()
    path.write_text("name: wf\ntasks: []\n")
    opened = editor.open()
    path.write_text("name: other\ntasks: []\n")
    body = {"workflow": workflow_body(), "base": opened.file_sha256}
    out = []
    serve(editor, browser(web, opened.id, "save", body, out))
    assert out[0].status_code == 409
    assert out[0].json()["detail"] == "the file changed on disk"
    assert load_workflow(path).name == "other"
    out = []
    serve(editor, browser(web, opened.id, "save", {**body, "force": True}, out))
    assert out[0].status_code == 200, out[0].text
    assert load_workflow(path).tasks[0].id == "squares"


def test_file_changed_on_disk_is_sent(make):
    web, _, path, editor = make()
    opened = editor.open()
    path.write_text("name: edited\ntasks: []\n")
    editor.step(0)
    now = session(web, opened.id)
    assert now["workflow"]["name"] == "edited"
    assert now["file_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_tree_changes_are_sent(make, repo):
    web, _, _, editor = make({"model": repo})
    opened = editor.open()
    assert opened.tree[0].branch == "main"
    assert opened.tree[0].changed_files == []
    (repo / "new.py").write_text("x\n")
    editor.step(0)
    assert "?? new.py" in session(web, opened.id)["tree"][0]["changed_files"]


def test_run_snapshots_the_tree_again(make, repo):
    web, client, _, editor = make({"model": repo})
    opened = editor.open()
    (repo / "model.txt").write_text("changed\n")
    out = []
    serve(editor, browser(web, opened.id, "run", {"workflow": workflow_body()}, out))
    assert out[0].status_code == 200, out[0].text
    record = client.status(out[0].json()["id"])
    assert record.sources[0].diff_sha256 != opened.sources[0].diff_sha256
    assert (
        session(web, opened.id)["sources"][0]["diff_sha256"]
        == record.sources[0].diff_sha256
    )


def test_run_refused_answers_422(make):
    web, _, _, editor = make()
    opened = editor.open()
    body = {"name": "wf", "tasks": [{"id": "a", "type": "no.such"}]}
    out = []
    serve(editor, browser(web, opened.id, "run", {"workflow": body}, out))
    assert out[0].status_code == 422
    assert "no.such" in out[0].json()["detail"]


def test_close_on_stop(make):
    web, client, path, _ = make()
    calls = []

    def stop():
        calls.append(1)
        return len(calls) > 1

    run_edit(client, path, {}, None, should_stop=stop)
    (session_id,) = [p.name for p in (web.settings.store / "edits").iterdir()]
    assert session(web, session_id)["closed"] is True


def test_cli_edit_help():
    result = CliRunner().invoke(app, ["edit", "--help"])
    assert result.exit_code == 0
    assert "--source" in result.output and "--group" in result.output


def test_invalid_file_does_not_kill_the_command(make):
    web, _, path, editor = make()
    opened = editor.open()
    for text in ("", "a: [", "- 1\n"):
        path.write_text(text)
        editor.step(0)
    out = []
    body = {"workflow": workflow_body(), "base": hashlib.sha256(b"- 1\n").hexdigest()}
    serve(editor, browser(web, opened.id, "save", body, out))
    assert out[0].status_code == 200, out[0].text
    assert load_workflow(path).tasks[0].id == "squares"


def test_save_keeps_the_mode(make):
    web, _, path, editor = make()
    path.write_text("name: wf\ntasks: []\n")
    path.chmod(0o640)
    opened = editor.open()
    body = {"workflow": workflow_body(), "base": opened.file_sha256}
    serve(editor, browser(web, opened.id, "save", body, []))
    assert path.stat().st_mode & 0o777 == 0o640


def test_run_edit_retries_a_transport_error(make, monkeypatch):
    monkeypatch.setattr(edit, "BACKOFF_START_S", 0.01)
    web, client, path, _ = make()
    real = Editor.step
    calls = []

    def flaky(self, timeout):
        calls.append(1)
        if len(calls) <= 2:
            raise httpx.ConnectError("down")
        real(self, timeout)

    monkeypatch.setattr(Editor, "step", flaky)
    run_edit(client, path, {}, None, should_stop=lambda: len(calls) >= 4)
    (session_id,) = [p.name for p in (web.settings.store / "edits").iterdir()]
    assert len(calls) == 4
    assert session(web, session_id)["closed"] is True


def serve_through(editor, thread, errors=()):
    """Serve the browser's request, swallowing the errors the loop backs off from."""
    while thread.is_alive():
        with contextlib.suppress(*errors):
            editor.step(0.2)
    thread.join()


def test_a_result_which_failed_to_post_is_posted_again_not_run_again(make, monkeypatch):
    web, client, _, editor = make()
    opened = editor.open()
    submits = []
    real_submit = client.submit

    def submit(*args):
        submits.append(1)
        return real_submit(*args)

    real_result = client.edit_result
    failures = [httpx.ConnectError("down")]

    def edit_result(*args):
        if failures:
            raise failures.pop()
        real_result(*args)

    monkeypatch.setattr(client, "submit", submit)
    monkeypatch.setattr(client, "edit_result", edit_result)
    out = []
    thread = browser(web, opened.id, "run", {"workflow": workflow_body()}, out)
    serve_through(editor, thread, (httpx.TransportError,))
    assert out[0].status_code == 200, out[0].text
    assert len(submits) == 1
    assert not failures


def test_a_result_of_a_request_the_browser_gave_up_is_skipped(
    make, monkeypatch, caplog
):
    monkeypatch.setattr(app_module, "EDIT_WAIT_S", 0.3)
    web, _, _, editor = make()
    opened = editor.open()

    def slow(request):
        time.sleep(0.6)
        return EditResult(ok=True, file_sha256="x")

    monkeypatch.setattr(editor, "handle", slow)
    out = []
    thread = browser(
        web, opened.id, "save", {"workflow": workflow_body(), "base": ""}, out
    )
    serve(editor, thread)
    editor.step(0)  # the result goes after the browser gave up: a 404
    assert out[0].status_code == 503
    assert "no longer waited for" in caplog.text
    editor.step(0)  # nothing is posted again
    assert caplog.text.count("no longer waited for") == 1


def test_run_edit_retries_a_503_of_a_proxy(make, monkeypatch):
    monkeypatch.setattr(edit, "BACKOFF_START_S", 0.01)
    _, client, path, _ = make()
    real = client.edit_requests
    calls = []

    def flaky(*args):
        calls.append(1)
        if len(calls) <= 2:
            raise ClientError(503, "Service Unavailable")
        return real(*args)

    monkeypatch.setattr(client, "edit_requests", flaky)
    run_edit(client, path, {}, None, should_stop=lambda: len(calls) >= 4)
    assert len(calls) == 4


def test_the_session_stays_connected_while_a_request_is_handled(make, monkeypatch):
    monkeypatch.setattr(edits_module, "EDIT_CONNECTED_S", 0.5)
    monkeypatch.setattr(edit, "EDIT_WATCH_S", 0.1)
    web, _, _, editor = make()
    opened = editor.open()
    seen = []

    def slow(request):
        time.sleep(1.2)
        seen.append(session(web, opened.id)["connected"])
        return EditResult(ok=True, file_sha256="x")

    monkeypatch.setattr(editor, "handle", slow)
    out = []
    thread = browser(
        web, opened.id, "save", {"workflow": workflow_body(), "base": ""}, out
    )
    serve(editor, thread)
    assert seen == [True]
    assert out[0].status_code == 200, out[0].text


def test_an_invalid_file_still_sends_the_tree(make, repo):
    web, _, path, editor = make({"model": repo})
    opened = editor.open()
    path.write_text("a: [")
    (repo / "new.py").write_text("x\n")
    editor.step(0)
    now = session(web, opened.id)
    assert "?? new.py" in now["tree"][0]["changed_files"]
    assert now["file_sha256"] == opened.file_sha256


def test_git_failing_is_a_warning(make, repo, caplog):
    web, _, path, editor = make({"model": repo})
    opened = editor.open()
    shutil.rmtree(repo / ".git")
    path.write_text("name: edited\ntasks: []\n")
    editor.step(0)
    assert "the state of the trees cannot be read" in caplog.text
    assert session(web, opened.id)["workflow"]["name"] == "edited"


def test_a_refused_state_is_a_warning(make, monkeypatch, caplog):
    _, client, path, editor = make()
    editor.open()

    def refuse(*args):
        raise ClientError(500, "boom")

    monkeypatch.setattr(client, "edit_state", refuse)
    path.write_text("name: edited\ntasks: []\n")
    editor.step(0)
    assert "the state could not be sent" in caplog.text


def test_close_is_best_effort_when_the_service_does_not_answer(
    make, monkeypatch, caplog
):
    monkeypatch.setattr(edit, "EDIT_CLOSE_TIMEOUT_S", 0.3)
    _, client, _, editor = make()
    editor.open()
    with socket.socket() as silent:  # accepts connections, never answers
        silent.bind(("127.0.0.1", 0))
        silent.listen()
        port = silent.getsockname()[1]
        client._http = httpx.Client(base_url=f"http://127.0.0.1:{port}")
        started = time.monotonic()
        editor.close()
        assert time.monotonic() - started < 3
    assert "the session could not be closed" in caplog.text


def test_cli_edit_second_ctrl_c_interrupts_and_exits_0(monkeypatch, tmp_path):
    handlers = {n: signal.getsignal(n) for n in (signal.SIGINT, signal.SIGTERM)}
    checked = []

    def fake_run_edit(client, path, sources, group, *, should_stop):
        assert not should_stop()
        first = signal.getsignal(signal.SIGINT)
        assert callable(first)
        first(signal.SIGINT, None)  # the first Ctrl-C
        assert should_stop()
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
        assert signal.getsignal(signal.SIGTERM) is signal.SIG_DFL
        checked.append(1)
        raise KeyboardInterrupt  # the second Ctrl-C

    monkeypatch.setattr(cli, "run_edit", fake_run_edit)
    monkeypatch.setattr(cli, "make_client", lambda: None)
    try:
        result = CliRunner().invoke(app, ["edit", str(tmp_path / "wf.yaml")])
    finally:
        for number, handler in handlers.items():
            signal.signal(number, handler)
    assert checked == [1]
    assert result.exit_code == 0, result.output


def test_a_result_refused_with_500_is_not_run_again(make, monkeypatch):
    monkeypatch.setattr(app_module, "EDIT_WAIT_S", 2.0)
    web, client, _, editor = make()
    opened = editor.open()
    submits = []
    real_submit = client.submit

    def submit(*args):
        submits.append(1)
        return real_submit(*args)

    real_result = client.edit_result
    refusals = [ClientError(500, "the store could not be written")]

    def edit_result(*args):
        if refusals:
            raise refusals.pop()
        real_result(*args)

    monkeypatch.setattr(client, "submit", submit)
    monkeypatch.setattr(client, "edit_result", edit_result)
    out = []
    thread = browser(web, opened.id, "run", {"workflow": workflow_body()}, out)
    for _ in range(5):
        editor.step(0.2)
    assert len(submits) == 1
    assert not refusals
    thread.join(10)  # the browser waits until `ask` gives up
    assert out[0].status_code == 503


def test_a_second_ctrl_c_skips_the_close_and_the_heartbeat(make, monkeypatch):
    monkeypatch.setattr(app_module, "EDIT_WAIT_S", 1.0)
    web, client, path, _ = make()
    closed = []
    monkeypatch.setattr(Editor, "close", lambda self: closed.append(1))
    joins = []
    real_join = threading.Thread.join

    def join(self, timeout=None):
        if self.name == "edit-heartbeat":
            joins.append(1)
        real_join(self, timeout)

    monkeypatch.setattr(threading.Thread, "join", join)

    def interrupted(self, request):
        raise KeyboardInterrupt

    monkeypatch.setattr(Editor, "handle", interrupted)
    out = []

    def stop_after_request():
        return bool(out)

    thread = None

    def start_browser():
        nonlocal thread
        sid = next((web.settings.store / "edits").iterdir()).name
        thread = browser(
            web, sid, "save", {"workflow": workflow_body(), "base": ""}, out
        )

    real_open = Editor.open

    def open_then_ask(self):
        opened = real_open(self)
        start_browser()
        return opened

    monkeypatch.setattr(Editor, "open", open_then_ask)
    with pytest.raises(KeyboardInterrupt):
        run_edit(client, path, {}, None, should_stop=stop_after_request)
    assert closed == []
    assert joins == []
    assert thread is not None
    thread.join(10)
