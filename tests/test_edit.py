import hashlib
import subprocess
import threading
from pathlib import Path

import pytest
from typer.testing import CliRunner

from simrun.cli import app
from simrun.client import Client
from simrun.edit import Editor, run_edit
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
    import httpx

    from simrun import edit

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
