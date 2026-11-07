"""The command `simrun edit`: the machine with the code behind the Editor.

It opens an edit session in the service, writes the saves of the browser into the
workflow file, runs the workflow with fresh snapshots of the working trees, and
sends the file and the trees to the session when they change.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
import socket
import subprocess
from collections.abc import Callable
from pathlib import Path

import typer

from simrun.client import Client, ClientError
from simrun.service.edits import (
    EditRequest,
    EditResult,
    EditSession,
    EditState,
    OpenEdit,
    TreeState,
)
from simrun.snapshot import SourceProvenance, snapshot
from simrun.workflow import Workflow, load_workflow, workflow_yaml

logger = logging.getLogger(__name__)

EDIT_POLL_S = 20.0
EDIT_WATCH_S = 2.0
CONFLICT = "the file changed on disk"


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} in {repo}: {result.stderr.strip()}")
    return result.stdout


def tree_state(name: str, repo: Path) -> TreeState:
    """The live state of a working tree: branch, commit and changed files.

    Args:
        name: Name of the source.
        repo: The working tree.

    Returns:
        The state, with no branch for a detached HEAD.

    Raises:
        RuntimeError: When git fails in the tree.
    """
    branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").strip()
    changed = [
        line.strip()
        for line in _git(repo, "status", "--porcelain").splitlines()
        if line.strip()
    ]
    return TreeState(
        name=name,
        branch=None if branch == "HEAD" else branch,
        commit=_git(repo, "rev-parse", "HEAD").strip(),
        changed_files=changed,
    )


def file_state(path: Path) -> tuple[Workflow, str]:
    """The workflow in a file and the sha256 of its bytes.

    A missing file is a new workflow named by the file stem, with no tasks, and
    the empty hash.

    Raises:
        WorkflowError: When the file is not a valid workflow.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return Workflow(name=path.stem, tasks=[]), ""
    return load_workflow(path), hashlib.sha256(data).hexdigest()


class Editor:
    """One edit session of one workflow file."""

    def __init__(
        self,
        client: Client,
        path: Path,
        sources: dict[str, Path],
        group: str | None,
    ) -> None:
        """Create the editor, the session is opened by `open`.

        Args:
            client: The client of the service.
            path: The workflow file.
            sources: Working trees by source name.
            group: Group of the runs, when the request has none.
        """
        self.client = client
        self.path = path.expanduser().resolve()
        self.sources = sources
        self.group = group
        self.session_id = ""
        self._sent: EditState | None = None

    def _trees(self) -> list[TreeState]:
        return [tree_state(name, repo) for name, repo in self.sources.items()]

    def open(self) -> EditSession:
        """Snapshot the sources and open the session."""
        workflow, digest = file_state(self.path)
        provenance = [
            snapshot(repo, self.client.store, name)
            for name, repo in self.sources.items()
        ]
        trees = self._trees()
        session = self.client.open_edit(
            OpenEdit(
                host=socket.gethostname(),
                path=str(self.path),
                workflow=workflow,
                file_sha256=digest,
                sources=provenance,
                tree=trees,
            )
        )
        self.session_id = session.id
        self._sent = EditState(workflow=workflow, file_sha256=digest, tree=trees)
        return session

    def _send(
        self,
        workflow: Workflow,
        digest: str,
        sources: list[SourceProvenance] | None = None,
    ) -> None:
        state = EditState(
            workflow=workflow,
            file_sha256=digest,
            tree=self._trees(),
            sources=sources,
        )
        self.client.edit_state(self.session_id, state)
        self._sent = state.model_copy(update={"sources": None})

    def step(self, timeout: float) -> None:
        """Poll once for requests and answer them, then watch the file and trees.

        Args:
            timeout: Seconds to wait for a request, at most `EDIT_WATCH_S`.
        """
        poll = min(timeout, EDIT_WATCH_S)
        for request in self.client.edit_requests(self.session_id, poll):
            result = self.handle(request)
            self.client.edit_result(self.session_id, request.id, result)
        workflow, digest = file_state(self.path)
        state = EditState(workflow=workflow, file_sha256=digest, tree=self._trees())
        if state != self._sent:
            self._send(workflow, digest)

    def handle(self, request: EditRequest) -> EditResult:
        """Do one request of the browser: save the file or run the workflow."""
        if request.kind == "save":
            return self._save(request)
        return self._run(request)

    def _save(self, request: EditRequest) -> EditResult:
        _, current = file_state(self.path)
        if not request.force and request.base != current:
            return EditResult(ok=False, status=409, detail=CONFLICT)
        text = workflow_yaml(request.workflow)
        tmp = self.path.parent / f".{self.path.name}.{secrets.token_hex(4)}.tmp"
        try:
            tmp.write_text(text)
            os.replace(tmp, self.path)
        except OSError as error:
            tmp.unlink(missing_ok=True)
            return EditResult(ok=False, status=422, detail=str(error))
        return EditResult(
            ok=True, file_sha256=hashlib.sha256(text.encode()).hexdigest()
        )

    def _run(self, request: EditRequest) -> EditResult:
        try:
            summary = self.client.submit(
                request.workflow,
                self.sources,
                self.group or request.group,
                request.split,
                request.reserve,
            )
        except ClientError as error:
            return EditResult(ok=False, status=error.status, detail=error.detail)
        except (ValueError, RuntimeError, OSError) as error:
            return EditResult(ok=False, status=422, detail=str(error))
        try:
            sources = self.client.status(summary.id).sources
            workflow, digest = file_state(self.path)
            self._send(workflow, digest, sources)
        except ClientError, OSError:
            logger.warning("the sources of run %s could not be sent", summary.id)
        return EditResult(ok=True, run=summary)

    def close(self) -> None:
        """Close the session."""
        if self.session_id:
            self.client.close_edit(self.session_id)


def run_edit(
    client: Client,
    path: Path,
    sources: dict[str, Path],
    group: str | None,
    *,
    should_stop: Callable[[], bool] = lambda: False,
) -> None:
    """Open a session, print its link and serve it until stopped.

    Args:
        client: The client of the service.
        path: The workflow file.
        sources: Working trees by source name.
        group: Group of the runs.
        should_stop: Checked after every step, true ends the loop.
    """
    editor = Editor(client, path, sources, group)
    session = editor.open()
    typer.echo(f"Editing {session.path} on {session.host}")
    typer.echo(f"Open {client.url}/ui/#/edit/{session.id}")
    try:
        while not should_stop():
            editor.step(EDIT_POLL_S)
    finally:
        editor.close()
