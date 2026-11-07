"""The command `simrun edit`: the machine with the code behind the Editor.

It opens an edit session in the service, writes the saves of the browser into the
workflow file, runs the workflow with fresh snapshots of the working trees, and
sends the file and the trees to the session when they change.
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import secrets
import shutil
import socket
import subprocess
import time
from collections.abc import Callable
from pathlib import Path

import httpx
import typer

from simrun.client import BACKOFF_MAX_S, BACKOFF_START_S, Client, ClientError
from simrun.service.edits import (
    EditRequest,
    EditResult,
    EditSession,
    EditState,
    OpenEdit,
    TreeState,
)
from simrun.snapshot import SourceProvenance, snapshot
from simrun.workflow import Workflow, WorkflowError, load_workflow, workflow_yaml

logger = logging.getLogger(__name__)

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


def file_hash(path: Path) -> str:
    """The sha256 of the bytes of a file, empty for a file which does not exist."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except FileNotFoundError:
        return ""


def file_state(path: Path) -> tuple[Workflow, str]:
    """The workflow in a file and the sha256 of its bytes.

    A missing file is a new workflow named by the file stem, with no tasks, and
    the empty hash.

    Args:
        path: The workflow file.

    Returns:
        The workflow and the hash of the file bytes.

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
        self._last: tuple[Workflow, str] | None = None

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
        self._last = (workflow, digest)
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
        self._last = (workflow, digest)

    def step(self, timeout: float) -> None:
        """Poll once for requests and answer them, then watch the file and trees.

        Args:
            timeout: Seconds to wait for a request, at most `EDIT_WATCH_S`: the
                poll is the interval at which the file and trees are watched.
        """
        poll = min(timeout, EDIT_WATCH_S)
        for request in self.client.edit_requests(self.session_id, poll):
            result = self.handle(request)
            self.client.edit_result(self.session_id, request.id, result)
        try:
            workflow, digest = file_state(self.path)
        except WorkflowError as error:
            logger.warning("the workflow file cannot be read yet: %s", error)
            return
        state = EditState(workflow=workflow, file_sha256=digest, tree=self._trees())
        if state != self._sent:
            self._send(workflow, digest)

    def handle(self, request: EditRequest) -> EditResult:
        """Do one request of the browser: save the file or run the workflow."""
        if request.kind == "save":
            return self._save(request)
        return self._run(request)

    def _save(self, request: EditRequest) -> EditResult:
        if not request.force and request.base != file_hash(self.path):
            return EditResult(ok=False, status=409, detail=CONFLICT)
        data = workflow_yaml(request.workflow).encode()
        tmp = self.path.parent / f".{self.path.name}.{secrets.token_hex(4)}.tmp"
        try:
            tmp.write_bytes(data)
            with contextlib.suppress(FileNotFoundError):
                shutil.copymode(self.path, tmp)
            os.replace(tmp, self.path)
        except OSError as error:
            tmp.unlink(missing_ok=True)
            return EditResult(ok=False, status=422, detail=str(error))
        return EditResult(ok=True, file_sha256=hashlib.sha256(data).hexdigest())

    def _run(self, request: EditRequest) -> EditResult:
        try:
            summary = self.client.submit(
                request.workflow,
                self.sources,
                request.group or self.group,
                request.split,
                request.reserve,
            )
        except ClientError as error:
            return EditResult(ok=False, status=error.status, detail=error.detail)
        except (ValueError, RuntimeError, OSError) as error:
            return EditResult(ok=False, status=422, detail=str(error))
        try:
            sources = self.client.status(summary.id).sources
            if self._last is not None:
                self._send(*self._last, sources)
        except ClientError, OSError, httpx.TransportError, RuntimeError:
            logger.warning("the sources of run %s could not be sent", summary.id)
        return EditResult(ok=True, run=summary)

    def close(self) -> None:
        """Close the session."""
        if self.session_id:
            self.client.close_edit(self.session_id)


def _pause(seconds: float, should_stop: Callable[[], bool]) -> None:
    """Sleep, in slices, so that a stop is noticed promptly."""
    end = time.monotonic() + seconds
    while not should_stop() and (left := end - time.monotonic()) > 0:
        time.sleep(min(0.2, left))


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
    backoff = BACKOFF_START_S
    try:
        while not should_stop():
            try:
                editor.step(EDIT_WATCH_S)
                backoff = BACKOFF_START_S
            except httpx.TransportError as error:
                logger.warning("the service is not reachable: %s", error)
                typer.echo(f"The service is not reachable, retrying in {backoff:.0f} s")
                _pause(backoff, should_stop)
                backoff = min(backoff * 2, BACKOFF_MAX_S)
    finally:
        editor.close()
