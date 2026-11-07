"""Edit sessions: how the browser asks the machine with the code to save and run.

A session lives in the store under `edits/<id>/`: `session.json`, and in
`requests/` the `<rid>.json` of a request of the browser and the
`<rid>.result.json` with the answer of the command `simrun edit`. All files are
written atomically, and the service keeps nothing in memory.
"""

from __future__ import annotations

import builtins
import fcntl
import logging
import os
import secrets
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, model_validator

from simrun.service.auth import Identity
from simrun.snapshot import SourceProvenance
from simrun.store import RunSummary
from simrun.workflow import SplitMode, Workflow

logger = logging.getLogger(__name__)

EDIT_CONNECTED_S = 30.0
# how long a save or a run of the browser waits for the answer of the command; a
# request older than this is no longer waited for
EDIT_WAIT_S = 60.0
SESSION_FILE = "session.json"
LOCK_FILE = ".lock"
REQUESTS_DIR = "requests"
RESULT_SUFFIX = ".result.json"


class TreeState(BaseModel):
    """The live state of the working tree of a source.

    Attributes:
        name: Name of the source.
        branch: The branch, none when detached.
        commit: The commit.
        changed_files: The files which differ from the commit.
    """

    name: str
    branch: str | None
    commit: str
    changed_files: list[str]


class EditSession(BaseModel):
    """An edit session.

    Attributes:
        id: Id of the session.
        owner: The user of the token which opened it.
        host: The machine of the command.
        path: The absolute path of the workflow file there.
        workflow: The workflow as in the file.
        file_sha256: Hash of the file content, empty for a file which does not exist.
        sources: The provenance of the last snapshots.
        tree: Per source the state of the working tree.
        created: When the session was opened.
        seen: The last poll of the command.
        closed: The command stopped.
        connected: Not closed and seen lately; computed when read.
    """

    id: str
    owner: str
    host: str
    path: str
    workflow: Workflow
    file_sha256: str
    sources: list[SourceProvenance]
    tree: list[TreeState]
    created: datetime
    seen: datetime
    closed: bool = False
    connected: bool = False


class OpenEdit(BaseModel):
    """The body which opens a session."""

    host: str
    path: str
    workflow: Workflow
    file_sha256: str
    sources: list[SourceProvenance]
    tree: list[TreeState]


class EditState(BaseModel):
    """What the command read from disk, sent when it changed."""

    workflow: Workflow
    file_sha256: str
    tree: list[TreeState]
    sources: list[SourceProvenance] | None = None


class EditRequest(BaseModel):
    """A request of the browser to the command.

    Attributes:
        id: Id of the request.
        kind: `save` writes the file, `run` submits the workflow.
        workflow: The workflow of the draft.
        base: For a save, the hash of the file the draft started from.
        force: Save although the file changed on disk.
        group: Group of the run.
        split: Split mode of the run.
        reserve: Node reserved for the run.
        created: When the request was made.
    """

    id: str
    kind: Literal["save", "run"]
    workflow: Workflow
    base: str
    force: bool = False
    group: str | None = None
    split: SplitMode | None = None
    reserve: str | None = None
    created: datetime


class EditResult(BaseModel):
    """The answer of the command to a request.

    Attributes:
        ok: Whether the request was done.
        status: The status of a refusal, 4xx or 5xx: 409 conflict, 422 refused.
        detail: The reason of a refusal.
        file_sha256: For a save, the hash of the written file.
        run: For a run, the submitted run.
    """

    ok: bool
    status: int = 200
    detail: str = ""
    file_sha256: str | None = None
    run: RunSummary | None = None

    @model_validator(mode="after")
    def _refusal_has_an_error_status(self) -> Self:
        if not self.ok and not 400 <= self.status <= 599:
            raise ValueError(f"a refusal needs a 4xx or 5xx status, not {self.status}")
        return self


class SaveBody(BaseModel):
    """The body of a save."""

    workflow: Workflow
    base: str
    force: bool = False


class RunBody(BaseModel):
    """The body of a run, with the options of `SubmitRequest`."""

    workflow: Workflow
    group: str | None = None
    split: SplitMode | None = None
    reserve: str | None = None


def _write(path: Path, text: str) -> None:
    """Write a file atomically (temporary file in the directory, rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{secrets.token_hex(4)}.tmp"
    tmp.write_text(text)
    os.replace(tmp, path)


def _now() -> datetime:
    return datetime.now(UTC)


class EditStore:
    """The edit sessions under `<store>/edits`."""

    def __init__(self, root: Path) -> None:
        """Create the store.

        Args:
            root: The store of the service.
        """
        self.root = root / "edits"

    def _dir(self, session_id: str) -> Path:
        return self.root / session_id

    @contextmanager
    def _locked(self, session_id: str) -> Generator[None]:
        """Hold the lock of a session while its file is read and written."""
        directory = self._dir(session_id)
        if not (directory / SESSION_FILE).is_file():
            raise KeyError(session_id)
        with (directory / LOCK_FILE).open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            yield

    def _load(self, session_id: str) -> EditSession:
        try:
            text = (self._dir(session_id) / SESSION_FILE).read_text()
        except FileNotFoundError:
            raise KeyError(session_id) from None
        return EditSession.model_validate_json(text)

    def _mutate(
        self, session_id: str, change: Callable[[EditSession], None]
    ) -> EditSession:
        with self._locked(session_id):
            session = self._load(session_id)
            change(session)
            _write(
                self._dir(session_id) / SESSION_FILE,
                session.model_dump_json(exclude={"connected"}, indent=2),
            )
        return self._connected(session)

    @staticmethod
    def _connected(session: EditSession) -> EditSession:
        age = _now() - session.seen
        session.connected = not session.closed and age <= timedelta(
            seconds=EDIT_CONNECTED_S
        )
        return session

    def open(self, identity: Identity, body: OpenEdit) -> EditSession:
        """Open a session owned by the owner of the identity."""
        now = _now()
        session = EditSession(
            id=uuid.uuid4().hex[:12],
            owner=identity.owner,
            created=now,
            seen=now,
            **body.model_dump(),
        )
        _write(
            self._dir(session.id) / SESSION_FILE,
            session.model_dump_json(exclude={"connected"}, indent=2),
        )
        return self._connected(session)

    def read(self, session_id: str) -> EditSession:
        """Read a session, with `connected` computed; `KeyError` when unknown."""
        return self._connected(self._load(session_id))

    def list(self, owner: str) -> builtins.list[EditSession]:
        """The sessions of an owner, connected first, newest first."""
        found: list[EditSession] = []
        for path in self.root.glob(f"*/{SESSION_FILE}"):
            try:
                session = self.read(path.parent.name)
            except KeyError, ValueError:
                logger.warning("the edit session %s cannot be read", path.parent.name)
                continue
            if session.owner == owner:
                found.append(session)
        found.sort(key=lambda s: s.created, reverse=True)
        found.sort(key=lambda s: not s.connected)
        return found

    def update(self, session_id: str, state: EditState) -> EditSession:
        """Store what the command read from disk."""

        def change(session: EditSession) -> None:
            session.workflow = state.workflow
            session.file_sha256 = state.file_sha256
            session.tree = state.tree
            if state.sources is not None:
                session.sources = state.sources

        return self._mutate(session_id, change)

    def seen(self, session_id: str) -> EditSession:
        """Record a poll of the command."""

        def change(session: EditSession) -> None:
            session.seen = _now()

        return self._mutate(session_id, change)

    def close(self, session_id: str) -> EditSession:
        """Close a session, the command stopped."""

        def change(session: EditSession) -> None:
            session.closed = True

        return self._mutate(session_id, change)

    def saved(
        self, session_id: str, workflow: Workflow, sha256: str | None
    ) -> EditSession:
        """Record a save: the workflow, and the hash when the result carried one."""

        def change(session: EditSession) -> None:
            session.workflow = workflow
            if sha256 is not None:
                session.file_sha256 = sha256

        return self._mutate(session_id, change)

    def add_request(
        self,
        session_id: str,
        kind: Literal["save", "run"],
        workflow: Workflow,
        base: str = "",
        force: bool = False,
        group: str | None = None,
        split: SplitMode | None = None,
        reserve: str | None = None,
    ) -> EditRequest:
        """Write a request for the command to answer."""
        self._load(session_id)
        request = EditRequest(
            id=uuid.uuid4().hex,
            kind=kind,
            workflow=workflow,
            base=base,
            force=force,
            group=group,
            split=split,
            reserve=reserve,
            created=_now(),
        )
        _write(
            self._dir(session_id) / REQUESTS_DIR / f"{request.id}.json",
            request.model_dump_json(indent=2),
        )
        return request

    def requests(self, session_id: str) -> builtins.list[EditRequest]:
        """The requests without an answer, oldest first.

        A request older than `EDIT_WAIT_S` by the clock of the service is no longer
        waited for, e.g. one left by a service which stopped while it waited: it is
        dropped instead of returned, so that the command does not do it late.
        """
        directory = self._dir(session_id) / REQUESTS_DIR
        stale = _now() - timedelta(seconds=EDIT_WAIT_S)
        found: list[EditRequest] = []
        for path in directory.glob("*.json"):
            if path.name.endswith(RESULT_SUFFIX):
                continue
            if (directory / f"{path.stem}{RESULT_SUFFIX}").exists():
                continue
            try:
                request = EditRequest.model_validate_json(path.read_text())
            except FileNotFoundError, ValueError:
                continue  # dropped or half gone meanwhile
            if request.created < stale:
                logger.info("the stale edit request %s is dropped", request.id)
                self.drop_request(session_id, request.id)
                continue
            found.append(request)
        found.sort(key=lambda r: r.created)
        return found

    def answer(self, session_id: str, request_id: str, result: EditResult) -> None:
        """Store the answer of the command; `KeyError` for an unknown request."""
        directory = self._dir(session_id) / REQUESTS_DIR
        if not (directory / f"{request_id}.json").is_file():
            raise KeyError(request_id)
        _write(directory / f"{request_id}{RESULT_SUFFIX}", result.model_dump_json())

    def result(self, session_id: str, request_id: str) -> EditResult | None:
        """The answer to a request, none while there is none."""
        path = self._dir(session_id) / REQUESTS_DIR / f"{request_id}{RESULT_SUFFIX}"
        try:
            return EditResult.model_validate_json(path.read_text())
        except FileNotFoundError:
            return None

    def drop_request(self, session_id: str, request_id: str) -> None:
        """Delete a request and its answer."""
        directory = self._dir(session_id) / REQUESTS_DIR
        for name in (f"{request_id}.json", f"{request_id}{RESULT_SUFFIX}"):
            (directory / name).unlink(missing_ok=True)


__all__ = [
    "EDIT_CONNECTED_S",
    "EDIT_WAIT_S",
    "EditRequest",
    "EditResult",
    "EditSession",
    "EditState",
    "EditStore",
    "OpenEdit",
    "RunBody",
    "SaveBody",
    "TreeState",
]
