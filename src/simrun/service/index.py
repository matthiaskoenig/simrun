"""The index of the runs in SQLite (spec section 4), beside the run store.

The store stays the source of truth: the index is rebuilt from the records at the
start of the service and updated after every record the service writes
(`IndexedRunStore.write`), the record first. It holds what the controller and the
lists need without reading the records over NFS: the id, the owner, the agent, the
group, the name of the workflow, the state, the times of creation and end, the
pending release and cancellation, the snapshots and the summary of every run. The
controller follows only the live runs (not ended, or with a release or a cancellation
pending); `GET /runs`, a cleanup and the usage filter in the index.
"""

import json
import logging
import sqlite3
import threading
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from simrun.store import RunRecord, RunState, RunStore, RunSummary

logger = logging.getLogger(__name__)

TERMINAL = (RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELLED)
COLUMNS = (
    "id, owner, agent, grp, workflow, state, created, finished, pending_release, "
    "pending_cancel, pending_logs, snapshots, summary"
)


@dataclass(frozen=True)
class IndexedRun:
    """A run as the index has it.

    Attributes:
        summary: Its summary.
        owner: Its owner.
        workflow: The name of its workflow.
        pending: Whether a release of its node or a cancellation of its workflow is
            pending.
        snapshots: The snapshots of its sources, relative to the store.
    """

    summary: RunSummary
    owner: str
    workflow: str
    pending: bool
    snapshots: tuple[str, ...]


def _row(record: RunRecord) -> tuple[object, ...]:
    return (
        record.id,
        record.owner,
        record.agent,
        record.group,
        record.workflow.name,
        record.state.value,
        record.created.isoformat(),
        None if record.finished is None else record.finished.isoformat(),
        record.pending_release,
        int(record.pending_cancel),
        int(record.pending_logs),
        json.dumps([s.snapshot for s in record.sources]),
        RunSummary.of(record).model_dump_json(),
    )


class RunIndex:
    """The index of the runs in a SQLite file on a local volume.

    A connection is opened per call, so the index is safe to use from several
    threads of one process.
    """

    def __init__(self, db: Path) -> None:
        """Create the table if needed.

        Args:
            db: Path of the SQLite file.
        """
        self.db = db
        db.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            columns = {row[1] for row in con.execute("PRAGMA table_info(runs)")}
            if columns and "pending_logs" not in columns:
                # an index of an earlier version: a cache, rebuilt at the start
                con.execute("DROP TABLE runs")
            con.execute(
                "CREATE TABLE IF NOT EXISTS runs ("
                "id TEXT PRIMARY KEY, owner TEXT NOT NULL, agent TEXT, grp TEXT, "
                "workflow TEXT NOT NULL, state TEXT NOT NULL, created TEXT NOT NULL, "
                "finished TEXT, pending_release TEXT, pending_cancel INTEGER NOT NULL, "
                "pending_logs INTEGER NOT NULL DEFAULT 0, "
                "snapshots TEXT NOT NULL, summary TEXT NOT NULL)"
            )
            con.execute("CREATE INDEX IF NOT EXISTS runs_state ON runs (state)")
            con.commit()

    def _connect(self) -> closing[sqlite3.Connection]:
        return closing(sqlite3.connect(self.db, timeout=30))

    def rebuild(self, records: Iterable[RunRecord]) -> None:
        """Replace the index by the runs of `records`."""
        rows = [_row(r) for r in records]
        with self._connect() as con:
            con.execute("DELETE FROM runs")
            con.executemany(f"INSERT INTO runs ({COLUMNS}) VALUES ({_marks()})", rows)
            con.commit()
        logger.info("the index of the runs was rebuilt: %d runs", len(rows))

    def put(self, record: RunRecord) -> None:
        """Add or update a run."""
        with self._connect() as con:
            con.execute(
                f"INSERT OR REPLACE INTO runs ({COLUMNS}) VALUES ({_marks()})",
                _row(record),
            )
            con.commit()

    def remove(self, run_id: str) -> None:
        """Remove a run."""
        with self._connect() as con:
            con.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            con.commit()

    def any_unended(self) -> bool:
        """Whether a run has not ended, without reading its row."""
        terminal = ", ".join("?" for _ in TERMINAL)
        with self._connect() as con:
            row = con.execute(
                f"SELECT 1 FROM runs WHERE state NOT IN ({terminal}) LIMIT 1",
                [s.value for s in TERMINAL],
            ).fetchone()
        return row is not None

    def live_ids(self) -> list[str]:
        """The ids of the runs the controller follows, sorted.

        Runs which have not ended, and ended runs with a release of their node, a
        cancellation of their workflow or the logs of their pods pending.
        """
        terminal = ", ".join("?" for _ in TERMINAL)
        with self._connect() as con:
            rows = con.execute(
                f"SELECT id FROM runs WHERE state NOT IN ({terminal}) "
                "OR pending_release IS NOT NULL OR pending_cancel OR pending_logs "
                "ORDER BY id",
                [s.value for s in TERMINAL],
            ).fetchall()
        return [r[0] for r in rows]

    def runs(
        self,
        group: str | None = None,
        owner: str | None = None,
        agent: str | None = None,
        state: RunState | None = None,
    ) -> list[IndexedRun]:
        """The runs with the given group, owner, agent and state, sorted by id."""
        filters = {"grp": group, "owner": owner, "agent": agent}
        clauses = [f"{column} = ?" for column, v in filters.items() if v is not None]
        values: list[object] = [v for v in filters.values() if v is not None]
        if state is not None:
            clauses.append("state = ?")
            values.append(state.value)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as con:
            rows = con.execute(
                "SELECT summary, owner, workflow, pending_release, pending_cancel, "
                f"snapshots FROM runs {where} ORDER BY id",
                values,
            ).fetchall()
        return [
            IndexedRun(
                summary=RunSummary.model_validate_json(summary),
                owner=owner_,
                workflow=workflow,
                pending=release is not None or bool(cancel),
                snapshots=tuple(json.loads(snapshots)),
            )
            for summary, owner_, workflow, release, cancel, snapshots in rows
        ]


def _marks() -> str:
    return ", ".join("?" for _ in COLUMNS.split(","))


class IndexedRunStore(RunStore):
    """The run store of the service: every record it writes is indexed too.

    A record whose index row could not be written (the index raised after the
    record was written) and a record which could not be read when the index was
    rebuilt are kept aside and indexed again at every `reindex`, which the
    controller calls at every poll; the ones which still cannot be read are logged
    then.
    """

    def __init__(self, root: Path, index: RunIndex) -> None:
        """Create the store.

        Args:
            root: Root directory of the store.
            index: The index of its runs.
        """
        super().__init__(root)
        self.index = index
        self._lock = threading.Lock()
        self._unindexed: set[str] = set()

    def write(self, record: RunRecord) -> None:
        """Write a record, then index it.

        A failure of the index is logged and the run indexed at the next `reindex`:
        the record is written, so the caller goes on.
        """
        super().write(record)
        try:
            self.index.put(record)
        except Exception:
            logger.exception("indexing run %s failed, retried later", record.id)
            with self._lock:
                self._unindexed.add(record.id)
            return
        with self._lock:
            self._unindexed.discard(record.id)

    def forget(self, run_id: str) -> None:
        """Remove a run from the index, once it was moved out of the runs."""
        with self._lock:
            self._unindexed.discard(run_id)
        self.index.remove(run_id)

    def ids(self) -> list[str]:
        """The ids of the runs in the store, sorted; hidden directories are no runs."""
        runs = self.root / "runs"
        if not runs.is_dir():
            return []
        return sorted(p.name for p in runs.iterdir() if not p.name.startswith("."))

    def rebuild(self) -> None:
        """Rebuild the index from the records.

        A record which cannot be read is logged, left out and tried again at every
        `reindex`.
        """
        records: list[RunRecord] = []
        unreadable: set[str] = set()
        for run_id in self.ids():
            try:
                records.append(self.read(run_id))
            except KeyError:
                continue
            except (ValueError, OSError) as error:
                logger.warning("the record of run %s cannot be read: %s", run_id, error)
                unreadable.add(run_id)
        self.index.rebuild(records)
        with self._lock:
            self._unindexed = unreadable

    def reindex(self) -> list[str]:
        """Index the runs whose index row is missing, from their records.

        A record which cannot be read is logged at every call, so a run the
        controller cannot follow is never silent. Only the index row is written,
        never the record file, so the caller holds the lock of the record writes
        around the call: a record is read and indexed without a write in between.

        Returns:
            The runs whose records were read but whose index rows still cannot be
            written, which the controller follows besides the live runs.
        """
        with self._lock:
            pending = sorted(self._unindexed)
        unindexed: list[str] = []
        for run_id in pending:
            try:
                record = self.read(run_id)
            except KeyError:
                with self._lock:
                    self._unindexed.discard(run_id)
                continue
            except (ValueError, OSError) as error:
                logger.warning(
                    "the record of run %s cannot be read, the run is not followed: %s",
                    run_id,
                    error,
                )
                continue
            try:
                self.index.put(record)
            except Exception:
                logger.exception("indexing run %s failed, retried later", run_id)
                unindexed.append(run_id)
                continue
            with self._lock:
                self._unindexed.discard(run_id)
        return unindexed
