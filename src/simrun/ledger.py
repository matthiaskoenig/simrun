"""The unit ledger: workers of a split task claim units through atomic renames."""

import errno
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Self

logger = logging.getLogger(__name__)

SIZE_FILE = "size"
HEARTBEATS_DIR = "heartbeats"
SIZE_RETRIES = 20
SIZE_RETRY_SECONDS = 0.25


@dataclass(frozen=True)
class LedgerCounts:
    """The number of units per state."""

    pending: int
    claimed: int
    done: int
    total: int


def _check_worker(worker: str) -> None:
    if not worker or "@" in worker or "/" in worker:
        raise ValueError(f"invalid worker id {worker!r}")


class Ledger:
    """A directory with `pending/<i>`, `claimed/<i>@<worker>` and `done/<i>`.

    Workers touch `heartbeats/<worker>`, so that the claims of a worker which died
    without releasing them are found by the age of its heartbeat (`release_stale`).

    Every transition is one `os.rename`, atomic on a local file system and on NFS v4,
    so a unit is held by at most one worker. On NFS the mount must be NFS >= 4.1
    (sessions give renames exactly-once semantics; `claim` also tolerates a resent
    rename that answers ENOENT although it happened).
    """

    def __init__(self, root: Path, n_units: int) -> None:
        """Open the ledger at `root` of `n_units` units (see `create`)."""
        self.root = root
        self.n_units = n_units
        self._candidates: list[int] | None = None
        self._position = 0

    @classmethod
    def create(cls, root: Path, n_units: int) -> Self:
        """Create the ledger, or open an existing one of the same size.

        The ledger is built in a temporary directory and renamed, so it appears
        complete or not at all, also when several processes create it at once or one
        of them crashes.

        Args:
            root: The ledger directory.
            n_units: The number of units, indices `0..n_units-1`.

        Returns:
            The ledger.

        Raises:
            ValueError: An existing ledger has another size.
        """
        if not root.exists():
            root.parent.mkdir(parents=True, exist_ok=True)
            tmp = root.parent / f".{root.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}"
            try:
                (tmp / "pending").mkdir(parents=True)
                (tmp / "claimed").mkdir()
                (tmp / "done").mkdir()
                for i in range(n_units):
                    os.close(
                        os.open(
                            tmp / "pending" / str(i),
                            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                            0o644,
                        )
                    )
                (tmp / SIZE_FILE).write_text(str(n_units))
                try:
                    os.rename(tmp, root)
                except OSError as error:
                    # Another process published the ledger first. A cached lookup
                    # on NFS may still deny that root exists, so decide by errno.
                    if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                        raise
                    logger.debug("ledger %s was created by another process", root)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
        size = cls._read_size(root)
        if size != n_units:
            raise ValueError(f"ledger {root} has {size} units, not {n_units}")
        return cls(root, size)

    @staticmethod
    def _read_size(root: Path) -> int:
        """Read the size file, retrying while a cached negative lookup expires."""
        for attempt in range(SIZE_RETRIES):
            try:
                return int((root / SIZE_FILE).read_text())
            except FileNotFoundError:
                if attempt == SIZE_RETRIES - 1:
                    raise
                time.sleep(SIZE_RETRY_SECONDS)
        raise AssertionError("unreachable")

    def _names(self, state: str) -> list[str]:
        return os.listdir(self.root / state)

    def claim(self, worker: str) -> int | None:
        """Claim the lowest pending unit, or return `None` when none is pending."""
        _check_worker(worker)
        # A cached listing may have been passed by released units; list afresh once.
        for fresh in (self._candidates is None, True):
            if self._candidates is None:
                self._candidates = sorted(int(n) for n in self._names("pending"))
                self._position = 0
            while self._position < len(self._candidates):
                index = self._candidates[self._position]
                self._position += 1
                try:
                    os.rename(
                        self.root / "pending" / str(index),
                        self.root / "claimed" / f"{index}@{worker}",
                    )
                except FileNotFoundError:
                    # Another worker won it, or a resent rename of ours (NFS) that
                    # had happened answered ENOENT: then the claim is ours.
                    if (self.root / "claimed" / f"{index}@{worker}").exists():
                        return index
                    continue
                return index
            self._candidates = None
            if fresh:
                return None
        return None

    def complete(self, index: int, worker: str) -> None:
        """Mark a unit claimed by `worker` as done."""
        _check_worker(worker)
        try:
            os.rename(
                self.root / "claimed" / f"{index}@{worker}",
                self.root / "done" / str(index),
            )
        except FileNotFoundError:
            raise ValueError(f"unit {index} is not claimed by {worker}") from None

    def release(self, index: int, worker: str) -> None:
        """Return a unit claimed by `worker` to pending."""
        _check_worker(worker)
        try:
            os.rename(
                self.root / "claimed" / f"{index}@{worker}",
                self.root / "pending" / str(index),
            )
        except FileNotFoundError:
            raise ValueError(f"unit {index} is not claimed by {worker}") from None
        self._candidates = None

    def release_all(self, worker: str) -> list[int]:
        """Free every unit claimed by `worker`, as a restarted worker does.

        Returns:
            The indices released, sorted.
        """
        _check_worker(worker)
        suffix = f"@{worker}"
        released: list[int] = []
        for name in self._names("claimed"):
            if name.endswith(suffix):
                index = int(name[: -len(suffix)])
                try:
                    self.release(index, worker)
                except ValueError:
                    continue
                released.append(index)
        return sorted(released)

    def heartbeat(self, worker: str) -> None:
        """Touch the heartbeat file of `worker`, created with its directory lazily.

        Its modification time is set by the file server, so the heartbeats of workers
        on different servers compare without their clocks.

        Raises:
            OSError: The time of the file was not set.
        """
        _check_worker(worker)
        path = self.root / HEARTBEATS_DIR / worker
        try:
            # not Path.touch, which ignores a failure of utime: a heartbeat which
            # does not advance must raise
            os.utime(path, None)
        except FileNotFoundError:
            path.parent.mkdir(exist_ok=True)
            os.close(os.open(path, os.O_CREAT | os.O_WRONLY, 0o644))

    def _mtime(self, worker: str) -> float | None:
        """The time of the last heartbeat of `worker`, `None` without one.

        The file is opened, which revalidates its attributes on NFS (close-to-open).
        """
        try:
            fd = os.open(self.root / HEARTBEATS_DIR / worker, os.O_RDONLY)
        except FileNotFoundError:
            return None
        try:
            return os.fstat(fd).st_mtime
        finally:
            os.close(fd)

    def release_stale(self, stale_s: float, reference: str) -> list[int]:
        """Return the claims of workers without a recent heartbeat to pending.

        The heartbeat of `reference` is touched and its time is "now"; a claim of
        another worker which has no heartbeat or one older than `now - stale_s` is
        released. The claims of `reference` are never released.

        Args:
            stale_s: Seconds after which a heartbeat is stale.
            reference: The worker which releases, e.g. the coordinator.

        Returns:
            The indices released, sorted.

        Raises:
            OSError: The heartbeat of `reference` was not touched.
        """
        self.heartbeat(reference)
        now = self._mtime(reference)
        assert now is not None
        beats: dict[str, float | None] = {}
        released: list[int] = []
        for name in self._names("claimed"):
            index, _, worker = name.partition("@")
            if worker == reference:
                continue
            if worker not in beats:
                beats[worker] = self._mtime(worker)
            beat = beats[worker]
            if beat is not None and beat >= now - stale_s:
                continue
            try:
                os.rename(self.root / "claimed" / name, self.root / "pending" / index)
            except FileNotFoundError:
                # completed, released or released by another reference meanwhile
                continue
            logger.info("released unit %s of %s, its heartbeat is stale", index, worker)
            released.append(int(index))
        if released:
            self._candidates = None
        return sorted(released)

    def counts(self) -> LedgerCounts:
        """The number of units per state."""
        pending = len(self._names("pending"))
        claimed = len(self._names("claimed"))
        done = len(self._names("done"))
        return LedgerCounts(pending, claimed, done, self.n_units)

    def claims(self) -> dict[str, list[int]]:
        """The claimed units by worker id, the indices sorted."""
        claimed: dict[str, list[int]] = {}
        for name in self._names("claimed"):
            index, _, worker = name.partition("@")
            claimed.setdefault(worker, []).append(int(index))
        return {worker: sorted(units) for worker, units in claimed.items()}

    def complete_ledger(self) -> bool:
        """Whether every unit is done."""
        return len(self._names("done")) == self.n_units

    def missing(self) -> list[int]:
        """The units not done, sorted."""
        done = {int(n) for n in self._names("done")}
        return [i for i in range(self.n_units) if i not in done]
