"""The unit ledger: workers of a split task claim units through atomic renames."""

import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Self

logger = logging.getLogger(__name__)

SIZE_FILE = "size"


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

    Every transition is one `os.rename`, atomic on a local file system and on NFS v4,
    so a unit is held by at most one worker.
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
                    (tmp / "pending" / str(i)).touch()
                (tmp / SIZE_FILE).write_text(str(n_units))
                try:
                    tmp.rename(root)
                except OSError:
                    if not root.exists():
                        raise
                    logger.debug("ledger %s was created by another process", root)
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
        size = int((root / SIZE_FILE).read_text())
        if size != n_units:
            raise ValueError(f"ledger {root} has {size} units, not {n_units}")
        return cls(root, size)

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

    def counts(self) -> LedgerCounts:
        """The number of units per state."""
        pending = len(self._names("pending"))
        claimed = len(self._names("claimed"))
        done = len(self._names("done"))
        return LedgerCounts(pending, claimed, done, self.n_units)

    def complete_ledger(self) -> bool:
        """Whether every unit is done."""
        return len(self._names("done")) == self.n_units

    def missing(self) -> list[int]:
        """The units not done, sorted."""
        done = {int(n) for n in self._names("done")}
        return [i for i in range(self.n_units) if i not in done]
