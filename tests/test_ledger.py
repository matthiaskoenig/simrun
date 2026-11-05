import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from simrun.ledger import Ledger

CLAIMER = """
import sys
from pathlib import Path
from simrun.ledger import Ledger
ledger = Ledger.create(Path(sys.argv[1]), int(sys.argv[3]))
worker = sys.argv[2]
while (i := ledger.claim(worker)) is not None:
    print(i, flush=True)
"""

HOLDER = """
import sys, time
from pathlib import Path
from simrun.ledger import Ledger
ledger = Ledger.create(Path(sys.argv[1]), int(sys.argv[3]))
print(ledger.claim(sys.argv[2]), flush=True)
time.sleep(60)
"""


def test_claim_order(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 3)
    assert [ledger.claim("w") for _ in range(4)] == [0, 1, 2, None]


def test_complete_and_counts(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 3)
    assert ledger.counts().pending == 3
    a = ledger.claim("w")
    b = ledger.claim("w")
    assert a is not None and b is not None
    ledger.complete(a, "w")
    counts = ledger.counts()
    assert (counts.pending, counts.claimed, counts.done, counts.total) == (1, 1, 1, 3)
    assert not ledger.complete_ledger()
    assert ledger.missing() == [1, 2]
    ledger.complete(b, "w")
    c = ledger.claim("w")
    assert c is not None
    ledger.complete(c, "w")
    assert ledger.complete_ledger()
    assert ledger.missing() == []


def test_complete_unclaimed_raises(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 2)
    ledger.claim("w0")
    with pytest.raises(ValueError):
        ledger.complete(0, "w1")
    with pytest.raises(ValueError):
        ledger.release(1, "w0")


def test_release_all_returns_own_claims_only(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 3)
    assert ledger.claim("w0") == 0
    assert ledger.claim("w0") == 1
    assert ledger.claim("w1") == 2
    ledger.release(0, "w0")
    assert ledger.release_all("w0") == [1]
    assert ledger.counts().claimed == 1
    assert ledger.claim("w2") == 0


def test_create_is_idempotent(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 3)
    ledger.claim("w")
    again = Ledger.create(tmp_path / "units", 3)
    assert again.counts().claimed == 1
    assert again.counts().total == 3


def test_create_other_size_raises(tmp_path: Path) -> None:
    Ledger.create(tmp_path / "units", 3)
    with pytest.raises(ValueError):
        Ledger.create(tmp_path / "units", 4)


def test_half_created_ledger_is_not_taken(tmp_path: Path) -> None:
    root = tmp_path / "units"
    (tmp_path / ".units.tmp.crashed").mkdir()
    ledger = Ledger.create(root, 2)
    assert ledger.counts().total == 2


def test_zero_units(tmp_path: Path) -> None:
    ledger = Ledger.create(tmp_path / "units", 0)
    assert ledger.claim("w") is None
    assert ledger.complete_ledger()


def test_concurrent_claims_never_duplicate(tmp_path: Path) -> None:
    root = tmp_path / "units"
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", CLAIMER, str(root), f"w{k}", "2000"],
            stdout=subprocess.PIPE,
            text=True,
        )
        for k in range(8)
    ]
    claimed: list[int] = []
    for proc in procs:
        out, _ = proc.communicate(timeout=120)
        assert proc.returncode == 0
        claimed += [int(x) for x in out.split()]
    assert sorted(claimed) == list(range(2000))


def test_killed_worker_units_recomputed(tmp_path: Path) -> None:
    root = tmp_path / "units"
    proc = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(root), "dead", "5"],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    held = int(proc.stdout.readline())
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait()
    ledger = Ledger.create(root, 5)
    assert ledger.release_all("dead") == [held]
    completed: list[int] = []
    while (i := ledger.claim("alive")) is not None:
        ledger.complete(i, "alive")
        completed.append(i)
    counts = ledger.counts()
    assert counts.done == counts.total == 5
    assert sorted(completed) == list(range(5))


def test_create_lost_race_with_stale_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "units"
    Ledger.create(root, 3).claim("w")
    real_exists = Path.exists
    monkeypatch.setattr(
        Path, "exists", lambda self: False if self == root else real_exists(self)
    )
    ledger = Ledger.create(root, 3)
    assert ledger.counts().claimed == 1
    assert not list(tmp_path.glob(".units.tmp.*"))


def test_create_reraises_other_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import errno

    def deny(src: object, dst: object) -> None:
        raise OSError(errno.EACCES, "denied")

    monkeypatch.setattr("simrun.ledger.os.rename", deny)
    with pytest.raises(PermissionError):
        Ledger.create(tmp_path / "units", 3)
    assert not list(tmp_path.glob(".units.tmp.*"))


def test_create_retries_stale_size_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "units"
    Ledger.create(root, 3)
    real = Path.read_text
    calls = []

    def flaky(self: Path, *args: object, **kwargs: object) -> str:
        if self.name == "size" and len(calls) < 2:
            calls.append(1)
            raise FileNotFoundError(str(self))
        return real(self, *args, **kwargs)  # ty: ignore[invalid-argument-type]

    monkeypatch.setattr(Path, "read_text", flaky)
    assert Ledger.create(root, 3).counts().total == 3
    assert len(calls) == 2


def test_claim_whose_rename_was_resent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = Ledger.create(tmp_path / "units", 2)
    real = os.rename
    fired = []

    def resent(src: str | Path, dst: str | Path) -> None:
        real(src, dst)
        if not fired:
            fired.append(1)
            raise FileNotFoundError(str(src))

    monkeypatch.setattr("simrun.ledger.os.rename", resent)
    assert ledger.claim("w") == 0
    assert ledger.claim("w") == 1
    assert ledger.counts().claimed == 2
