import os
import subprocess
import sys
from pathlib import Path

import pytest

from simrun.snapshot import snapshot


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    git(r, "config", "user.email", "a@b.c")
    git(r, "config", "user.name", "T")
    (r / ".gitignore").write_text("big.bin\n")
    (r / "a.py").write_text("a = 1\n")
    (r / "gone.py").write_text("gone\n")
    (r / "same.txt").write_text("same\n")
    git(r, "add", "-A")
    git(r, "commit", "-q", "-m", "init")
    return r


def test_snapshot_contents(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "a.py").write_text("a = 2\n")
    (repo / "new.py").write_text("new\n")
    (repo / "big.bin").write_text("x" * 100)
    (repo / "gone.py").unlink()
    prov = snapshot(repo, store, "proj")
    snap = store / prov.snapshot
    assert (snap / "a.py").read_text() == "a = 2\n"
    assert (snap / "new.py").exists()
    assert not (snap / "big.bin").exists()
    assert not (snap / "gone.py").exists()
    assert not (snap / ".git").exists()
    assert prov.dirty
    lines = [line.strip() for line in prov.changed_files]
    assert {"M a.py", "?? new.py", "D gone.py"} <= set(lines)
    assert prov.branch == "main"
    assert prov.commit == git(repo, "rev-parse", "HEAD").strip()
    assert prov.patch is not None
    assert "a = 2" in (store / prov.patch).read_text()
    assert (store / "snapshots" / "proj" / "latest").resolve() == snap.resolve()


def test_clean_snapshot_has_no_patch(repo: Path, tmp_path: Path) -> None:
    prov = snapshot(repo, tmp_path / "store", "proj")
    assert not prov.dirty
    assert prov.patch is None
    assert prov.changed_files == []


def test_detached_head_and_worktree(repo: Path, tmp_path: Path) -> None:
    wt = tmp_path / "wt"
    git(repo, "worktree", "add", "-q", "--detach", str(wt))
    assert wt.joinpath(".git").is_file()
    prov = snapshot(wt, tmp_path / "store", "proj")
    assert prov.branch is None
    assert (tmp_path / "store" / prov.snapshot / "a.py").exists()


def test_unchanged_files_are_hard_links(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    p1 = snapshot(repo, store, "proj")
    (repo / "a.py").write_text("a = 3\n")
    p2 = snapshot(repo, store, "proj")
    assert p1.snapshot != p2.snapshot
    s1, s2 = store / p1.snapshot, store / p2.snapshot
    assert os.stat(s1 / "same.txt").st_ino == os.stat(s2 / "same.txt").st_ino
    assert os.stat(s1 / "a.py").st_ino != os.stat(s2 / "a.py").st_ino


def test_identical_trees_stored_once(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "new.py").write_text("n\n")
    assert snapshot(repo, store, "proj") == snapshot(repo, store, "proj")


def test_concurrent_snapshots(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "new.py").write_text("n\n")
    code = (
        "import sys; from pathlib import Path; from simrun.snapshot import snapshot;"
        "print(snapshot(Path(sys.argv[1]), Path(sys.argv[2]), 'proj').snapshot)"
    )
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(repo), str(store)],
            stdout=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    outs = [p.communicate()[0].strip() for p in procs]
    assert all(p.returncode == 0 for p in procs)
    assert len(set(outs)) == 1
    assert (store / outs[0] / "new.py").exists()
    assert (store / outs[0] / "a.py").exists()
    assert not list((store / "snapshots" / "proj").glob(".tmp-*"))
