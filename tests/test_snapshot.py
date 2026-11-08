import os
import subprocess
import sys
from pathlib import Path

import pytest

from simrun.snapshot import provenance, snapshot
from simrun.worker import shadow_tree


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


def test_equal_size_edit_with_restored_mtime(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    old = (repo / "a.py").stat()
    snapshot(repo, store, "proj")
    (repo / "a.py").write_text("a = 9\n")
    os.utime(repo / "a.py", ns=(old.st_atime_ns, old.st_mtime_ns))
    prov = snapshot(repo, store, "proj")
    assert (store / prov.snapshot / "a.py").read_text() == "a = 9\n"


def test_mode_change_gives_other_snapshot(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "run.sh").write_text("x\n")
    p1 = snapshot(repo, store, "proj")
    (repo / "run.sh").chmod(0o755)
    p2 = snapshot(repo, store, "proj")
    assert p1.snapshot != p2.snapshot
    assert os.access(store / p2.snapshot / "run.sh", os.X_OK)
    assert not os.access(store / p1.snapshot / "run.sh", os.X_OK)


def test_symlink_differs_from_file(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "l").symlink_to("a.py")
    p1 = snapshot(repo, store, "proj")
    (repo / "l").unlink()
    (repo / "l").write_text("a.py")
    p2 = snapshot(repo, store, "proj")
    assert p1.snapshot != p2.snapshot
    assert (store / p1.snapshot / "l").is_symlink()
    assert not (store / p2.snapshot / "l").is_symlink()


def test_vanished_file_is_left_out(
    repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import simrun.snapshot as mod

    real = mod._git

    def listing(r: Path, *args: str) -> bytes:
        out = real(r, *args)
        return out + b"ghost.py\0" if args[0] == "ls-files" else out

    monkeypatch.setattr(mod, "_git", listing)
    # the existence check drops the name; make rsync see it anyway
    monkeypatch.setattr(mod.os.path, "lexists", lambda p: True)
    store = tmp_path / "store"
    prov = snapshot(repo, store, "proj")
    assert not (store / prov.snapshot / "ghost.py").exists()
    assert (store / prov.snapshot / "a.py").exists()


def test_failing_command_names_stderr(repo: Path, tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="not a git repository"):
        snapshot(tmp_path, tmp_path / "store", "proj")


def test_directory_symlink_target_is_hashed(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (repo / "d1").mkdir()
    (repo / "d2").mkdir()
    (repo / "l").symlink_to("d1")
    p1 = snapshot(repo, store, "proj")
    (repo / "l").unlink()
    (repo / "l").symlink_to("d2")
    p2 = snapshot(repo, store, "proj")
    assert p1.snapshot != p2.snapshot
    assert os.readlink(store / p1.snapshot / "l") == "d1"
    assert os.readlink(store / p2.snapshot / "l") == "d2"


def test_snapshot_files_are_read_only(repo: Path, tmp_path: Path) -> None:
    (repo / "run.sh").write_text("#!/bin/sh\n")
    (repo / "run.sh").chmod(0o755)
    (repo / "dirty.py").write_text("x = 1\n")
    store = tmp_path / "store"
    prov = snapshot(repo, store, "model")
    root = store / prov.snapshot
    files = [p for p in root.rglob("*") if p.is_file() and not p.is_symlink()]
    assert files
    for path in files:
        assert not path.stat().st_mode & 0o222, path
    assert (root / "run.sh").stat().st_mode & 0o111  # still executable
    assert prov.patch is not None
    assert not (store / prov.patch).stat().st_mode & 0o222
    # a build backend which writes into a file of a shadow tree of the snapshot
    # fails loudly instead of changing the content-addressed snapshot
    shadow = shadow_tree(root, tmp_path / "shadow")
    with pytest.raises(PermissionError):
        (shadow / "same.txt").write_text("changed\n")
    assert (root / "same.txt").read_text() == "same\n"
    # unchanged files of the next snapshot are still hard links
    (repo / "dirty.py").write_text("x = 2\n")
    second = store / snapshot(repo, store, "model").snapshot
    assert os.stat(root / "same.txt").st_ino == os.stat(second / "same.txt").st_ino


def test_provenance_matches_the_snapshot_without_copying(
    repo: Path, tmp_path: Path
) -> None:
    store = tmp_path / "store"
    (repo / "a.py").write_text("a = 2\n")
    (repo / "sub").mkdir()
    (repo / "sub" / "n.py").write_text("n\n")
    (repo / "a.txt").write_text("t\n")
    (repo / "run.sh").write_text("#!/bin/sh\n")
    (repo / "run.sh").chmod(0o755)
    (repo / "l").symlink_to("a.py")
    (repo / "gone.py").unlink()
    prov = provenance(repo, "proj")
    assert not store.exists()
    assert (prov.snapshot, prov.patch) == ("", None)
    assert prov.dirty
    full = snapshot(repo, store, "proj")
    assert prov.model_dump(exclude={"snapshot", "patch"}) == full.model_dump(
        exclude={"snapshot", "patch"}
    )


def test_provenance_hash_follows_the_content(repo: Path) -> None:
    before = provenance(repo, "proj")
    assert not before.dirty
    (repo / "a.py").write_text("a = 3\n")
    assert provenance(repo, "proj").diff_sha256 != before.diff_sha256


def test_hash_of_a_tree_is_the_one_of_release_0_1_0(tmp_path: Path) -> None:
    # the value of the `_tree_hash` of 0.1.0 for this tree with read-only files
    pinned = "517bc4fde6ab69c9a211afa22e867b03c7530df1f814a6da7088bd572d0f5e90"
    repo = tmp_path / "pin"
    for rel, text in {
        "b.txt": "1\n",
        "a.txt": "2\n",
        "sub/x.py": "3\n",
        "sub/deep/y": "4\n",
        "a/z": "5\n",
        "run.sh": "#!/bin/sh\n",
    }.items():
        (repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (repo / rel).write_text(text)
    (repo / "run.sh").chmod(0o755)
    (repo / "link").symlink_to("sub/x.py")
    (repo / "dirlink").symlink_to("sub")
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "a@b.c")
    git(repo, "config", "user.name", "T")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "init")
    assert provenance(repo, "pin").diff_sha256 == pinned
    assert snapshot(repo, tmp_path / "store", "pin").diff_sha256 == pinned
