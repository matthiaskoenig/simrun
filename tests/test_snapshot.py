import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from simrun.snapshot import (
    Scan,
    _tree_hash,
    assemble,
    manifest_path,
    provenance,
    scan,
    snapshot,
)
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
    # the existence check drops the name; keep it, so the scan meets a gone file
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
    # the working tree, which a task imports from (`podspec.import_root`)
    assert prov.path == str(repo.resolve())
    assert prov.dirty
    full = snapshot(repo, store, "proj")
    assert full.path is None
    assert prov.model_dump(exclude={"snapshot", "patch", "path"}) == full.model_dump(
        exclude={"snapshot", "patch", "path"}
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


def _rsync_copy(repo: Path, dest: Path) -> str:
    """The copy of release 0.1.0 (rsync), the reference of the snapshot of today."""
    files = [
        line
        for line in git(
            repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard"
        ).split("\0")
        if line and os.path.lexists(repo / line)
    ]
    dest.mkdir(parents=True)
    subprocess.run(
        [
            *("rsync", "-a", "--checksum", "--chmod=F-w", "--files-from=-"),
            *("--from0", f"{repo}/", f"{dest}/"),
        ],
        input="\0".join(files).encode(),
        check=True,
    )
    return _tree_hash(dest, files)


def _tree(root: Path) -> dict[str, tuple[str, int, bytes]]:
    found = {}
    for path in sorted(root.rglob("*")):
        st = path.lstat()
        if stat.S_ISDIR(st.st_mode):
            found[path.relative_to(root).as_posix()] = (
                "d",
                stat.S_IMODE(st.st_mode),
                b"",
            )
        elif stat.S_ISLNK(st.st_mode):
            found[path.relative_to(root).as_posix()] = (
                "l",
                0,
                os.fsencode(os.readlink(path)),
            )
        elif stat.S_ISREG(st.st_mode):
            found[path.relative_to(root).as_posix()] = (
                "f",
                stat.S_IMODE(st.st_mode),
                path.read_bytes(),
            )
    return found


@pytest.mark.skipif(shutil.which("rsync") is None, reason="needs rsync")
def test_snapshot_equals_the_rsync_copy_of_0_1_0(repo: Path, tmp_path: Path) -> None:
    (repo / "a.py").write_text("a = 2\n")
    (repo / "sub" / "deep").mkdir(parents=True)
    (repo / "sub" / "deep" / "n.py").write_text("n\n")
    (repo / "sub").chmod(0o750)
    (repo / "run.sh").write_text("#!/bin/sh\n")
    (repo / "run.sh").chmod(0o755)
    (repo / "sgid.sh").write_text("#!/bin/sh\n")
    (repo / "sgid.sh").chmod(0o2755)
    (repo / "sticky").write_text("s\n")
    (repo / "sticky").chmod(0o1644)
    (repo / "l").symlink_to("a.py")
    (repo / "dl").symlink_to("sub")
    (repo / "gone.py").unlink()
    reference = tmp_path / "reference"
    diff = _rsync_copy(repo, reference)
    prov = snapshot(repo, tmp_path / "store", "proj")
    commit = git(repo, "rev-parse", "HEAD").strip()
    assert prov.diff_sha256 == diff == provenance(repo, "proj").diff_sha256
    assert Path(prov.snapshot).name == f"{commit[:12]}-{diff[:12]}"
    tree = _tree(tmp_path / "store" / prov.snapshot)
    assert tree == _tree(reference)
    assert tree["sgid.sh"][1] == 0o2555 and tree["sub"][1] == 0o750


def test_scan_lists_files_hashes_and_links(repo: Path) -> None:
    (repo / "a.py").write_text("a = 2\n")
    (repo / "run.sh").write_text("#!/bin/sh\n")
    (repo / "run.sh").chmod(0o775)
    (repo / "l").symlink_to("a.py")
    found = scan(repo)
    assert found.dirty and found.branch == "main"
    assert b"a = 2" in found.patch
    by_path = {f.path: f for f in found.files}
    assert sorted(by_path) == [
        ".gitignore",
        "a.py",
        "gone.py",
        "l",
        "run.sh",
        "same.txt",
    ]
    assert by_path["run.sh"].mode == 0o555  # as a snapshot holds it
    assert by_path["a.py"].mode == 0o444
    assert by_path["a.py"].link is None
    assert by_path["l"].link == "a.py" and by_path["l"].sha256 is None
    assert by_path["a.py"].sha256 == hashlib.sha256(b"a = 2\n").hexdigest()
    assert Scan.model_validate_json(found.model_dump_json()) == found


def test_snapshot_writes_a_manifest_the_next_one_links_by(
    repo: Path, tmp_path: Path
) -> None:
    store = tmp_path / "store"
    p1 = snapshot(repo, store, "proj")
    manifest = json.loads(manifest_path(store / p1.snapshot).read_text())
    assert manifest == {f.path: f.sha256 for f in scan(repo).files}
    assert not manifest_path(store / p1.snapshot).stat().st_mode & 0o222
    (repo / "renamed.txt").write_text("same\n")  # the content of same.txt
    p2 = snapshot(repo, store, "proj")
    s1, s2 = store / p1.snapshot, store / p2.snapshot
    assert os.stat(s1 / "same.txt").st_ino == os.stat(s2 / "renamed.txt").st_ino


def test_snapshot_without_a_manifest_links_nothing(repo: Path, tmp_path: Path) -> None:
    store = tmp_path / "store"
    p1 = snapshot(repo, store, "proj")
    manifest_path(store / p1.snapshot).unlink()  # as 0.1.0 made it
    (repo / "a.py").write_text("a = 3\n")
    p2 = snapshot(repo, store, "proj")
    s1, s2 = store / p1.snapshot, store / p2.snapshot
    assert os.stat(s1 / "same.txt").st_ino != os.stat(s2 / "same.txt").st_ino
    assert (s2 / "same.txt").read_text() == "same\n"


def test_assemble_takes_bytes_and_copies_a_known_hash_of_another_mode(
    repo: Path, tmp_path: Path
) -> None:
    store = tmp_path / "store"
    first = snapshot(repo, store, "proj")
    (repo / "same.txt").chmod(0o755)  # same content, other mode: a copy, no link
    found = scan(repo)
    prov = assemble(store, "proj", found, lambda p: (repo / p).read_bytes())
    assert prov == snapshot(repo, store, "proj")
    same = store / prov.snapshot / "same.txt"
    assert stat.S_IMODE(same.stat().st_mode) == 0o555
    assert same.stat().st_ino != (store / first.snapshot / "same.txt").stat().st_ino
