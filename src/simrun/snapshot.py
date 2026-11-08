"""Snapshots of working trees with provenance.

A snapshot is a copy of the files git knows of a working tree (tracked and
untracked, not ignored, uncommitted changes included) in the run store. Files
unchanged since the latest snapshot of the same name are hard links to it. The files
of a snapshot are read-only (`a-w`, part of the hash of its content): a snapshot is
named by its content and never changes, and a build backend which writes through a
link of a shadow tree into one fails loudly instead.
Submodules are out of scope: their content is not copied.
"""

import hashlib
import logging
import os
import secrets
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import NamedTuple

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SourceProvenance(BaseModel):
    """Where the files of a snapshot come from.

    Attributes:
        name: Name of the source.
        branch: Branch of the working tree, `None` for a detached HEAD.
        commit: Commit of HEAD.
        dirty: Whether the working tree differs from HEAD.
        changed_files: Porcelain status lines, e.g. `M src/a.py`.
        diff_sha256: Hash of the snapshot tree (paths, types, modes, contents);
            the patch stays as provenance.
        snapshot: Directory of the snapshot, relative to the store root.
        patch: Stored `git diff HEAD --binary`, relative to the store root.
    """

    name: str
    branch: str | None
    commit: str
    dirty: bool
    changed_files: list[str]
    diff_sha256: str
    snapshot: str
    patch: str | None


def _run(cmd: list[str], input: bytes | None = None, ok: tuple[int, ...] = ()) -> bytes:
    result = subprocess.run(cmd, input=input, capture_output=True)
    if result.returncode == 23 and _only_missing_files(result.stderr):
        return result.stdout  # a file vanished between the listing and the copy
    if result.returncode != 0 and result.returncode not in ok:
        raise RuntimeError(
            f"{cmd[0]} failed with exit code {result.returncode}: "
            f"{result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout


def _only_missing_files(stderr: bytes) -> bool:
    """Whether every error of rsync (exit 23) is a source file which is gone."""
    errors = [
        line
        for line in stderr.decode(errors="replace").splitlines()
        if line.startswith("rsync:")
    ]
    return bool(errors) and all(
        "link_stat" in line and "No such file" in line for line in errors
    )


def _git(repo: Path, *args: str) -> bytes:
    return _run(["git", "-C", str(repo), *args])


def _tree_hash(root: Path, files: list[str]) -> str:
    """Hash the files of a tree as a snapshot holds them.

    Per file in the order of `os.walk` (a directory's own entries sorted, then its
    subdirectories in sorted order, as releases before this one hashed, so a hash
    stays the same): path, type, mode bits without the write bits (a
    snapshot is read-only), content or link target. A file which is gone is left
    out, as a snapshot leaves it out.

    Args:
        root: The tree, a working tree or a snapshot.
        files: Paths below `root`.
    """
    digest = hashlib.sha256()
    for name in sorted(files, key=lambda f: (Path(f).parts[:-1], Path(f).name)):
        path = root / name
        try:
            st = path.lstat()
            link = stat.S_ISLNK(st.st_mode)
            content = os.fsencode(os.readlink(path)) if link else path.read_bytes()
        except FileNotFoundError:
            continue
        mode = stat.S_IMODE(st.st_mode) & (~0 if link else ~0o222)
        digest.update(os.fsencode(name) + b"\0")
        digest.update((b"l" if link else b"f") + b"%o" % mode + b"\0")
        digest.update(b"%d\0" % len(content) + content)
    return digest.hexdigest()


def _split(data: bytes) -> list[str]:
    return [os.fsdecode(p) for p in data.split(b"\0") if p]


def _write_atomic(path: Path, data: bytes, mode: int | None = None) -> None:
    tmp = path.with_name(f".tmp-{secrets.token_hex(6)}-{path.name}")
    tmp.write_bytes(data)
    if mode is not None:
        tmp.chmod(mode)
    os.replace(tmp, path)


class _GitState(NamedTuple):
    """What git says of a working tree."""

    commit: str
    branch: str | None
    changed: list[str]
    patch: bytes
    files: list[str]

    @property
    def dirty(self) -> bool:
        """Whether the working tree differs from HEAD."""
        return bool(self.changed)


def _git_state(repo: Path) -> _GitState:
    """The commit, branch, status, diff and files of a working tree."""
    commit = _git(repo, "rev-parse", "HEAD").decode().strip()
    branch = _git(repo, "branch", "--show-current").decode().strip() or None
    status = _git(repo, "status", "--porcelain", "--untracked-files=all")
    changed = [line for line in status.decode().splitlines() if line]
    patch = _git(repo, "diff", "HEAD", "--binary")
    listed = _split(
        _git(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    )
    # skip deleted files and the directories of submodules
    files = sorted(
        {
            f
            for f in listed
            if os.path.lexists(repo / f)
            and (os.path.islink(repo / f) or not os.path.isdir(repo / f))
        }
    )
    return _GitState(commit, branch, changed, patch, files)


def provenance(repo: Path, name: str) -> SourceProvenance:
    """The provenance of a working tree without copying it.

    The same git calls as `snapshot`; `diff_sha256` is the hash a snapshot of the
    tree would have, `snapshot` is empty and there is no patch.

    Args:
        repo: A git working tree.
        name: Name of the source.

    Returns:
        The provenance, with `snapshot=""` and `patch=None`.
    """
    state = _git_state(repo)
    return SourceProvenance(
        name=name,
        branch=state.branch,
        commit=state.commit,
        dirty=state.dirty,
        changed_files=state.changed,
        diff_sha256=_tree_hash(repo, state.files),
        snapshot="",
        patch=None,
    )


def snapshot(repo: Path, store: Path, name: str) -> SourceProvenance:
    """Copy a working tree into `store/snapshots/<name>`.

    Args:
        repo: A git working tree (a worktree with a `.git` file works too).
        store: Root of the run store.
        name: Name of the source.

    Returns:
        The provenance of the snapshot.

    Raises:
        RuntimeError: When rsync is not installed.
    """
    if shutil.which("rsync") is None:
        raise RuntimeError("rsync is required to snapshot a working tree")
    state = _git_state(repo)
    commit, branch, changed, patch, files = (
        state.commit,
        state.branch,
        state.changed,
        state.patch,
        state.files,
    )
    dirty = state.dirty

    parent = store / "snapshots" / name
    parent.mkdir(parents=True, exist_ok=True)
    latest = parent / "latest"
    tmp = Path(tempfile.mkdtemp(prefix=".tmp-", dir=parent))
    try:
        # --checksum: link only files of identical content; exit 24 (a file
        # vanished before the copy) leaves it out of the snapshot and its hash
        # --chmod: the files read-only, also the ones linked to the latest snapshot,
        # whose modes then match
        cmd = ["rsync", "-a", "--checksum", "--chmod=F-w", "--files-from=-"]
        cmd.append("--from0")
        if latest.is_dir():
            cmd.append(f"--link-dest={latest.resolve()}")
        cmd += [f"{repo}/", f"{tmp}/"]
        _run(cmd, input=b"".join(os.fsencode(f) + b"\0" for f in files), ok=(24,))
        diff_sha256 = _tree_hash(tmp, files)
        dirname = f"{commit[:12]}-{diff_sha256[:12]}"
        final = parent / dirname
        tmp.chmod(0o755)
        if not final.is_dir():
            try:
                os.rename(tmp, final)
            except OSError:
                if not final.is_dir():
                    raise
                logger.info("snapshot %s was created concurrently", final)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    patch_rel: str | None = None
    if dirty:
        patch_path = parent / f"{dirname}.patch"
        if not patch_path.exists():
            _write_atomic(patch_path, patch, mode=0o444)
        patch_rel = str(patch_path.relative_to(store))

    link = parent / f".tmp-link-{secrets.token_hex(6)}"
    link.symlink_to(dirname)
    os.replace(link, latest)
    return SourceProvenance(
        name=name,
        branch=branch,
        commit=commit,
        dirty=dirty,
        changed_files=changed,
        diff_sha256=diff_sha256,
        snapshot=str(final.relative_to(store)),
        patch=patch_rel,
    )
