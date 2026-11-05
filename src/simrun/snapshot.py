"""Snapshots of working trees with provenance.

A snapshot is a copy of the files git knows of a working tree (tracked and
untracked, not ignored, uncommitted changes included) in the run store. Files
unchanged since the latest snapshot of the same name are hard links to it.
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


def _tree_hash(root: Path) -> str:
    """Hash the entries of a tree: path, type, mode bits, content or target."""
    digest = hashlib.sha256()
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        # a symlink to a directory is listed in dirnames and not descended into
        links = [d for d in dirnames if (Path(dirpath) / d).is_symlink()]
        for fname in sorted(filenames + links):
            path = Path(dirpath) / fname
            st = path.lstat()
            kind = b"l" if stat.S_ISLNK(st.st_mode) else b"f"
            content = (
                os.fsencode(os.readlink(path)) if kind == b"l" else path.read_bytes()
            )
            digest.update(os.fsencode(str(path.relative_to(root))) + b"\0")
            digest.update(kind + b"%o" % stat.S_IMODE(st.st_mode) + b"\0")
            digest.update(b"%d\0" % len(content) + content)
    return digest.hexdigest()


def _split(data: bytes) -> list[str]:
    return [os.fsdecode(p) for p in data.split(b"\0") if p]


def _write_atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".tmp-{secrets.token_hex(6)}-{path.name}")
    tmp.write_bytes(data)
    os.replace(tmp, path)


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
    commit = _git(repo, "rev-parse", "HEAD").decode().strip()
    branch = _git(repo, "branch", "--show-current").decode().strip() or None
    status = _git(repo, "status", "--porcelain", "--untracked-files=all")
    changed = [line for line in status.decode().splitlines() if line]
    patch = _git(repo, "diff", "HEAD", "--binary")
    listed = _split(
        _git(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard")
    )
    dirty = bool(changed)

    # skip deleted files and the directories of submodules
    files = sorted(
        {
            f
            for f in listed
            if os.path.lexists(repo / f)
            and (os.path.islink(repo / f) or not os.path.isdir(repo / f))
        }
    )

    parent = store / "snapshots" / name
    parent.mkdir(parents=True, exist_ok=True)
    latest = parent / "latest"
    tmp = Path(tempfile.mkdtemp(prefix=".tmp-", dir=parent))
    try:
        # --checksum: link only files of identical content; exit 24 (a file
        # vanished before the copy) leaves it out of the snapshot and its hash
        cmd = ["rsync", "-a", "--checksum", "--files-from=-", "--from0"]
        if latest.is_dir():
            cmd.append(f"--link-dest={latest.resolve()}")
        cmd += [f"{repo}/", f"{tmp}/"]
        _run(cmd, input=b"".join(os.fsencode(f) + b"\0" for f in files), ok=(24,))
        diff_sha256 = _tree_hash(tmp)
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
            _write_atomic(patch_path, patch)
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
