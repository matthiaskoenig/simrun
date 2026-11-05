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
        diff_sha256: Hash of the patch and the sorted untracked files.
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


def _git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True
    )
    return result.stdout


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
    untracked = sorted(
        _split(_git(repo, "ls-files", "-z", "--others", "--exclude-standard"))
    )
    digest = hashlib.sha256(patch)
    for rel in untracked:
        digest.update(b"\0" + os.fsencode(rel) + b"\0")
        path = repo / rel
        digest.update(
            os.readlink(path).encode() if path.is_symlink() else path.read_bytes()
        )
    diff_sha256 = digest.hexdigest()
    dirty = bool(changed)

    # nanosecond mtimes (--modify-window=-1): same size and second is no match
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
    dirname = f"{commit[:12]}-{diff_sha256[:12]}"
    final = parent / dirname
    latest = parent / "latest"
    if not final.is_dir():
        tmp = Path(tempfile.mkdtemp(prefix=".tmp-", dir=parent))
        try:
            cmd = ["rsync", "-a", "--modify-window=-1", "--files-from=-", "--from0"]
            if latest.is_dir():
                cmd.append(f"--link-dest={latest.resolve()}")
            cmd += [f"{repo}/", f"{tmp}/"]
            subprocess.run(
                cmd,
                input=b"".join(os.fsencode(f) + b"\0" for f in files),
                check=True,
                capture_output=True,
            )
            tmp.chmod(0o755)
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
