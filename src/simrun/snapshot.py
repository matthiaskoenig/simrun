"""Snapshots of working trees with provenance.

A snapshot is a copy of the files git knows of a working tree (tracked and
untracked, not ignored, uncommitted changes included) in the run store. A snapshot
is made in two steps: `scan` lists the files of a working tree with their modes and
content hashes, `assemble` writes them into the store; `snapshot` does both on the
machine of the store, the service does the second for an upload of a client
(`check_scan`, `missing`, `upload`). Next to every snapshot `<dirname>.files.json`
maps its paths to the hashes of their contents, so files whose content the latest
snapshot of the same name has are hard links to it (copies when their modes
differ) and an upload sends only the others. The files of a snapshot are read-only
(`a-w`, part of the hash of its content): a snapshot is named by its content and
never changes, and a build backend which writes through a link of a shadow tree
into one fails loudly instead.
Submodules are out of scope: their content is not copied.
"""

import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import IO, NamedTuple

from pydantic import BaseModel, ConfigDict

logger = logging.getLogger(__name__)

LATEST_LINK = "latest"
MANIFEST_SUFFIX = ".files.json"
# the name of a source: a directory of `snapshots/`
SOURCE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")
SHA256 = re.compile(r"[0-9a-f]{64}")
# the mode bits a file of an upload may have: read and execute
FILE_MODES = 0o555
# the mode bits a directory of an upload may have: permissions and setgid
DIR_MODES = 0o2777
# the bits a directory of a snapshot always has, so the store can remove it
DIR_OWNER = 0o700
CHUNK = 1 << 20
# the refusal of an upload whose file changed between the scan and the tar
CHANGED_DURING_UPLOAD = "it changed during the upload"
# the links followed to resolve the target of a link of an upload, as `MAXSYMLINKS`
MAX_LINK_HOPS = 40


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


def _run(cmd: list[str]) -> bytes:
    result = subprocess.run(cmd, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"{cmd[0]} failed with exit code {result.returncode}: "
            f"{result.stderr.decode(errors='replace').strip()}"
        )
    return result.stdout


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


class FileEntry(BaseModel):
    """A file of a working tree as a snapshot holds it.

    Attributes:
        path: Path below the tree, with `/`.
        mode: Mode bits without the write bits (a snapshot is read-only), special
            bits kept as release 0.1.0 kept them; the ones of the link for a
            symbolic link.
        sha256: Hash of the content of a regular file, None for a link.
        link: Target of a symbolic link, None for a regular file.
    """

    path: str
    mode: int
    sha256: str | None
    link: str | None


class Scan(BaseModel):
    """What a snapshot of a working tree is made of, before it is copied.

    Attributes:
        branch: Branch of the working tree, `None` for a detached HEAD.
        commit: Commit of HEAD.
        dirty: Whether the working tree differs from HEAD.
        changed_files: Porcelain status lines, e.g. `M src/a.py`.
        patch: `git diff HEAD --binary`, base64 in JSON.
        files: The files of the snapshot, in the order of git.
        dirs: The mode bits of the directories which hold the files, by path; the
            snapshot keeps them as release 0.1.0 did, with the owner bits added.
    """

    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")

    branch: str | None
    commit: str
    dirty: bool
    changed_files: list[str]
    patch: bytes
    files: list[FileEntry]
    dirs: dict[str, int] = {}


class SnapshotError(ValueError):
    """An upload which cannot become a snapshot.

    A path outside the tree, a member of the archive which is not a listed file, or
    a hash which does not match.
    """


class MissingContent(RuntimeError):
    """A file of an upload whose content neither the archive nor the store has.

    The latest snapshot changed since the client asked what is missing; the
    client asks again.
    """


def _sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def scan(repo: Path) -> Scan:
    """List the files of a working tree with their modes and content hashes.

    A file which is gone between the listing of git and its hash is left out.

    Args:
        repo: A git working tree (a worktree with a `.git` file works too).

    Returns:
        The scan of the tree.

    Raises:
        RuntimeError: When git fails.
    """
    state = _git_state(repo)
    files: list[FileEntry] = []
    for name in state.files:
        path = repo / name
        try:
            st = path.lstat()
            if stat.S_ISLNK(st.st_mode):
                target = os.readlink(path)
                entry = FileEntry(
                    path=name, mode=stat.S_IMODE(st.st_mode), sha256=None, link=target
                )
            elif stat.S_ISREG(st.st_mode):
                entry = FileEntry(
                    path=name,
                    mode=stat.S_IMODE(st.st_mode) & ~0o222,
                    sha256=_sha256_of(path),
                    link=None,
                )
            else:
                continue
        except FileNotFoundError:
            continue
        files.append(entry)
    dirs: dict[str, int] = {}
    for entry in files:
        for parent in PurePosixPath(entry.path).parents:
            key = str(parent)
            if key == "." or key in dirs:
                continue
            try:
                dirs[key] = stat.S_IMODE((repo / key).lstat().st_mode)
            except FileNotFoundError:
                continue
    return Scan(
        branch=state.branch,
        commit=state.commit,
        dirty=state.dirty,
        changed_files=state.changed,
        patch=state.patch,
        files=files,
        dirs=dirs,
    )


def manifest_path(snapshot_dir: Path) -> Path:
    """The manifest of a snapshot: its paths and the hashes of their contents."""
    return snapshot_dir.with_name(snapshot_dir.name + MANIFEST_SUFFIX)


def _known(parent: Path) -> dict[str, list[Path]]:
    """The files of the latest snapshot of a source by the hash of their content.

    Empty without a latest snapshot or without its manifest (a snapshot of 0.1.0).
    """
    latest = parent / LATEST_LINK
    if not latest.is_dir():
        return {}
    target = latest.resolve()
    try:
        manifest = json.loads(manifest_path(target).read_text())
    except FileNotFoundError:
        return {}
    known: dict[str, list[Path]] = {}
    for rel, digest in manifest.items():
        if _path_problem(rel) is None:
            known.setdefault(digest, []).append(target / rel)
    return known


def _copy(source: Path | bytes, dst: Path) -> str | None:
    """Write a file of a snapshot; the hash of what was written, None when gone.

    The modification time of a source file is kept, as `rsync -a` kept it.
    """
    digest = hashlib.sha256()
    if isinstance(source, bytes):
        with dst.open("xb") as out:
            out.write(source)
        digest.update(source)
        return digest.hexdigest()
    try:
        file = source.open("rb")
    except FileNotFoundError:
        return None
    with file, dst.open("xb") as out:
        while chunk := file.read(CHUNK):
            digest.update(chunk)
            out.write(chunk)
        st = os.fstat(file.fileno())
    os.utime(dst, ns=(st.st_atime_ns, st.st_mtime_ns))
    return digest.hexdigest()


def _place(
    entry: FileEntry,
    dst: Path,
    known: dict[str, list[Path]],
    files: Callable[[str], Path | bytes],
    move: bool,
) -> str | None:
    """Put a regular file into a snapshot being assembled; the hash of its content.

    A file of the latest snapshot with the same content and mode is linked, one
    with the same content and another mode copied; else the content comes from
    `files`, a file renamed into place with `move`. None for a file which is gone.
    """
    assert entry.sha256 is not None
    candidates = known.get(entry.sha256, [])
    for candidate in candidates:
        try:
            if stat.S_IMODE(candidate.lstat().st_mode) == entry.mode:
                os.link(candidate, dst)
                return entry.sha256
        except OSError:  # gone, or too many links: a copy
            continue
    digest: str | None = None
    for candidate in candidates:
        digest = _copy(candidate, dst)
        if digest is not None:
            break
    else:
        try:
            source = files(entry.path)
            if move and isinstance(source, Path):
                os.rename(source, dst)
                digest = entry.sha256
            else:
                digest = _copy(source, dst)
        except FileNotFoundError:
            digest = None
    if digest is not None:
        dst.chmod(entry.mode)
    return digest


def assemble(
    store: Path,
    name: str,
    scan: Scan,
    files: Callable[[str], Path | bytes],
    move: bool = False,
) -> SourceProvenance:
    """Write a scanned tree into `store/snapshots/<name>`.

    The files whose content the latest snapshot of the name has are hard links to
    it (copies when their modes differ), the others come from `files`: the file to
    copy or the content. A snapshot is assembled in a temporary directory, its
    directories get the modes of the scan, it is named by its content and renamed
    into place; one which exists already is kept.

    Args:
        store: Root of the run store.
        name: Name of the source.
        scan: The files and the git state of the tree.
        files: The content of a path of `scan`, a file or bytes; a file which is
            gone (`FileNotFoundError`) is left out of the snapshot.
        move: The files of `files` are given up: renamed into the snapshot
            instead of copied (on the file system of the store), their content
            trusted to have the hash of the scan.

    Returns:
        The provenance of the snapshot.
    """
    parent = store / "snapshots" / name
    parent.mkdir(parents=True, exist_ok=True)
    known = _known(parent)
    tmp = Path(tempfile.mkdtemp(prefix=".tmp-", dir=parent))
    manifest: dict[str, str] = {}
    try:
        for entry in scan.files:
            dst = tmp / entry.path
            dst.parent.mkdir(parents=True, exist_ok=True)
            if entry.link is not None:
                dst.symlink_to(entry.link)
            elif (digest := _place(entry, dst, known, files, move)) is not None:
                manifest[entry.path] = digest
        for rel, mode in scan.dirs.items():
            if (tmp / rel).is_dir() and not (tmp / rel).is_symlink():
                (tmp / rel).chmod(mode | DIR_OWNER)
        diff_sha256 = _tree_hash(tmp, [entry.path for entry in scan.files])
        dirname = f"{scan.commit[:12]}-{diff_sha256[:12]}"
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

    if not manifest_path(final).exists():
        data = json.dumps(manifest, indent=0, sort_keys=True).encode()
        _write_atomic(manifest_path(final), data, mode=0o444)
    patch_rel: str | None = None
    if scan.dirty:
        patch_path = parent / f"{dirname}.patch"
        if not patch_path.exists():
            _write_atomic(patch_path, scan.patch, mode=0o444)
        patch_rel = str(patch_path.relative_to(store))

    link = parent / f".tmp-link-{secrets.token_hex(6)}"
    link.symlink_to(dirname)
    os.replace(link, parent / LATEST_LINK)
    return SourceProvenance(
        name=name,
        branch=scan.branch,
        commit=scan.commit,
        dirty=scan.dirty,
        changed_files=scan.changed_files,
        diff_sha256=diff_sha256,
        snapshot=str(final.relative_to(store)),
        patch=patch_rel,
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
        RuntimeError: When git fails.
    """
    return assemble(store, name, scan(repo), lambda path: repo / path)


def _path_problem(path: str) -> str | None:
    """Why a path of an upload is not one of a tree, None when it is."""
    if not path or "\0" in path:
        return "is empty or has a NUL"
    pure = PurePosixPath(path)
    if pure.is_absolute() or str(pure) != path or not pure.parts:
        return "is not a plain relative path"
    if ".." in pure.parts or ".git" in pure.parts:
        return "has a component .. or .git"
    return None


def _stays_inside(path: str, links: dict[str, str]) -> bool:
    """Whether the target of the link `path` resolves within the tree.

    Resolved as the system would in the snapshot: component by component from the
    directory of the link, following the links of the tree (a link to `.` makes
    `..` behind it leave the tree), every other component a directory or a file.
    """
    current = list(PurePosixPath(path).parent.parts)
    pending = links[path].split("/")
    if not links[path] or "\0" in links[path] or links[path].startswith("/"):
        return False
    hops = 0
    while pending:
        part = pending.pop(0)
        if part in ("", "."):
            continue
        if part == "..":
            if not current:
                return False
            current.pop()
            continue
        candidate = "/".join([*current, part])
        if candidate in links:
            hops += 1
            target = links[candidate]
            if hops > MAX_LINK_HOPS or not target or target.startswith("/"):
                return False
            pending = target.split("/") + pending
            continue
        current.append(part)
    return True


def check_name(name: str) -> None:
    """Refuse a source name which is not a directory name of `snapshots/`.

    Raises:
        SnapshotError: For a name of other characters than letters, digits, `-`,
            `_` and `.`, or one which does not start with a letter or digit.
    """
    if SOURCE_NAME.fullmatch(name) is None:
        raise SnapshotError(
            f"source name {name!r} is not letters, digits, '-', '_' and '.'"
        )


def check_files(files: list[FileEntry]) -> None:
    """Refuse a file list which would write outside the tree of a snapshot.

    Raises:
        SnapshotError: For a path which is absolute, not normalized, has a NUL or
            a component `..` or `.git`, which is listed twice or below another
            entry; a regular file without a hash or with write or special mode
            bits; a link whose target is absolute or leaves the tree.
    """
    paths = {entry.path for entry in files}
    links = {e.path: e.link for e in files if e.link is not None}
    if len(paths) != len(files):
        raise SnapshotError("a path is listed twice")
    for entry in files:
        if (problem := _path_problem(entry.path)) is not None:
            raise SnapshotError(f"path {entry.path!r} {problem}")
        parents = PurePosixPath(entry.path).parents
        if any(str(p) in paths for p in parents):
            raise SnapshotError(f"path {entry.path!r} is below another entry")
        if (entry.link is None) == (entry.sha256 is None):
            raise SnapshotError(f"{entry.path!r} is neither a file nor a link")
        if entry.link is not None:
            if not _stays_inside(entry.path, links):
                raise SnapshotError(
                    f"link {entry.path!r} points out of the tree: {entry.link!r}"
                )
        elif entry.sha256 is None or SHA256.fullmatch(entry.sha256) is None:
            raise SnapshotError(f"{entry.path!r} has no sha256 hash")
        elif entry.mode & ~FILE_MODES:
            raise SnapshotError(
                f"{entry.path!r} has mode {entry.mode:o}; an upload takes only read "
                "and execute bits (setuid, setgid and sticky files need a client "
                "with the store mounted)"
            )


def check_scan(scan: Scan) -> None:
    """Refuse a scan which cannot become a snapshot, see `check_files`.

    Raises:
        SnapshotError: For a file list `check_files` refuses, a commit which is
            not a hexadecimal hash, or a directory which is not one of the tree, is
            also a file, or has other mode bits than permissions and setgid.
    """
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", scan.commit) is None:
        raise SnapshotError(f"commit {scan.commit!r} is not a git hash")
    check_files(scan.files)
    paths = {entry.path for entry in scan.files}
    for rel, mode in scan.dirs.items():
        if (problem := _path_problem(rel)) is not None:
            raise SnapshotError(f"directory {rel!r} {problem}")
        if rel in paths or any(str(p) in paths for p in PurePosixPath(rel).parents):
            raise SnapshotError(f"directory {rel!r} is or is below a file or link")
        if mode & ~DIR_MODES:
            raise SnapshotError(
                f"directory {rel!r} has mode {mode:o}; an upload takes permission "
                "bits and setgid only"
            )


def missing(store: Path, name: str, files: list[FileEntry]) -> list[str]:
    """The paths of regular files whose content the latest snapshot lacks.

    Args:
        store: Root of the run store.
        name: Name of the source.
        files: The files of a scan, checked with `check_files`.
    """
    known = _known(store / "snapshots" / name)
    return [e.path for e in files if e.sha256 is not None and e.sha256 not in known]


def upload(store: Path, name: str, scan: Scan, archive: IO[bytes]) -> SourceProvenance:
    """Write a snapshot from a scan and a tar of the files the store lacks.

    Every member of the tar is a regular file of the scan whose content has the
    hash of the scan; it is extracted, under a name of its own, into a temporary
    directory beside the snapshots of the name, and the snapshot assembled from it
    (its files renamed into place) and the latest snapshot.

    Args:
        store: Root of the run store.
        name: Name of the source, checked with `check_name`.
        scan: The files and the git state of the tree, checked with `check_scan`.
        archive: The tar, read once from its start.

    Returns:
        The provenance of the snapshot.

    Raises:
        SnapshotError: For a member which is not a regular file, not a file of the
            scan or sent twice, or whose content does not match its hash; or a tar
            which cannot be read.
        MissingContent: For a file whose content neither the tar nor the latest
            snapshot has.
    """
    parent = store / "snapshots" / name
    parent.mkdir(parents=True, exist_ok=True)
    expected = {e.path: e.sha256 for e in scan.files if e.sha256 is not None}
    staging = Path(tempfile.mkdtemp(prefix=".tmp-upload-", dir=parent))
    try:
        received: dict[str, Path] = {}
        try:
            with tarfile.open(fileobj=archive, mode="r|") as tar:
                for member in tar:
                    if not member.isreg():
                        raise SnapshotError(
                            f"member {member.name!r} is not a regular file"
                        )
                    if member.name not in expected or member.name in received:
                        raise SnapshotError(
                            f"member {member.name!r} is not a file of the scan or "
                            "sent twice"
                        )
                    source = tar.extractfile(member)
                    assert source is not None  # a regular file
                    dst = staging / str(len(received))
                    if _copy_member(source, dst) != expected[member.name]:
                        raise SnapshotError(
                            f"the content of {member.name!r} does not match its "
                            f"hash: {CHANGED_DURING_UPLOAD}?"
                        )
                    os.utime(dst, (member.mtime, member.mtime))
                    received[member.name] = dst
        except tarfile.TarError as error:
            raise SnapshotError(f"the archive cannot be read: {error}") from error
        known = _known(parent)
        lacking = [
            path
            for path, digest in expected.items()
            if path not in received and digest not in known
        ]
        if lacking:
            raise MissingContent(
                f"the store lacks the content of {', '.join(sorted(lacking)[:5])}; "
                "ask again which files are missing"
            )

        def content(path: str) -> Path:
            if path not in received:  # the latest snapshot lost it meanwhile
                raise MissingContent(f"the store lacks the content of {path!r}")
            return received[path]

        return assemble(store, name, scan, content, move=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _copy_member(source: IO[bytes], dst: Path) -> str:
    """Write a member of an upload; the hash of its content."""
    digest = hashlib.sha256()
    with source, dst.open("xb") as out:
        while chunk := source.read(CHUNK):
            digest.update(chunk)
            out.write(chunk)
    return digest.hexdigest()
