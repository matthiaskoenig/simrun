"""The limits and the usage of the cgroup (v2) a worker runs in."""

from pathlib import Path

CGROUP_ROOT = Path("/sys/fs/cgroup")


def _read(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def cpu_quota(root: Path = CGROUP_ROOT) -> int | None:
    """The number of cores the CPU quota of `cpu.max` allows.

    Args:
        root: The directory of the cgroup.

    Returns:
        The quota over the period, rounded down and at least 1, or `None` without a
        quota or without the file.
    """
    text = _read(root / "cpu.max")
    if text is None:
        return None
    quota, _, period = text.partition(" ")
    if quota == "max":
        return None
    return max(1, int(quota) // int(period or "100000"))


def memory_limit(root: Path = CGROUP_ROOT) -> int | None:
    """The memory limit of `memory.max` in bytes, `None` without a limit."""
    text = _read(root / "memory.max")
    if text is None or text == "max":
        return None
    return int(text)


def memory_anon(root: Path = CGROUP_ROOT) -> int | None:
    """The anonymous memory of `memory.stat` (`anon`) in bytes.

    The memory the processes allocated, without the page cache which `memory.peak`
    counts and the kernel reclaims at the limit instead of killing.

    Returns:
        The bytes, `None` without the file or the field.
    """
    text = _read(root / "memory.stat")
    if text is None:
        return None
    for line in text.splitlines():
        key, _, value = line.partition(" ")
        if key == "anon":
            return int(value)
    return None


def memory_oom_kills(root: Path = CGROUP_ROOT) -> int | None:
    """The processes of the cgroup the out of memory killer killed (`oom_kill`).

    The counter of `memory.events`, the evidence of a kill for memory: a process
    which exits 137 may have been killed by anyone with SIGKILL.

    Returns:
        The count, `None` without the file or the field.
    """
    text = _read(root / "memory.events")
    if text is None:
        return None
    for line in text.splitlines():
        key, _, value = line.partition(" ")
        if key == "oom_kill":
            return int(value)
    return None
