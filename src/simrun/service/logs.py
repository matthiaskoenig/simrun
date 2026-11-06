"""The logs of the pods of a task, kept in the run store.

The logs of a pod are gone with the pod: Kueue deletes a preempted pod, Argo deletes
the pods of a workflow with it after its time to live, and stopping a workflow ends
its pods. So the controller copies the logs of the main containers of the pods of a
task into `tasks/<task>/logs/` once the task is final and its pods terminated, the
ones which ended before it stops or cancels a workflow (the last `KEPT_TAIL_LINES`
lines, at most what the backend reads of a log): a file `<pod>.log` per pod (every
attempt the backend can still read) and `index.json`, the pods in their order,
written last, which marks the logs as kept. An ended run is followed until its logs
are kept or its workflow is gone (`RunRecord.pending_logs`); a run which never had a
workflow keeps none. The API serves the kept logs first and the logs of the live pods
otherwise.
"""

import json
import os
import uuid
from collections import deque
from pathlib import Path

from simrun.service.backend import PodLog

LOGS_DIR = "logs"
INDEX_FILE = "index.json"
# the lines of every pod an agent or a person gets by default
DEFAULT_TAIL_LINES = 300
# the last lines of every pod kept in the store
KEPT_TAIL_LINES = 100_000


def _write(path: Path, text: str) -> None:
    """Write a file atomically (a temporary file in its directory, renamed)."""
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def kept(task_dir: Path) -> bool:
    """Whether the logs of a task are kept in its directory."""
    return (task_dir / LOGS_DIR / INDEX_FILE).is_file()


def keep(task_dir: Path, logs: list[PodLog]) -> None:
    """Keep the logs of the pods of a task in `task_dir/logs`.

    Args:
        task_dir: The directory of the task.
        logs: The logs of its pods, oldest first.
    """
    directory = task_dir / LOGS_DIR
    directory.mkdir(parents=True, exist_ok=True)
    index = []
    for log in logs:
        name = f"{log.pod}.log"
        _write(directory / name, log.text)
        index.append({"node": log.node, "pod": log.pod, "file": name})
    _write(directory / INDEX_FILE, json.dumps(index, indent=2))


def _tail(path: Path, lines: int | None) -> str:
    with path.open(encoding="utf-8", errors="replace") as handle:
        if lines is None:
            return handle.read()
        return "".join(deque(handle, maxlen=lines))


def read_kept(task_dir: Path, tail_lines: int | None = None) -> list[PodLog] | None:
    """The kept logs of a task, the last `tail_lines` lines of each.

    Returns:
        The logs, None when they are not kept (yet).
    """
    directory = task_dir / LOGS_DIR
    try:
        index = json.loads((directory / INDEX_FILE).read_text())
    except FileNotFoundError:
        return None
    logs = []
    for entry in index:
        try:
            text = _tail(directory / entry["file"], tail_lines)
        except FileNotFoundError:
            text = "(the kept log is missing)\n"
        logs.append(PodLog(node=entry["node"], pod=entry["pod"], text=text))
    return logs


def render(logs: list[PodLog]) -> str:
    """The logs of the pods of a task as one text, each pod under a header."""
    return "\n".join(f"==> {log.node} ({log.pod}) <==\n{log.text}" for log in logs)
