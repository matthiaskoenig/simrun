"""The comparison of runs: provenance and the summaries of their tasks side by side."""

import json
import logging
from typing import Any

from pydantic import BaseModel

from simrun.store import RunStore

logger = logging.getLogger(__name__)

SUMMARY_FILE = "summary.json"


class CompareTable(BaseModel):
    """A table of runs.

    Attributes:
        columns: The columns in order: `run`, `group`, `agent`, `state`, then per
            source `<source>.changed_files` and `<source>.diff_sha256`, then every
            key of `out/summary.json` of every task as `<task>.<key>`.
        rows: One row per run, with every column (None where a run has no value).
    """

    columns: list[str]
    rows: list[dict[str, Any]]


def _summary(store: RunStore, run_id: str, task_id: str) -> dict[str, Any]:
    path = store.run_dir(run_id) / "tasks" / task_id / "out" / SUMMARY_FILE
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except ValueError as error:
        logger.warning("summary %s is no JSON: %s", path, error)
        return {}
    if not isinstance(data, dict):
        logger.warning("summary %s is no JSON object", path)
        return {}
    return data


def compare_runs(store: RunStore, run_ids: list[str]) -> CompareTable:
    """Compare runs by their provenance and the summaries their merges wrote.

    Args:
        store: The run store.
        run_ids: The runs, one row each in this order.

    Returns:
        The table.

    Raises:
        KeyError: A run does not exist.
    """
    rows: list[dict[str, Any]] = []
    for run_id in run_ids:
        record = store.read(run_id)
        row: dict[str, Any] = {
            "run": record.id,
            "group": record.group,
            "agent": record.agent,
            "state": record.state.value,
        }
        for source in record.sources:
            row[f"{source.name}.changed_files"] = len(source.changed_files)
            row[f"{source.name}.diff_sha256"] = source.diff_sha256
        for task in record.workflow.tasks:
            for key, value in _summary(store, run_id, task.id).items():
                row[f"{task.id}.{key}"] = value
        rows.append(row)
    columns = list(dict.fromkeys(key for row in rows for key in row))
    return CompareTable(
        columns=columns, rows=[{c: row.get(c) for c in columns} for row in rows]
    )
