from pathlib import Path

import pytest
from pydantic import BaseModel

from simrun.plugins import ResourceHint, TaskType
from simrun.resources import (
    DEFAULT_PER_WORKER_GB,
    WINDOW,
    ResourceKey,
    ResourceStore,
    resource_key,
)
from simrun.worker import WorkerReport
from simrun.workflow import ResourceOverride

GIB = 2**30
KEY = ResourceKey(task_type="scan", model="hctz")


def report(
    peak_gb: float | None, units: int = 4, limit_gb: float | None = 188.0
) -> WorkerReport:
    return WorkerReport(
        worker="w",
        node="n",
        processes=36,
        units_done=list(range(units)),
        seconds=40.0,
        unit_seconds=10.0 * units,
        peak_memory_bytes=None if peak_gb is None else int(peak_gb * GIB),
        memory_limit_bytes=None if limit_gb is None else int(limit_gb * GIB),
    )


def filled(tmp_path: Path) -> ResourceStore:
    store = ResourceStore(tmp_path / "res.db")
    for _ in range(12):
        store.record(KEY, report(2 + 4.6 * 36), base_gb=2.0)
    return store


def test_default_without_records(tmp_path: Path) -> None:
    res = ResourceStore(tmp_path / "res.db").resolve(KEY, ResourceHint(), None)
    assert res.memory_per_worker_gb == DEFAULT_PER_WORKER_GB == 6.0
    assert res.source == "default"
    assert res.samples == 0
    assert res.seconds_per_unit is None


def test_plugin_hint_source(tmp_path: Path) -> None:
    res = ResourceStore(tmp_path / "res.db").resolve(
        KEY, ResourceHint(memory_per_worker_gb=3.0), None
    )
    assert res.memory_per_worker_gb == 3.0
    assert res.source == "plugin hint"


def test_learned_p95_with_margin(tmp_path: Path) -> None:
    res = filled(tmp_path).resolve(KEY, ResourceHint(), None)
    assert res.memory_per_worker_gb == 5.1
    assert res.source == "learned from 12 runs"
    assert res.samples == 12
    assert res.seconds_per_unit == 10.0


def test_oom_record_raises_next_request(tmp_path: Path) -> None:
    store = filled(tmp_path)
    store.record(KEY, report(None), base_gb=2.0, oom=True)
    assert store.resolve(KEY, ResourceHint(), None).memory_per_worker_gb > 5.1


def test_no_cgroup_records_time_only(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    store.record(KEY, report(None, limit_gb=None), base_gb=2.0)
    res = store.resolve(KEY, ResourceHint(), None)
    assert res.samples == 0
    assert res.seconds_per_unit == 10.0


def test_override_wins(tmp_path: Path) -> None:
    res = filled(tmp_path).resolve(
        KEY,
        ResourceHint(scratch_gb=1.0, gpus=1),
        ResourceOverride(memory_per_worker_gb=9.0, scratch_gb=5.0),
    )
    assert res.memory_per_worker_gb == 9.0
    assert res.scratch_gb == 5.0
    assert res.gpus == 1


def test_keys_are_separate(tmp_path: Path) -> None:
    store = filled(tmp_path)
    other = ResourceKey(task_type="scan", model="midazolam")
    assert store.resolve(other, ResourceHint(), None).source == "default"
    variant = ResourceKey(task_type="scan", model="hctz", variant="x")
    assert store.resolve(variant, ResourceHint(), None).samples == 0


def test_peak_below_base_records_no_memory(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    store.record(KEY, report(1.0), base_gb=2.0)
    res = store.resolve(KEY, ResourceHint(), None)
    assert res.samples == 0
    assert res.seconds_per_unit == 10.0
    assert res.memory_per_worker_gb == 6.0


def test_resolved_never_below_minimum(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    for _ in range(5):
        store.record(KEY, report(2.0 + 0.001), base_gb=2.0)
    assert store.resolve(KEY, ResourceHint(), None).memory_per_worker_gb >= 0.1


def test_oom_not_diluted_by_successes(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    for _ in range(100):
        store.record(KEY, report(2 + 4.6 * 36), base_gb=2.0)
    store.record(KEY, report(None), base_gb=2.0, oom=True)
    assert store.resolve(KEY, ResourceHint(), None).memory_per_worker_gb >= 7.8


def test_oom_without_limit_warns(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = ResourceStore(tmp_path / "res.db")
    with caplog.at_level("WARNING", logger="simrun.resources"):
        store.record(KEY, report(None, limit_gb=None), base_gb=2.0, oom=True)
    assert "without a memory limit" in caplog.text


def test_window_of_the_last_observations(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    for _ in range(5):  # older than the window: never read again
        store.record(KEY, report(2 + 20.0 * 36), base_gb=2.0, run="old")
    for _ in range(WINDOW):
        store.record(KEY, report(2 + 4.6 * 36), base_gb=2.0, run="new")
    res = store.resolve(KEY, ResourceHint(), None)
    assert res.memory_per_worker_gb == 5.1
    assert res.samples == WINDOW == 20


def test_oom_counts_until_a_later_run_succeeds(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    store.record(
        KEY, report(None, limit_gb=2 + 4.0 * 36), base_gb=2.0, oom=True, run="r1"
    )
    # the retry of the same run succeeds: the kill still teaches the next run
    store.record(KEY, report(2 + 4.5 * 36), base_gb=2.0, run="r1")
    assert store.resolve(KEY, ResourceHint(), None).memory_per_worker_gb == 6.0
    # a later run succeeds at the learned request: the kill no longer counts
    store.record(KEY, report(2 + 4.5 * 36), base_gb=2.0, run="r2")
    res = store.resolve(KEY, ResourceHint(), None)
    assert res.memory_per_worker_gb == 5.0
    assert res.samples == 2


def test_oom_alone_raises_the_request(tmp_path: Path) -> None:
    store = ResourceStore(tmp_path / "res.db")
    store.record(
        KEY, report(None, limit_gb=2 + 4.0 * 36), base_gb=2.0, oom=True, run="r1"
    )
    res = store.resolve(KEY, ResourceHint(), None)
    assert res.memory_per_worker_gb == 6.0
    assert res.samples == 1


def test_store_of_an_older_schema_is_migrated(tmp_path: Path) -> None:
    import sqlite3

    db = tmp_path / "res.db"
    with sqlite3.connect(db) as con:
        con.execute(
            "CREATE TABLE observations (key TEXT NOT NULL, per_worker_gb REAL, "
            "seconds_per_unit REAL, oom INTEGER NOT NULL, created REAL NOT NULL)"
        )
        con.execute(
            "INSERT INTO observations VALUES (?, 4.6, 10.0, 0, 1.0)", (KEY.text(),)
        )
    store = ResourceStore(db)
    store.record(KEY, report(2 + 4.6 * 36), base_gb=2.0, run="r1")
    assert store.resolve(KEY, ResourceHint(), None).samples == 2


class _FitSettings(BaseModel):
    subset: str = "all"
    data: list[int] = [1, 2]
    n: int = 1


def test_resource_key_from_the_cost_setting() -> None:
    plain = TaskType(name="fit", settings=_FitSettings)
    assert resource_key(plain, {"subset": "a"}, "hctz") == ResourceKey("fit", "hctz")
    by_subset = TaskType(name="fit", settings=_FitSettings, cost_setting="subset")
    assert resource_key(by_subset, {"subset": "a", "n": 3}, "hctz") == ResourceKey(
        "fit", "hctz", "a"
    )
    # the default of the setting when it is not given
    assert resource_key(by_subset, {}, "hctz").variant == "all"
    by_data = TaskType(name="fit", settings=_FitSettings, cost_setting="data")
    assert resource_key(by_data, {"data": [3]}, "").variant == "[3]"


def test_seconds_per_unit_is_the_time_of_a_unit_in_a_process(tmp_path: Path) -> None:
    # 360 units of 10 s on 36 processes take 100 s; the worker waited 20 s more
    store = ResourceStore(tmp_path / "res.db")
    done = WorkerReport(
        worker="w",
        node="n",
        processes=36,
        units_done=list(range(360)),
        seconds=120.0,
        unit_seconds=3600.0,
    )
    store.record(KEY, done, base_gb=2.0)
    assert store.resolve(KEY, ResourceHint(), None).seconds_per_unit == 10.0
