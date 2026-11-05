from pathlib import Path

import pytest

from simrun.plugins import ResourceHint
from simrun.resources import DEFAULT_PER_WORKER_GB, ResourceKey, ResourceStore
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
