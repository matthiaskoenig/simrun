from pathlib import Path

from simrun.cgroup import cpu_quota, memory_limit, memory_peak


def test_cpu_quota_parses(tmp_path: Path) -> None:
    cpu_max = tmp_path / "cpu.max"
    cpu_max.write_text("max 100000\n")
    assert cpu_quota(tmp_path) is None
    cpu_max.write_text("350000 100000\n")
    assert cpu_quota(tmp_path) == 3
    cpu_max.write_text("200000 100000\n")
    assert cpu_quota(tmp_path) == 2
    cpu_max.write_text("50000 100000\n")
    assert cpu_quota(tmp_path) == 1


def test_missing_files_give_none(tmp_path: Path) -> None:
    assert cpu_quota(tmp_path) is None
    assert memory_limit(tmp_path) is None
    assert memory_peak(tmp_path) is None


def test_memory_files(tmp_path: Path) -> None:
    (tmp_path / "memory.max").write_text("max\n")
    assert memory_limit(tmp_path) is None
    (tmp_path / "memory.max").write_text(f"{8 * 2**30}\n")
    assert memory_limit(tmp_path) == 8 * 2**30
    (tmp_path / "memory.peak").write_text("123456\n")
    assert memory_peak(tmp_path) == 123456
