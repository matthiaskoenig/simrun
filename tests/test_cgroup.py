from pathlib import Path

from simrun.cgroup import cpu_quota, memory_anon, memory_limit, memory_oom_kills


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
    assert memory_anon(tmp_path) is None


def test_memory_files(tmp_path: Path) -> None:
    (tmp_path / "memory.max").write_text("max\n")
    assert memory_limit(tmp_path) is None
    (tmp_path / "memory.max").write_text(f"{8 * 2**30}\n")
    assert memory_limit(tmp_path) == 8 * 2**30


def test_memory_anon(tmp_path: Path) -> None:
    (tmp_path / "memory.stat").write_text(
        "anon 4096\nfile 999999\nkernel 77\nanon_thp 0\n"
    )
    assert memory_anon(tmp_path) == 4096
    (tmp_path / "memory.stat").write_text("file 1\n")
    assert memory_anon(tmp_path) is None


def test_memory_oom_kills(tmp_path: Path) -> None:
    assert memory_oom_kills(tmp_path) is None
    (tmp_path / "memory.events").write_text(
        "low 0\nhigh 0\nmax 12\noom 2\noom_kill 1\noom_group_kill 0\n"
    )
    assert memory_oom_kills(tmp_path) == 1
