import json
from pathlib import Path

import pytest

from simrun.demo import SQUARES, SquaresSettings
from simrun.plugins import TaskPaths, load_task_types, resolve
from simrun.plugins_builtin import COMMAND


def test_entry_points_load() -> None:
    types = load_task_types()
    assert "command" in types
    assert "demo.squares" in types


def test_squares_splittable() -> None:
    assert SQUARES.splittable
    assert not COMMAND.splittable


def test_resolve() -> None:
    assert resolve("simrun.demo:count_units")(SquaresSettings(n=5)) == 5


def test_squares_end_to_end_in_process(tmp_path: Path) -> None:
    task_dir = tmp_path / "task"
    paths = TaskPaths(
        store=tmp_path,
        run_dir=tmp_path,
        task_dir=task_dir,
        out=task_dir / "out",
        inputs={},
        scratch=tmp_path / "scratch",
    )
    settings = SquaresSettings(n=5)
    assert SQUARES.count_units is not None
    assert SQUARES.setup is not None
    assert SQUARES.compute_unit is not None
    assert SQUARES.merge is not None
    n = resolve(SQUARES.count_units)(settings)
    resolve(SQUARES.setup)(settings, paths)
    for i in range(n):
        resolve(SQUARES.compute_unit)(settings, i, paths)
    resolve(SQUARES.merge)(settings, paths)
    summary = json.loads((paths.out / "summary.json").read_text())
    assert summary == {"sum": 30, "n": 5}
    assert json.loads((paths.out / "units" / "3.json").read_text()) == {
        "i": 3,
        "square": 9,
    }


def test_fail_at(tmp_path: Path) -> None:
    paths = TaskPaths(tmp_path, tmp_path, tmp_path, tmp_path / "out", {}, tmp_path)
    with pytest.raises(RuntimeError):
        resolve("simrun.demo:compute_unit")(SquaresSettings(n=3, fail_at=1), 1, paths)


def test_duplicate_name_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from importlib.metadata import EntryPoint

    eps = [
        EntryPoint("a", "simrun.plugins_builtin:COMMAND", "simrun.task_types"),
        EntryPoint("b", "simrun.plugins_builtin:COMMAND", "simrun.task_types"),
    ]
    monkeypatch.setattr("simrun.plugins.entry_points", lambda group: eps)
    with pytest.raises(ValueError):
        load_task_types()
