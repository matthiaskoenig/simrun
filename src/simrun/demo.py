"""The demo plugin: sums of squares, split into one unit per number."""

import json
import time

from pydantic import BaseModel

from simrun.plugins import ResourceHint, TaskPaths, TaskType


class SquaresSettings(BaseModel):
    """Settings of the squares demo.

    Attributes:
        n: Number of units.
        sleep_s: Seconds each unit sleeps.
        fail_at: Index of a unit which fails.
        memory_mb: Megabytes each unit allocates.
    """

    n: int
    sleep_s: float = 0.0
    fail_at: int | None = None
    memory_mb: int = 0


def count_units(settings: SquaresSettings) -> int:
    """Count the units of work.

    Args:
        settings: Settings of the task.

    Returns:
        The number of units.
    """
    return settings.n


def setup(settings: SquaresSettings, paths: TaskPaths) -> None:
    """Create the output directories, once per process.

    Args:
        settings: Settings of the task.
        paths: Locations of the task.
    """
    (paths.out / "units").mkdir(parents=True, exist_ok=True)


def compute_unit(settings: SquaresSettings, index: int, paths: TaskPaths) -> None:
    """Write the square of the index.

    Args:
        settings: Settings of the task.
        index: Index of the unit.
        paths: Locations of the task.

    Raises:
        RuntimeError: The unit is the one set to fail.
    """
    ballast = bytearray(settings.memory_mb * 2**20)
    ballast[::4096] = b"\x01" * len(ballast[::4096])
    time.sleep(settings.sleep_s)
    if settings.fail_at == index:
        raise RuntimeError(f"unit {index} failed on request")
    units = paths.out / "units"
    units.mkdir(parents=True, exist_ok=True)
    (units / f"{index}.json").write_text(
        json.dumps({"i": index, "square": index * index})
    )


def merge(settings: SquaresSettings, paths: TaskPaths) -> None:
    """Write the sum of the squares.

    Args:
        settings: Settings of the task.
        paths: Locations of the task.
    """
    total = sum(
        json.loads((paths.out / "units" / f"{i}.json").read_text())["square"]
        for i in range(settings.n)
    )
    (paths.out / "summary.json").write_text(json.dumps({"sum": total, "n": settings.n}))


SQUARES = TaskType(
    name="demo.squares",
    settings=SquaresSettings,
    hint=ResourceHint(memory_base_gb=0.5, memory_per_worker_gb=0.5),
    count_units="simrun.demo:count_units",
    setup="simrun.demo:setup",
    compute_unit="simrun.demo:compute_unit",
    merge="simrun.demo:merge",
)
