"""The built-in task types."""

from pydantic import BaseModel

from simrun.plugins import TaskType


class CommandSettings(BaseModel):
    """Settings of a plain command task.

    Attributes:
        image: Image the command runs in.
        args: Argument vector of the command.
        cores: Number of cores.
        memory_gb: Memory in GiB.
        gpus: Number of GPUs.
        scratch_gb: Local scratch in GiB.
    """

    image: str
    args: list[str]
    cores: int
    memory_gb: float
    gpus: int = 0
    scratch_gb: float = 0


# `command` is None: the settings carry the image and the args.
COMMAND = TaskType(name="command", settings=CommandSettings)
