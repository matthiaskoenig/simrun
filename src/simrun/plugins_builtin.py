"""The built-in task types."""

from pydantic import BaseModel, Field

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

    image: str = Field(title="Image")
    args: list[str] = Field(
        title="Args",
        description="The argv; {input}, {out} and {settings} as in a plugin command",
    )
    cores: int = Field(title="Cores")
    memory_gb: float = Field(title="Memory (GB)")
    gpus: int = Field(0, title="GPUs")
    scratch_gb: float = Field(0, title="Scratch (GB)")


# `command` is None: the settings carry the image and the args.
COMMAND = TaskType(
    name="command",
    settings=CommandSettings,
    description="A command in an image of its own",
)
