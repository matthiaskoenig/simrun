"""CWL v1.2 workflows from runs, executed with cwltool (or Toil on Slurm).

The same run as `simrun.argo`, with the same commands and environment, as one CWL
`Workflow`. The steps work on the run store by absolute path, the workflow input
`store` (a directory every step sees: a local directory, or the NFS mount on Slurm),
and communicate through it: the ledger and the outputs live in the store. The only
CWL output of a step is its standard output, which exists to order the steps.

A split task is three steps of command line tools: `<task>-w0` (worker
`<task>-w0`, `SIMRUN_ROLE=base`), the coordinator, which returns once the ledger is
complete; `<task>-extra`, scattering over the worker indices `1..pods - 1` (workers
`<task>-w<index>`, `SIMRUN_ROLE=extra`; left out for one pod), which exit when nothing
is pending; and `<task>-merge` after `<task>-w0` only. With `--parallel` the
coordinator and the extra workers run side by side; without it the steps run one after
the other and the run still ends, since either one worker computes everything or the
next finds nothing pending. A task waits for the merge of the tasks it reads from. A
failed extra worker does not fail the workflow (`successCodes` of the step), as
`continueOn` in Argo; the coordinator releases its claims and computes them, and a
deterministic unit failure fails the workflow through `<task>-w0`.

The export targets a shared file system without containers (`cwltool --no-container`,
Toil on Slurm with the store on NFS): `store` is a path, not a CWL `Directory`, and
the image is only a `DockerRequirement` hint. Running the steps in containers is
deferred to the Slurm plan.

The scratch directory of a worker is `<scratch>/<run>/<task>/<worker>` below the
workflow input `scratch` (`<run>/<task>/merge` for a merge, `<run>/<task>` for an
unsplit task). The image is a `DockerRequirement` hint, so `--no-container` runs the
commands of the environment.
"""

import math
from pathlib import Path
from typing import Any

import yaml

from simrun import podspec
from simrun.config import ClusterConfig
from simrun.planner import PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.store import RunRecord
from simrun.workflow import TaskSpec

CWL_VERSION = "v1.2"
DEFAULT_SCRATCH = "/tmp/simrun-scratch"
# an extra worker is tolerated as `continueOn` tolerates it in Argo (Ruling 17b):
# done, analysis error, configuration error, out of memory, stopped; the coordinator
# recomputes what it released and fails itself on a deterministic error
EXTRA_SUCCESS_CODES = [0, 3, 4, 137, 143]
STORE = "$(inputs.store)"
MIB = 1024


def _env(
    record: RunRecord,
    task: TaskSpec,
    task_type: TaskType,
    shape: PodShape,
    scratch: str,
    worker: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    """The environment of the worker harness, see `simrun.podspec`."""
    values = {
        **podspec.env_values(record, task, task_type, shape, STORE, scratch),
        **(worker or {}),
    }
    return [{"envName": k, "envValue": v} for k, v in values.items()]


def _requirements(env: list[dict[str, str]], shape: PodShape) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = [
        {"class": "EnvVarRequirement", "envDef": env},
        {
            "class": "ResourceRequirement",
            "coresMin": shape.cores,
            "ramMin": math.ceil(shape.memory_gb * MIB),
        },
    ]
    return requirements


def _tool(
    name: str,
    env: list[dict[str, str]],
    shape: PodShape,
    image: str | None,
    command: list[str],
    after: bool,
    index: bool = False,
    success_codes: list[int] | None = None,
) -> dict[str, Any]:
    """A command line tool: `command`, with the store and scratch as inputs."""
    inputs: dict[str, Any] = {"store": "string", "scratch": "string"}
    if after:
        inputs["after"] = "File[]"
    if index:
        inputs["index"] = "int"
    tool: dict[str, Any] = {
        "class": "CommandLineTool",
        "requirements": _requirements(env, shape),
        "inputs": inputs,
        "arguments": command,
        "stdout": f"{name}.log",
        "outputs": {"log": "stdout"},
    }
    if image is not None:
        # a hint, so that `--no-container` runs the commands of the environment
        tool["hints"] = [{"class": "DockerRequirement", "dockerPull": image}]
    if success_codes is not None:
        tool["successCodes"] = success_codes
    return tool


def _step(
    tool: dict[str, Any],
    after: list[str],
    scatter: str | None = None,
) -> dict[str, Any]:
    inputs: dict[str, Any] = {"store": "store", "scratch": "scratch"}
    if after:
        inputs["after"] = {"source": after, "linkMerge": "merge_nested"}
    step: dict[str, Any] = {"run": tool, "in": inputs, "out": ["log"]}
    if scatter is not None:
        inputs["index"] = scatter
        step["scatter"] = "index"
    return step


def to_cwl(
    record: RunRecord,
    plans: dict[str, TaskPlan],
    types: dict[str, TaskType],
    *,
    scratch: str = DEFAULT_SCRATCH,
    grace_s: int = ClusterConfig.model_fields["grace_s"].default,
) -> dict[str, Any]:
    """The CWL workflow of a run.

    The settings of every task are written to `<task dir>/settings.json` before the
    workflow runs; the workflow does not carry them. Its inputs are `store` (the
    directory of the run store) and `scratch` (the local scratch, default `scratch`).

    Args:
        record: The run.
        plans: The plan of every task, by task id.
        types: The task types, by name.
        scratch: The default of the workflow input `scratch`.
        grace_s: Seconds a worker has to stop after SIGTERM.

    Returns:
        The workflow as a CWL document.

    Raises:
        ValueError: A task has no image or no command, or stages a port which is no
            input.
    """
    workflow = record.workflow
    split = {t.id: types[t.type].splittable for t in workflow.tasks}
    inputs: dict[str, Any] = {
        "store": "string",
        "scratch": {"type": "string", "default": scratch},
    }
    steps: dict[str, Any] = {}
    outputs: dict[str, Any] = {}
    run_scratch = f"$(inputs.scratch)/{record.id}"
    for task in workflow.order():
        task_type = types[task.type]
        plan = plans[task.id]
        shape = plan.shape
        after = [
            f"{dep}-merge/log" if split[dep] else f"{dep}/log"
            for dep in task.dependencies()
        ]
        if not split[task.id]:
            image, argv = podspec.unsplit_command(record, task, task_type, STORE)
            env = _env(record, task, task_type, shape, f"{run_scratch}/{task.id}")
            tool = _tool(task.id, env, shape, image, argv, bool(after))
            steps[task.id] = _step(tool, after)
            outputs[f"{task.id}_log"] = {
                "type": "File",
                "outputSource": f"{task.id}/log",
            }
            continue
        image = podspec.image(record, task, task_type)
        stale = str(grace_s + podspec.STALE_MARGIN_S)
        w0 = f"{task.id}-w0"
        env = _env(
            record,
            task,
            task_type,
            shape,
            f"{run_scratch}/{task.id}/{w0}",
            {"SIMRUN_WORKER": w0, "SIMRUN_ROLE": "base", "SIMRUN_STALE_S": stale},
        )
        steps[w0] = _step(
            _tool(w0, env, shape, image, [podspec.WORKER_COMMAND], bool(after)), after
        )
        if plan.pods > 1:
            extra = f"{task.id}-extra"
            indices = f"{task.id}_extra_indices"
            inputs[indices] = {
                "type": "int[]",
                "default": list(range(1, plan.pods)),
            }
            env = _env(
                record,
                task,
                task_type,
                shape,
                f"{run_scratch}/{task.id}/{task.id}-w$(inputs.index)",
                {
                    "SIMRUN_WORKER": f"{task.id}-w$(inputs.index)",
                    "SIMRUN_ROLE": "extra",
                    "SIMRUN_STALE_S": stale,
                },
            )
            tool = _tool(
                extra,
                env,
                shape,
                image,
                [podspec.WORKER_COMMAND],
                bool(after),
                index=True,
                success_codes=EXTRA_SUCCESS_CODES,
            )
            steps[extra] = _step(tool, after, scatter=indices)
        merge_shape = podspec.merge_shape(shape, task_type.hint.memory_base_gb)
        merge = f"{task.id}-merge"
        env = _env(
            record, task, task_type, merge_shape, f"{run_scratch}/{task.id}/merge"
        )
        steps[merge] = _step(
            _tool(merge, env, merge_shape, image, [podspec.MERGE_COMMAND], True),
            [f"{w0}/log"],
        )
        outputs[f"{task.id}_log"] = {"type": "File", "outputSource": f"{merge}/log"}
    return {
        "cwlVersion": CWL_VERSION,
        "class": "Workflow",
        "id": f"simrun-{record.id}",
        "requirements": [
            {"class": "ScatterFeatureRequirement"},
            {"class": "MultipleInputFeatureRequirement"},
        ],
        "inputs": inputs,
        "outputs": outputs,
        "steps": steps,
    }


def export_cwl(
    record: RunRecord,
    plans: dict[str, TaskPlan],
    types: dict[str, TaskType],
    path: Path,
    **options: Any,
) -> Path:
    """Write the CWL workflow of a run as YAML and validate it with cwl-utils.

    cwl-utils is the optional extra `cwl` (`pip install "simrun[cwl]"`), which also
    installs cwltool, the runner of the workflow.

    Args:
        record: The run.
        plans: The plan of every task, by task id.
        types: The task types, by name.
        path: The file to write.
        **options: The keyword arguments of `to_cwl`.

    Returns:
        The path.
    """
    try:
        from cwl_utils.parser import load_document_by_uri
    except ImportError as error:
        raise ImportError(
            'the CWL export needs cwl-utils: pip install "simrun[cwl]"'
        ) from error
    document = to_cwl(record, plans, types, **options)
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    load_document_by_uri(path)
    return path
