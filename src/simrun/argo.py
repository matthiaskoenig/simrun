"""Argo workflows from runs: one DAG, a scatter of workers per split task.

A split task of `pods` pods is three DAG tasks of one worker template running
`simrun-worker`: `<task>-w0`, the coordinator (worker `<task>-w0`, `SIMRUN_ROLE=base`,
base priority), which returns once the ledger of the task is complete; `<task>-extra`
over `withSequence: {start: 1, count: pods - 1}` (workers `<task>-w<item>`,
`SIMRUN_ROLE=extra`, extra priority; left out for one pod), which exit when nothing is
pending; and `<task>-merge` running `simrun-merge`, which depends on `<task>-w0` only,
so extra workers still waiting for admission never hold up the merge. An unsplit task
is one pod running its command. A task waits for the tasks it reads from, a split one
for their merge.

The extra workers continue on failure (`continueOn: {failed: true, error: true}`), so
an extra worker which fails does not fail the run; the coordinator releases the
claims of workers whose heartbeat is older than `SIMRUN_STALE_S` (`grace_s + 60`) and
computes them. The workers carry the input parameters `worker`, `priority`, `task`
and `role`. Extra workers may still be pending or running after the last merge of a
run: the controller stops the whole workflow then and records the state of the run
itself.

Every pod carries the Kueue label of the LocalQueue, the WorkloadPriorityClass label
and the Kubernetes `priorityClassName` (the coordinator, the merge and an unsplit task
the base priority, the extra workers the extra priority), the annotation which requires
one node per pod (topology aware scheduling), requests equal to limits, the run
directory of the store mounted read-write and the snapshots read-only, a scratch
directory on the local disk (`<run>/<task>/<worker>` for a worker, `<run>/<task>/merge`
for a merge, `<run>/<task>` for an unsplit task), and the environment of the worker
harness (`simrun.worker`). The label `simrun/group` is the group made a valid label
value (`label_value`), the annotation `simrun/group` the group as given.

The Argo features this relies on (Argo Workflows v4.1.6):

- `retryStrategy.expression` with `lastRetry.exitCode`, a string ("-1" when the pod
  has no exit code), and `retryPolicy: Always`, which must hold together to retry
  (docs/retries.md, "Conditional retries"; the type in docs/variables.md,
  "RetryStrategy"). The expression compares strings, since `"3" != 3` holds in expr.
- `{{retries}}` and `lastRetry.*` in a container template with a retry strategy,
  substituted per attempt, `lastRetry.exitCode` "0" in the first attempt
  (docs/variables.md, "Container/Script Templates"; `executeTemplate` in
  workflow/controller/operator.go), also in `{{=...}}` expressions, which are left
  for the attempt until `lastRetry` is known (`variablesToCheck` in
  util/template/expression_template.go). So `podSpecPatch` sets the memory and
  `SIMRUN_MEMORY_PER_WORKER_GB` of an attempt after an exit 137 to the retry values,
  and every other attempt keeps the first values.
- `podSpecPatch`, a strategic merge patch of the pod spec (containers and env merged
  by name), also for `terminationGracePeriodSeconds`, which a template has no field
  for; the emissary executor reads it after the patch (`createWorkflowPod` in
  workflow/controller/workflowpod.go).
- `{{item}}` in the arguments of an expanded DAG task (`processItem` in
  workflow/controller/operator.go), and input parameters substituted anywhere in a
  template, its metadata and `priorityClassName` included (`SubstituteParams` in
  workflow/common/util.go).
- `continueOn` of a task which no task depends on: the DAG ignores its failure
  (`assessDAGPhase` in workflow/controller/dag.go).
"""

import hashlib
import json
import math
import re
import shlex
from typing import Any

from hera.workflows import Workflow
from hera.workflows import models as m

from simrun.config import ClusterConfig
from simrun.planner import PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND, CommandSettings
from simrun.store import RunRecord
from simrun.worker import (
    EXIT_ANALYSIS,
    EXIT_CONFIGURATION,
    EXIT_POOL_BROKEN,
    SETTINGS_FILE,
)
from simrun.workflow import TaskSpec

QUEUE_LABEL = "kueue.x-k8s.io/queue-name"
PRIORITY_LABEL = "kueue.x-k8s.io/priority-class"
TOPOLOGY_ANNOTATION = "kueue.x-k8s.io/podset-required-topology"
TOPOLOGY_LEVEL = "kubernetes.io/hostname"
RESERVED_KEY = "simrun/reserved"
RUN_LABEL = "simrun/run"
GROUP_LABEL = "simrun/group"
GPU_RESOURCE = "nvidia.com/gpu"
LABEL_MAX = 63
LABEL_HASH = 8
RETRY_LIMIT = 20
RETRY_EXPRESSION = (
    f"lastRetry.exitCode != '{EXIT_ANALYSIS}' "
    f"&& lastRetry.exitCode != '{EXIT_CONFIGURATION}'"
)
# seconds a pod gets beyond the grace of the worker, for the executor to finish
GRACE_MARGIN_S = 30
# seconds beyond the grace after which the coordinator takes a worker as gone
STALE_MARGIN_S = 60
WORKER_PARAMETERS = ("worker", "priority", "task", "role")
ENTRYPOINT = "dag"
CONTAINER = "main"
STORE_VOLUME = "store"
SCRATCH_VOLUME = "scratch"
WORKER_COMMAND = "simrun-worker"
MERGE_COMMAND = "simrun-merge"


def _number(value: float) -> str:
    """A number of GiB for the environment, e.g. `0.5`."""
    return str(float(value))


def _gi(value: float) -> str:
    """A quantity of GiB, e.g. `3Gi` or `2.5Gi`."""
    return f"{int(value)}Gi" if value == int(value) else f"{value}Gi"


def _on_oom(retry: str, first: str) -> str:
    """An expression of an attempt: `retry` after an out of memory exit, else `first`."""
    return (
        f"{{{{=lastRetry.exitCode == '{EXIT_POOL_BROKEN}' ? '{retry}' : '{first}'}}}}"
    )


def label_value(text: str) -> str:
    """A valid label value for free text.

    Lowercase, every character but `[a-z0-9-_.]` replaced by `-`, starting and ending
    alphanumeric, at most 63 characters; a value which had to be changed ends in a
    hash of the text, so different texts keep different values.

    Args:
        text: The text.

    Returns:
        The label value, `text` itself when it is one.
    """
    value = re.sub(r"[^a-z0-9._-]", "-", text.lower()).strip("-_.")
    if value == text and len(value) <= LABEL_MAX:
        return value
    digest = hashlib.sha256(text.encode()).hexdigest()[:LABEL_HASH]
    stem = value[: LABEL_MAX - LABEL_HASH - 1].rstrip("-_.")
    return f"{stem}-{digest}" if stem else digest


def _parameter(name: str) -> str:
    """The reference to an input parameter of a template."""
    return f"{{{{inputs.parameters.{name}}}}}"


def _worker_arguments(worker: str, priority: str, task: str, role: str) -> m.Arguments:
    """The arguments of the worker template, in the order of `WORKER_PARAMETERS`."""
    values = dict(zip(WORKER_PARAMETERS, (worker, priority, task, role), strict=True))
    return m.Arguments(
        parameters=[m.Parameter(name=k, value=v) for k, v in values.items()]
    )


def _resources(shape: PodShape) -> m.ResourceRequirements:
    amounts = {"cpu": str(shape.cores), "memory": _gi(shape.memory_gb)}
    if shape.gpus > 0:
        amounts[GPU_RESOURCE] = str(shape.gpus)
    if shape.scratch_gb > 0:
        amounts["ephemeral-storage"] = _gi(shape.scratch_gb)
    quantities = {k: m.Quantity(v) for k, v in amounts.items()}
    return m.ResourceRequirements(requests=quantities, limits=dict(quantities))


def _pod_spec_patch(shape: PodShape, config: ClusterConfig) -> str:
    """The patch of an attempt: the retry memory after an exit 137, and the grace."""
    memory = _on_oom(_gi(shape.retry_memory_gb), _gi(shape.memory_gb))
    per_worker = _on_oom(
        _number(shape.retry_per_worker_gb), _number(shape.per_worker_gb)
    )
    patch = {
        "terminationGracePeriodSeconds": config.grace_s + GRACE_MARGIN_S,
        "containers": [
            {
                "name": CONTAINER,
                "resources": {
                    "requests": {"memory": memory},
                    "limits": {"memory": memory},
                },
                "env": [{"name": "SIMRUN_MEMORY_PER_WORKER_GB", "value": per_worker}],
            }
        ],
    }
    return json.dumps(patch)


def _task_dir(record: RunRecord, task_id: str, config: ClusterConfig) -> str:
    return f"{config.store_mount}/runs/{record.id}/tasks/{task_id}"


def _output(record: RunRecord, ref: str) -> str:
    """The output a reference `<task>.<port>` names, relative to the store."""
    task_id, _, port = ref.partition(".")
    return f"runs/{record.id}/tasks/{task_id}/out/{port}"


def _env(
    record: RunRecord,
    task: TaskSpec,
    task_type: TaskType,
    shape: PodShape,
    config: ClusterConfig,
    worker: dict[str, str] | None = None,
) -> list[m.EnvVar]:
    """The environment of the worker harness, see `simrun.worker`.

    `worker` holds the variables of a worker pod, e.g. `SIMRUN_WORKER`.
    """
    inputs = {port: _output(record, ref) for port, ref in task.inputs.items()}
    for port in task.stage:
        if port not in inputs:
            raise ValueError(f"task {task.id!r} stages {port!r}, which is no input")
    values = {
        "SIMRUN_TASK_DIR": _task_dir(record, task.id, config),
        "SIMRUN_TASK_TYPE": task_type.name,
        "SIMRUN_ATTEMPT": "{{retries}}",
        "SIMRUN_MEMORY_BASE_GB": _number(task_type.hint.memory_base_gb),
        "SIMRUN_MEMORY_PER_WORKER_GB": _number(shape.per_worker_gb),
        "SIMRUN_INPUTS": ",".join(f"{port}={path}" for port, path in inputs.items()),
        "SIMRUN_STAGE": ",".join(
            f"{inputs[port]}={config.scratch_path}/{port}" for port in task.stage
        ),
        "SIMRUN_EDITABLE": ",".join(
            f"{config.store_mount}/{source.snapshot}" for source in record.sources
        ),
        "SIMRUN_SCRATCH": config.scratch_path,
        **(worker or {}),
    }
    env = [m.EnvVar(name=k, value=v) for k, v in values.items()]
    node = m.EnvVarSource(field_ref=m.ObjectFieldSelector(field_path="spec.nodeName"))
    env.append(m.EnvVar(name="SIMRUN_NODE", value_from=node))
    return env


def _mounts(
    record: RunRecord, scratch: str, config: ClusterConfig
) -> list[m.VolumeMount]:
    """The mounts of a pod; `scratch` is its directory below the scratch path."""
    run = f"runs/{record.id}"
    return [
        m.VolumeMount(
            name=STORE_VOLUME, mount_path=f"{config.store_mount}/{run}", sub_path=run
        ),
        m.VolumeMount(
            name=STORE_VOLUME,
            mount_path=f"{config.store_mount}/snapshots",
            sub_path="snapshots",
            read_only=True,
        ),
        m.VolumeMount(
            name=SCRATCH_VOLUME,
            mount_path=config.scratch_path,
            sub_path=scratch,
        ),
    ]


def _template(
    name: str,
    image: str,
    command: list[str],
    env: list[m.EnvVar],
    mounts: list[m.VolumeMount],
    shape: PodShape,
    priority: str,
    config: ClusterConfig,
    inputs: list[str] | None = None,
) -> m.Template:
    """A pod template with its Kueue metadata, resources and retries."""
    return m.Template(
        name=name,
        inputs=m.Inputs(parameters=[m.Parameter(name=p) for p in inputs])
        if inputs
        else None,
        metadata=m.Metadata(
            labels={QUEUE_LABEL: config.local_queue, PRIORITY_LABEL: priority},
            annotations={TOPOLOGY_ANNOTATION: TOPOLOGY_LEVEL},
        ),
        priority_class_name=priority,
        container=m.Container(
            name=CONTAINER,
            image=image,
            command=command or None,
            env=env,
            resources=_resources(shape),
            volume_mounts=mounts,
        ),
        retry_strategy=m.RetryStrategy(
            limit=m.IntOrString(RETRY_LIMIT),
            retry_policy="Always",
            expression=RETRY_EXPRESSION,
        ),
        pod_spec_patch=_pod_spec_patch(shape, config),
    )


def _image(record: RunRecord, task: TaskSpec, task_type: TaskType) -> str:
    if task_type.image is not None:
        return task_type.image
    if record.image is None:
        raise ValueError(
            f"task {task.id!r} of type {task_type.name!r} needs the image of the run, "
            "and the run has none"
        )
    return record.image


def _merge_shape(shape: PodShape, base_gb: float) -> PodShape:
    """The merge: one core and the memory of one worker process besides the base."""
    return PodShape(
        cores=1,
        memory_gb=math.ceil(base_gb + shape.per_worker_gb),
        processes=1,
        per_worker_gb=shape.per_worker_gb,
        retry_memory_gb=math.ceil(base_gb + shape.retry_per_worker_gb),
        retry_per_worker_gb=shape.retry_per_worker_gb,
        scratch_gb=0.0,
        gpus=0,
    )


def _unsplit_command(
    record: RunRecord, task: TaskSpec, task_type: TaskType, config: ClusterConfig
) -> tuple[str, list[str]]:
    """The image and argv of an unsplit task."""
    if task_type.name == COMMAND.name:
        settings = CommandSettings.model_validate(task.settings)
        return settings.image, settings.args
    if task_type.command is None:
        raise ValueError(
            f"task {task.id!r}: type {task_type.name!r} is neither splittable "
            "nor has a command"
        )
    task_dir = _task_dir(record, task.id, config)
    argv = [
        part.replace("{settings}", f"{task_dir}/{SETTINGS_FILE}").replace(
            "{out}", f"{task_dir}/out"
        )
        for part in shlex.split(task_type.command)
    ]
    return _image(record, task, task_type), argv


def to_argo(
    record: RunRecord,
    plans: dict[str, TaskPlan],
    types: dict[str, TaskType],
    config: ClusterConfig,
) -> dict[str, Any]:
    """The Argo workflow of a run.

    The settings of every task are written to `<task dir>/settings.json` before the
    workflow is submitted; the manifest does not carry them.

    Args:
        record: The run.
        plans: The plan of every task, by task id.
        types: The task types, by name.
        config: The cluster.

    Returns:
        The manifest of the workflow.

    Raises:
        ValueError: A task has no image or no command, or stages a port which is no
            input.
    """
    workflow = record.workflow
    split = {t.id: types[t.type].splittable for t in workflow.tasks}
    templates: list[m.Template] = []
    tasks: list[m.DAGTask] = []
    for task in workflow.order():
        task_type = types[task.type]
        plan = plans[task.id]
        shape = plan.shape
        dependencies = [
            f"{dep}-merge" if split[dep] else dep for dep in task.dependencies()
        ] or None
        if not split[task.id]:
            image, argv = _unsplit_command(record, task, task_type, config)
            env = _env(record, task, task_type, shape, config)
            name = f"{task.id}-run"
            mounts = _mounts(record, f"{record.id}/{task.id}", config)
            templates.append(
                _template(
                    name, image, argv, env, mounts, shape, plan.base_priority, config
                )
            )
            tasks.append(
                m.DAGTask(name=task.id, template=name, dependencies=dependencies)
            )
            continue
        image = _image(record, task, task_type)
        worker = _parameter("worker")
        worker_env = _env(
            record,
            task,
            task_type,
            shape,
            config,
            {
                "SIMRUN_WORKER": worker,
                "SIMRUN_ROLE": _parameter("role"),
                "SIMRUN_STALE_S": str(config.grace_s + STALE_MARGIN_S),
            },
        )
        templates.append(
            _template(
                f"{task.id}-worker",
                image,
                [WORKER_COMMAND],
                worker_env,
                _mounts(record, f"{record.id}/{task.id}/{worker}", config),
                shape,
                _parameter("priority"),
                config,
                inputs=list(WORKER_PARAMETERS),
            )
        )
        tasks.append(
            m.DAGTask(
                name=f"{task.id}-w0",
                template=f"{task.id}-worker",
                dependencies=dependencies,
                arguments=_worker_arguments(
                    f"{task.id}-w0", plan.base_priority, task.id, "base"
                ),
            )
        )
        if plan.pods > 1:
            tasks.append(
                m.DAGTask(
                    name=f"{task.id}-extra",
                    template=f"{task.id}-worker",
                    dependencies=dependencies,
                    continue_on=m.ContinueOn(failed=True, error=True),
                    with_sequence=m.Sequence(
                        start=m.IntOrString("1"),
                        count=m.IntOrString(str(plan.pods - 1)),
                    ),
                    arguments=_worker_arguments(
                        f"{task.id}-w{{{{item}}}}",
                        plan.extra_priority,
                        task.id,
                        "extra",
                    ),
                )
            )
        merge_shape = _merge_shape(shape, task_type.hint.memory_base_gb)
        merge_env = _env(record, task, task_type, merge_shape, config)
        templates.append(
            _template(
                f"{task.id}-merge",
                image,
                [MERGE_COMMAND],
                merge_env,
                _mounts(record, f"{record.id}/{task.id}/merge", config),
                merge_shape,
                plan.base_priority,
                config,
            )
        )
        tasks.append(
            m.DAGTask(
                name=f"{task.id}-merge",
                template=f"{task.id}-merge",
                dependencies=[f"{task.id}-w0"],
            )
        )
    templates.insert(0, m.Template(name=ENTRYPOINT, dag=m.DAGTemplate(tasks=tasks)))

    labels = {RUN_LABEL: record.id}
    annotations = None
    if record.group is not None:
        labels[GROUP_LABEL] = label_value(record.group)
        annotations = {GROUP_LABEL: record.group}
    node_selector = None
    tolerations = None
    if record.reserve is not None:
        node_selector = {RESERVED_KEY: record.id}
        tolerations = [
            m.Toleration(
                key=RESERVED_KEY, operator="Equal", value=record.id, effect="NoSchedule"
            )
        ]
    volumes = [
        m.Volume(
            name=STORE_VOLUME,
            persistent_volume_claim=m.PersistentVolumeClaimVolumeSource(
                claim_name=config.store_claim
            ),
        ),
        m.Volume(
            name=SCRATCH_VOLUME,
            host_path=m.HostPathVolumeSource(
                path=config.scratch_path, type="DirectoryOrCreate"
            ),
        ),
    ]
    return Workflow(
        name=f"simrun-{record.id}",
        namespace=config.namespace,
        labels=labels,
        annotations=annotations,
        entrypoint=ENTRYPOINT,
        templates=list(templates),  # a list of the union hera declares
        volumes=volumes,
        node_selector=node_selector,
        tolerations=tolerations,
        ttl_strategy=m.TTLStrategy(
            seconds_after_completion=config.ttl_after_finished_s
        ),
    ).to_dict()
