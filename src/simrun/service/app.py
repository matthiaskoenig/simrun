"""The API of the service: people and agents submit, follow and compare runs.

Every route of the API needs a bearer token of the tokens file
(`simrun.service.auth`); only the files of the web app at `/ui/` (setting `web`)
answer without one. The identity of the token decides the priority of its runs. A
submission is validated, its resources resolved and its tasks planned on the nodes
of the backend, and the record (`submitting`) and the settings of every task
written; `POST /runs` returns then, and the environment, the image and the workflow
follow in the background (`simrun.service.submission`). The controller
(`simrun.service.controller`) runs the submissions and follows the workflows in a
background task.

Run it with `uvicorn --factory simrun.service.app:serve --timeout-graceful-shutdown 5`,
which reads the `ServiceSettings` from the YAML file named by `SIMRUN_CONFIG`; the
timeout ends the open event streams when the service stops.
"""

import asyncio
import dataclasses
import json
import logging
import os
import shutil
import tempfile
import time
from collections.abc import AsyncGenerator, Callable, Iterable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal

import yaml
from fastapi import Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    TypeAdapter,
    ValidationError,
)

from simrun.argo import workflow_name
from simrun.config import ClusterConfig
from simrun.ledger import Ledger, LedgerCounts
from simrun.planner import Node, PlanError, TaskPlan, plan_task
from simrun.plugins import TaskType, load_task_types, resolve
from simrun.plugins_builtin import COMMAND
from simrun.resources import ResolvedResources, ResourceStore, resource_key
from simrun.service.auth import Identity, authenticate, load_tokens, priority_of
from simrun.service.backend import Backend, Placement
from simrun.service.compare import CompareTable, compare_runs
from simrun.service.controller import (
    FINAL_TASK_STATES,
    TERMINAL_RUN_STATES,
    Controller,
    attempt_of,
    is_oom,
)
from simrun.service.edits import (
    EditRequest,
    EditResult,
    EditSession,
    EditState,
    EditStore,
    OpenEdit,
    RunBody,
    SaveBody,
)
from simrun.service.index import IndexedRunStore, RunIndex
from simrun.service.logs import DEFAULT_TAIL_LINES, read_kept, render
from simrun.service.submission import Submitter, has_model_project
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, RunSummary, TaskState
from simrun.worker import (
    MERGED_MARKER,
    PROGRESS_FILE,
    PROVENANCE_FILE,
    SETTINGS_FILE,
    UNITS_DIR,
    WORKERS_DIR,
    WorkerReport,
)
from simrun.workflow import Priority, SplitMode, Workflow, workflow_yaml

logger = logging.getLogger(__name__)

CONFIG_ENV = "SIMRUN_CONFIG"
WAIT_POLL_S = 0.2
WAIT_MAX_S = 300.0
# the longest long poll of the command of an edit session, and how long a save or
# a run of the browser waits for its answer
EDIT_POLL_MAX_S = 30.0
EDIT_WAIT_S = 60.0
# a snapshot younger than this is kept by a cleanup: a client may be submitting it
SNAPSHOT_GRACE_S = 3600.0
LATEST_LINK = "latest"
# an ended run is moved to `runs/<prefix><id>-<pid>` under the lock, then removed
TRASH_PREFIX = ".trash-"
PROGRESS_TAIL_BYTES = 8192
# the most a task which is not over counts towards the progress of its run
UNFINISHED_TASK_MAX = 0.99


class ServiceSettings(BaseModel):
    """The settings of the service.

    Attributes:
        store: Root of the run store.
        db: SQLite file of the resource observations and the index of the runs, on
            a local volume.
        tokens: YAML file of the tokens, see `simrun.service.auth.load_tokens`.
        owner: The person who owns the cluster; their runs have the top priority.
        cluster: The cluster.
        poll_s: Seconds between two reconciliations of the controller, and
            between two checks of an event stream.
        events_max_s: Seconds after which an event stream ends and the client
            reconnects.
        default_image: Image of a run whose model has no `pyproject.toml`.
        simrun_wheel: The simrun wheel the images of the environments install.
        python: Python version of the images.
        build_constraints: The pinned versions of the build backends of the
            environments, the file of simrun (`simrun.environment.BUILD_CONSTRAINTS`)
            by default.
        backend: `module:callable` returning the backend, for `serve`.
        web: Directory of the built web app, served at `/ui/` without a token; the
            app itself talks to the API with the token of the user.
    """

    store: Path
    db: Path
    tokens: Path
    owner: str
    cluster: ClusterConfig
    poll_s: float = 5.0
    events_max_s: float = 300.0
    default_image: str | None = None
    simrun_wheel: Path | None = None
    python: str = "3.14"
    build_constraints: Path | None = None
    backend: str | None = None
    web: Path | None = None


class SubmitRequest(BaseModel):
    """A submission of a workflow.

    Attributes:
        workflow: The workflow.
        sources: The snapshots of the sources, the model first.
        group: Group of the run, e.g. a study.
        split: Split mode of every task, else the one of the task or the workflow.
        reserve: A node reserved for the run, else the one of the workflow.
    """

    workflow: Workflow
    sources: list[SourceProvenance] = Field(default_factory=list)
    group: str | None = None
    split: SplitMode | None = None
    reserve: str | None = None


class ValidationResult(BaseModel):
    """The result of a validation: the errors of a workflow, none when valid."""

    ok: bool
    errors: list[str]


class TaskEstimate(BaseModel):
    """The resources, the plan and the expected duration of a task.

    Attributes:
        resources: The resolved resources.
        plan: The plan of the pods.
        seconds: The expected seconds of a split task: the median time of a unit in
            one process of past runs times the rounds of its units over the
            processes of its pods, None without past runs.
    """

    resources: ResolvedResources
    plan: TaskPlan
    seconds: float | None


class TaskProgress(BaseModel):
    """The progress of a task: its ledger and the first and last lines of its progress file.

    The first line is the first unit done, whose time is when the task began to
    deliver units.
    """

    counts: LedgerCounts | None
    # a default: a client validates the answers of a service not yet redeployed
    first: dict[str, Any] | None = None
    last: dict[str, Any] | None


class RunItem(RunSummary):
    """A run of the list of runs: its summary with its owner and workflow name.

    Attributes:
        owner: The owner of the run, of an agent the person it acts for.
        workflow: The name of the workflow.
        progress: The progress of an active run (submitting, queued, running) from
            0 to 1, from the states of its tasks and their ledgers (`run_progress`);
            None for a run which ended.
    """

    owner: str
    workflow: str
    progress: float | None = None


class PodUsage(BaseModel):
    """A pod of a run on a server.

    Attributes:
        run_id: The run of the pod, None when the pod belongs to no known run.
        task_id: The task of the pod.
        pod: The name of the pod.
        phase: The phase of the pod.
        cores: The cores it requests.
        memory_gb: The GiB of memory it requests.
    """

    run_id: str | None
    task_id: str
    pod: str
    phase: str
    cores: float
    memory_gb: float


class ServerUsage(BaseModel):
    """A server of the cluster with the pods of runs on it.

    Attributes:
        name: The name of the server.
        cores: The cores the scheduler gives the pods of the runs: the allocatable
            ones less the requests of other pods, e.g. of DaemonSets.
        memory_gb: The GiB of memory the scheduler gives the pods of the runs.
        gpus: The GPUs of the server.
        reserved_by: The run which reserved the server, None if free.
        pods: The pods of runs on the server, with their requests, which count
            against `cores` and `memory_gb`.
    """

    name: str
    cores: float
    memory_gb: float
    gpus: int
    reserved_by: str | None
    pods: list[PodUsage]


class WorkerView(BaseModel):
    """A worker of a task, from its reports, its pod and its claims.

    Attributes:
        worker: The worker id.
        node: The server of the attempt shown: of the live pod while it runs,
            None while that pod is not bound, else of the last report.
        state: `running` while a pod of an attempt without a report lives (a
            retry after an eviction, a preemption, a lost node or a kill for
            memory, as a worker reports also when it is stopped), else from the
            last report: `failed` when it was killed for memory, else `done`.
        attempts: The attempts of the worker: one more than the highest attempt
            of its reports and of its live pod.
        processes: The processes of its pool, None before a report.
        units_done: The distinct units its `unit_done` lines in the progress
            file of the task name, live and over all its attempts.
        units_claimed: The units it holds claimed now.
        peak_memory_gb: The peak memory of the last report.
        memory_limit_gb: The memory limit of the last report.
        oom_killed: Whether the attempt of the last report was killed for memory.
    """

    worker: str
    node: str | None
    state: Literal["running", "done", "failed"]
    attempts: int
    processes: int | None
    units_done: int
    units_claimed: int
    peak_memory_gb: float | None
    memory_limit_gb: float | None
    oom_killed: bool


class OutputFile(BaseModel):
    """A file of the output of a task, its path relative to the store."""

    path: str
    bytes: int


class HintView(BaseModel):
    """The default resources of a task type."""

    memory_base_gb: float
    memory_per_worker_gb: float
    scratch_gb: float
    gpus: int


class TaskTypeView(BaseModel):
    """A task type with the JSON Schema of its settings, for the editor."""

    name: str
    settings_schema: dict[str, Any]
    inputs: list[str]
    outputs: list[str]
    splittable: bool
    uses_cores: bool
    has_plan: bool
    cost_setting: str | None
    hint: HintView
    description: str

    @classmethod
    def of(cls, task_type: TaskType) -> TaskTypeView:
        """The view of a task type."""
        return cls(
            name=task_type.name,
            settings_schema=task_type.settings.model_json_schema(),
            inputs=list(task_type.inputs),
            outputs=list(task_type.outputs),
            splittable=task_type.splittable,
            uses_cores=task_type.uses_cores,
            has_plan=task_type.plan is not None,
            cost_setting=task_type.cost_setting,
            hint=HintView(**dataclasses.asdict(task_type.hint)),
            description=task_type.description,
        )


class CleanupRequest(BaseModel):
    """Which ended runs a cleanup removes.

    Attributes:
        older_than_days: Runs which ended at least this many days ago.
        group: Only the runs of this group.
        failed_only: Only failed runs.
        dry_run: List what would be removed, remove nothing.
    """

    older_than_days: float = Field(ge=0)
    group: str | None = None
    failed_only: bool = False
    dry_run: bool = False


class CleanupResult(BaseModel):
    """What a cleanup or a deletion removed, paths relative to the store."""

    runs: list[str]
    snapshots: list[str]
    bytes_freed: int
    dry_run: bool


class Usage(BaseModel):
    """Bytes of the run store per run and per group (`""` without a group)."""

    runs: dict[str, int]
    groups: dict[str, int]


class CompareRequest(BaseModel):
    """The runs to compare."""

    runs: list[str]


def unknown_settings(model: type[BaseModel], settings: dict[str, Any]) -> list[str]:
    """The keys of settings which are no field of their model, sorted.

    A field counts by its name and its aliases; a model which allows extra keys
    (`extra="allow"`) has none unknown.
    """
    if model.model_config.get("extra") == "allow":
        return []
    known: set[str] = set()
    for name, field in model.model_fields.items():
        known.add(name)
        if field.alias is not None:
            known.add(field.alias)
        alias = field.validation_alias
        if isinstance(alias, str):
            known.add(alias)
        elif isinstance(alias, AliasChoices):
            known.update(a for a in alias.choices if isinstance(a, str))
    return sorted(set(settings) - known)


def validate_workflow(workflow: Workflow, types: dict[str, TaskType]) -> list[str]:
    """The errors of a workflow beyond its structure: types, settings and ports.

    A setting which is no field of the settings model of its type is an error, so a
    misspelled one is not silently dropped. An invalid setting is one error per
    field, `task '<id>': <loc>: <msg>`, the location dotted.

    Args:
        workflow: The workflow.
        types: The task types by name.

    Returns:
        The errors, empty when the workflow is valid.
    """
    errors: list[str] = []
    for task in workflow.tasks:
        task_type = types.get(task.type)
        if task_type is None:
            errors.append(f"task {task.id!r}: unknown task type {task.type!r}")
            continue
        errors.extend(
            f"task {task.id!r}: unknown setting {key!r}"
            for key in unknown_settings(task_type.settings, task.settings)
        )
        try:
            task_type.settings.model_validate(task.settings)
        except ValidationError as error:
            # one message per field, `<loc>: <msg>`, which the Editor puts under the field
            errors.extend(f"task {task.id!r}: {message}" for message in _errors(error))
        for port in task.stage:
            if port not in task.inputs:
                errors.append(f"task {task.id!r} stages {port!r}, which is no input")
        for port, ref in task.inputs.items():
            source, _, output = ref.partition(".")
            source_type = types.get(workflow.task(source).type)
            if source_type is not None and output not in source_type.outputs:
                errors.append(
                    f"input {port!r} of task {task.id!r}: task {source!r} has no "
                    f"output {output!r}"
                )
    return errors


def _errors(error: ValidationError) -> list[str]:
    return [
        f"{'.'.join(map(str, d['loc']))}: {d['msg']}" if d["loc"] else d["msg"]
        for d in error.errors()
    ]


def _files(root: Path) -> Iterable[os.stat_result]:
    """The stat of every file below `root`, or of `root` itself, links not followed."""
    if root.is_symlink() or root.is_file():
        yield root.lstat()
        return
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            with suppress(FileNotFoundError):
                yield (Path(dirpath) / name).lstat()


def freed_bytes(paths: Iterable[Path]) -> int:
    """The bytes removing `paths` frees: files of which no hard link stays."""
    seen: dict[tuple[int, int], list[int]] = {}
    for path in paths:
        for st in _files(path):
            entry = seen.setdefault(
                (st.st_dev, st.st_ino), [st.st_size, st.st_nlink, 0]
            )
            entry[2] += 1
    return sum(size for size, links, count in seen.values() if count >= links)


def disk_usage(path: Path) -> int:
    """The bytes of the files below `path`, every hard linked file once."""
    sizes = {(st.st_dev, st.st_ino): st.st_size for st in _files(path)}
    return sum(sizes.values())


def _task_of(dag_task: str) -> str:
    """The task id of a DAG task: `<task>-w0`, `<task>-extra`, `<task>-merge`."""
    for suffix in ("-w0", "-extra", "-merge"):
        if dag_task.endswith(suffix):
            return dag_task.removesuffix(suffix)
    return dag_task


def _gb(value: int | None) -> float | None:
    """Bytes as GiB, None stays None."""
    return None if value is None else value / 1024**3


def _newer(pod: Placement, than: Placement) -> bool:
    """Whether a pod is of a later attempt; a pod of no known attempt is newest."""
    if pod.attempt is None or than.attempt is None:
        return than.attempt is not None
    return pod.attempt > than.attempt


def run_progress(
    tasks: dict[str, TaskState], counts: dict[str, LedgerCounts | None]
) -> float:
    """The progress of a run from 0 to 1, as the web app counts it.

    The mean over its tasks: a task which is over (done or skipped) counts whole, a
    task with a ledger the share of its units done, at most `UNFINISHED_TASK_MAX`
    (its merge and outputs are still to come), any other task nothing; so the
    progress is 1 only once every task is over.

    Args:
        tasks: The states of the tasks.
        counts: The ledger counts by task, None or missing without a ledger.

    Returns:
        The progress, 0 for a run without tasks.
    """
    if not tasks:
        return 0.0
    total = 0.0
    for task_id, state in tasks.items():
        found = counts.get(task_id)
        if state in (TaskState.DONE, TaskState.SKIPPED):
            total += 1.0
        elif found is not None and found.total > 0:
            total += min(found.done / found.total, UNFINISHED_TASK_MAX)
    return total / len(tasks)


def _pending(record: RunRecord) -> bool:
    """Whether a run still has to release its node or cancel its workflow."""
    return record.pending_release is not None or record.pending_cancel


def _units_by_worker(path: Path) -> dict[str, set[int]]:
    """The distinct units of the `unit_done` lines of a progress file, per worker.

    A worker appends a line per unit it completes, as it completes it, so the
    count is live; a unit done twice (a lost claim, a retried attempt) counts
    once. Other lines, and lines which are no JSON (a line being written),
    are skipped.
    """
    units: dict[str, set[int]] = {}
    try:
        with path.open("rb") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if (
                    isinstance(value, dict)
                    and value.get("event") == "unit_done"
                    and isinstance(value.get("worker"), str)
                    and isinstance(value.get("unit"), int)
                ):
                    units.setdefault(value["worker"], set()).add(value["unit"])
    except FileNotFoundError:
        pass
    return units


def _json_object(line: bytes) -> dict[str, Any] | None:
    """A line of a JSON lines file as an object, None for anything else."""
    try:
        value = json.loads(line)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def _first_line(path: Path) -> dict[str, Any] | None:
    """The first line of a JSON lines file, None without one."""
    try:
        with path.open("rb") as handle:
            line = handle.readline(PROGRESS_TAIL_BYTES)
    except FileNotFoundError:
        return None
    return _json_object(line) if line.strip() else None


def _last_line(path: Path) -> dict[str, Any] | None:
    """The last line of a JSON lines file, None without one."""
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            handle.seek(max(0, handle.tell() - PROGRESS_TAIL_BYTES))
            lines = handle.read().splitlines()
    except FileNotFoundError:
        return None
    for line in reversed(lines):
        if line.strip():
            return _json_object(line)
    return None


class _Service:
    """The work behind the routes."""

    def __init__(
        self,
        settings: ServiceSettings,
        backend: Backend,
        types: dict[str, TaskType],
    ) -> None:
        self.settings = settings
        self.backend = backend
        self.types = types
        # the index is rebuilt from the records, the store is the source of truth
        self.store = IndexedRunStore(settings.store, RunIndex(settings.db))
        self.store.rebuild()
        self.resources = ResourceStore(settings.db)
        self.controller = Controller(self.store, self.resources, backend, types)
        self.submitter = Submitter(
            self.store,
            backend,
            types,
            self.controller,
            settings.cluster,
            settings.python,
            settings.simrun_wheel,
            settings.build_constraints,
        )
        self.controller.resume = self.submitter.run
        self.tokens = load_tokens(settings.tokens)
        self.edits = EditStore(settings.store)

    # submission

    def _nodes(self, reserve: str | None) -> list[Node]:
        nodes = self.backend.nodes()
        if reserve is None:
            return nodes
        node = self._free_node(nodes, reserve)
        return [dataclasses.replace(node, reserved_by=None)]

    @staticmethod
    def _free_node(nodes: list[Node], name: str) -> Node:
        node = next((n for n in nodes if n.name == name), None)
        if node is None:
            raise HTTPException(422, f"unknown node {name!r} to reserve")
        if node.reserved_by is not None:
            raise HTTPException(
                409, f"node {name!r} is reserved by run {node.reserved_by}"
            )
        return node

    def plan(
        self, request: SubmitRequest, priority: Priority
    ) -> dict[str, tuple[ResolvedResources, TaskPlan]]:
        """Resolve the resources and plan every task, in the order of the workflow.

        Raises:
            HTTPException: 422 for an invalid workflow or a task which fits no node,
                409 for a reserved node which is taken.
        """
        workflow = request.workflow
        errors = validate_workflow(workflow, self.types)
        if errors:
            raise HTTPException(422, "; ".join(errors))
        self._check_sources(request.sources)
        nodes = self._nodes(request.reserve or workflow.reserve)
        model = request.sources[0].name if request.sources else ""
        planned: dict[str, tuple[ResolvedResources, TaskPlan]] = {}
        for task in workflow.order():
            task_type = self.types[task.type]
            units = None
            if task_type.splittable and task_type.count_units is not None:
                settings = task_type.settings.model_validate(task.settings)
                units = resolve(task_type.count_units)(settings)
            key = resource_key(task_type, task.settings, model)
            resources = self.resources.resolve(key, task_type.hint, task.resources)
            mode = request.split or task.split or workflow.split
            try:
                plan = plan_task(
                    task, task_type, resources, nodes, mode, priority, units
                )
            except PlanError as error:
                raise HTTPException(422, f"task {task.id!r}: {error}") from error
            planned[task.id] = (resources, plan)
        return planned

    def _check_sources(self, sources: list[SourceProvenance]) -> None:
        """Check that the snapshots are below the store and exist in it.

        Raises:
            HTTPException: 422 for a path outside the store or a snapshot which the
                store of the service does not have, e.g. one a client wrote with a
                `SIMRUN_STORE` other than the store of the service.
        """
        for source in sources:
            for rel in (source.snapshot, source.patch):
                if rel is None:
                    continue
                path = PurePosixPath(rel)
                if path.is_absolute() or ".." in path.parts:
                    raise HTTPException(
                        422,
                        f"source {source.name!r}: {rel!r} is not relative to the store",
                    )
            if not (self.settings.store / source.snapshot).is_dir():
                raise HTTPException(
                    422,
                    f"source {source.name!r}: the snapshot {source.snapshot!r} is not "
                    "in the store of the service; is SIMRUN_STORE the mount of its "
                    "store?",
                )

    def _needs_run_image(self, workflow: Workflow) -> bool:
        return any(
            self.types[t.type].image is None and t.type != COMMAND.name
            for t in workflow.tasks
        )

    def _image(self, request: SubmitRequest) -> str | None:
        """The image of a run without its own environment, the default image.

        A run whose model has a `pyproject.toml` gets the image of its environment
        in the background (None until then).

        Raises:
            HTTPException: 422 when the run needs an image and the service has no
                default image.
        """
        if has_model_project(self.settings.store, request.sources):
            return None
        if self.settings.default_image is None and self._needs_run_image(
            request.workflow
        ):
            raise HTTPException(
                422,
                "the run needs an image: its model has no pyproject.toml and the "
                "service has no default_image",
            )
        return self.settings.default_image

    def submit(self, request: SubmitRequest, identity: Identity) -> RunRecord:
        """Plan a run and write it `submitting`; the rest follows in the background.

        The environment, the image and the workflow are resolved, built and
        submitted by the `Submitter` in the pool of the controller.
        """
        priority = priority_of(identity, self.settings.owner)
        planned = self.plan(request, priority)
        image = self._image(request)
        workflow = request.workflow
        run_id = self.store.new_run_id()
        record = RunRecord(
            id=run_id,
            submitted_by=identity.name,
            owner=identity.owner,
            agent=identity.name if identity.kind == "agent" else None,
            priority=priority,
            group=request.group,
            workflow=workflow,
            sources=request.sources,
            image=image,
            reserve=request.reserve or workflow.reserve,
            state=RunState.SUBMITTING,
            tasks=dict.fromkeys((t.id for t in workflow.tasks), TaskState.WAITING),
            plans={k: dataclasses.asdict(plan) for k, (_, plan) in planned.items()},
            # named before the submission, so a workflow a failed or an interrupted
            # submission created is found and cancelled
            backend_name=workflow_name(run_id),
            created=datetime.now(UTC),
            finished=None,
            message=None,
        )
        self._write(record)
        self.controller.start_submission(record.id, self.submitter.run)
        logger.info("run %s accepted from %s", record.id, identity.name)
        return record

    def _write(self, record: RunRecord) -> None:
        """Write the record, the settings and the provenance of every task.

        The settings are written as submitted, not as the model of the service
        validates them: the worker validates them with the plugin of the snapshot,
        whose defaults may differ from the ones of the service. The provenance is
        the list of the sources of the run, which a task reads through
        `SIMRUN_PROVENANCE`.
        """
        with self.controller.lock:
            self.store.write(record)
        run_dir = self.store.run_dir(record.id)
        provenance = json.dumps(
            [source.model_dump(mode="json") for source in record.sources], indent=2
        )
        for task in record.workflow.tasks:
            task_dir = run_dir / "tasks" / task.id
            task_dir.mkdir(parents=True, exist_ok=True)
            (task_dir / SETTINGS_FILE).write_text(json.dumps(task.settings, indent=2))
            (task_dir / PROVENANCE_FILE).write_text(provenance)

    # queries

    def read(self, run_id: str) -> RunRecord:
        """The record of a run; 404 when unknown."""
        try:
            return self.store.read(run_id)
        except KeyError:
            raise HTTPException(404, f"unknown run {run_id!r}") from None

    def task_dir(self, record: RunRecord, task_id: str) -> Path:
        """The directory of a task of a run; 404 when the run has no such task."""
        if task_id not in record.tasks:
            raise HTTPException(404, f"run {record.id} has no task {task_id!r}")
        return self.store.run_dir(record.id) / "tasks" / task_id

    def counts(self, record: RunRecord, task_id: str) -> LedgerCounts | None:
        """The ledger counts of a task, None for a task without a ledger.

        A ledger which vanishes while it is read (a run cleaned up, the store
        unreachable for a moment) counts as none, so one run does not break the
        list of runs or a stream.
        """
        task_dir = self.task_dir(record, task_id)
        units = record.plans.get(task_id, {}).get("units")
        if units is None or not (task_dir / UNITS_DIR).is_dir():
            return None
        try:
            return Ledger(task_dir / UNITS_DIR, units).counts()
        except OSError as e:
            logger.warning("the ledger of %s/%s: %s", record.id, task_id, e)
            return None

    def progress(self, record: RunRecord) -> dict[str, TaskProgress]:
        """The ledger counts and the first and the last progress line of every task."""
        result: dict[str, TaskProgress] = {}
        for task_id in record.tasks:
            path = self.task_dir(record, task_id) / PROGRESS_FILE
            result[task_id] = TaskProgress(
                counts=self.counts(record, task_id),
                first=_first_line(path),
                last=_last_line(path),
            )
        return result

    def outputs(self, record: RunRecord, task_id: str) -> list[OutputFile]:
        """The files of the output of a task, relative to the store."""
        out = self.task_dir(record, task_id) / "out"
        files = (
            [p for p in out.rglob("*") if p.is_file() and p.name != MERGED_MARKER]
            if out.is_dir()
            else []
        )
        return [
            OutputFile(
                path=p.relative_to(self.settings.store).as_posix(),
                bytes=p.stat().st_size,
            )
            for p in sorted(files)
        ]

    def run_items(
        self,
        group: str | None,
        owner: str | None,
        agent: str | None,
        state: RunState | None,
        limit: int,
    ) -> list[RunItem]:
        """The most recent runs matching the filters, the newest first.

        An active run carries its progress, from its record and its ledgers.
        """
        runs = self.store.index.runs(group, owner, agent, state)
        runs.sort(key=lambda r: (r.summary.created, r.summary.id), reverse=True)
        return [
            RunItem(
                **r.summary.model_dump(),
                owner=r.owner,
                workflow=r.workflow,
                progress=self._run_progress(r.summary),
            )
            for r in runs[:limit]
        ]

    def _run_progress(self, summary: RunSummary) -> float | None:
        """The progress of an active run, None for one which ended or is gone."""
        if summary.state in TERMINAL_RUN_STATES:
            return None
        try:
            record = self.store.read(summary.id)
        except KeyError:
            return None
        counts = {t: self.counts(record, t) for t in record.tasks}
        return run_progress(record.tasks, counts)

    def servers(self) -> list[ServerUsage]:
        """Every node with the pods of the runs on it."""
        run_ids = {
            workflow_name(r.summary.id): r.summary.id for r in self.store.index.runs()
        }
        pods: dict[str, list[PodUsage]] = {}
        for p in self.backend.placements():
            if p.node is None:
                continue
            pods.setdefault(p.node, []).append(
                PodUsage(
                    run_id=run_ids.get(p.workflow),
                    task_id=_task_of(p.dag_task),
                    pod=p.pod,
                    phase=p.phase,
                    cores=p.cores,
                    memory_gb=p.memory_gb,
                )
            )
        return [
            ServerUsage(
                name=n.name,
                # what the scheduler counts the requests of the pods against, not
                # the capacity of one worker pod the planner plans with
                cores=n.allocatable_cores,
                memory_gb=n.allocatable_memory_gb,
                gpus=n.gpus,
                reserved_by=n.reserved_by,
                pods=pods.get(n.name, []),
            )
            for n in self.backend.nodes()
        ]

    def live_pods(self, record: RunRecord) -> list[Placement]:
        """The pods of the workers of a run which have not terminated.

        None before the workflow of the run was submitted and none once the run
        ended: the backend is not asked then.
        """
        if record.backend_name is None or record.state in TERMINAL_RUN_STATES:
            return []
        return [
            p
            for p in self.backend.placements()
            if p.workflow == record.backend_name and p.worker is not None
        ]

    def task_workers(self, record: RunRecord) -> dict[str, list[WorkerView]]:
        """The workers of every task of a run, from one listing of the pods."""
        pods = self.live_pods(record)
        return {t: self.workers(record, t, pods) for t in record.tasks}

    def run_state(self, record: RunRecord) -> tuple[dict[str, str], bool]:
        """A tick of the stream of a run: its events and whether they are final.

        Args:
            record: The record of the run.

        Returns:
            The JSON text of the events `run`, `progress` and `workers`, and
            whether the run is terminal with the states of its tasks final.
        """
        final = record.state in TERMINAL_RUN_STATES and all(
            state in FINAL_TASK_STATES for state in record.tasks.values()
        )
        return {
            "run": record.model_dump_json(),
            "progress": _PROGRESS.dump_json(self.progress(record)).decode(),
            "workers": _WORKERS.dump_json(self.task_workers(record)).decode(),
        }, final

    def workers(
        self,
        record: RunRecord,
        task_id: str,
        pods: list[Placement] | None = None,
    ) -> list[WorkerView]:
        """The workers of a task, sorted by id; 404 for an unknown task.

        Args:
            record: The record of the run.
            task_id: The task.
            pods: The live pods of the run (`live_pods`), listed when None.

        Returns:
            A view per worker of its reports or its pod.
        """
        task_dir = self.task_dir(record, task_id)
        reports: dict[str, dict[int, WorkerReport]] = {}
        for path in sorted((task_dir / WORKERS_DIR).glob("*.json")):
            try:
                report = WorkerReport(**json.loads(path.read_text()))
            except (OSError, ValueError, TypeError) as error:
                logger.warning("report %s of run %s: %s", path.name, record.id, error)
                continue
            reports.setdefault(report.worker, {})[attempt_of(path.name)] = report
        live: dict[str, Placement] = {}
        for p in self.live_pods(record) if pods is None else pods:
            if p.worker is None or _task_of(p.dag_task) != task_id:
                continue
            # of two pods of a worker, the one of the later attempt
            seen = live.get(p.worker)
            if seen is None or _newer(p, seen):
                live[p.worker] = p
        units = record.plans.get(task_id, {}).get("units")
        claims: dict[str, list[int]] = {}
        if units is not None and (task_dir / UNITS_DIR).is_dir():
            claims = Ledger(task_dir / UNITS_DIR, units).claims()
        done = _units_by_worker(task_dir / PROGRESS_FILE)
        views = []
        for worker in sorted(reports.keys() | live.keys()):
            attempts = reports.get(worker, {})
            reported = max(attempts, default=-1)
            last = attempts.get(reported)
            oom = last is not None and is_oom(last, reported, record.oom_kills)
            pod = live.get(worker)
            # a worker reports in `finally`, also when it was evicted: a pod of an
            # attempt without a report is a retry which waits or runs, and a pod of
            # no known attempt counts as newer than every report
            state: Literal["running", "done", "failed"]
            if pod is not None and pod.attempt not in attempts:
                state = "running"
                node = pod.node  # None while unbound, never an earlier node
                current = reported + 1 if pod.attempt is None else pod.attempt
            else:
                state = "failed" if oom else "done"
                node = last.node if last is not None and last.node else None
                current = reported
            views.append(
                WorkerView(
                    worker=worker,
                    node=node,
                    state=state,
                    attempts=max(current, reported) + 1,
                    processes=last.processes if last else None,
                    units_done=len(done.get(worker, ())),
                    units_claimed=len(claims.get(worker, [])),
                    peak_memory_gb=_gb(last.peak_memory_bytes if last else None),
                    memory_limit_gb=_gb(last.memory_limit_bytes if last else None),
                    oom_killed=oom,
                )
            )
        return views

    # removal

    def delete(self, run_id: str) -> CleanupResult:
        """Remove an ended run; 409 for a run which has not ended."""
        trash = self._trash(run_id, check=True)
        if trash is None:
            raise HTTPException(404, f"unknown run {run_id!r}")
        freed = freed_bytes([trash])
        shutil.rmtree(trash)
        logger.info("run %s deleted", run_id)
        return CleanupResult(
            runs=[run_id], snapshots=[], bytes_freed=freed, dry_run=False
        )

    def _trash(self, run_id: str, check: bool = False) -> Path | None:
        """Move an ended run out of the runs, to remove it without the lock.

        Returns:
            The directory it was moved to, None when the run is gone or has not
            ended and `check` is false.

        Raises:
            HTTPException: 409 when `check` is set and the run has not ended.
        """
        with self.controller.lock:
            try:
                record = self.store.read(run_id)
            except KeyError:
                return None
            if record.state not in TERMINAL_RUN_STATES:
                if check:
                    raise HTTPException(
                        409, f"run {run_id} is {record.state}, not ended"
                    )
                return None
            if _pending(record):
                if check:
                    raise HTTPException(
                        409,
                        f"run {run_id} still has to release its node or cancel its "
                        "workflow",
                    )
                return None
            trash = self.store.root / "runs" / f"{TRASH_PREFIX}{run_id}-{os.getpid()}"
            os.rename(self.store.run_dir(run_id), trash)
            self.store.forget(run_id)
        return trash

    def _unreferenced_snapshots(
        self, referenced: set[str], cutoff: datetime
    ) -> list[Path]:
        """The snapshots older than `cutoff` which no kept run references.

        The target of `latest` and snapshots younger than `SNAPSHOT_GRACE_S` stay,
        since a client may be submitting them.
        """
        root = self.settings.store / "snapshots"
        if not root.is_dir():
            return []
        young = datetime.now(UTC).timestamp() - SNAPSHOT_GRACE_S
        found: list[Path] = []
        for parent in sorted(p for p in root.iterdir() if p.is_dir()):
            latest = parent / LATEST_LINK
            target = os.readlink(latest) if latest.is_symlink() else None
            for entry in sorted(parent.iterdir()):
                if (
                    entry.name in (LATEST_LINK, target)
                    or entry.name.startswith(".")
                    or entry.is_symlink()
                    or not entry.is_dir()
                ):
                    continue
                st = entry.stat()
                if (
                    entry.relative_to(self.settings.store).as_posix() in referenced
                    or st.st_mtime >= cutoff.timestamp()
                    or st.st_ctime >= young
                ):
                    continue
                found.append(entry)
        return found

    def cleanup(self, request: CleanupRequest) -> CleanupResult:
        """Remove ended runs and the snapshots no remaining run references."""
        cutoff = datetime.now(UTC) - timedelta(days=request.older_than_days)
        runs = self.store.index.runs()
        chosen = [
            r.summary.id
            for r in runs
            if r.summary.state in TERMINAL_RUN_STATES
            and (r.summary.finished or r.summary.created) <= cutoff
            and (request.group is None or r.summary.group == request.group)
            and (not request.failed_only or r.summary.state == RunState.FAILED)
            and not r.pending
        ]
        referenced = {
            s for r in runs if r.summary.id not in chosen for s in r.snapshots
        }
        snapshots = self._unreferenced_snapshots(referenced, cutoff)
        patches = [
            p for s in snapshots if (p := s.with_name(f"{s.name}.patch")).is_file()
        ]
        if request.dry_run:
            runs = [self.store.run_dir(run_id) for run_id in chosen]
        else:
            # an ended run does not change, so moving it under the lock suffices
            moved = {run_id: self._trash(run_id) for run_id in chosen}
            chosen = [run_id for run_id, path in moved.items() if path is not None]
            runs = [path for path in moved.values() if path is not None]
            runs += [  # left by an interrupted removal
                p
                for p in (self.store.root / "runs").glob(f"{TRASH_PREFIX}*")
                if p not in runs
            ]
        paths = runs + snapshots + patches
        freed = freed_bytes(paths)
        if not request.dry_run:
            for path in paths:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink(missing_ok=True)
            logger.info(
                "cleanup removed %d runs and %d snapshots", len(chosen), len(snapshots)
            )
        return CleanupResult(
            runs=sorted(chosen),
            snapshots=[
                s.relative_to(self.settings.store).as_posix() for s in snapshots
            ],
            bytes_freed=freed,
            dry_run=request.dry_run,
        )

    def usage(self) -> Usage:
        """The bytes of every run and every group."""
        runs: dict[str, int] = {}
        groups: dict[str, int] = {}
        for run in self.store.index.runs():
            size = disk_usage(self.store.run_dir(run.summary.id))
            runs[run.summary.id] = size
            group = run.summary.group or ""
            groups[group] = groups.get(group, 0) + size
        return Usage(runs=runs, groups=groups)


EVENTS_MAX_RUNS = 200
KEEP_ALIVE_S = 15.0
_RUN_ITEMS = TypeAdapter(list[RunItem])
_SERVERS = TypeAdapter(list[ServerUsage])
_PROGRESS = TypeAdapter(dict[str, TaskProgress])
_WORKERS = TypeAdapter(dict[str, list[WorkerView]])


def _sse(event: str, data: str) -> str:
    """One server-sent event; `data` is a single line of JSON."""
    return f"event: {event}\ndata: {data}\n\n"


async def _stream(
    request: Request,
    settings: ServiceSettings,
    compute: Callable[[], tuple[dict[str, str], bool]],
) -> AsyncGenerator[str]:
    """Send the events `compute` returns at the start and whenever they change.

    Args:
        request: The request, whose disconnect ends the stream.
        settings: The settings, `poll_s` and `events_max_s`.
        compute: Returns the JSON text of every event by name and whether the
            stream is complete after them; it runs in a thread.

    Yields:
        The events changed since the last check, and a keep-alive comment after
        `KEEP_ALIVE_S` seconds without one. The stream ends when it is complete,
        after `events_max_s` seconds or on a disconnect.
    """
    started = last_sent = time.monotonic()
    sent: dict[str, str] = {}
    while True:
        try:
            payloads, complete = await asyncio.to_thread(compute)
        except HTTPException as error:
            logger.debug("an event stream ends: %s", error.detail)
            return
        except Exception:
            logger.warning(
                "an event stream ends, its state cannot be read", exc_info=True
            )
            return
        for event, data in payloads.items():
            if sent.get(event) != data:
                sent[event] = data
                last_sent = time.monotonic()
                yield _sse(event, data)
        if complete or time.monotonic() - started >= settings.events_max_s:
            return
        if time.monotonic() - last_sent >= KEEP_ALIVE_S:
            last_sent = time.monotonic()
            yield ": keep-alive\n\n"
        await asyncio.sleep(settings.poll_s)
        if await request.is_disconnected():
            return


def _event_response(stream: AsyncGenerator[str]) -> StreamingResponse:
    """The response of an event stream: no caching, and no buffering by a proxy.

    Args:
        stream: The events, e.g. of `_stream`.

    Returns:
        A streaming response of `text/event-stream`.
    """
    return StreamingResponse(
        stream,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _control(controller: Controller, poll_s: float) -> None:
    """Recover, then reconcile every `poll_s` seconds in a thread, off the event loop.

    A failure is logged and the loop continues; a recovery which failed is tried
    again at the next poll.
    """
    recovered = False
    while True:
        if not recovered:
            try:
                await asyncio.to_thread(controller.recover)
                recovered = True
            except Exception:
                logger.exception("the recovery of the controller failed")
        try:
            await asyncio.to_thread(controller.reconcile)
        except Exception:
            logger.exception("the reconciliation failed")
        await asyncio.sleep(poll_s)


def create_app(
    settings: ServiceSettings,
    backend: Backend,
    types: dict[str, TaskType] | None = None,
) -> FastAPI:
    """Create the app of the service.

    Args:
        settings: The settings.
        backend: The backend the workflows run on.
        types: The task types by name, the ones of the entry points by default.

    Returns:
        The app; its lifespan runs the controller every `settings.poll_s` seconds.
    """
    service = _Service(settings, backend, types or load_task_types())
    bearer = HTTPBearer(auto_error=False)

    def identity(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> Identity:
        found = (
            None
            if credentials is None
            else authenticate(service.tokens, credentials.credentials)
        )
        if found is None:
            raise HTTPException(
                401, "a valid bearer token is required", {"WWW-Authenticate": "Bearer"}
            )
        return found

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        task = asyncio.create_task(_control(service.controller, settings.poll_s))
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await asyncio.to_thread(service.controller.close)

    # no documentation routes: they would answer without a token
    app = FastAPI(
        title="simrun",
        lifespan=lifespan,
        dependencies=[Depends(identity)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.state.backend = backend
    app.state.controller = service.controller
    app.state.service = service
    Caller = Annotated[Identity, Depends(identity)]

    @app.post("/validate")
    def validate(body: Annotated[dict[str, Any], Body()]) -> ValidationResult:
        try:
            workflow = Workflow.model_validate(body)
        except ValidationError as error:
            return ValidationResult(ok=False, errors=_errors(error))
        errors = validate_workflow(workflow, service.types)
        return ValidationResult(ok=not errors, errors=errors)

    @app.get("/task-types")
    def task_types() -> list[TaskTypeView]:
        return [TaskTypeView.of(service.types[name]) for name in sorted(service.types)]

    @app.post("/workflows/yaml", response_class=PlainTextResponse)
    def workflow_to_yaml(workflow: Workflow) -> str:
        return workflow_yaml(workflow)

    def own_edit(session_id: str, caller: Identity, owned: bool = True) -> EditSession:
        """The session, 404 when unknown, 403 when `owned` and not the caller's."""
        try:
            session = service.edits.read(session_id)
        except KeyError:
            raise HTTPException(404, f"unknown edit session {session_id}") from None
        if owned and session.owner != caller.owner:
            raise HTTPException(403, "only the owner of the session may do this")
        return session

    async def ask(session_id: str, request_id: str) -> EditResult:
        """Wait for the answer of the command to a request, then drop the request."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + EDIT_WAIT_S
        try:
            while True:
                result = await asyncio.to_thread(
                    service.edits.result, session_id, request_id
                )
                remaining = deadline - loop.time()
                if result is not None:
                    return result
                if remaining <= 0:
                    raise HTTPException(
                        503, "the machine of the session did not answer"
                    )
                await asyncio.sleep(min(WAIT_POLL_S, remaining))
        finally:
            await asyncio.to_thread(service.edits.drop_request, session_id, request_id)

    @app.post("/edits", status_code=201)
    def open_edit(body: OpenEdit, caller: Caller) -> EditSession:
        return service.edits.open(caller, body)

    @app.get("/edits")
    def list_edits(caller: Caller) -> list[EditSession]:
        return service.edits.list(caller.owner)

    @app.get("/edits/{session_id}")
    def get_edit(session_id: str, caller: Caller) -> EditSession:
        return own_edit(session_id, caller, owned=False)

    @app.put("/edits/{session_id}/state")
    def put_edit_state(
        session_id: str, state: EditState, caller: Caller
    ) -> EditSession:
        own_edit(session_id, caller)
        return service.edits.update(session_id, state)

    @app.post("/edits/{session_id}/close")
    def close_edit(session_id: str, caller: Caller) -> EditSession:
        own_edit(session_id, caller)
        return service.edits.close(session_id)

    @app.get("/edits/{session_id}/requests")
    async def edit_requests(
        session_id: str,
        request: Request,
        caller: Caller,
        timeout: Annotated[float, Query(ge=0, le=EDIT_POLL_MAX_S)] = 20.0,
    ) -> list[EditRequest]:
        await asyncio.to_thread(own_edit, session_id, caller)
        await asyncio.to_thread(service.edits.seen, session_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            found = await asyncio.to_thread(service.edits.requests, session_id)
            remaining = deadline - loop.time()
            if await request.is_disconnected():
                return found  # the command went away: no `seen`
            if found or remaining <= 0:
                await asyncio.to_thread(service.edits.seen, session_id)
                return found
            await asyncio.sleep(min(WAIT_POLL_S, remaining))

    @app.post("/edits/{session_id}/requests/{request_id}/result", status_code=204)
    def edit_result(
        session_id: str, request_id: str, result: EditResult, caller: Caller
    ) -> None:
        own_edit(session_id, caller)
        try:
            service.edits.answer(session_id, request_id, result)
        except KeyError:
            raise HTTPException(404, f"unknown request {request_id}") from None

    @app.post("/edits/{session_id}/save")
    async def save_edit(session_id: str, body: SaveBody, caller: Caller) -> EditSession:
        session = await asyncio.to_thread(own_edit, session_id, caller)
        if not session.connected:
            raise HTTPException(503, "the machine of the session is not connected")
        request = await asyncio.to_thread(
            lambda: service.edits.add_request(
                session_id,
                "save",
                body.workflow,
                base=body.base,
                force=body.force,
            )
        )
        result = await ask(session_id, request.id)
        if not result.ok:
            raise HTTPException(result.status, result.detail)
        return await asyncio.to_thread(
            service.edits.saved,
            session_id,
            body.workflow,
            result.file_sha256,
        )

    @app.post("/edits/{session_id}/run")
    async def run_edit(session_id: str, body: RunBody, caller: Caller) -> RunSummary:
        session = await asyncio.to_thread(own_edit, session_id, caller)
        if not session.connected:
            raise HTTPException(503, "the machine of the session is not connected")
        request = await asyncio.to_thread(
            lambda: service.edits.add_request(
                session_id,
                "run",
                body.workflow,
                group=body.group,
                split=body.split,
                reserve=body.reserve,
            )
        )
        result = await ask(session_id, request.id)
        if not result.ok:
            raise HTTPException(result.status, result.detail)
        if result.run is None:
            raise HTTPException(502, "the machine of the session sent no run")
        return result.run

    @app.get("/edits/{session_id}/events")
    def edit_events(
        session_id: str, request: Request, caller: Caller
    ) -> StreamingResponse:
        own_edit(session_id, caller, owned=False)

        def compute() -> tuple[dict[str, str], bool]:
            try:
                session = service.edits.read(session_id)
            except KeyError:
                raise HTTPException(404, "the edit session is gone") from None
            return {"edit": session.model_dump_json()}, False

        return _event_response(_stream(request, settings, compute))

    @app.post("/estimate")
    def estimate(request: SubmitRequest, caller: Caller) -> dict[str, TaskEstimate]:
        priority = priority_of(caller, settings.owner)
        result: dict[str, TaskEstimate] = {}
        for task_id, (resources, plan) in service.plan(request, priority).items():
            seconds = None
            if resources.seconds_per_unit is not None and plan.units is not None:
                slots = plan.pods * plan.shape.processes
                rounds = -(-plan.units // slots)
                seconds = resources.seconds_per_unit * rounds
            result[task_id] = TaskEstimate(
                resources=resources, plan=plan, seconds=seconds
            )
        return result

    @app.post("/runs", status_code=201)
    def submit(request: SubmitRequest, caller: Caller) -> RunSummary:
        return RunSummary.of(service.submit(request, caller))

    @app.get("/me")
    def me(caller: Caller) -> Identity:
        return caller

    @app.get("/servers")
    def servers() -> list[ServerUsage]:
        return service.servers()

    @app.get("/events")
    def events(
        request: Request,
        group: str | None = None,
        owner: str | None = None,
        agent: str | None = None,
        state: RunState | None = None,
    ) -> StreamingResponse:
        def compute() -> tuple[dict[str, str], bool]:
            runs = service.run_items(group, owner, agent, state, EVENTS_MAX_RUNS)
            return {
                "runs": _RUN_ITEMS.dump_json(runs).decode(),
                "servers": _SERVERS.dump_json(service.servers()).decode(),
            }, False

        return _event_response(_stream(request, settings, compute))

    @app.get("/runs/{run_id}/events")
    def run_events(run_id: str, request: Request) -> StreamingResponse:
        service.read(run_id)  # 404 before the stream starts

        def compute() -> tuple[dict[str, str], bool]:
            return service.run_state(service.read(run_id))

        return _event_response(_stream(request, settings, compute))

    @app.get("/runs/{run_id}/tasks/{task_id}/workers")
    def workers(run_id: str, task_id: str) -> list[WorkerView]:
        return service.workers(service.read(run_id), task_id)

    @app.get("/runs")
    def list_runs(
        group: str | None = None,
        owner: str | None = None,
        agent: str | None = None,
        state: RunState | None = None,
    ) -> list[RunSummary]:
        runs = service.store.index.runs(group, owner, agent, state)
        return [r.summary for r in runs]

    @app.get("/runs/{run_id}")
    def get_run(run_id: str) -> RunRecord:
        return service.read(run_id)

    @app.get("/runs/{run_id}/wait")
    async def wait(
        run_id: str,
        timeout: Annotated[float, Query(ge=0, le=WAIT_MAX_S)] = 60.0,
    ) -> RunSummary:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            record = await asyncio.to_thread(service.read, run_id)
            remaining = deadline - loop.time()
            if record.state in TERMINAL_RUN_STATES or remaining <= 0:
                return RunSummary.of(record)
            await asyncio.sleep(min(WAIT_POLL_S, remaining))

    @app.get("/runs/{run_id}/progress")
    def progress(run_id: str) -> dict[str, TaskProgress]:
        return service.progress(service.read(run_id))

    @app.get("/runs/{run_id}/logs/{task_id}", response_class=PlainTextResponse)
    def logs(
        run_id: str,
        task_id: str,
        tail_lines: Annotated[int, Query(ge=0)] = DEFAULT_TAIL_LINES,
    ) -> str:
        """The logs of the pods of a task, the last `tail_lines` of each (0: all).

        The logs kept in the store once the task was final, else the ones of its
        live pods.
        """
        record = service.read(run_id)
        task_dir = service.task_dir(record, task_id)
        lines = tail_lines or None
        found = read_kept(task_dir, lines)
        if found is None:
            if record.backend_name is None:
                raise HTTPException(409, f"run {run_id} has no workflow")
            found = backend.logs(record.backend_name, task_id, lines)
        return render(found)

    @app.get("/runs/{run_id}/outputs/{task_id}")
    def outputs(run_id: str, task_id: str) -> list[OutputFile]:
        return service.outputs(service.read(run_id), task_id)

    @app.post("/runs/{run_id}/cancel")
    def cancel(run_id: str) -> RunSummary:
        try:
            return RunSummary.of(service.controller.cancel(run_id))
        except KeyError:
            raise HTTPException(404, f"unknown run {run_id!r}") from None
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    @app.delete("/runs/{run_id}")
    def delete(run_id: str) -> CleanupResult:
        return service.delete(run_id)

    @app.post("/cleanup")
    def cleanup(request: CleanupRequest) -> CleanupResult:
        return service.cleanup(request)

    @app.get("/usage")
    def usage() -> Usage:
        return service.usage()

    @app.post("/compare")
    def compare(request: CompareRequest) -> CompareTable:
        try:
            return compare_runs(service.store, request.runs)
        except KeyError as error:
            raise HTTPException(404, f"unknown run {error.args[0]!r}") from None

    if settings.web is not None:
        # the dependencies of the app apply to its routes only, not to a mount
        app.mount("/ui", StaticFiles(directory=settings.web, html=True), name="ui")

    return app


def openapi_document() -> dict[str, Any]:
    """The OpenAPI document of the service, the input of the types of the frontend.

    The app is created on a fake backend in a temporary directory; the routes do not
    depend on either, so the document is the same on every machine.
    """
    from simrun.testing import FakeBackend

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "tokens.yaml").write_text("{}\n")
        settings = ServiceSettings(
            store=root / "store",
            db=root / "db" / "resources.sqlite",
            tokens=root / "tokens.yaml",
            owner="owner",
            cluster=ClusterConfig(registry="localhost:5000"),
        )
        app = create_app(settings, FakeBackend(), {"command": COMMAND})
        document = app.openapi()
    # the models only the event streams send have no response schema in the routes
    schemas = document.setdefault("components", {}).setdefault("schemas", {})
    for model in (RunItem,):
        schema = model.model_json_schema(ref_template="#/components/schemas/{model}")
        schemas.update(schema.pop("$defs", {}) | {model.__name__: schema})
    return json.loads(json.dumps(document))


def load_settings(path: Path) -> ServiceSettings:
    """Read the settings of the service from a YAML file."""
    return ServiceSettings.model_validate(yaml.safe_load(path.read_text()))


def serve() -> FastAPI:
    """The app of the settings in the YAML file named by `SIMRUN_CONFIG`.

    The backend is the `backend` of the settings, a `module:callable` called without
    arguments.

    Raises:
        RuntimeError: `SIMRUN_CONFIG` or the backend is not set.
    """
    path = os.environ.get(CONFIG_ENV)
    if not path:
        raise RuntimeError(f"{CONFIG_ENV} must name the YAML file of the settings")
    settings = load_settings(Path(path))
    if settings.backend is None:
        raise RuntimeError(f"{path}: no backend is configured")
    return create_app(settings, resolve(settings.backend)())
