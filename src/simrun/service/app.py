"""The API of the service: people and agents submit, follow and compare runs.

Every route needs a bearer token of the tokens file (`simrun.service.auth`); the
identity of the token decides the priority of its runs. A submission is validated,
its resources resolved and its tasks planned on the nodes of the backend, the image
of its environment built when the registry does not have it, and the record and the
settings of every task written before the workflow is submitted. The controller
(`simrun.service.controller`) follows the workflows in a background task.

Run it with `uvicorn --factory simrun.service.app:serve`, which reads the
`ServiceSettings` from the YAML file named by `SIMRUN_CONFIG`.
"""

import asyncio
import dataclasses
import json
import logging
import os
import shutil
import tempfile
import threading
from collections.abc import AsyncGenerator, Iterable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, NoReturn

import yaml
from fastapi import Body, Depends, FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field, ValidationError

import simrun
from simrun.argo import to_argo
from simrun.config import ClusterConfig
from simrun.environment import Environment, dockerfile, image_ref, resolve_environment
from simrun.ledger import Ledger, LedgerCounts
from simrun.planner import Node, PlanError, TaskPlan, plan_task
from simrun.plugins import TaskType, load_task_types, resolve
from simrun.plugins_builtin import COMMAND
from simrun.resources import ResolvedResources, ResourceKey, ResourceStore
from simrun.service.auth import Identity, authenticate, load_tokens, priority_of
from simrun.service.backend import Backend
from simrun.service.compare import CompareTable, compare_runs
from simrun.service.controller import TERMINAL_RUN_STATES, Controller, read_records
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, RunStore, TaskState
from simrun.worker import MERGED_MARKER, PROGRESS_FILE, SETTINGS_FILE, UNITS_DIR
from simrun.workflow import Priority, SplitMode, Workflow

logger = logging.getLogger(__name__)

CONFIG_ENV = "SIMRUN_CONFIG"
ENVIRONMENT_LOCK = "environment.lock"
WAIT_POLL_S = 0.2
WAIT_MAX_S = 300.0
# a snapshot younger than this is kept by a cleanup: a client may be submitting it
SNAPSHOT_GRACE_S = 3600.0
LATEST_LINK = "latest"
# an ended run is moved to `runs/<prefix><id>-<pid>` under the lock, then removed
TRASH_PREFIX = ".trash-"
PROGRESS_TAIL_BYTES = 8192


class ServiceSettings(BaseModel):
    """The settings of the service.

    Attributes:
        store: Root of the run store.
        db: SQLite file of the resource observations, on a local volume.
        tokens: YAML file of the tokens, see `simrun.service.auth.load_tokens`.
        owner: The person who owns the cluster; their runs have the top priority.
        cluster: The cluster.
        poll_s: Seconds between two reconciliations of the controller.
        default_image: Image of a run whose model has no `pyproject.toml`.
        simrun_wheel: The simrun wheel the images of the environments install.
        python: Python version of the images.
        backend: `module:callable` returning the backend, for `serve`.
    """

    store: Path
    db: Path
    tokens: Path
    owner: str
    cluster: ClusterConfig
    poll_s: float = 5.0
    default_image: str | None = None
    simrun_wheel: Path | None = None
    python: str = "3.14"
    backend: str | None = None


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


class RunSummary(BaseModel):
    """The state of a run without its workflow and plans."""

    id: str
    state: RunState
    group: str | None
    submitted_by: str
    agent: str | None
    priority: Priority
    tasks: dict[str, TaskState]
    created: datetime
    finished: datetime | None
    message: str | None

    @classmethod
    def of(cls, record: RunRecord) -> RunSummary:
        """The summary of a record."""
        return cls.model_validate(record.model_dump(include=set(cls.model_fields)))


class ValidationResult(BaseModel):
    """The result of a validation: the errors of a workflow, none when valid."""

    ok: bool
    errors: list[str]


class TaskEstimate(BaseModel):
    """The resources, the plan and the expected duration of a task.

    Attributes:
        resources: The resolved resources.
        plan: The plan of the pods.
        seconds: The expected seconds of a split task from the median seconds per
            unit of past runs, None without them.
    """

    resources: ResolvedResources
    plan: TaskPlan
    seconds: float | None


class TaskProgress(BaseModel):
    """The progress of a task: its ledger and the last line of its progress file."""

    counts: LedgerCounts | None
    last: dict[str, Any] | None


class OutputFile(BaseModel):
    """A file of the output of a task, its path relative to the store."""

    path: str
    bytes: int


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


def validate_workflow(workflow: Workflow, types: dict[str, TaskType]) -> list[str]:
    """The errors of a workflow beyond its structure: types, settings and ports.

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
        try:
            task_type.settings.model_validate(task.settings)
        except ValidationError as error:
            errors.append(f"task {task.id!r}: invalid settings: {error}")
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


def _pending(record: RunRecord) -> bool:
    """Whether a run still has to release its node or cancel its workflow."""
    return record.pending_release is not None or record.pending_cancel


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
            try:
                value = json.loads(line)
            except ValueError:
                return None
            return value if isinstance(value, dict) else None
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
        self.store = RunStore(settings.store)
        self.resources = ResourceStore(settings.db)
        self.controller = Controller(self.store, self.resources, backend, types)
        self.tokens = load_tokens(settings.tokens)
        self._reserve_lock = threading.Lock()

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
        nodes = self._nodes(request.reserve or workflow.reserve)
        model = request.sources[0].name if request.sources else ""
        planned: dict[str, tuple[ResolvedResources, TaskPlan]] = {}
        for task in workflow.order():
            task_type = self.types[task.type]
            units = None
            if task_type.splittable and task_type.count_units is not None:
                settings = task_type.settings.model_validate(task.settings)
                units = resolve(task_type.count_units)(settings)
            key = ResourceKey(task.type, model, "")
            resources = self.resources.resolve(key, task_type.hint, task.resources)
            mode = request.split or task.split or workflow.split
            try:
                plan = plan_task(
                    task, task_type, resources, nodes, mode, priority, units
                )
            except PlanError as error:
                raise HTTPException(422, str(error)) from error
            planned[task.id] = (resources, plan)
        return planned

    def _check_sources(self, sources: list[SourceProvenance]) -> None:
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

    def _needs_run_image(self, workflow: Workflow) -> bool:
        return any(
            self.types[t.type].image is None and t.type != COMMAND.name
            for t in workflow.tasks
        )

    def environment(self, request: SubmitRequest) -> tuple[str | None, str | None]:
        """The image of a run and the lock of its environment, built when missing.

        Raises:
            HTTPException: 422 when the environment cannot be resolved or no image
                is configured for a model without `pyproject.toml`; 500 without a
                simrun wheel to build with; 502 when the build fails.
        """
        if request.sources:
            model, *libraries = request.sources
            if (self.settings.store / model.snapshot / "pyproject.toml").is_file():
                try:
                    env = resolve_environment(
                        self.settings.store,
                        model,
                        libraries,
                        python=self.settings.python,
                        simrun_version=simrun.__version__,
                    )
                except (ValueError, RuntimeError) as error:
                    raise HTTPException(422, str(error)) from error
                ref = image_ref(env, self.settings.cluster.registry)
                if not self.backend.has_image(ref):
                    self._build(env, ref)
                return ref, env.lock
        if self.settings.default_image is None and self._needs_run_image(
            request.workflow
        ):
            raise HTTPException(
                422,
                "the run needs an image: its model has no pyproject.toml and the "
                "service has no default_image",
            )
        return self.settings.default_image, None

    def _build(self, env: Environment, ref: str) -> None:
        wheel = self.settings.simrun_wheel
        if wheel is None:
            raise HTTPException(500, "the service has no simrun_wheel to build with")
        with tempfile.TemporaryDirectory(prefix="simrun-build-") as tmp:
            context = Path(tmp)
            (context / "Dockerfile").write_text(
                dockerfile(env, self.settings.cluster.registry, wheel.name)
            )
            shutil.copy2(wheel, context / wheel.name)
            logger.info("building the image %s", ref)
            try:
                self.backend.build_image(context, ref)
            except Exception as error:
                raise HTTPException(
                    502, f"building the image {ref} failed: {error}"
                ) from error

    def submit(self, request: SubmitRequest, identity: Identity) -> RunRecord:
        """Plan, build, write and submit a run."""
        self._check_sources(request.sources)
        priority = priority_of(identity, self.settings.owner)
        planned = self.plan(request, priority)
        plans = {task_id: plan for task_id, (_, plan) in planned.items()}
        image, lock = self.environment(request)
        workflow = request.workflow
        reserve = request.reserve or workflow.reserve
        record = RunRecord(
            id=self.store.new_run_id(),
            submitted_by=identity.name,
            owner=identity.owner,
            agent=identity.name if identity.kind == "agent" else None,
            priority=priority,
            group=request.group,
            workflow=workflow,
            sources=request.sources,
            image=image,
            reserve=reserve,
            state=RunState.SUBMITTING,
            tasks=dict.fromkeys((t.id for t in workflow.tasks), TaskState.WAITING),
            plans={k: dataclasses.asdict(p) for k, p in plans.items()},
            backend_name=None,
            created=datetime.now(UTC),
            finished=None,
            message=None,
        )
        try:
            manifest = to_argo(record, plans, self.types, self.settings.cluster)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        # the name is known before the submission, so a workflow a failed or an
        # interrupted submission created is found and cancelled
        record = record.model_copy(
            update={"backend_name": manifest["metadata"]["name"]}
        )
        if reserve is not None:
            with self._reserve_lock:
                self._free_node(self.backend.nodes(), reserve)
                self.backend.reserve(reserve, record.id)
        try:
            self._write(record, lock)
            self.backend.submit(manifest)
        except Exception as error:
            self._abort(record, error)
            raise HTTPException(502, f"submitting the run failed: {error}") from error
        ended = False
        with self.controller.lock:
            try:
                current = self.store.read(record.id)
            except KeyError:
                current = None
            if current is not None:
                ended = current.state in TERMINAL_RUN_STATES
                if ended:  # cancelled or failed while it was submitted: cancel it
                    current = current.model_copy(update={"pending_cancel": True})
                else:
                    current = current.model_copy(update={"state": RunState.QUEUED})
                self.store.write(current)
        if current is None:
            self._deleted_while_submitted(record)
        if ended:
            self.controller.settle(record.id)
            current = self.store.read(record.id)
        logger.info("run %s submitted by %s", record.id, identity.name)
        return current

    def _deleted_while_submitted(self, record: RunRecord) -> NoReturn:
        """Cancel the workflow of a run cancelled and deleted during its submission.

        Raises:
            HTTPException: 409, the run is gone.
        """
        name = record.backend_name
        if name is not None:
            try:
                self.backend.cancel(name)
            except KeyError:
                pass
            except Exception:
                logger.exception("cancelling the orphan workflow %s failed", name)
        raise HTTPException(
            409, f"run {record.id} was cancelled and deleted while it was submitted"
        )

    def _write(self, record: RunRecord, lock: str | None) -> None:
        with self.controller.lock:
            self.store.write(record)
        run_dir = self.store.run_dir(record.id)
        if lock is not None:
            (run_dir / ENVIRONMENT_LOCK).write_text(lock)
        for task in record.workflow.tasks:
            settings = self.types[task.type].settings.model_validate(task.settings)
            task_dir = run_dir / "tasks" / task.id
            task_dir.mkdir(parents=True, exist_ok=True)
            (task_dir / SETTINGS_FILE).write_text(settings.model_dump_json(indent=2))

    def _abort(self, record: RunRecord, error: Exception) -> None:
        """Fail the record of a run whose submission failed, then settle it.

        The workflow may have been created although the submission raised, so it is
        cancelled like the node is released, retried by the controller on failure.
        """
        logger.error("submitting run %s failed: %s", record.id, error)
        failed = record.model_copy(
            update={
                "state": RunState.FAILED,
                "tasks": dict.fromkeys(record.tasks, TaskState.SKIPPED),
                "finished": datetime.now(UTC),
                "message": f"submitting failed: {error}",
                "pending_release": record.reserve,
                "pending_cancel": True,
            }
        )
        try:
            with self.controller.lock:
                self.store.write(failed)
        except OSError:
            logger.exception("writing the failed record of run %s failed", record.id)
            if record.reserve is not None:
                with suppress(Exception):
                    self.backend.unreserve(record.reserve, record.id)
            return
        self.controller.settle(record.id)

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

    def progress(self, record: RunRecord) -> dict[str, TaskProgress]:
        """The ledger counts and the last progress line of every task."""
        result: dict[str, TaskProgress] = {}
        for task_id in record.tasks:
            task_dir = self.task_dir(record, task_id)
            units = record.plans.get(task_id, {}).get("units")
            counts = None
            if units is not None and (task_dir / UNITS_DIR).is_dir():
                counts = Ledger(task_dir / UNITS_DIR, units).counts()
            result[task_id] = TaskProgress(
                counts=counts, last=_last_line(task_dir / PROGRESS_FILE)
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
        return trash

    def _unreferenced_snapshots(
        self, kept: list[RunRecord], cutoff: datetime
    ) -> list[Path]:
        """The snapshots older than `cutoff` which no kept run references.

        The target of `latest` and snapshots younger than `SNAPSHOT_GRACE_S` stay,
        since a client may be submitting them.
        """
        root = self.settings.store / "snapshots"
        if not root.is_dir():
            return []
        referenced = {s.snapshot for r in kept for s in r.sources}
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
        records = list(read_records(self.store))
        chosen = [
            r.id
            for r in records
            if r.state in TERMINAL_RUN_STATES
            and (r.finished or r.created) <= cutoff
            and (request.group is None or r.group == request.group)
            and (not request.failed_only or r.state == RunState.FAILED)
            and not _pending(r)
        ]
        kept = [r for r in records if r.id not in chosen]
        snapshots = self._unreferenced_snapshots(kept, cutoff)
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
        for record in read_records(self.store):
            size = disk_usage(self.store.run_dir(record.id))
            runs[record.id] = size
            group = record.group or ""
            groups[group] = groups.get(group, 0) + size
        return Usage(runs=runs, groups=groups)


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
    Caller = Annotated[Identity, Depends(identity)]

    @app.post("/validate")
    def validate(body: Annotated[dict[str, Any], Body()]) -> ValidationResult:
        try:
            workflow = Workflow.model_validate(body)
        except ValidationError as error:
            return ValidationResult(ok=False, errors=_errors(error))
        errors = validate_workflow(workflow, service.types)
        return ValidationResult(ok=not errors, errors=errors)

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

    @app.get("/runs")
    def list_runs(
        group: str | None = None,
        owner: str | None = None,
        agent: str | None = None,
        state: RunState | None = None,
    ) -> list[RunSummary]:
        return [
            RunSummary.of(r)
            for r in read_records(service.store)
            if (group is None or r.group == group)
            and (owner is None or r.owner == owner)
            and (agent is None or r.agent == agent)
            and (state is None or r.state == state)
        ]

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
    def logs(run_id: str, task_id: str) -> str:
        record = service.read(run_id)
        service.task_dir(record, task_id)
        if record.backend_name is None:
            raise HTTPException(409, f"run {run_id} has no workflow")
        return backend.logs(record.backend_name, task_id)

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

    return app


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
