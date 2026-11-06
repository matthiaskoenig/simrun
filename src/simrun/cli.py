"""The command line of simrun: submit, follow and compare runs.

Every command prints the detail of a refusal of the service to stderr and exits 1;
`--json` prints machine-readable JSON instead of tables.
"""

import json
import sys
from collections.abc import Callable, Sequence
from functools import wraps
from pathlib import Path
from typing import Annotated, Any, NoReturn

import click
import httpx
import typer
from pydantic import BaseModel
from rich.console import Console
from rich.table import Column, Table

from simrun.client import Client, ClientError
from simrun.service.app import RunSummary
from simrun.store import RunState
from simrun.workflow import SplitMode

app = typer.Typer(help="Submit and follow simulation workflows.", no_args_is_help=True)
console = Console()

AS_JSON = Annotated[bool, typer.Option("--json", help="Print JSON.")]
WORKFLOW = Annotated[Path, typer.Argument(help="The workflow YAML file.")]
SOURCE = Annotated[
    list[str] | None,
    typer.Option("--source", help="A source as NAME=PATH, the model first."),
]
GROUP = Annotated[str | None, typer.Option(help="Group of the run, e.g. a study.")]
SINGLE_SERVER = Annotated[
    bool, typer.Option("--single-server", help="Place the pieces on one server.")
]
RESERVE = Annotated[str | None, typer.Option(help="A node to reserve for the run.")]

EXIT_FAILED = 1
EXIT_TIMEOUT = 2


def make_client() -> Client:
    """The client of the environment (`SIMRUN_URL`, `SIMRUN_TOKEN`, `SIMRUN_STORE`)."""
    return Client.from_env()


def _fail(message: str) -> NoReturn:
    typer.echo(message, err=True)
    raise typer.Exit(EXIT_FAILED)


def _guarded[**P, R](function: Callable[P, R]) -> Callable[P, R]:
    @wraps(function)
    def run(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return function(*args, **kwargs)
        except typer.Exit, click.exceptions.ClickException:
            raise
        except ClientError as error:
            _fail(error.detail)
        except (ValueError, RuntimeError, OSError) as error:
            _fail(str(error))
        except httpx.TransportError as error:
            _fail(f"the service is not reachable: {error}")

    return run


def _json(data: Any) -> None:
    def plain(value: Any) -> Any:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, dict):
            return {k: plain(v) for k, v in value.items()}
        if isinstance(value, list):
            return [plain(v) for v in value]
        return value

    typer.echo(json.dumps(plain(data), indent=2))


def _sources(items: Sequence[str] | None) -> dict[str, Path]:
    sources: dict[str, Path] = {}
    for item in items or []:
        name, sep, path = item.partition("=")
        if not sep or not name or not path:
            _fail(f"--source must be NAME=PATH, got {item!r}")
        if name in sources:
            _fail(f"duplicate source {name!r}")
        sources[name] = Path(path).expanduser()
    return sources


def _runs_table(runs: Sequence[RunSummary]) -> Table:
    table = Table(
        Column("run", no_wrap=True, overflow="ignore"),
        "state",
        "group",
        "by",
        "agent",
        "priority",
        "created",
    )
    for r in runs:
        table.add_row(
            r.id,
            r.state.value,
            r.group or "",
            r.submitted_by,
            r.agent or "",
            r.priority.value,
            f"{r.created:%Y-%m-%d %H:%M}",
        )
    return table


@app.command()
@_guarded
def submit(
    workflow: WORKFLOW,
    source: SOURCE = None,
    group: GROUP = None,
    single_server: SINGLE_SERVER = False,
    reserve: RESERVE = None,
    as_json: AS_JSON = False,
) -> None:
    """Snapshot the sources and submit a workflow."""
    split = SplitMode.SINGLE_SERVER if single_server else None
    summary = make_client().submit(workflow, _sources(source), group, split, reserve)
    if as_json:
        _json(summary)
    else:
        console.print(f"submitted [bold]{summary.id}[/bold] ({summary.state.value})")


@app.command()
@_guarded
def wait(
    run: Annotated[str, typer.Argument(help="The run id.")],
    timeout: Annotated[float | None, typer.Option(help="Seconds to wait.")] = None,
    as_json: AS_JSON = False,
) -> None:
    """Wait for a run to end; exit 0 if it succeeded, 1 if not, 2 on timeout."""
    summary = make_client().wait(run, timeout)
    if as_json:
        _json(summary)
    else:
        console.print(f"{summary.id}: {summary.state.value}")
        if summary.message:
            console.print(summary.message)
    if summary.state == RunState.SUCCEEDED:
        return
    ended = summary.state in (RunState.FAILED, RunState.CANCELLED)
    raise typer.Exit(EXIT_FAILED if ended else EXIT_TIMEOUT)


@app.command()
@_guarded
def status(
    run: Annotated[str, typer.Argument(help="The run id.")],
    as_json: AS_JSON = False,
) -> None:
    """Show the state of a run and its tasks."""
    client = make_client()
    record = client.status(run)
    progress = client.progress(run)
    if as_json:
        _json({"run": record, "progress": progress})
        return
    console.print(_runs_table([RunSummary.of(record)]))
    if record.message:
        console.print(record.message)
    table = Table("task", "state", "done", "total")
    for task_id, state in record.tasks.items():
        counts = progress[task_id].counts if task_id in progress else None
        done = "" if counts is None else str(counts.done)
        total = "" if counts is None else str(counts.total)
        table.add_row(task_id, state.value, done, total)
    console.print(table)


@app.command("list")
@_guarded
def list_(
    group: GROUP = None,
    agent: Annotated[str | None, typer.Option(help="Only runs of this agent.")] = None,
    state: Annotated[RunState | None, typer.Option(help="Only this state.")] = None,
    as_json: AS_JSON = False,
) -> None:
    """List runs."""
    runs = make_client().list_runs(group, agent, state)
    if as_json:
        _json(runs)
    else:
        console.print(_runs_table(runs))


@app.command()
@_guarded
def compare(
    runs: Annotated[list[str], typer.Argument(help="The run ids.")],
    as_json: AS_JSON = False,
) -> None:
    """Compare the provenance and the summaries of runs."""
    result = make_client().compare(runs)
    if as_json:
        _json(result)
        return
    table = Table(*result.columns)
    for row in result.rows:
        table.add_row(
            *("" if row.get(c) is None else str(row[c]) for c in result.columns)
        )
    console.print(table)


@app.command()
@_guarded
def cancel(
    run: Annotated[str, typer.Argument(help="The run id.")],
    as_json: AS_JSON = False,
) -> None:
    """Cancel a run."""
    summary = make_client().cancel(run)
    if as_json:
        _json(summary)
    else:
        console.print(f"{summary.id}: {summary.state.value}")


@app.command()
@_guarded
def logs(
    run: Annotated[str, typer.Argument(help="The run id.")],
    task: Annotated[str, typer.Argument(help="The task id.")],
) -> None:
    """Print the log of a task."""
    sys.stdout.write(make_client().logs(run, task))


@app.command()
@_guarded
def estimate(
    workflow: WORKFLOW,
    source: SOURCE = None,
    single_server: SINGLE_SERVER = False,
    reserve: RESERVE = None,
    as_json: AS_JSON = False,
) -> None:
    """Show the plan and the expected duration of every task of a submission."""
    split = SplitMode.SINGLE_SERVER if single_server else None
    plans = make_client().estimate(workflow, _sources(source), None, split, reserve)
    if as_json:
        _json(plans)
        return
    table = Table(
        "task", "pods", "cores", "memory GiB", "units", "seconds", "resources"
    )
    for task_id, e in plans.items():
        table.add_row(
            task_id,
            str(e.plan.pods),
            str(e.plan.shape.cores),
            f"{e.plan.shape.memory_gb:g}",
            "" if e.plan.units is None else str(e.plan.units),
            "" if e.seconds is None else f"{e.seconds:.0f}",
            e.resources.source,
        )
    console.print(table)


@app.command()
@_guarded
def cleanup(
    older_than: Annotated[float, typer.Option(help="Days since the run ended.")] = 30,
    group: GROUP = None,
    failed: Annotated[bool, typer.Option(help="Only failed runs.")] = False,
    dry_run: Annotated[bool, typer.Option(help="Remove nothing.")] = False,
    as_json: AS_JSON = False,
) -> None:
    """Remove ended runs and the snapshots nothing refers to."""
    result = make_client().cleanup(older_than, group, failed, dry_run)
    if as_json:
        _json(result)
        return
    verb = "would remove" if result.dry_run else "removed"
    console.print(
        f"{verb} {len(result.runs)} runs, {len(result.snapshots)} snapshots, "
        f"{result.bytes_freed / 2**30:.2f} GiB"
    )
    for run in result.runs:
        console.print(f"  {run}")


@app.command()
@_guarded
def usage(as_json: AS_JSON = False) -> None:
    """Show the bytes of the store per group and run."""
    result = make_client().usage()
    if as_json:
        _json(result)
        return
    table = Table("group", "GiB")
    for group, size in sorted(result.groups.items(), key=lambda kv: -kv[1]):
        table.add_row(group or "(none)", f"{size / 2**30:.2f}")
    console.print(table)
    runs = Table("run", "GiB")
    for run, size in sorted(result.runs.items(), key=lambda kv: -kv[1])[:20]:
        runs.add_row(run, f"{size / 2**30:.2f}")
    console.print(runs)
