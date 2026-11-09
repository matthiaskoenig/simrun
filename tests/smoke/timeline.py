"""Where the time of a run went: the timeline of its tasks from the run store.

For every task the reports of its workers (`tasks/<task>/workers/*.json`) give when
its harness started, the seconds it prepared (the install of the sources, the
restart, the imports), the seconds of its work, the cpu it used and, in a pod, the
seconds its CPU quota held it back (`thrott`); the record of
the run gives its creation, its end and the dependencies of the tasks. A task
`waited` from the end of the tasks it reads (or the creation of the run) to the
start of its harness: the submission, the pod, its admission and its start on the
cluster, the free cores locally.

On the local test system (`--k3d`) the pods of the run, while Argo keeps them, split
the wait: `dag` from the end of the dependencies to the pod, `admit` from the pod
to its admission by Kueue, `start` from the admission to the start of its main
container (the image, Argo's init container). The record gives the seconds the
service took to resolve the environment and to build the image when missing.

    uv run python tests/smoke/timeline.py RUN --store .tools/k3d/store --k3d
    uv run python tests/smoke/timeline.py RUN --store .tools/smoke/local/.simrun/store
"""

import argparse
import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
KUBECTL = REPO / ".tools" / "bin" / "kubectl"
KUBECONFIG = REPO / ".tools" / "kubeconfig"
NAMESPACE = "simrun"


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


@dataclass
class Task:
    """The workers of a task, summed up."""

    id: str
    dependencies: list[str]
    workers: int = 0
    started: float | None = None
    work_started: float | None = None
    ended: float | None = None
    prepare_s: float = 0.0
    cpu_s: float = 0.0
    throttled_s: float | None = None
    merged: float | None = None
    processes: int = 0
    pods: list[dict] = field(default_factory=list)

    @property
    def finished(self) -> float | None:
        """The end of the task: the end of its merge, else of its last worker."""
        if self.ended is None:
            return None
        return max(self.ended, self.merged or self.ended)

    @property
    def merge_s(self) -> float:
        """Seconds of the merge after the work of the coordinator."""
        return (
            0.0
            if self.finished is None or self.ended is None
            else self.finished - self.ended
        )

    @property
    def work_s(self) -> float:
        """Seconds from the first work of a worker to the end of the last."""
        if self.work_started is None or self.ended is None:
            return 0.0
        return self.ended - self.work_started


@dataclass
class Timeline:
    """The tasks of a run and its times."""

    run_id: str
    name: str
    state: str
    created: float
    finished: float | None
    tasks: dict[str, Task]
    built: float | None = None
    workflow_created: float | None = None

    def ready(self, task: Task) -> float:
        """When a task could start: the end of its dependencies, else the creation."""
        ends = [self.tasks[d].finished for d in task.dependencies]
        return max([self.created, *(e for e in ends if e is not None)])


def read(store: Path, run_id: str) -> Timeline:
    """The timeline of a run from the store."""
    run_dir = store / "runs" / run_id
    record = json.loads((run_dir / "run.json").read_text())
    tasks: dict[str, Task] = {}
    for spec in record["workflow"]["tasks"]:
        task = Task(
            spec["id"], sorted({i.split(".")[0] for i in spec["inputs"].values()})
        )
        reports = sorted((run_dir / "tasks" / task.id / "workers").glob("*.json"))
        # the last attempt of every worker
        latest: dict[str, dict] = {}
        for path in reports:
            report = json.loads(path.read_text())
            latest[report["worker"]] = report
        for report in latest.values():
            if report.get("started") is None:
                continue
            started = report["started"]
            work_started = started + report.get("prepare_seconds", 0.0)
            ended = work_started + report["seconds"]
            task.workers += 1
            task.processes += report["processes"]
            task.cpu_s += report.get("cpu_seconds", 0.0)
            if report.get("throttled_seconds") is not None:
                task.throttled_s = (task.throttled_s or 0.0) + report[
                    "throttled_seconds"
                ]
            task.prepare_s = max(task.prepare_s, report.get("prepare_seconds", 0.0))
            task.started = min(started, task.started or started)
            task.work_started = min(work_started, task.work_started or work_started)
            task.ended = max(ended, task.ended or ended)
        # the coordinator of a split task merges after its report; the marker of
        # the merge is written at its end
        marker = run_dir / "tasks" / task.id / "out" / ".simrun-merged"
        if marker.exists() and task.ended is not None:
            task.merged = marker.stat().st_mtime
        tasks[task.id] = task
    return Timeline(
        run_id=run_id,
        name=record["workflow"]["name"],
        state=record["state"],
        created=_epoch(record["created"]),
        finished=_epoch(record["finished"]) if record.get("finished") else None,
        tasks=tasks,
        built=_epoch(record["built"]) if record.get("built") else None,
    )


def _kubectl(*args: str) -> dict:
    result = subprocess.run(
        [str(KUBECTL), "--kubeconfig", str(KUBECONFIG), "-n", NAMESPACE, *args],
        capture_output=True,
        text=True,
        timeout=60,
    )
    return json.loads(result.stdout) if result.returncode == 0 else {}


def add_k3d(timeline: Timeline) -> None:
    """The pods, the workflow and the builds of a run on the local test system."""
    name = f"simrun-{timeline.run_id}"
    workflow = _kubectl("get", "workflow", name, "-o", "json")
    if workflow:
        timeline.workflow_created = _epoch(workflow["metadata"]["creationTimestamp"])
    pods = _kubectl(
        "get", "pods", "-l", f"workflows.argoproj.io/workflow={name}", "-o", "json"
    )
    for pod in pods.get("items", []):
        node = pod["metadata"]["annotations"].get("workflows.argoproj.io/node-name", "")
        # `<workflow>.<task>-w0(0)`, `<workflow>.<task>-extra(1:2)(0)`, `<workflow>.<task>(0)`
        label = node.split(".", 1)[-1].split("(")[0]
        task_id = next(
            (
                t
                for t in sorted(timeline.tasks, key=len, reverse=True)
                if label.startswith(t)
            ),
            None,
        )
        if task_id is None:
            continue
        times = {"created": _epoch(pod["metadata"]["creationTimestamp"])}
        for condition in pod["status"].get("conditions", []):
            if condition["type"] == "PodScheduled" and condition["status"] == "True":
                times["scheduled"] = _epoch(condition["lastTransitionTime"])
        for status in pod["status"].get("containerStatuses", []):
            if status["name"] == "main":
                state = status["state"].get("terminated") or status["state"].get(
                    "running"
                )
                if state and state.get("startedAt"):
                    times["main"] = _epoch(state["startedAt"])
                if state and state.get("finishedAt"):
                    times["finished"] = _epoch(state["finishedAt"])
        timeline.tasks[task_id].pods.append(times)
    for task in timeline.tasks.values():
        # the reports of simrun 0.3.0 have no start: the main containers instead,
        # whose work includes the preparation
        mains = [p["main"] for p in task.pods if "main" in p]
        ends = [p["finished"] for p in task.pods if "finished" in p]
        if task.started is None and mains and ends:
            task.started = task.work_started = min(mains)
            task.ended = max(ends)


def _offset(timeline: Timeline, value: float | None) -> str:
    return "" if value is None else f"{value - timeline.created:+.0f}"


def show(timeline: Timeline) -> None:
    """Print the timeline: a row per task in the order of its start, and the totals."""
    total = (timeline.finished or timeline.created) - timeline.created
    print(f"{timeline.name} {timeline.run_id} {timeline.state} in {total:.0f} s")
    if timeline.built is not None:
        # the environment resolved and the image built when missing, then the
        # workflow created
        line = f"  environment and image {timeline.built - timeline.created:.0f} s"
        if timeline.workflow_created is not None:
            line += f", workflow {max(0.0, timeline.workflow_created - timeline.built):.0f} s"
        print(line)
    k3d = any(t.pods for t in timeline.tasks.values())
    header = f"  {'task':16s} {'start':>6s} {'waited':>6s}"
    if k3d:
        header += f" {'dag':>5s} {'admit':>5s} {'start':>5s}"
    throttled = any(t.throttled_s is not None for t in timeline.tasks.values())
    header += f" {'prep':>5s} {'work':>6s} {'merge':>5s} {'end':>6s} {'cpu':>6s} {'procs':>5s}"
    if throttled:
        header += f" {'thrott':>6s}"
    print(header)
    ordered = sorted(timeline.tasks.values(), key=lambda t: t.started or float("inf"))
    waited = prepared = 0.0
    for task in ordered:
        ready = timeline.ready(task)
        wait = (task.started - ready) if task.started is not None else 0.0
        waited += wait
        prepared += task.prepare_s
        row = f"  {task.id:16s} {_offset(timeline, task.started):>6s} {wait:6.0f}"
        if k3d:
            first = min(task.pods, key=lambda p: p["created"]) if task.pods else {}
            dag = first.get("created", ready) - ready
            admit = first.get("scheduled", first.get("created", 0)) - first.get(
                "created", 0
            )
            start = first.get("main", first.get("scheduled", 0)) - first.get(
                "scheduled", 0
            )
            row += f" {dag:5.0f} {admit:5.0f} {start:5.0f}"
        row += (
            f" {task.prepare_s:5.0f} {task.work_s:6.0f} {task.merge_s:5.0f}"
            f" {_offset(timeline, task.finished):>6s}"
            f" {task.cpu_s:6.0f} {task.processes:5d}"
        )
        if throttled:
            row += f" {task.throttled_s or 0.0:6.0f}"
        print(row)
    work = sum(t.work_s for t in ordered)
    cpu = sum(t.cpu_s for t in ordered)
    print(
        f"  summed: waited {waited:.0f} s, prepared {prepared:.0f} s, worked {work:.0f} s, "
        f"cpu {cpu:.0f} s"
    )


def main() -> None:
    """Print the timeline of a run."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("run")
    parser.add_argument("--store", type=Path, default=REPO / ".tools" / "k3d" / "store")
    parser.add_argument(
        "--k3d", action="store_true", help="add the pods and the builds"
    )
    args = parser.parse_args()
    timeline = read(args.store, args.run)
    if args.k3d:
        add_k3d(timeline)
    show(timeline)


if __name__ == "__main__":
    main()
