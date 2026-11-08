import dataclasses
import json
import re
from datetime import UTC, datetime
from typing import Any

import pytest
from hera.workflows import models as m

from simrun.argo import to_argo
from simrun.config import ClusterConfig
from simrun.demo import SQUARES
from simrun.planner import PodShape, TaskPlan
from simrun.plugins import TaskType
from simrun.plugins_builtin import COMMAND
from simrun.snapshot import SourceProvenance
from simrun.store import RunRecord, RunState, TaskState
from simrun.workflow import Priority, Workflow

RUN = "20261105-120000-abc123"
CONFIG = ClusterConfig(registry="localhost:5000")
TYPES = {SQUARES.name: SQUARES, COMMAND.name: COMMAND}


def _record(reserve: str | None = None) -> RunRecord:
    workflow = Workflow(
        name="demo",
        tasks=[
            {"id": "squares", "type": "demo.squares", "settings": {"n": 10}},
            {
                "id": "report",
                "type": "command",
                "settings": {
                    "image": "localhost:5000/tools:1",
                    "args": ["python", "-m", "report"],
                    "cores": 2,
                    "memory_gb": 3.0,
                },
                "inputs": {"sums": "squares.squares"},
                "stage": ["sums"],
            },
        ],
    )
    return RunRecord(
        id=RUN,
        submitted_by="u",
        owner="o",
        agent=None,
        priority=Priority.OWNER,
        group="g1",
        workflow=workflow,
        sources=[
            SourceProvenance(
                name="sbmlsim",
                branch="develop",
                commit="abc",
                dirty=False,
                changed_files=[],
                diff_sha256="00",
                snapshot="snapshots/sbmlsim/00",
                patch=None,
            )
        ],
        image="localhost:5000/env:1",
        reserve=reserve,
        state=RunState.QUEUED,
        tasks={"squares": TaskState.WAITING, "report": TaskState.WAITING},
        plans={},
        backend_name=None,
        created=datetime.now(UTC),
        finished=None,
        message=None,
    )


PLANS = {
    "squares": TaskPlan(
        "squares",
        3,
        PodShape(
            cores=4,
            memory_gb=3,
            processes=4,
            per_worker_gb=0.5,
            retry_memory_gb=5,
            retry_per_worker_gb=0.75,
            scratch_gb=2.0,
            gpus=1,
        ),
        "simrun-owner-base",
        "simrun-owner-extra",
        10,
    ),
    "report": TaskPlan(
        "report",
        1,
        PodShape(
            cores=2,
            memory_gb=3,
            processes=1,
            per_worker_gb=3.0,
            retry_memory_gb=5,
            retry_per_worker_gb=5.0,
            scratch_gb=0.0,
            gpus=0,
        ),
        "simrun-owner-base",
        "simrun-owner-extra",
        None,
    ),
}


def _manifest(reserve: str | None = None) -> dict[str, Any]:
    return to_argo(_record(reserve), PLANS, TYPES, CONFIG)


def _dag(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entry = manifest["spec"]["entrypoint"]
    (dag,) = [t for t in manifest["spec"]["templates"] if t["name"] == entry]
    return {task["name"]: task for task in dag["dag"]["tasks"]}


def _template(manifest: dict[str, Any], task: str) -> dict[str, Any]:
    name = _dag(manifest)[task]["template"]
    (template,) = [t for t in manifest["spec"]["templates"] if t["name"] == name]
    return template


def _env(template: dict[str, Any]) -> dict[str, Any]:
    return {
        e["name"]: e.get("value", e.get("valueFrom"))
        for e in template["container"]["env"]
    }


def _args(manifest: dict[str, Any], task: str) -> dict[str, str]:
    params = _dag(manifest)[task].get("arguments", {}).get("parameters", [])
    return {p["name"]: p["value"] for p in params}


def _resolved(template: dict[str, Any], args: dict[str, str]) -> str:
    """The template as JSON with its input parameters substituted, as Argo does."""
    text = json.dumps(template)
    for name, value in args.items():
        text = text.replace("{{inputs.parameters." + name + "}}", value)
    return text


def test_requests_equal_limits() -> None:
    manifest = _manifest()
    worker = _template(manifest, "squares-w0")["container"]["resources"]
    assert worker["requests"] == worker["limits"]
    assert worker["requests"] == {
        "cpu": "4",
        "memory": "3Gi",
        "nvidia.com/gpu": "1",
        "ephemeral-storage": "2Gi",
    }
    merge = _template(manifest, "squares-merge")["container"]["resources"]
    assert merge["requests"] == merge["limits"] == {"cpu": "1", "memory": "1Gi"}
    report = _template(manifest, "report")["container"]["resources"]
    assert report["requests"] == report["limits"] == {"cpu": "2", "memory": "3Gi"}


def test_retry_strategy() -> None:
    manifest = _manifest()
    for task in ("squares-w0", "squares-extra", "squares-merge", "report"):
        template = _template(manifest, task)
        retry = template["retryStrategy"]
        # evictions count against it; the controller ends a second kill for memory
        assert retry["limit"] == 100
        assert retry["retryPolicy"] == "Always"
        # only a pod which was stopped: no exit code, killed, SIGTERM
        assert retry["expression"] == (
            "lastRetry.exitCode == '-1' || lastRetry.exitCode == '137' "
            "|| lastRetry.exitCode == '143'"
        )
        patch = json.loads(template["podSpecPatch"])
        assert patch["terminationGracePeriodSeconds"] == 90
        (container,) = patch["containers"]
        assert container["name"] == template["container"]["name"] == "main"
    worker = json.loads(_template(manifest, "squares-w0")["podSpecPatch"])
    (container,) = worker["containers"]
    memory = "{{=lastRetry.exitCode == '137' ? '5Gi' : '3Gi'}}"
    assert container["resources"] == {
        "requests": {"memory": memory},
        "limits": {"memory": memory},
    }
    assert container["env"] == [
        {
            "name": "SIMRUN_MEMORY_PER_WORKER_GB",
            "value": "{{=lastRetry.exitCode == '137' ? '0.75' : '0.5'}}",
        }
    ]
    merge = json.loads(_template(manifest, "squares-merge")["podSpecPatch"])
    assert "'2Gi' : '1Gi'" in merge["containers"][0]["resources"]["limits"]["memory"]
    report = json.loads(_template(manifest, "report")["podSpecPatch"])
    assert "'5Gi' : '3Gi'" in report["containers"][0]["resources"]["limits"]["memory"]


def test_environment() -> None:
    manifest = _manifest()
    worker = _env(_template(manifest, "squares-w0"))
    task_dir = f"/store/runs/{RUN}/tasks/squares"
    assert worker["SIMRUN_TASK_DIR"] == task_dir
    assert worker["SIMRUN_TASK_TYPE"] == "demo.squares"
    assert worker["SIMRUN_ATTEMPT"] == "{{retries}}"
    assert worker["SIMRUN_MEMORY_BASE_GB"] == "0.5"
    assert worker["SIMRUN_MEMORY_PER_WORKER_GB"] == "0.5"
    assert worker["SIMRUN_EDITABLE"] == "/store/snapshots/sbmlsim/00"
    assert worker["SIMRUN_SCRATCH"] == "/scratch"
    assert worker["SIMRUN_INPUTS"] == ""
    assert worker["SIMRUN_NODE"] == {"fieldRef": {"fieldPath": "spec.nodeName"}}
    report = _env(_template(manifest, "report"))
    assert report["SIMRUN_INPUTS"] == f"sums=runs/{RUN}/tasks/squares/out/squares"
    assert (
        report["SIMRUN_STAGE"] == f"runs/{RUN}/tasks/squares/out/squares=/scratch/sums"
    )


def test_unsplit_plugin_command() -> None:
    plot = TaskType(
        name="plot",
        settings=SQUARES.settings,
        command="plot --settings {settings} --out {out}",
        image="localhost:5000/plot:2",
    )
    record = _record()
    record.workflow = Workflow(
        name="p", tasks=[{"id": "plot", "type": "plot", "settings": {"n": 1}}]
    )
    plans = {"plot": PLANS["report"]}
    manifest = to_argo(record, plans, {"plot": plot}, CONFIG)
    container = _template(manifest, "plot")["container"]
    task_dir = f"/store/runs/{RUN}/tasks/plot"
    # through the harness, which installs the snapshots and writes a report
    assert container["command"] == [
        "simrun-exec",
        "plot",
        "--settings",
        f"{task_dir}/settings.json",
        "--out",
        f"{task_dir}/out",
    ]
    assert container["image"] == "localhost:5000/plot:2"


def test_command_args_get_the_paths_of_the_inputs_and_the_output() -> None:
    record = _record()
    record.workflow = Workflow(
        name="p",
        tasks=[
            {"id": "squares", "type": "demo.squares", "settings": {"n": 10}},
            {
                "id": "sum",
                "type": "command",
                "settings": {
                    "image": "localhost:5000/tools:1",
                    "args": [
                        "sum",
                        "--input={input}",
                        "{out}",
                        "--settings={settings}",
                    ],
                    "cores": 1,
                    "memory_gb": 1.0,
                },
                "inputs": {"b": "squares.squares", "a": "squares.squares"},
            },
        ],
    )
    plans = {"squares": PLANS["squares"], "sum": PLANS["report"]}
    manifest = to_argo(record, plans, TYPES, CONFIG)
    run = f"/store/runs/{RUN}/tasks"
    container = _template(manifest, "sum")["container"]
    # no harness: the args as given, with the templates of a plugin command
    assert container["command"] == [
        "sum",
        f"--input=a={run}/squares/out/squares",
        f"--input=b={run}/squares/out/squares",
        f"{run}/sum/out",
        f"--settings={run}/sum/settings.json",
    ]


def test_pod_env_has_provenance() -> None:
    manifest = _manifest()
    for task in ("squares-w0", "squares-extra", "squares-merge", "report"):
        env = _env(_template(manifest, task))
        assert (
            env["SIMRUN_PROVENANCE"]
            == f"/store/runs/{RUN}/tasks/{task.split('-')[0]}/provenance.json"
        )


def test_unsplit_command_repeats_input_parts() -> None:
    plot = TaskType(
        name="plot",
        settings=SQUARES.settings,
        inputs=("a", "b"),
        command="plot --input={input} --settings {settings} {input} --out {out}",
        image="localhost:5000/plot:2",
    )
    record = _record()
    record.workflow = Workflow(
        name="p",
        tasks=[
            {"id": "squares", "type": "demo.squares", "settings": {"n": 10}},
            {
                "id": "plot",
                "type": "plot",
                "settings": {"n": 1},
                # sorted by port, not in the order of the workflow
                "inputs": {"b": "squares.squares", "a": "other.out"},
                "stage": ["b"],
            },
            {"id": "other", "type": "plot", "settings": {"n": 1}},
        ],
    )
    plans = {"squares": PLANS["squares"], "plot": PLANS["report"]}
    plans["other"] = PLANS["report"]
    manifest = to_argo(record, plans, {**TYPES, "plot": plot}, CONFIG)
    run = f"/store/runs/{RUN}/tasks"
    a = f"a={run}/other/out/out"
    # a staged port is the copy `simrun-exec` stages into the scratch
    b = "b=/scratch/b"
    assert _template(manifest, "plot")["container"]["command"] == [
        "simrun-exec",
        "plot",
        f"--input={a}",
        f"--input={b}",
        "--settings",
        f"{run}/plot/settings.json",
        a,
        b,
        "--out",
        f"{run}/plot/out",
    ]
    # without inputs, the parts with `{input}` are dropped
    assert _template(manifest, "other")["container"]["command"] == [
        "simrun-exec",
        "plot",
        "--settings",
        f"{run}/other/settings.json",
        "--out",
        f"{run}/other/out",
    ]


def test_mounts() -> None:
    manifest = _manifest()
    volumes = {v["name"]: v for v in manifest["spec"]["volumes"]}
    assert volumes["store"]["persistentVolumeClaim"]["claimName"] == "simrun-store"
    assert volumes["scratch"]["hostPath"] == {
        "path": "/scratch",
        "type": "DirectoryOrCreate",
    }
    for task in ("squares-w0", "squares-extra", "squares-merge", "report"):
        mounts = {
            v["mountPath"]: v
            for v in _template(manifest, task)["container"]["volumeMounts"]
        }
        run = mounts[f"/store/runs/{RUN}"]
        assert run["name"] == "store"
        assert run["subPath"] == f"runs/{RUN}"
        assert not run.get("readOnly", False)
        snapshots = mounts["/store/snapshots"]
        assert snapshots["name"] == "store"
        assert snapshots["subPath"] == "snapshots"
        assert snapshots["readOnly"] is True
        assert mounts["/scratch"]["name"] == "scratch"


def test_scratch_per_worker() -> None:
    manifest = _manifest()

    def scratch(task: str) -> str:
        mounts = _template(manifest, task)["container"]["volumeMounts"]
        (path,) = [v["subPath"] for v in mounts if v["mountPath"] == "/scratch"]
        return path

    worker = f"{RUN}/squares/{{{{inputs.parameters.worker}}}}"
    assert scratch("squares-w0") == scratch("squares-extra") == worker
    assert scratch("squares-merge") == f"{RUN}/squares/merge"
    assert scratch("report") == f"{RUN}/report"
    worker = _template(manifest, "squares-w0")
    resolved = _resolved(worker, _args(manifest, "squares-w0"))
    assert f'"subPath": "{RUN}/squares/squares-w0"' in resolved
    resolved = _resolved(
        worker, {**_args(manifest, "squares-extra"), "worker": "squares-w2"}
    )
    assert f'"subPath": "{RUN}/squares/squares-w2"' in resolved


def test_reservation() -> None:
    plain = _manifest()["spec"]
    assert "nodeSelector" not in plain
    assert "tolerations" not in plain
    reserved = _manifest(reserve="srv-01")["spec"]
    assert reserved["nodeSelector"] == {"simrun/reserved": RUN}
    # no effect: the run tolerates its taint whatever its effect (NoExecute)
    assert reserved["tolerations"] == [
        {"key": "simrun/reserved", "operator": "Equal", "value": RUN}
    ]


def test_service_account() -> None:
    assert _manifest()["spec"]["serviceAccountName"] == "simrun-workflow"
    config = CONFIG.model_copy(update={"service_account": "runner"})
    manifest = to_argo(_record(), PLANS, TYPES, config)
    assert manifest["spec"]["serviceAccountName"] == "runner"


def test_workflow_metadata() -> None:
    manifest = _manifest()
    assert manifest["metadata"]["name"] == f"simrun-{RUN}"
    assert manifest["metadata"]["namespace"] == "simrun"
    assert manifest["metadata"]["labels"] == {"simrun/run": RUN, "simrun/group": "g1"}
    assert manifest["metadata"]["annotations"] == {"simrun/group": "g1"}
    assert manifest["spec"]["ttlStrategy"] == {"secondsAfterCompletion": 86400}


def test_manifest_is_valid() -> None:
    manifest = _manifest(reserve="srv-01")
    m.Workflow.model_validate(manifest)
    # the templates stay valid once Argo substitutes the input parameters
    worker = _template(manifest, "squares-w0")
    for task in ("squares-w0", "squares-extra"):
        args = {**_args(manifest, task), "worker": "squares-w1"}
        m.Template.model_validate(json.loads(_resolved(worker, args)))


def test_missing_image_raises() -> None:
    record = _record()
    record.image = None
    with pytest.raises(ValueError, match="squares"):
        to_argo(record, PLANS, TYPES, CONFIG)


LABEL_VALUE = re.compile(r"^([a-z0-9]([a-z0-9._-]*[a-z0-9])?)?$")


def test_group_label_sanitised() -> None:
    record = _record()
    group = "Team A/" + "x" * 80
    record.group = group
    metadata = to_argo(record, PLANS, TYPES, CONFIG)["metadata"]
    label = metadata["labels"]["simrun/group"]
    assert LABEL_VALUE.match(label)
    assert len(label) <= 63
    assert label.startswith("team-a-xxx")
    assert metadata["annotations"]["simrun/group"] == group
    record.group = "Team A/" + "x" * 79 + "y"
    other = to_argo(record, PLANS, TYPES, CONFIG)["metadata"]["labels"]
    assert other["simrun/group"] != label
    m.Workflow.model_validate(to_argo(record, PLANS, TYPES, CONFIG))


def test_no_group() -> None:
    record = _record()
    record.group = None
    metadata = to_argo(record, PLANS, TYPES, CONFIG)["metadata"]
    assert "simrun/group" not in metadata["labels"]
    assert "annotations" not in metadata


def test_split_task_dag() -> None:
    manifest = _manifest()
    dag = _dag(manifest)
    assert list(dag) == ["squares-w0", "squares-extra", "squares-merge", "report"]
    assert "withSequence" not in dag["squares-w0"]
    assert dag["squares-extra"]["withSequence"] == {"start": "1", "count": "2"}
    assert dag["squares-merge"]["dependencies"] == ["squares-w0"]
    assert dag["report"]["dependencies"] == ["squares-merge"]
    assert "dependencies" not in dag["squares-w0"]
    assert "dependencies" not in dag["squares-extra"]
    assert dag["squares-extra"]["continueOn"] == {"failed": True, "error": True}
    for task in ("squares-w0", "squares-merge", "report"):
        assert "continueOn" not in dag[task]
    assert dag["squares-w0"]["template"] == dag["squares-extra"]["template"]
    worker = _template(manifest, "squares-w0")
    assert worker["container"]["command"] == ["simrun-worker"]
    inputs = worker["inputs"]["parameters"]
    assert [p["name"] for p in inputs] == ["worker", "priority", "task", "role"]
    assert _args(manifest, "squares-w0") == {
        "worker": "squares-w0",
        "priority": "simrun-owner-base",
        "task": "squares",
        "role": "base",
    }
    assert _args(manifest, "squares-extra") == {
        "worker": "squares-w{{item}}",
        "priority": "simrun-owner-extra",
        "task": "squares",
        "role": "extra",
    }
    merge = _template(manifest, "squares-merge")
    assert merge["container"]["command"] == ["simrun-merge"]
    report = _template(manifest, "report")
    assert report["container"]["command"] == ["python", "-m", "report"]
    assert report["container"]["image"] == "localhost:5000/tools:1"
    assert worker["container"]["image"] == "localhost:5000/env:1"
    assert merge["container"]["image"] == "localhost:5000/env:1"


def test_a_coordinator_which_merges_has_no_merge_node() -> None:
    plans = {
        k: dataclasses.replace(p, coordinator_merges=True) for k, p in PLANS.items()
    }
    manifest = to_argo(_record(), plans, TYPES, CONFIG)
    dag = _dag(manifest)
    assert "squares-merge" not in dag
    assert dag["report"]["dependencies"] == ["squares-w0"]
    assert _env(_template(manifest, "squares-w0"))["SIMRUN_MERGE"] == "1"
    assert "SIMRUN_MERGE" not in _env(_template(_manifest(), "squares-w0"))
    m.Workflow.model_validate(manifest)


def test_single_pod_has_no_extra_workers() -> None:
    plans = {**PLANS, "squares": dataclasses.replace(PLANS["squares"], pods=1)}
    manifest = to_argo(_record(), plans, TYPES, CONFIG)
    dag = _dag(manifest)
    assert list(dag) == ["squares-w0", "squares-merge", "report"]
    assert dag["squares-merge"]["dependencies"] == ["squares-w0"]
    m.Workflow.model_validate(manifest)


def test_kueue_labels_and_priorities() -> None:
    manifest = _manifest()
    for task in ("squares-w0", "squares-extra", "squares-merge", "report"):
        template = _template(manifest, task)
        metadata = template["metadata"]
        assert metadata["labels"]["kueue.x-k8s.io/queue-name"] == "simrun"
        assert (
            metadata["annotations"]["kueue.x-k8s.io/podset-required-topology"]
            == "kubernetes.io/hostname"
        )
        assert (
            template["priorityClassName"]
            == metadata["labels"]["kueue.x-k8s.io/priority-class"]
        )
    worker = _template(manifest, "squares-w0")
    assert worker["priorityClassName"] == "{{inputs.parameters.priority}}"
    for task in ("squares-merge", "report"):
        assert _template(manifest, task)["priorityClassName"] == "simrun-owner-base"


def test_worker_roles() -> None:
    manifest = _manifest()
    worker = _template(manifest, "squares-w0")
    env = _env(worker)
    assert env["SIMRUN_WORKER"] == "{{inputs.parameters.worker}}"
    assert env["SIMRUN_ROLE"] == "{{inputs.parameters.role}}"
    assert env["SIMRUN_STALE_S"] == "120"
    for task, role in (("squares-w0", "base"), ("squares-extra", "extra")):
        resolved = json.loads(_resolved(worker, _args(manifest, task)))
        env = _env(resolved)
        assert env["SIMRUN_ROLE"] == role
        priority = f"simrun-owner-{role}"
        assert resolved["priorityClassName"] == priority
        assert (
            resolved["metadata"]["labels"]["kueue.x-k8s.io/priority-class"] == priority
        )
    for task in ("squares-merge", "report"):
        env = _env(_template(manifest, task))
        for name in ("SIMRUN_WORKER", "SIMRUN_ROLE", "SIMRUN_STALE_S"):
            assert name not in env


def test_a_source_without_snapshot_is_not_editable() -> None:
    record = _record()
    record.sources.append(
        record.sources[0].model_copy(update={"name": "plain", "snapshot": ""})
    )
    manifest = to_argo(record, PLANS, TYPES, CONFIG)
    worker = _env(_template(manifest, "squares-w0"))
    assert worker["SIMRUN_EDITABLE"] == "/store/snapshots/sbmlsim/00"
