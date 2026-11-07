from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from simrun.workflow import (
    SplitMode,
    TaskSpec,
    Workflow,
    WorkflowError,
    dump_workflow,
    load_workflow,
    workflow_yaml,
)


def test_order_is_topological() -> None:
    wf = Workflow(
        name="w",
        tasks=[
            TaskSpec(id="report", type="r", inputs={"in": "fit.result"}),
            TaskSpec(id="fit", type="f", inputs={"in": "model.package"}),
            TaskSpec(id="model", type="m"),
        ],
    )
    assert [t.id for t in wf.order()] == ["model", "fit", "report"]
    assert wf.task("fit").type == "f"


def test_cycle_raises() -> None:
    with pytest.raises(WorkflowError, match="cycle"):
        Workflow(
            name="w",
            tasks=[
                TaskSpec(id="a", type="x", inputs={"i": "b.x"}),
                TaskSpec(id="b", type="x", inputs={"i": "a.y"}),
            ],
        )


def test_unknown_reference_raises() -> None:
    with pytest.raises(WorkflowError, match="missing"):
        Workflow(
            name="w",
            tasks=[TaskSpec(id="a", type="x", inputs={"i": "missing.out"})],
        )


def test_yaml_round_trip(tmp_path: Path) -> None:
    wf = Workflow(
        name="w",
        split=SplitMode.SINGLE_SERVER,
        reserve="node1",
        tasks=[
            TaskSpec(id="a", type="x", settings={"n": 1}, stage=["in"]),
            TaskSpec(id="b", type="y", inputs={"i": "a.out"}, split=SplitMode.FASTEST),
        ],
    )
    path = tmp_path / "wf.yaml"
    dump_workflow(wf, path)
    assert "single-server" in path.read_text()
    assert load_workflow(path) == wf


def test_task_id_pattern() -> None:
    with pytest.raises(ValidationError):
        TaskSpec(id="Fit", type="x")


def test_workflow_yaml_is_compact_and_round_trips() -> None:
    wf = Workflow(
        name="w",
        tasks=[
            TaskSpec(id="fit", type="f", split=SplitMode.SINGLE_SERVER),
            TaskSpec(id="report", type="r", inputs={"in": "fit.result"}),
        ],
    )
    text = workflow_yaml(wf)
    assert text.startswith("name:")
    for absent in ("stage:", "resources:", "null"):
        assert absent not in text
    assert Workflow.model_validate(yaml.safe_load(text)) == wf


def test_workflow_yaml_keeps_empty_tasks() -> None:
    assert "tasks: []" in workflow_yaml(Workflow(name="x", tasks=[]))
