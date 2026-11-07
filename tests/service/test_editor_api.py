import yaml

from simrun.workflow import Workflow


def test_task_types(service):
    response = service.get("/task-types")
    assert response.status_code == 200
    listed = response.json()
    names = [t["name"] for t in listed]
    assert names == sorted(names)
    assert {"demo.report", "demo.squares"} <= set(names)
    squares = next(t for t in listed if t["name"] == "demo.squares")
    assert squares["splittable"] is True
    titles = {
        k: v["title"] for k, v in squares["settings_schema"]["properties"].items()
    }
    assert titles == {
        "n": "Units",
        "sleep_s": "Sleep per unit (s)",
        "fail_at": "Fail at unit",
        "memory_mb": "Memory per unit (MB)",
    }
    assert squares["outputs"] == ["squares"]
    types = service.client.app.state.service.types
    hint = types["demo.squares"].hint
    assert squares["hint"]["memory_per_worker_gb"] == hint.memory_per_worker_gb
    assert squares["description"] == "Squares of numbers, split over servers"


def test_builtin_types_describe_themselves():
    from simrun.demo import SQUARES
    from simrun.plugins import TaskType
    from simrun.plugins_builtin import COMMAND

    assert COMMAND.description == "A command in the image of the run"
    assert SQUARES.description == "Squares of numbers, split over servers"
    assert TaskType(name="x", settings=COMMAND.settings).description == ""


def test_workflow_yaml_route(service):
    response = service.post("/workflows/yaml", service.workflow())
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    loaded = Workflow.model_validate(yaml.safe_load(response.text))
    assert loaded == Workflow.model_validate(service.workflow())
    bad = {"name": "x", "tasks": [{"id": "Bad", "type": "t"}]}
    assert service.post("/workflows/yaml", bad).status_code == 422


def test_settings_errors_are_one_message_per_field():
    from simrun.demo import SQUARES
    from simrun.service.app import validate_workflow

    workflow = Workflow.model_validate(
        {
            "name": "w",
            "tasks": [
                {"id": "squares-2", "type": "demo.squares", "settings": {}},
                {
                    "id": "b",
                    "type": "demo.squares",
                    "settings": {"n": "x", "sleep_s": "y"},
                },
            ],
        }
    )
    errors = validate_workflow(workflow, {SQUARES.name: SQUARES})
    assert errors == [
        "task 'squares-2': n: Field required",
        "task 'b': n: Input should be a valid integer, unable to parse string as an integer",
        "task 'b': sleep_s: Input should be a valid number, unable to parse string as a number",
    ]
