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
    assert "n" in squares["settings_schema"]["properties"]
    assert squares["outputs"] == ["squares"]
    types = service.client.app.state.service.types
    hint = types["demo.squares"].hint
    assert squares["hint"]["memory_per_worker_gb"] == hint.memory_per_worker_gb


def test_workflow_yaml_route(service):
    response = service.post("/workflows/yaml", service.workflow())
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    loaded = Workflow.model_validate(yaml.safe_load(response.text))
    assert loaded == Workflow.model_validate(service.workflow())
    bad = {"name": "x", "tasks": [{"id": "Bad", "type": "t"}]}
    assert service.post("/workflows/yaml", bad).status_code == 422
