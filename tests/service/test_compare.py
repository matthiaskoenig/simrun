import json

from simrun.snapshot import SourceProvenance


def _source(changed: list[str], digest: str) -> dict:
    return SourceProvenance(
        name="model",
        branch="develop",
        commit="0" * 40,
        dirty=bool(changed),
        changed_files=changed,
        diff_sha256=digest,
        snapshot="snapshots/model/x",
        patch=None,
    ).model_dump()


def test_compare_rows(service):
    (service.settings.store / "snapshots/model/x").mkdir(parents=True)
    a = service.submit(group="g1", sources=[_source(["M a.py"], "aa")])["id"]
    b = service.submit("t-mk-bot", group="g1", sources=[_source([], "bb")])["id"]
    for run_id, total in ((a, 30), (b, 14)):
        out = service.run_dir(run_id) / "tasks/squares/out"
        out.mkdir(parents=True)
        (out / "summary.json").write_text(json.dumps({"sum": total, "n": 4}))
    service.backend.finish(service.name(a))
    service.controller.reconcile()

    response = service.post("/compare", {"runs": [a, b]})
    assert response.status_code == 200, response.text
    table = response.json()
    assert table["columns"] == [
        "run",
        "group",
        "agent",
        "state",
        "model.changed_files",
        "model.diff_sha256",
        "squares.sum",
        "squares.n",
    ]
    first, second = table["rows"]
    assert first == {
        "run": a,
        "group": "g1",
        "agent": None,
        "state": "succeeded",
        "model.changed_files": 1,
        "model.diff_sha256": "aa",
        "squares.sum": 30,
        "squares.n": 4,
    }
    assert second["agent"] == "mk-bot"
    assert second["state"] == "queued"
    assert second["squares.sum"] == 14
    assert second["model.changed_files"] == 0


def test_compare_unknown_run_404(service):
    assert service.post("/compare", {"runs": ["nope"]}).status_code == 404


def test_compare_missing_summary(service):
    a = service.submit()["id"]
    table = service.post("/compare", {"runs": [a]}).json()
    assert table["columns"] == ["run", "group", "agent", "state"]
    assert table["rows"][0]["run"] == a
