# CLAUDE.md

This file provides guidance when working with code in this repository.

## Project

`simrun` is a runner of simulation workflows on a small k3s cluster with Argo Workflows and Kueue, for people and agents (sbmlsim#440): a workflow is a YAML file of tasks of task types from plugins, submitted with snapshots of the working trees of the model and its libraries, and run as an Argo workflow of pods which Kueue admits and preempts. Requires python >= 3.14, Linux only (cgroups v2, k3s), packaged with hatchling (version read from `src/simrun/__init__.py`). Runtime dependencies are `fastapi` and `uvicorn` (the service), `fastmcp` (the MCP server), `hera` and `kubernetes` (Argo and the cluster), `httpx`, `pydantic`, `pyyaml`, `rich`, `typer` (client, models, command line) and `tomlkit` (the temporary project of an environment). The extra `cwl` (`cwl-utils`, `cwltool`) is the CWL export of a run, `simrun.cwl`, which imports `cwl_utils` lazily in `write_cwl`; `dev` includes it. Entry points: `simrun` (CLI), `simrun-mcp` (MCP server on stdio), `simrun-worker`, `simrun-merge`, `simrun-exec` (the harness of the pods), and the task types `command` and `demo.squares` of the group `simrun.task_types`.

## Commands

```bash
# environment (uv based), the versions of the committed uv.lock
uv sync --extra dev
uv run pre-commit install
uv lock                                            # after every change of a dependency in pyproject.toml

# tests
pytest                                             # the unit tests, in parallel with pytest-xdist (`-m 'not cluster'` of addopts)
pytest -n 0                                        # in one process, e.g. for --pdb
SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster                     # the tests on a k3d cluster (docker, ~20 GiB disk)
SIMRUN_K3D=1 SIMRUN_K3D_KEEP=1 uv run pytest -m cluster tests/cluster   # keep the cluster for the next session
uv run python tests/cluster/local.py up            # the k3d cluster as a local test system, service on the host (docs/testing.md); `down` deletes it
uv run python tests/smoke/run.py [--k3d]           # the smoke workflows: the analyses of sbmlsim and pkdb_models (~/git checkouts) through simrun, locally or on the local test system
uv run simrun submit examples/squares.yaml --local   # on this machine without a cluster, state in .simrun/ (docs/local.md)
uv run simrun edit /tmp/squares.yaml               # a copy of examples/squares.yaml in the Editor of the local test system (docs/web-app.md)
tox r -e py3.15                                    # single tox env (py3.14, py3.15)
tox run-parallel                                   # every supported python, before opening a pull request

# lint / format / types
ruff check
ruff format
uv run ty check                                    # config in [tool.ty] of pyproject.toml

# documentation
uv run zensical build --clean --strict             # into the gitignored site/
uv run python scripts/llms_txt.py                  # llms.txt, llms-full.txt and the markdown of every page into site/

# web app (frontend/, node 24, the versions of the committed package-lock.json)
cd frontend && npm ci                              # the dependencies of the lock
npm run dev                                        # dev server at http://localhost:5173/ui/, the API proxied to SIMRUN_URL or http://localhost:8000
npm test                                           # vitest
npm run typecheck                                  # vue-tsc
npm run build                                      # into frontend/dist, served at /ui/ with the setting `web`
uv run python scripts/openapi.py && (cd frontend && npm run types)   # after a change of the API: openapi.json, then schema.d.ts
```

`develop` is the default branch and takes every change through a pull request; the rulesets in `.github/rulesets/` (applied with `.github/rulesets/apply.sh`) require the checks `tests`, `ruff`, `ty` and `docs`. `tests` (`ci-cd.yml`) aggregates the unit tests on python 3.14 and 3.15 on Linux, the `plain install` (the package from `pyproject.toml` without the lock and without extras, every module imported, `simrun --help`) and the job `frontend` (`npm ci`, types, tests, build, `schema.d.ts` equal to `openapi.json`). The job `cluster` (`cluster.yml`, called by `ci-cd.yml`) runs the k3d tests on a push to `develop`, on a tag and on `workflow_dispatch`, and on a pull request with the label `cluster`; it is not required. Every job sets up uv and python with `.github/actions/setup` (`uv sync --locked`). A release: bump on a branch with `uvx bump-my-version bump [major|minor|patch]` (`src/simrun/__init__.py`, `CITATION.cff`, no tag), merge, tag the merged commit on `develop`; the tag waits for the run of its commit on develop (`tested on develop`) and the cluster job, publishes to PyPI with trusted publishing (environment `pypi`), creates the GitHub release from `release-notes/<version>.md` (Zenodo archives it) and fast-forwards `main` (`sync-main`). Details in `docs/development.md`. The documentation is Zensical (`zensical.toml`, `docs/`), published from `develop` to https://matthiaskoenig.github.io/simrun; a page in `docs/api/` is just `::: simrun.<module>`.

## Architecture

- **`workflow.py`** the model of a workflow (`Workflow`, `TaskSpec`, `SplitMode` `fastest`/`single-server`, `Priority` `owner`/`standard`/`agent`, `ResourceOverride`), inputs `<task>.<port>`, order by inputs, cycles refused.
- **`plugins.py`** `TaskType` (settings model, ports, `ResourceHint`, `count_units`/`setup`/`compute_unit`/`merge`/`plan` as `"module:function"` strings, `command` (an argv template with `{settings}`, `{out}` and `{input}`, a part repeated per input port as `<port>=<path>`, the scratch copy of a staged port), `uses_cores`, `image`, `cost_setting`), `TaskPaths`, discovery from the entry points; `plugins_builtin.py` the type `command`; `demo.py` the split demo `demo.squares`.
- **`client.py`, `cli.py`, `mcp.py`** the client (`Client.from_env`: `SIMRUN_URL`, `SIMRUN_TOKEN`, `SIMRUN_STORE` optional, without it the snapshots are uploaded; plans the tasks of the types installed locally which have a `plan` for `submit`, `estimate` and `validate`, `plan_tasks`, sending the fields given or set by the plan, snapshots the sources, then calls the API), the typer CLI on it (every command but `edit` `--json`, a refusal to stderr with exit 1, `wait` exits 2 on timeout) and the FastMCP server on it (answers carry `store`, a refusal is `{"error", "status"}`).
- **`local.py`** the local service of a project (`simrun serve --local`): `.simrun/` with the store, the index and a generated token, `service.json` (instance, pid, port, token, url; the token is sent only to a server answering the instance at `/local/instance`) while it listens on `127.0.0.1`, `ensure_local` which starts it in the background (`python -m simrun`, `__main__.py`) under a lock and finds it again, the idle exit, `environment` in `service.json` (a service in the background with another python, simrun or task types is restarted when idle), `stop_local` (`serve --local --stop`, `/local/stop`), its end cancels the runs of the `LocalBackend`; `web_dir` the built web app, `simrun/web` of the wheel (copied by the release) or `frontend/dist` of a checkout.
- **`edit.py`** the command `simrun edit`: opens an edit session, long-polls its requests, writes a save into the workflow file atomically after the hash check of a conflict, runs with fresh snapshots through `Client.submit`, sends the file and the git state of the trees every `EDIT_WATCH_S` when they changed, closes the session on Ctrl-C.
- **`snapshot.py`** the copy of the files git knows of a working tree into `snapshots/<name>/` of the store (`scan`, then `assemble`; `upload` for the routes `/snapshots/{name}`), hard links to the last snapshot by the hashes of its manifest `<dir>.files.json`, read-only, named by content, with provenance; **`environment.py`** the lock of a run (`uv lock` in a writable shadow tree against the snapshots) with the build backends at the pins of `build_constraints.txt`, the Dockerfile of the image, named by the hash of the lock, the python and the simrun version.
- **`planner.py`** the pod shape (processes the cores and memory of the most common node allow, the retry shape with `RETRY_FACTOR`) and the number of pods (`fastest`: min of `MAX_PIECES` 8, slots, units over processes, and with the learned seconds of a unit the work over `pod_start_s` of `ClusterConfig`), the six priority classes (`PRIORITY_VALUES`); **`resources.py`** learned resources in SQLite (95th percentile of the last 20 observations times 1.1, floor after a kill for memory), keyed by type, model and `cost_setting`.
- **`ledger.py`** the unit ledger (`pending/`, `claimed/<i>@<worker>`, `done/`, heartbeats), one `os.rename` per transition (NFS >= 4.1); **`worker.py`** the harness: `simrun-worker` (coordinator `SIMRUN_ROLE=base` waits for the whole ledger and releases stale claims, extra workers exit when nothing is pending, a pool of processes, memory sampling of the cgroup, reports), `simrun-merge`, `simrun-exec` (exit codes 0, 3, 4, 137, 143); **`cgroup.py`** the limits and usage of the cgroup v2.
- **`store.py`** the run record (`RunRecord`, `RunState`, `TaskState`) and the layout `runs/<id>/` (`run.json`, `tasks/<task>/{settings.json,provenance.json,out/,units/,workers/,logs/,progress.jsonl}`); **`config.py`** `ClusterConfig`; **`podspec.py`** what every backend shares (image, shapes, command, environment `SIMRUN_*`, `SIMRUN_PROVENANCE` the `provenance.json` of the task); **`argo.py`** the Argo workflow of a run (Hera: `<task>-w0`, `<task>-extra`, `<task>-merge`, retry strategy and `podSpecPatch` for the retry memory, Kueue labels); **`cwl.py`** the same run as CWL; **`testing.py`** `FakeBackend`, the backend in memory of the tests.
- **`service/`** `app.py` the FastAPI app (`ServiceSettings` from `SIMRUN_CONFIG`, `uvicorn --factory simrun.service.app:serve`, every route needs a bearer token), `auth.py` identities and priorities, `backend.py` the backend protocol, `argo_backend.py` Argo (a thread watches the workflows and wakes the controller), Kueue, node capacity, reservations (taint and label `simrun/reserved`) and BuildKit jobs, `local_backend.py` the `LocalBackend` (the tasks of a run as processes, cores shared across runs, `--docker` for `command` tasks, retries of a stop, `SIMRUN_CORES`, `SIMRUN_MEMORY_LIMIT_GB` and `SIMRUN_MEMORY_SOURCE=process`, the coordinator of a split task merges, so no merge pod and one pod per split task (`extra_workers`), pid files beside the logs and `kill_orphans`), `local_env.py` the venv of a local run with snapshots in `.simrun/envs/<run>`, `submission.py` the background submission (environment, image, workflow), `controller.py` the stateless reconciliation (task states, OOM evidence, observations, logs, release, cancel), `index.py` the SQLite index of the runs rebuilt from the store, `logs.py` the kept logs, `compare.py` the comparison of runs, `edits.py` the edit sessions of the Editor (`EditSession`, the requests of the browser and the results of the command in `edits/<id>/` of the store, connected while polled within `EDIT_CONNECTED_S`); the read models of the web app (`/me`, `/servers`, the workers of a task, `/task-types`, `/workflows/yaml`), the routes of the edit sessions (`/edits`, a save or run waits up to `EDIT_WAIT_S` for the command) and the event streams (`/events`, `/runs/{id}/events`), and the built app at `/ui/` from the setting `web`.
- **`frontend/`** the web app (Vite, Vue 3, TypeScript, Vue Flow, Tailwind v4), served at `/ui/` with hash routing, talking only to the API with the token of the login: `src/api/` the client, the event streams read with `fetch` and the types generated from `openapi.json`, `src/views/` the login, the Executions (runs, run bar, canvas, servers strip, inspector) and the Editor (sessions, palette, canvas, inspector with the settings form of the JSON Schema, bar; the draft in `src/stores/edit.ts`), `mockup/` the approved mockup; the service image has the build at `/opt/simrun/web` (`docs/web-app.md`).
- **`deploy/`** `cluster/` the kustomization of the cluster (Kueue v0.20.5 and Argo v4.1.6 release manifests committed unchanged, priorities, queue, NFS store, Zot, BuildKit, service), `k3d/` the overlay of the tests, `service/Dockerfile` the service image (with the built web app). **`tests/cluster/`** the k3d tests (`k3d_cluster.py` creates the cluster, `.tools/` holds k3d, kubectl, the kubeconfig and the store).

## Conventions

- Type checking with ty, `error-on-warning = true`, zero diagnostics; suppress with a rule-specific `# ty: ignore[rule-name]`, never `# type: ignore`.
- Every module, class and function of the package carries type annotations and a google style docstring (ruff `D`; `tests/` exempt). Modules log with `logging.getLogger(__name__)` and lazy `%s` formatting (ruff `G`), the CLI prints.
- The store is the source of truth: a record is written first, the index after it; the controller keeps no state in memory, and no call of the backend is made while the record lock is held.
- A unit output depends on the settings and the index only and is written atomically (temporary file, rename); the result of a split task does not depend on the number of pods.
- The release manifests of Argo and Kueue in `deploy/cluster` are committed as released and excluded from pre-commit; never edit them by hand.
- Markdown carries no hard line wraps. Release notes go in `release-notes/` as part of a release and are included in `docs/release-notes.md`.
- `uv.lock` is committed; the plain install job, tox and the `uvx` tools of the release do not use it. Dependabot moves the lock monthly (`build`) and the actions (`ci`); the revisions of the ruff and ty hooks follow the lock by hand.
