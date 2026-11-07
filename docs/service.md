# Service

The service is the API (FastAPI) which the command line, the MCP server and scripts call, and the controller behind it, which runs the submissions in the background and follows the workflows of the runs. It runs in the cluster as the Deployment `simrun` of `deploy/cluster/simrun.yaml`:

```bash
SIMRUN_CONFIG=/etc/simrun/settings.yaml uvicorn --factory simrun.service.app:serve --host 0.0.0.0 --port 8000 --timeout-graceful-shutdown 5
```

## Authentication

Every route needs a bearer token of the tokens file, a YAML mapping of token to identity:

```yaml title="tokens.yaml"
<token of a person>: {name: mk, kind: person}
<token of an agent>: {name: fit-agent-1, kind: agent, owner: mk}
```

The identity of the token is recorded with the run and decides its priority: the owner of the cluster and the owner's agents get `owner`, other people `standard`, their agents `agent`. The service has no documentation routes, which would answer without a token.

## Endpoints

| method and path | what it does |
| --- | --- |
| `POST /validate` | validate a workflow against the known task types; `{ok, errors}` |
| `POST /estimate` | the resources, the plan and the expected seconds of every task of a submission |
| `POST /runs` | submit a workflow with the provenance of its sources; returns the run in the state `submitting` (201) |
| `GET /runs` | the runs, filtered by `group`, `owner`, `agent` and `state` |
| `GET /runs/{run}` | the record of a run |
| `GET /me` | the identity of the token |
| `GET /servers` | the servers of the cluster with their cores, memory and reservation and the cores and memory the pods on them request |
| `GET /events` | server-sent events: `runs` (the runs, filtered as `GET /runs`, with owner and workflow) and `servers`, each when it changed |
| `GET /runs/{run}/events` | server-sent events: `run` (the record), `progress` and `workers` of a run, each when it changed; it ends after a run ended |
| `GET /runs/{run}/tasks/{task}/workers` | the workers of a task: server, state, attempts, processes, memory and units |
| `GET /runs/{run}/wait` | a long poll: the summary once the run ended or after `timeout` seconds (60, at most 300) |
| `GET /runs/{run}/progress` | the progress of every task: the counts of its ledger and its first and last progress lines |
| `GET /runs/{run}/logs/{task}` | the logs of the pods of a task as text, the last `tail_lines` of each (300, 0 for all) |
| `GET /runs/{run}/outputs/{task}` | the output files of a task, paths relative to the store |
| `POST /runs/{run}/cancel` | cancel a run |
| `DELETE /runs/{run}` | remove an ended run and the snapshots nothing refers to |
| `POST /cleanup` | remove the runs which ended more than `older_than_days` ago, of a `group` or only failed ones (`failed_only`), `dry_run` lists them |
| `GET /usage` | the bytes of the store per group and run |
| `POST /compare` | the provenance and the summaries of the tasks of runs side by side |

`simrun.client.Client` is the python client of these endpoints, which the command line and the MCP server use.

## Settings

The service reads a YAML file named by `SIMRUN_CONFIG`, a `simrun.service.app.ServiceSettings`:

| setting | meaning |
| --- | --- |
| `store` | the root of the run store, `/store` in the pod |
| `db` | the SQLite file of the learned resources and the index of the runs, on a local volume |
| `tokens` | the YAML file of the tokens, see above |
| `owner` | the person who owns the cluster: their runs and their agents' get the top priority |
| `cluster` | the cluster, a `ClusterConfig`, see below |
| `poll_s` | seconds between two reconciliations of the controller and between two checks of an event stream (5) |
| `events_max_s` | seconds after which an event stream ends and the client connects again (300) |
| `default_image` | the image of a run whose model has no `pyproject.toml` |
| `simrun_wheel` | the simrun wheel the images of the environments install |
| `python` | the python of the images of the environments (3.14) |
| `build_constraints` | the pinned versions of the build backends, `src/simrun/build_constraints.txt` by default |
| `backend` | `module:callable` of the backend, `simrun.service.argo_backend:backend` |
| `web` | the directory of the built [web app](web-app.md), served at `/ui/` without a token; no `/ui/` without it |

`cluster`, a `simrun.config.ClusterConfig`:

| setting | meaning |
| --- | --- |
| `registry` | the registry inside the network, as the nodes and BuildKit name it in image references |
| `registry_url` | the API of the registry as the service reaches it, `http://<registry>` by default |
| `base_image` | the base image of the images of the environments, uv and the python of the service; a repository of the registry by default (`astral-sh/uv:0.13.7-python3.14-trixie-slim@sha256:...`), which the registry mirrors with its digest |
| `namespace` | the namespace of the workflows and their pods (`simrun`) |
| `local_queue` | the Kueue LocalQueue of the pods (`simrun`) |
| `store_claim` | the PersistentVolumeClaim of the run store (`simrun-store`) |
| `store_mount` | the path of the run store in a pod (`/store`) |
| `scratch_path` | the local scratch, a hostPath on the local disk of a node (`/scratch`) |
| `service_account` | the service account of the pods of the workflows (`simrun-workflow`) |
| `buildkit` | the address of the BuildKit daemon (`tcp://buildkitd.simrun:1234`) |
| `buildctl_image` | the image of the job which sends a build to BuildKit |
| `grace_s` | the seconds a worker has to stop after SIGTERM (60) |
| `ttl_after_finished_s` | the seconds a finished workflow is kept (86400) |

To raise a pin of the build backends, edit `build_constraints.txt` (or point `build_constraints` at a file of your own) and redeploy the service; the images of the models which use the backend are built again.

## Environment variables

| variable | read by | meaning |
| --- | --- | --- |
| `SIMRUN_URL` | command line, MCP server | the address of the service |
| `SIMRUN_TOKEN` | command line, MCP server | the token of the person or the agent |
| `SIMRUN_STORE` | command line, MCP server | the run store as mounted on the machine of the client |
| `SIMRUN_CONFIG` | service | the YAML file of the `ServiceSettings` |
| `KUBECONFIG` | service | the cluster outside of a pod; in the pod the service account |
| `SIMRUN_TASK_DIR`, `SIMRUN_TASK_TYPE`, `SIMRUN_INPUTS`, `SIMRUN_STAGE`, `SIMRUN_EDITABLE`, `SIMRUN_SCRATCH`, `SIMRUN_PROVENANCE`, `SIMRUN_MEMORY_BASE_GB`, `SIMRUN_MEMORY_PER_WORKER_GB`, `SIMRUN_WORKER`, `SIMRUN_ROLE`, `SIMRUN_ATTEMPT`, `SIMRUN_STALE_S`, `SIMRUN_NODE` | pods | the environment of the harness of a pod, set by the workflow, see `simrun.podspec` and `simrun.worker` |
| `SIMRUN_K3D`, `SIMRUN_K3D_KEEP` | tests | run the tests on the k3d cluster and keep it, see [Testing](testing.md) |

## Submissions in the background

`POST /runs` validates and plans a run, writes its record in the state `submitting` and returns its id at once. The environment is resolved, the image built and the workflow submitted in the background, in a pool of the controller; a failure lands in the record (the run fails with the message). Two submissions of the same new environment build its image once. The service does not wait for the builds when it stops: a run submitting when the service stopped is resumed at its next start from its record (an image which was pushed is found, a reserved node kept, a workflow which was created taken as submitted).

## The controller

The controller reconciles every `poll_s` seconds: it reads the records of the live runs from the index, the states of their workflows from the backend, records the states of the tasks and the kills for memory, records the reports of the workers as resource observations once a task is final, keeps the logs of the pods in the store, and ends a run when every task is done or one failed. It keeps no state in memory, so a restarted service continues where the one before stopped. A release of a reserved node or a cancellation of a workflow which fails stays in the record and is retried at the next polls.
