# simrun

Runner of simulation workflows on a k3s cluster with Argo Workflows and Kueue.

A workflow is a YAML file of tasks which read the outputs of other tasks. People and
agents submit it with the working trees of their model and libraries; the service
snapshots nothing itself, the client writes the snapshots into the run store, which
the service, the pods and the clients share (NFS).

## The command line

```bash
export SIMRUN_URL=http://simrun.cluster.internal   # the service
export SIMRUN_TOKEN=...                             # a token of the tokens file
export SIMRUN_STORE=/mnt/cluster                    # the run store as mounted here

simrun submit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim \
    --group hctz
simrun wait <run>                 # exit 0 succeeded, 1 failed or cancelled, 2 timeout
simrun status <run>               # the tasks and their progress
simrun list --owner mk --state running
simrun logs <run> <task>          # the last 300 lines of every pod, --tail 0 for all
simrun estimate fit.yaml --source model=~/git/pkdb_models
simrun compare <run> <run>        # provenance and the summaries of the tasks
simrun cancel <run>
simrun cleanup --older-than 30 --dry-run
simrun usage
```

A source is `NAME=PATH` of a git working tree, the model first: the client copies
the working tree (with its uncommitted changes) into `snapshots/<name>/` of the store
and sends its provenance. `--single-server` places the pieces of every task on one
server, `--reserve <node>` reserves a node for the run. Every command takes `--json`;
a refusal of the service is printed to stderr with exit 1. `SIMRUN_URL`,
`SIMRUN_TOKEN` and `SIMRUN_STORE` are read by the CLI and the MCP server alike.

## The MCP server

`simrun-mcp` is an MCP server on stdio for an agent, with the tools `submit_workflow`,
`wait_for_run`, `run_status`, `run_progress`, `list_runs`, `compare_runs`,
`cancel_run`, `run_logs` (`tail_lines`, 300 by default, 0 for all), `run_outputs` and
`estimate_workflow`. Paths in the answers are relative to the store, and every answer
carries `store`, the local mount of the store. Register it with the environment of
the client, for Claude Code e.g.

```bash
claude mcp add simrun --env SIMRUN_URL=http://simrun.cluster.internal \
    --env SIMRUN_TOKEN=<token of the agent> --env SIMRUN_STORE=/mnt/cluster \
    -- uv run --project ~/git/simrun simrun-mcp
```

The token of an agent names its owner; the runs of an agent of the owner of the
cluster get the priority `owner`, the runs of other agents `agent`.

## Workflows

```yaml
name: hctz-fit
split: fastest            # or single-server: the pieces of a task on one server
reserve: null             # a node reserved for the run
tasks:
  - id: scan              # [a-z][a-z0-9-]*
    type: sbmlsim.scan    # a task type of a plugin
    settings:             # validated by the settings model of the type
      n: 1000
    resources:            # optional, replaces the learned resources
      memory_per_worker_gb: 4.0
      scratch_gb: 10
      gpus: 0
  - id: report
    type: sbmlsim.report
    inputs:
      results: scan.results   # the output port `results` of the task `scan`
    stage: [results]          # copied to the local scratch before the task starts
    split: single-server      # the split mode of this task
```

The tasks run in the order of their inputs (a cycle is refused). A setting which the
settings model of its type does not have is an error of `/validate` and of the
submission, so a misspelled setting never falls back to its default silently; the
service writes the settings as submitted to `tasks/<task>/settings.json`, and the
pods validate them with the plugin of the snapshot. The built-in type `command` runs a
command in an image of its own:

```yaml
  - id: plot
    type: command
    settings: {image: "registry.cluster.internal:5000/tools:1", args: [plot, --all],
               cores: 2, memory_gb: 4.0}
```

A run lives in `runs/<id>/` of the store: `run.json`, the record; `environment.lock`;
`tasks/<task>/` with `settings.json`, `out/` (the outputs: a directory per output
port, and `summary.json`, which `compare` reads), `workers/` (the reports of the
pods), `logs/` (the logs of the pods, once the task is final) and `progress.jsonl`.

## Writing a plugin

A plugin is a package with entry points of the group `simrun.task_types`, each a
`simrun.plugins.TaskType`:

```toml
[project.entry-points."simrun.task_types"]
"sbmlsim.scan" = "sbmlsim.simrun:SCAN"
```

```python
SCAN = TaskType(
    name="sbmlsim.scan",
    settings=ScanSettings,  # a pydantic model
    inputs=("model",),  # input ports
    outputs=("results",),  # output ports
    hint=ResourceHint(memory_base_gb=2.0, memory_per_worker_gb=3.0),
    count_units="sbmlsim.simrun:count_units",  # settings -> number of units
    setup="sbmlsim.simrun:setup",  # settings, paths; once per process
    compute_unit="sbmlsim.simrun:compute_unit",  # settings, index, paths
    merge="sbmlsim.simrun:merge",  # settings, paths; after all units
    cost_setting="subset",  # resources learned per subset
)
```

The contract:

- A type with `count_units`, `compute_unit` and `merge` is split: its units are
  claimed from a ledger by the processes of its pods (`simrun-worker`), and the merge
  (`simrun-merge`) runs once every unit is done. Other types run their `command`, an
  argv template with `{settings}` (the path of `settings.json`) and `{out}`, through
  `simrun-exec`; the command validates the settings itself with the settings model.
  An unsplit type is sized as one process unless it sets `uses_cores`.
- A unit may be computed more than once (a worker evicted before it completed the
  unit, a restarted pod whose earlier incarnation still runs), so `compute_unit`
  writes outputs which depend on the settings and the index only (deterministic), each
  written atomically: a temporary file in the same directory, then renamed.
- An output port `<port>` of a task is the directory `out/<port>` (`paths.out /
  port`); a downstream task reads it as the input `<task>.<port>`
  (`paths.inputs[<input>]`). `out/summary.json` is the summary of the task, no port.
- Exit codes of a command: 0 done, 137 (or killed with SIGKILL, e.g. by the out of
  memory killer) retried once with 1.5 times the memory per process, any other a
  failure, not retried.
- The service imports the plugins it knows: it validates the settings with their
  models and counts the units of a split type (`count_units`) when it plans a run.
  A task type which only exists in a snapshot (a plugin in a working tree) is unknown
  to the service: the service image must carry every plugin whose task types are
  submitted, and a new task type, a split one in particular, needs a rebuilt service
  image before its first run (see "The service image"). The pods run the plugin of
  the snapshot.
- `cost_setting` names the setting which changes the cost of a task (e.g. the
  subset of the data of a fit): the resources are learned per value of it.

## Service settings

The service reads a YAML file named by `SIMRUN_CONFIG`
(`uvicorn --factory simrun.service.app:serve`), a `ServiceSettings`:

| setting | meaning |
| --- | --- |
| `store` | the root of the run store, `/store` in the pod |
| `db` | the SQLite file of the learned resources and the index of the runs, on a local volume |
| `tokens` | the YAML file of the tokens: `<token>: {name, kind: person or agent, owner}` |
| `owner` | the person who owns the cluster: their runs and their agents' get the top priority |
| `cluster` | the cluster, a `ClusterConfig` (below) |
| `poll_s` | seconds between two reconciliations of the controller (5) |
| `default_image` | the image of a run whose model has no `pyproject.toml` |
| `simrun_wheel` | the simrun wheel the images of the environments install |
| `python` | the python of the images of the environments (3.14) |
| `backend` | `module:callable` of the backend, `simrun.service.argo_backend:backend` |

`cluster` (`ClusterConfig`): `registry` (the registry inside the network, as the
nodes and BuildKit name it), `registry_url` (its API as the service reaches it),
`base_image` (the base image of the images of the environments, uv and the python of
the service, a repository of the registry by default
`astral-sh/uv:0.13.7-python3.14-trixie-slim@sha256:...`, which the registry mirrors
with its digest), `namespace`, `local_queue`, `store_claim`, `store_mount`,
`scratch_path`, `service_account`, `buildkit`, `buildctl_image`, `grace_s` (the
seconds a pod has after SIGTERM) and `ttl_after_finished_s`.

The service keeps an index of the runs in SQLite (id, owner, agent, group, workflow,
state, times, pending release and cancellation, snapshots and summary), rebuilt from
the records at its start: the controller follows only the runs which have not ended
(or still have to release a node or cancel a workflow), and `GET /runs`, cleanups and
the usage filter in the index. The store stays the source of truth.

## The service image

`deploy/service/Dockerfile` is the image `simrun-service` of
`deploy/cluster/simrun.yaml`: uv (the service resolves the environments of the runs),
simrun with the dependencies of its lock, its wheel at `/opt/simrun/` (the
`simrun_wheel` of the settings) and the plugins. Build it from a context of the
requirements of the lock, the wheel and the wheels of the plugins:

```bash
mkdir -p build/service/plugins
uv build --wheel --out-dir build/service
uv export --frozen --no-dev --no-emit-project --no-header --no-annotate \
    > build/service/requirements.txt
cp ~/git/sbmlsim/dist/sbmlsim-*.whl build/service/plugins/   # the plugins
cp deploy/service/Dockerfile build/service/
docker build -t registry.cluster.internal:5000/simrun-service:0.1.0 \
    --build-arg WHEEL=simrun-0.1.0-py3-none-any.whl build/service
docker push registry.cluster.internal:5000/simrun-service:0.1.0
```

The base (`--build-arg BASE`) is the base image of the environments in the registry.
The integration tests build the image once (`tests/cluster/test_service_image.py`)
with a plugin and run the API from it.

## Deployment

`deploy/cluster` is the cluster as a kustomization: Kueue v0.20.5 and Argo Workflows
v4.1.6 (the release manifests, committed unchanged, so a setup needs no download),
the six priority classes of simrun (each a Kueue `WorkloadPriorityClass` and a
Kubernetes `PriorityClass` of the same value), the queue (a `Topology` by host, a
`ResourceFlavor` over the nodes labelled `simrun/worker=true`, the `ClusterQueue`
`simrun` preempting lower priorities, the `LocalQueue` `simrun`), the service account
of the workflows, the run store on NFS (set the export in
`deploy/cluster/store-settings.yaml`), Zot, BuildKit and the service. Apply it with
`kubectl apply --server-side -k deploy/cluster`; the resources of Kueue are accepted
once its CRDs and its webhook are up, so the first apply may have to be repeated.

The infrastructure (the controllers, BuildKit and its build Jobs, Zot, the service)
has a node affinity off the nodes labelled `simrun/worker=true`, whose resources Kueue
and the planner count as free for the runs.

`deploy/k3d` is the overlay of the integration tests.

## Submissions

`POST /runs` validates and plans a run, writes its record in the state `submitting`
and returns its id at once (an invalid workflow, a task which fits no node or a
reserved node which is taken is refused at once). The environment is resolved, the
image built and the workflow submitted in the background, in a pool of the
controller; a failure lands in the record (the run fails with the message). Two
submissions of the same new environment build its image once. The grace after which
the controller takes a submission which does not run in the service any more as lost
(120 s) counts from the end of the build (`built` of the record); a run submitting
when the service stopped fails at its next start. The client, the CLI and the MCP
show the state `submitting` until the run is queued.

## Environments

The image of a run whose model has a `pyproject.toml` holds the third party
dependencies of the model and its snapshotted libraries, locked with `uv lock`
against the snapshots, and their build backends: `[build-system].requires` of every
source (the legacy backend of setuptools without one) and what the backend asks for
an editable build (`get_requires_for_build_editable`, e.g. `editables` of
hatchling), asked at resolve time. The code comes from the snapshots, which a pod
mounts read-only and installs at its start with `uv pip install --no-deps
--no-index --no-build-isolation -e`, each from a writable tree of links to its files
(a backend may write a version file or metadata into the tree it builds). The image
is named by the hash of the lock, the python version and the simrun version, and
built only when the registry does not have it.

## Pods and their harness

A split task runs `simrun-worker` in its pods and `simrun-merge` in its merge; an
unsplit task of a plugin type runs its command through `simrun-exec`, which installs
the snapshots of the run, stages the inputs, sets `OMP_NUM_THREADS`,
`OPENBLAS_NUM_THREADS` and `MKL_NUM_THREADS` to 1, passes a SIGTERM on to the
command and writes a report of its memory, by the task id. Its exit codes: 0 done,
3 the command failed (not retried), 4 a configuration error, 137 the command was
killed (retried with the retry memory), 143 stopped on SIGTERM (retried). An unsplit
plugin task is sized as one process (one core, `base + per_worker`), unless its type
sets `uses_cores`, then it takes the processes a node allows like a worker. A task of
the built-in type `command` runs its argv as given, in its own image.

## Learned resources

A pod requests `base + per_worker x processes` of memory, limit equal to request.
Every worker (and every unsplit plugin task, see below) writes a report with the
largest sample of the anonymous memory of its cgroup (`memory.stat` `anon`, sampled
every second by its heartbeat thread); `memory.peak` is not used, since it counts
the page cache of staging, installing and writing outputs, which the kernel
reclaims at the limit instead of killing. A worker counts as out of memory only
with the evidence of the out of memory killer: its container `OOMKilled`, or its
report counting kills of the out of memory killer in its cgroup (`memory.events`
`oom_kill`). A request is resolved from the last 20 observations of its key (task
type, model, the value of the `cost_setting` of the type): the 95th percentile of
the measured memory per process times 1.1, and at least 1.5 times the memory per
process of a pod killed for memory, until a later run of the key succeeded.

A report also has the seconds of the units the worker completed, each measured in the
process which computed it; an observation is the time of one unit in one process,
without the waits of a coordinator for the units of other workers. `/estimate` gives
a split task the median of it times the rounds of its units over the processes of
its pods, `ceil(units / (pods x processes))`.

## Integration tests on k3d

```bash
SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster
SIMRUN_K3D=1 SIMRUN_K3D_KEEP=1 uv run pytest -m cluster tests/cluster  # keep the cluster
```

They need docker, about 20 GiB of free disk and the network (the images of k3s,
Kueue, Argo, BuildKit, `python:3.14-slim-trixie` and the uv base image). The session
downloads k3d and kubectl into `.tools/bin`, creates the k3d cluster `simrun-test`
(one server, with the infrastructure; three agents of 6 GiB labelled
`simrun/worker=true`) with the registry `k3d-simrun-registry:5000` (`localhost:5000`
from the host), writes its kubeconfig to `.tools/kubeconfig` (the contexts of the
user are not touched), applies `deploy/k3d`, builds the image of the runs without
sources from the working tree (`tests/cluster/Dockerfile`) and pushes it, and copies
the base image of the images of environments (`ClusterConfig.base_image`, from
`ghcr.io/astral-sh/uv`) into the registry with its digest.
Each test runs the service in its process with an `ArgoBackend` and the store
`.tools/k3d/store`, which k3d mounts into every node. `test_sources.py` runs the path
of a model with sources end to end: a model repository (hatchling, `uv.lock`) with
task types of its own and a library (setuptools, a dependency, an uncommitted change)
are snapshotted, the service resolves their environment and builds its image from
the generated Dockerfile with BuildKit after `POST /runs` returned, the pods install
the read-only snapshots, a split task and an unsplit task through `simrun-exec` run,
and the merged result, the report and the learned resources are checked. At the end
the cluster, the registry, the store and the images are deleted (the files the pods
of an environment wrote as root are given back to the user first), unless
`SIMRUN_K3D_KEEP=1`; a kept cluster is used by the next session.
`test_capacity.py` places a DaemonSet with requests on the agents and checks that the
nodes are planned without them, `test_service_image.py` builds the image of the
service.

The unit tests run on python 3.14 and 3.15 with tox (`tox r`, `tox r -e py3.15`),
without the tests on the cluster.
The tests run in one worker of pytest-xdist and take about nine minutes on a
created cluster.

## Findings on the cluster

The integration tests verified the design on k3s v1.35.5 (k3d v5.9.0) with Kueue
v0.20.5 and Argo Workflows v4.1.6; what differed from the assumptions is in the
manifests and the code:

- **Plain pods of Argo are Kueue workloads.** The queue label of a template reaches
  the pod; Kueue gates it, creates a workload per pod and admits it. An Argo node whose
  pod is gated is `Pending` (its `Retry` node too), so the backend reports it queued.
- **Preemption and the retries of Argo.** Kueue preempts a workload of a lower
  priority (`PreemptedWorkload`) by deleting its pod, within the grace period: the
  worker stops on SIGTERM, releases its claims and writes its report. Argo marks the
  attempt `Error` with the message `pod deleted` and no exit code
  (`lastRetry.exitCode` is `"-1"`), so the retry strategy retries it; the new pod is
  gated again and waits. The backend reports such a node `Preempted` until its next
  pod runs. Workloads of equal priority never preempt each other, so the extra
  workers of two runs of one priority wait for each other's end. A pod preempted
  before Argo saw it running is created again by Argo as the same attempt, without a
  retry, so such a node is reported queued and not `Preempted`; the take-back test
  checks the `PreemptedWorkload` events of Kueue instead.
- **Topology aware scheduling.** A `ResourceFlavor` with a topology needs node labels
  (`at least one nodeLabel is required when topology is set`), hence the label
  `simrun/worker=true` of the nodes of the runs, which `ArgoBackend.nodes` lists too.
  On admission Kueue adds the host and the flavor labels to the node selector of the
  pod; it honours the node selector and the toleration of a reserved run, and the
  taint `NoExecute` of a reservation keeps the pods of other runs off the node. Kueue
  preempts for the capacity of a node as well: with a quota far above the cluster a
  run of a higher priority still evicted a pod of a lower one from a full node.
- **The executor counts.** Kueue counts every container, so the init and wait
  containers of Argo request what they limit (`executor.resources` in the controller
  config). The init container copies the argoexec binary (about 150 MB) into the pod
  and with 128Mi it was killed for memory now and then (`init: OOMKilled`); it has
  256Mi. `ArgoBackend.nodes` subtracts one executor container from the allocatable
  resources and rounds down to whole cores and GiB, so a pod the planner sizes for a
  node fits it.
- **Out of memory.** The kernel kills the whole container (cgroup v2,
  `memory.oom.group`), so the worker writes no report; Argo v4 names the container in
  the message, `main: OOMKilled (exit code 137)`. A kill for memory needs this
  evidence of the out of memory killer (Ruling 39): the reason `OOMKilled`, or, for
  an attempt which exited 137 itself because its pool (the command of `simrun-exec`)
  lost a process (Ruling 7), its report, which counts the kills of the out of memory
  killer in its cgroup (`memory.events` `oom_kill`). An exit 137 without either is a
  pod killed after its grace period, an eviction. The `podSpecPatch` of the retry
  (`lastRetry.exitCode == '137'`) gives the next attempt the retry memory and memory
  per process, as designed. A kill for memory of an attempt with the retry memory
  after a first kill fails the run: the controller stops its workflow, with a message
  naming the retry memory (Ruling 35). The retry limit of a pod is 100
  (`RETRY_LIMIT`): every eviction by Kueue counts against it, and a pod of a long run
  of low priority on a busy cluster may be evicted many times.
- **The retry memory after a preemption (a gap).** Argo's expressions of a retry know
  only the last attempt (`lastRetry`), so an attempt after one which was preempted
  following a kill for memory gets the first memory again, not the retry memory. The
  controller does not count a kill of such an attempt toward the limit of Ruling 35
  (it counts a kill only when the attempt before it exited 137); the attempt after it
  has the retry memory again.
- **Stopping a workflow.** After `shutdown: Stop` Argo fails the nodes still pending
  (`workflow shutdown with strategy: Stop`); the backend deletes their gated pods.
- **Logs.** The logs of an attempt whose pod Kueue deleted are gone with the pod.
  The controller keeps the logs of the main containers of the pods of a task which
  still exist in `tasks/<task>/logs/` of the run store once the task is final, before
  it stops or cancels the workflow; `GET /runs/{id}/logs/{task}` serves them, and the
  logs of the live pods before.
- **Priority classes.** The pods name a Kubernetes `PriorityClass`, which must exist;
  it has `preemptionPolicy: Never`, so only Kueue preempts.
- **Registry and builds.** The nodes and BuildKit name the registry as
  `ClusterConfig.registry`, the service reaches its API at
  `ClusterConfig.registry_url`. BuildKit runs rootless (seccomp and AppArmor
  unconfined, `--oci-worker-no-process-sandbox`); a build is a Job of `buildctl` whose
  context is a ConfigMap (at most 1 MB: the Dockerfile and the simrun wheel). The
  service follows the Job by reading it, not its `jobs/status` subresource, for which
  its Role has no right; a unit test checks the Role of `deploy/cluster/simrun.yaml`
  against every call of the backend, since the cluster tests run with the admin
  kubeconfig.
