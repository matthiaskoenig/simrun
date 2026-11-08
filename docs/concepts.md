# Concepts

This page explains how `simrun` turns a submitted workflow into pods on the cluster and what it records. The components are k3s on every server, Argo Workflows, which runs a run as a workflow of pods, Kueue, which admits the pods by cores, memory and GPUs and preempts lower priorities, a run store on NFS, which the service, the pods and the development server of the agents share, the registry Zot and BuildKit, which builds the images of the environments in the cluster.

```text
  people (CLI)          agents (MCP)              dev server (working trees, /mnt/cluster)
        \                    |                              |
         +-------> simrun service (FastAPI) <---------------+  snapshots of the working trees
                       |            |
     learned resources |            | submit, follow
                       |            v
          k3s: Argo Workflows (execution, retries) + Kueue (admission, priorities, preemption)
                       |
     servers: pods of the tasks, image from the registry, snapshots and run store on NFS, scratch on the local disk
```

## Runs

A run is a submitted workflow. `POST /runs` validates the workflow, resolves the resources of its tasks, plans their pods on the nodes, writes the record of the run in the state `submitting` and returns its id at once; an invalid workflow, a task which fits on no node or a reserved node which is taken is refused right away. The environment of the run is resolved, its image built and its Argo workflow submitted in the background; the run is `queued` then, `running` once a pod runs, and ends `succeeded`, `failed` or `cancelled`.

A run lives in `runs/<id>/` of the store:

| path | content |
| --- | --- |
| `run.json` | the record: the workflow, the identity of the submitter, the plans, the provenance of the sources, the image, the states of the tasks and the out of memory kills |
| `environment.lock` | the lock of the environment of the run |
| `tasks/<task>/settings.json` | the settings of the task as submitted |
| `tasks/<task>/provenance.json` | the provenance of the sources of the run, `SIMRUN_PROVENANCE` in the pods of the task |
| `tasks/<task>/out/` | the outputs: a directory per output port and `summary.json`, the summary of the task, which `compare` reads |
| `tasks/<task>/units/` | the ledger of a split task |
| `tasks/<task>/workers/` | the reports of the pods: memory, units and their seconds |
| `tasks/<task>/logs/` | the logs of the pods, kept once the task is final |
| `tasks/<task>/progress.jsonl` | the progress of the task, one line per completed unit |

The store is the source of truth. The service keeps an index of the runs in SQLite beside it (id, owner, agent, group, workflow, state, times, pending release and cancellation, snapshots and summary), rebuilt from the records at its start, so a restarted service continues where the one before stopped. The state of a run is the one of the service, not the one of Argo: a split task is done when its merge succeeded (its coordinator, which merges), an unsplit task when its pod succeeded; when every task is done the run succeeded, when a task failed the run failed, and in both cases the service stops the workflow.

A run belongs to a group, e.g. a study or the subagents of one main agent, which lists and compares the runs of the group (`simrun list --group`, `simrun compare`).

## Tasks and ports

A task has a type, settings and inputs. The type comes from a [plugin](plugins.md) and declares the settings as a pydantic model and its input and output ports. An output port `<port>` of a task is the directory `tasks/<task>/out/<port>`; a downstream task reads it as the input `<task>.<port>`. The tasks run in the order of their inputs, a cycle is refused. A setting which the settings model of its type does not have is an error of the validation and of the submission, so a misspelled setting never falls back to its default silently.

## Splitting into units: the ledger

A task type with `count_units`, `compute_unit` and `merge` is split. The service counts the units of the task from its settings (e.g. the repeats of a fit or the points of a scan) when it plans the run. The units are claimed from a ledger, a directory `units/` of the task with `pending/<i>`, `claimed/<i>@<worker>` and `done/<i>`: every transition is one `os.rename`, atomic on a local file system and on NFS v4.1 or newer, so a unit is held by at most one worker. A worker claims a unit, computes it in a process of its local pool and marks it done; it touches `heartbeats/<worker>`, so the claims of a worker which died without releasing them are found by the age of its heartbeat.

A unit may be computed more than once: by a worker which is evicted before it completed the unit, or by a restarted pod whose earlier incarnation still runs. So `compute_unit` writes outputs which depend on the settings and the index only, each written atomically. The merge combines the units once all of them are done; the result does not depend on the number of pods the task ran on.

## Coordinator and extra workers

A split task of `pods` pods is three nodes of its Argo DAG:

- `<task>-w0`, the coordinator, a worker which claims units like the others and returns only once the ledger is complete and then merges the units. It releases the claims of workers whose heartbeat is older than the grace period plus a minute and computes them itself.
- `<task>-extra`, the extra workers `<task>-w1` to `<task>-w<pods-1>`, which exit when nothing is pending. An extra worker which fails does not fail the run.

The coordinator merges the units itself once the ledger is complete (`SIMRUN_MERGE=1`), so extra workers still waiting for admission never hold up the merge. A run recorded before that has a node `<task>-merge` running `simrun-merge` after the coordinator only.

An unsplit task is one pod which runs the command of its type through `simrun-exec`.

## Split modes

`fastest` (the default) splits a task into as many pods as there are units and slots on the nodes, at most eight (`MAX_PIECES`); each pod takes the processes its node allows by cores and memory. `single-server` places one pod on one server. The mode is set for the workflow, for a task or for the submission (`--single-server`). Extra pods are not limited by the idle capacity: Kueue queues them until a node is free.

## Priorities and preemption

The identity of the token of a submission decides the priority of its run:

| priority | who | base | extra |
| --- | --- | --- | --- |
| `owner` | the owner of the cluster and the owner's agents | 1000 | 900 |
| `standard` | the other people | 500 | 400 |
| `agent` | the agents of the other people | 200 | 100 |

Every priority is a Kueue `WorkloadPriorityClass` and a Kubernetes `PriorityClass` of the same name and value (`simrun-<priority>-base`, `simrun-<priority>-extra`). The coordinator (with its merge) and an unsplit task have the base priority of the run, the extra workers the extra priority. Kueue admits the pods and preempts workloads of a lower priority; the scheduler itself never preempts.

## Take-back by preemption

Since the extra workers have a lower priority than the base pods of their own level, a new run takes back the extra workers of the running runs of the same level: the pods of the new run preempt them, and every running run keeps its coordinator. A preempted worker stops on SIGTERM within the grace period, releases its claims and writes its report; Argo retries the pod, which waits for admission again. The units it gave back are computed by the coordinator or by another worker, so splitting is elastic and the results do not change. Workloads of equal priority never preempt each other, so the extra workers of two runs of one priority wait for each other's end.

## Learned resources

A pod requests `base + per_worker x processes` of memory, with the limit equal to the request. Every worker (and every unsplit plugin task) writes a report with the largest sample of the anonymous memory of its cgroup, sampled every second, and the seconds of the units it completed. A request is resolved from the last 20 observations of its key (the task type, the model and the value of the `cost_setting` of the type): the 95th percentile of the measured memory per process times 1.1, and at least 1.5 times the memory per process of a pod killed for memory, until a later run of the key succeeded. Without observations the resource hint of the type is used. `simrun estimate` gives a split task the median seconds of a unit times the rounds of its units over the processes of its pods.

A pod killed for memory (the evidence of the out of memory killer: the reason `OOMKilled`, or a report counting kills in its cgroup) is retried once with 1.5 times the memory per process; a second kill fails the run with a message which names the retry memory. Exit codes of a command: 0 done, 137 (or killed with SIGKILL) retried with the retry memory, 143 stopped on SIGTERM and retried, any other a failure, not retried.

## Snapshots and environments

A source is a git working tree, the model first. The client copies the files git knows of it (tracked and untracked, not ignored, uncommitted changes included) into `snapshots/<name>/` of the store and sends its provenance (branch, commit, changed files); files unchanged since the last snapshot of the same name are hard links to it, so a snapshot of an unchanged tree costs nearly nothing. A snapshot is named by its content and its files are read-only. The service never reads the disk of the client.

The image of a run whose model has a `pyproject.toml` holds the third party dependencies of the model and of its snapshotted libraries, locked with `uv lock` against the snapshots, and their build backends at the versions of the constraints file of the service, so a new release of a backend changes no lock and rebuilds no image. The code comes from the snapshots, which a pod mounts read-only and installs editable at its start without network access. The image is named by the hash of the lock, the python version and the simrun version and built with BuildKit only when the registry does not have it, so a change of the code never rebuilds an image and a change of a dependency does. A run whose model has no `pyproject.toml` runs in the `default_image` of the service.

## Reservations

A node can be reserved for one run, e.g. for a benchmark (`reserve:` of the workflow or `simrun submit --reserve <node>`). The service taints the node with `simrun/reserved=<run id>:NoExecute`, which evicts the pods of other runs, and labels it `simrun/reserved=<run id>`, which the pods of the run select and tolerate. The planner leaves a reserved node out for every other run, and the reservation is released when the run ends.
