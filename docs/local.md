# Local mode

The local mode runs a workflow on this machine, without a cluster, a registry or a mounted store. It is the same service, planner, harness, records and [web app](web-app.md) as on the cluster, with a backend which runs the tasks as processes. The time of a run is the time of its processes plus a fraction of a second per task.

```bash
pip install simrun
simrun submit fit.yaml --source model=. --local
simrun outputs <run> <task> --to results
```

## The switch

`--local` on a command makes it talk to the local service of the current directory instead of `SIMRUN_URL`; no environment variable is needed. It is accepted by `submit`, `estimate`, `wait`, `status`, `list`, `logs`, `outputs`, `cancel`, `compare`, `cleanup`, `usage` and `edit`. Without `--local` the same command runs on the cluster, see [Command line](command-line.md).

`submit --local` waits for the run by default and prints a live line per task on a terminal and plain lines otherwise; it exits like `wait` (0 if the run succeeded, 1 if it failed or was cancelled, 2 on `--timeout`). `--no-wait` returns at once with the run, as on the cluster.

## The service of a project

The local service keeps everything in `.simrun/` of the current directory, like `.venv`: `store/` (`runs/`, `snapshots/`), `scratch/`, the index `simrun.sqlite`, the generated tokens `tokens.yaml`, `service.json`, `service.log` and a `.gitignore` of everything, so none of it lands in git. It listens on `127.0.0.1` on a free port only.

A `--local` command starts the service in the background when none runs, waits for its `service.json` and uses it. The service exits after 30 minutes without a request and without a live run, and a stopped service is started again by the next command. `simrun serve --local` runs the service in the foreground instead and prints its address; Ctrl-C, SIGTERM or SIGHUP cancels the runs and waits for their processes, so no process of a run outlives the service. One service runs per directory. See [serve](command-line.md#serve) for the file `service.json` and the check of the instance before the token is sent.

## The web app

The service serves the web app at `/ui/`: the Executions with live updates, and the Editor (`simrun edit --local`). The wheel ships the built app (`simrun/web`); a checkout serves `frontend/dist` once it is built, see [Development](development.md). The app asks for a token: the one in `.simrun/service.json`. The server strip shows this machine as the one server.

## How tasks run

The `LocalBackend` of the service runs the pods of a run as processes of this machine:

- a split task is one coordinator process (`simrun-worker`) with as many processes as the machine has cores, which computes the units and merges them itself; there are no extra workers
- a task of a plugin type runs through `simrun-exec`
- a `command` task runs its args directly by default, or in its image with `docker run` when the run is submitted with `--docker`; the run records which. The store and the scratch directory are mounted at the same paths
- a task starts when the tasks it reads from succeeded and its cores are free; the cores are shared by all runs of the service, in the order of submission, and a waiting task is `queued`
- a process stopped from outside (no exit code, 137 or 143) is retried as on the cluster, a kill for memory with more memory. The harness limits its own memory where there is no cgroup

The backend sets `SIMRUN_CORES` (the cores of the worker) and `SIMRUN_MEMORY_LIMIT_GB` (its memory, which the harness enforces itself without a cgroup) in the environment of a process. The scratch directories are in `.simrun/scratch/`, the logs of every process are in `.simrun/store/runs/<run>/tasks/<task>/logs/`, and `simrun logs` reads them as it does on the cluster.

## Sources

Locally the sources are recorded, not copied: the provenance (branch, commit, uncommitted files, the hash of the diff) is stored as on the cluster, and the processes run with the python environment of the command, so the code is the working tree as installed in it, e.g. editable. Nothing is snapshotted or installed, which keeps a run as fast as running the code by hand.

`--snapshot` on `submit` and `edit` copies the working trees into `snapshots/` and installs them in the environment of the run as the cluster does, when a run must not see later changes of the tree.

## What differs from the cluster

| | cluster | local |
| --- | --- | --- |
| the machines | the servers of the cluster | this machine |
| priorities and preemption | Kueue, six priority classes | none, the tasks run in the order of submission |
| reservations of nodes | yes | refused |
| images | built with BuildKit per environment | none; a `command` may use its image with `--docker` |
| sources | snapshots, installed in the image | provenance only, unless `--snapshot` |
| store | shared drive, or uploads through the API | `.simrun/store/` |
| extra workers of a split task | spread over idle servers | none, one worker with all cores |

The records, logs and outputs have the layout of a cluster run, see [Concepts](concepts.md).
