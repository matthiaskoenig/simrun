# Command line

`simrun` is the command line of people and scripts: it submits, follows, compares, cancels and cleans up runs, and `simrun edit` opens a workflow file in the [Editor](web-app.md#the-editor) of the web app. It runs on the machine of the working trees, e.g. the development server, which mounts the run store, and reads three environment variables, which the [MCP server](mcp.md) reads alike:

| variable | meaning |
| --- | --- |
| `SIMRUN_URL` | the address of the service, e.g. `http://simrun.cluster.internal` |
| `SIMRUN_TOKEN` | a token of the tokens file of the service, which names the person or the agent |
| `SIMRUN_STORE` | optional: the run store as it is mounted on this machine, e.g. `/mnt/cluster`, where the client writes the snapshots itself; without it the client uploads them to the service |

```bash
export SIMRUN_URL=http://simrun.cluster.internal
export SIMRUN_TOKEN=...
export SIMRUN_STORE=/mnt/cluster
```

Every command takes `--json` (except `logs`, which prints the logs as text, and `edit`, which runs until it is stopped) and prints machine-readable JSON instead of tables. A refusal of the service is printed to stderr with exit code 1.

## Commands

| command | what it does |
| --- | --- |
| `simrun submit WORKFLOW` | snapshot the sources and submit a workflow; prints the run in the state `submitting` |
| `simrun wait RUN` | wait for a run to end; exit 0 if it succeeded, 1 if it failed or was cancelled, 2 on timeout |
| `simrun status RUN` | the state of a run and its tasks with their progress |
| `simrun list` | the runs, filtered by group, agent, state and owner |
| `simrun logs RUN TASK` | the logs of the pods of a task |
| `simrun edit WORKFLOW` | serve the Editor of the web app for a workflow file: saves go into the file, runs use the working trees |
| `simrun estimate WORKFLOW` | the plan and the expected duration of every task of a submission |
| `simrun compare RUN RUN...` | the provenance and the summaries of the tasks of runs side by side |
| `simrun cancel RUN` | cancel a run |
| `simrun cleanup` | remove ended runs and the snapshots nothing refers to |
| `simrun usage` | the bytes of the store per group and run |
| `simrun serve` | run the service in the foreground: `--local` the local service of the current directory, else the service of `SIMRUN_CONFIG` |

### submit

```bash
simrun submit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim --group hctz
```

| option | meaning |
| --- | --- |
| `--source NAME=PATH` | a source, the path of a git working tree; repeated, the model first |
| `--group GROUP` | the group of the run, e.g. a study |
| `--single-server` | place the pieces of every task on one server |
| `--reserve NODE` | reserve a node for the run |

The client copies every working tree with its uncommitted changes into `snapshots/<name>/` of the store, or without `SIMRUN_STORE` uploads the files the store lacks to the service which writes the snapshot, and sends its provenance, see [Snapshots and environments](concepts.md#snapshots-and-environments).

### edit

```bash
simrun edit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim
```

| option | meaning |
| --- | --- |
| `--source NAME=PATH` | a source, as for `submit`; snapshotted when the session opens and at every run |
| `--group GROUP` | the group of the runs started from the Editor which name none |

The command opens an edit session in the service, prints the link of the [Editor](web-app.md#the-editor) and runs until Ctrl-C, which closes the session and exits 0 (a second Ctrl-C exits without waiting for the close): it writes the saves of the browser into the file, runs the workflow of the browser as `submit` does, and sends a change of the file or of the working trees to the browser. A file which does not exist is a new workflow.

### wait

```bash
simrun wait <run> --timeout 3600
```

`--timeout` is in seconds, without it the command waits for the end of the run. The service answers a long poll which the client repeats, so no connection is held for hours.

### status, list and logs

```bash
simrun status <run>
simrun list --owner mk --state running
simrun logs <run> <task> --tail 0
```

`list` takes `--group`, `--agent`, `--state` (`submitting`, `queued`, `running`, `succeeded`, `failed`, `cancelled`) and `--owner`; the runs of an owner include the ones of their agents. `logs` prints the last `--tail` lines (300 by default, 0 for all) of every pod of the task: the logs kept in the store once the task is final, else the ones of its live pods.

### estimate and compare

```bash
simrun estimate fit.yaml --source model=~/git/pkdb_models
simrun compare <run> <run>
```

`estimate` takes the options of `submit` but `--group`. `compare` reads the provenance of the runs and the `summary.json` of their tasks.

### cancel, cleanup and usage

```bash
simrun cancel <run>
simrun cleanup --older-than 30 --dry-run
simrun usage
```

`cleanup` removes the runs which ended more than `--older-than` days ago (30 by default), only the ones of `--group` or only `--failed` runs, and the snapshots no remaining run refers to; `--dry-run` lists what it would remove.

### serve

```bash
simrun serve --local                  # the local service of the current directory
simrun serve --local --idle-exit 1800 # exits after 30 min without a request and without a live run
simrun serve --port 8000              # the service of SIMRUN_CONFIG, as uvicorn --factory simrun.service.app:serve
```

`serve --local` runs the service of the current directory with the local backend, which runs the tasks as processes on this machine: its store, index and token are in `.simrun/` (with a `.gitignore` of everything, so none of it lands in git), it listens on `127.0.0.1` only, on a free port unless `--port` names one, and serves the [web app](web-app.md) at `/ui/`. It prints `simrun at http://127.0.0.1:<port>/ui/ (token in .simrun/service.json)`; `.simrun/service.json` (mode 0600) holds the pid, the port, the token, the URL and a random instance of this start while it runs; a client sends the token only to a server which answers that instance at `/local/instance`. Ctrl-C, SIGTERM or SIGHUP (the terminal closed) cancels the runs and waits for their processes, so no process of a run outlives the service. One service runs per directory; a second one is refused with exit code 3. A command which needs the local service starts it in the background when none runs (`python -m simrun serve --local --idle-exit 1800`, its output in `.simrun/service.log`).
