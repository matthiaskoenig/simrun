# Plugins

A task type comes from a plugin: a python package with entry points of the group `simrun.task_types`, each a `simrun.plugins.TaskType`. simrun itself registers the built-in type `command` and the demo `demo.squares` (`simrun.demo`), which is the smallest complete example of a split type.

```toml title="pyproject.toml of the plugin"
[project.entry-points."simrun.task_types"]
"sbmlsim.scan" = "sbmlsim.simrun:SCAN"
```

```python
from simrun.plugins import ResourceHint, TaskType

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
    plan="sbmlsim.simrun:plan",  # settings -> settings; in the client
    cost_setting="subset",  # resources learned per subset
    description="Parameter scans of a model",  # the subtitle in the Editor
)
```

The functions are `"module:function"` strings, so a worker process resolves them by import. `paths` is a `simrun.plugins.TaskPaths`: the store, the run and task directories, `out`, the inputs by port and the local scratch.

## The contract

- **Split or unsplit.** A type with `count_units`, `compute_unit` and `merge` is split: its units are claimed from the ledger by the processes of its pods (`simrun-worker`), and the coordinator pod merges once every unit is done (`simrun-merge` remains for runs recorded before). Other types run their `command`, an argv template with `{settings}` (the path of `settings.json`), `{out}` (the output directory) and `{input}`, through `simrun-exec`; the command validates the settings itself with the settings model. A part of the template with `{input}` is repeated once per input port of the task, sorted by port, with `{input}` replaced by `<port>=<path>`, and dropped when the task has no inputs: the path is the absolute path of the output the port reads as the pod sees it, or `<scratch>/<port>` for a port the task stages, the local copy `simrun-exec` makes before the command starts. `--input={input}` becomes `--input=a=/store/runs/<id>/tasks/<task>/out/<port>` and, for a staged port `b`, `--input=b=/scratch/b`.
- **Deterministic, atomic unit outputs.** A unit may be computed more than once (a worker evicted before it completed the unit, a restarted pod whose earlier incarnation still runs), so `compute_unit` writes outputs which depend on the settings and the index only, each written atomically: a temporary file in the same directory, then renamed.
- **Ports are directories.** An output port `<port>` of a task is the directory `out/<port>` (`paths.out / port`); a downstream task reads it as the input `<task>.<port>` (`paths.inputs[<input>]`). `out/summary.json` is the summary of the task, no port, which `compare` reads.
- **`uses_cores`.** An unsplit plugin task is sized as one process (one core, `base + per_worker`), unless its type sets `uses_cores`: then it takes the processes a node allows, like a worker, and its command runs a pool of its own.
- **`plan`** completes the settings of a task before it is submitted, e.g. with values derived from its model: `plan(settings) -> settings` gets the settings validated with the settings model and returns the planned ones. The client (`simrun submit`, `simrun estimate`, the MCP server) calls it for every task whose type is installed where the client runs, before it takes the snapshots, and submits the planned settings, the fields which were given or which the plan set (`model_dump(mode="json", exclude_unset=True)`), so the defaults stay with the plugin of the snapshot which validates them in the pods; `simrun validate` plans the same way, so it validates what `simrun submit` submits. A task of a type which is not installed locally is submitted as written.
- **Provenance.** Every task gets `tasks/<task>/provenance.json`, the JSON list of the sources of its run (name, branch, commit, dirty, changed files, diff hash, snapshot), which the service writes next to `settings.json`; its path is `SIMRUN_PROVENANCE` in every pod of the task, so a command or a merge can record where its results come from.
- **`cost_setting`** names the setting which changes the cost of a task, e.g. the subset of the data of a fit: the resources are learned per value of it.
- **`description`** says in a few words what a task of the type does; the palette of the [Editor](web-app.md#the-editor) shows it under the name of the type ("Split over servers" or "One process" without one).
- **`hint`** is the `ResourceHint` of a type without observations: the base memory, the memory per process, the scratch and the GPUs.
- **Images need simrun.** A type with an `image` of its own runs its pods in that image, which must have simrun installed (`simrun-worker`, `simrun-merge` and `simrun-exec`, which every plugin command runs through); the image of an environment has it.

## Exit codes

`simrun-exec` installs the snapshots of the run, stages the inputs, sets `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS` and `MKL_NUM_THREADS` to 1, passes a SIGTERM on to the command and writes a report of its memory. Its exit codes:

| code | meaning |
| --- | --- |
| 0 | done |
| 3 | the command failed, not retried |
| 4 | a configuration error |
| 137 | the command was killed, e.g. by the out of memory killer; retried once with 1.5 times the memory per process |
| 143 | stopped on SIGTERM, e.g. preempted; retried |

## Plugins of the service and of the snapshots

The service imports the plugins it knows: it validates the settings with their models and counts the units of a split type when it plans a run. A task type which only exists in a snapshot (a plugin in a working tree) is unknown to the service: the service image must carry every plugin whose task types are submitted, and a new task type, a split one in particular, needs a rebuilt service image before its first run, see [The service image](deployment.md#the-service-image). The pods run the plugin of the snapshot.
