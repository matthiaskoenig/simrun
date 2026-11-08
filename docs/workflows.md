# Workflows

A workflow is a YAML file of tasks, usually kept in the repository of a project, such as `pkdb_models/caffeine_pbpk/workflows/fit.yaml`. People and agents submit the same files; a single fit is a workflow of one task. The model is `simrun.workflow.Workflow`.

## A complete example

```yaml title="fit.yaml"
name: hctz-fit
split: fastest            # or single-server: the pieces of every task on one server
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
  - id: plot
    type: command             # the built-in type: a command in an image of its own
    settings:
      image: registry.cluster.internal:5000/tools:1
      args: [plot, --all]
      cores: 2
      memory_gb: 4.0
```

Submit it with the sources of the model and of the libraries it uses, the model first:

```bash
simrun submit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim --group hctz
```

## The workflow

| field | meaning |
| --- | --- |
| `name` | the name of the workflow |
| `tasks` | the tasks, see below |
| `split` | the split mode of every task, `fastest` (default) or `single-server`, see [Split modes](concepts.md#split-modes) |
| `reserve` | a node reserved for the run, see [Reservations](concepts.md#reservations) |

The tasks run in the order of their inputs; a cycle, a duplicate id and an input which names an unknown task are refused.

## A task

| field | meaning |
| --- | --- |
| `id` | the identifier of the task within the workflow, `[a-z][a-z0-9-]*` |
| `type` | the name of a task type of a [plugin](plugins.md), or `command` |
| `settings` | the settings handed to the task type, validated by its settings model; an unknown setting is an error |
| `inputs` | input port to the output it reads, `<task>.<port>` |
| `split` | the split mode of this task, the one of the workflow by default |
| `resources` | resources which replace the learned or default ones: `workers`, `memory_per_worker_gb`, `scratch_gb`, `gpus` |
| `stage` | input ports copied to the local scratch of the node before the task starts |

The service writes the settings as submitted to `tasks/<task>/settings.json`, and the pods validate them with the plugin of the snapshot.

## The type `command`

The built-in type `command` runs a command in an image of its own, without the harness of simrun: it does not split, it cannot stage its inputs, and its resources are its settings:

| setting | meaning |
| --- | --- |
| `image` | the image, from the registry of the cluster |
| `args` | the argv of the command; as in the command of a [plugin](plugins.md), `{out}` is the output directory of the task, `{settings}` the path of its settings and a part with `{input}` is repeated once per input port, sorted by port, as `<port>=<path>`, e.g. `--input={input}` |
| `cores` | the cores of the pod |
| `memory_gb` | the memory of the pod |
| `scratch_gb` | the local scratch of the pod |
| `gpus` | the GPUs of the pod |

## Validation and estimate

`POST /validate` checks a workflow against the task types the service knows without submitting it, and `simrun estimate` shows the plan of every task (pods, processes and memory per pod) and the expected duration from the learned resources:

```bash
simrun estimate fit.yaml --source model=~/git/pkdb_models
```
