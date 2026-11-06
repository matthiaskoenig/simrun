# API reference

The API reference is generated from the docstrings of the package.

## simrun

The workflows, the plugins, the clients, the planning and the harness of the pods.

| module | description |
| --- | --- |
| [workflow](workflow.md) | the workflow model: tasks, the references between them and their order |
| [plugins](plugins.md) | the plugin interface: task types, their resource hints and their discovery |
| [plugins_builtin](plugins_builtin.md) | the built-in task types |
| [demo](demo.md) | the demo plugin: sums of squares, split into one unit per number |
| [client](client.md) | the client of the service: snapshots working trees, then calls the API |
| [cli](cli.md) | the command line of simrun: submit, follow and compare runs |
| [mcp](mcp.md) | the MCP server of simrun, a local stdio process of an agent |
| [planner](planner.md) | the planner: the shape of a worker pod and the number of pieces of a task |
| [resources](resources.md) | resources learned from the reports of workers |
| [ledger](ledger.md) | the unit ledger: workers of a split task claim units through atomic renames |
| [worker](worker.md) | the worker harness: a pod of a split task computes units of the ledger in a pool |
| [cgroup](cgroup.md) | the limits and the usage of the cgroup (v2) a worker runs in |
| [snapshot](snapshot.md) | snapshots of working trees with provenance |
| [environment](environment.md) | the environment of a run: a lock from the model and its snapshotted libraries |
| [store](store.md) | the run store: run records and the layout of a run on the shared drive |
| [config](config.md) | the configuration of the cluster the runs are submitted to |
| [podspec](podspec.md) | the parts of a pod which every backend shares: image, shapes, command, environment |
| [argo](argo.md) | Argo workflows from runs: one DAG, a scatter of workers per split task |
| [cwl](cwl.md) | CWL v1.2 workflows from runs, executed with cwltool (or Toil on Slurm) |
| [testing](testing.md) | a backend in memory for tests of the service, advanced by the test |

## simrun.service

The service: the API, the controller and the backends, see [Service](../service.md).

| module | description |
| --- | --- |
| [service.app](service.app.md) | the API of the service: people and agents submit, follow and compare runs |
| [service.auth](service.auth.md) | identities of the callers of the service and the priorities of their runs |
| [service.backend](service.backend.md) | the backend the service runs workflows on: Argo Workflows, or a fake in tests |
| [service.argo_backend](service.argo_backend.md) | the backend of the service on a cluster: Argo Workflows, Kueue and BuildKit |
| [service.controller](service.controller.md) | the controller: follows the workflows of the runs and records their end |
| [service.submission](service.submission.md) | the submission of a run in the background: its environment, image and workflow |
| [service.index](service.index.md) | the index of the runs in SQLite, beside the run store |
| [service.logs](service.logs.md) | the logs of the pods of a task, kept in the run store |
| [service.compare](service.compare.md) | the comparison of runs: provenance and the summaries of their tasks side by side |
