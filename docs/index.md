# simrun: simulation workflows on a k3s cluster
[![CI-CD](https://github.com/matthiaskoenig/simrun/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/matthiaskoenig/simrun/actions/workflows/ci-cd.yml) [![Documentation](https://img.shields.io/badge/docs-simrun-3f51b5.svg)](https://matthiaskoenig.github.io/simrun) [![Version](https://img.shields.io/pypi/v/simrun.svg)](https://pypi.org/project/simrun/) [![Python Versions](https://img.shields.io/pypi/pyversions/simrun.svg)](https://pypi.org/project/simrun/) [![MIT License](https://img.shields.io/pypi/l/simrun.svg)](https://opensource.org/licenses/MIT) [![DOI](https://img.shields.io/badge/DOI-after%20the%20first%20release-lightgrey.svg)](development.md#pypi-and-zenodo)

`simrun` runs simulation workflows on a small cluster of servers with [k3s](https://k3s.io), [Argo Workflows](https://argoproj.github.io/workflows/) and [Kueue](https://kueue.sigs.k8s.io). A workflow is a YAML file of tasks which read the outputs of other tasks. People and agents submit it together with the working trees of their model and their libraries, uncommitted changes included, and get a run id at once; the run is queued, placed, executed and recorded without the submitter staying connected, and its results and provenance stay in a shared run store. The source code is on [GitHub](https://github.com/matthiaskoenig/simrun).

It is made for a research group which runs many analyses of models at once on its own servers, e.g. many parameter fits of individual models, each optimized by another agent in its own git worktree. A task which splits into units, such as the repeats of a fit or the points of a scan, spreads over the idle servers to finish as fast as possible and gives servers back when runs of a higher priority need them; the memory a task needs is learned from its earlier runs.

## A first run

The package ships a demo task type, `demo.squares`, which squares the numbers `0..n-1`, one unit per number, and sums them. A workflow of one task of it, `examples/squares.yaml` of the repository:

```yaml title="squares.yaml"
name: squares
tasks:
  - id: squares
    type: demo.squares
    settings:
      n: 1000
      sleep_s: 0.1
```

The command line reads the address of the service, a token and the local mount of the run store from the environment, submits the workflow and waits for its end:

```bash
export SIMRUN_URL=http://simrun.cluster.internal   # the service
export SIMRUN_TOKEN=...                             # a token of the tokens file
export SIMRUN_STORE=/mnt/cluster                    # the run store as mounted here

simrun estimate squares.yaml      # the plan and the expected duration of every task
simrun submit squares.yaml        # prints the run id at once
simrun wait <run>                 # exit 0 succeeded, 1 failed or cancelled, 2 timeout
simrun status <run>               # the tasks and their progress
```

The task runs as up to eight pods on as many servers, which claim the units from a ledger in the run store; its merge writes the sum into `runs/<run>/tasks/squares/out/summary.json` of the store, which `simrun compare` puts side by side for several runs. A model of your own is submitted with its sources, e.g. `simrun submit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim`, see [Workflows](workflows.md).

## Features

- [Concepts](concepts.md): runs, tasks and ports, the split of a task into units, priorities and preemption, learned resources, snapshots and environments.
- [Workflows](workflows.md): the YAML format of a workflow, with a complete example.
- [Command line](command-line.md): submit, follow, compare, cancel and clean up runs.
- [Agents and MCP](mcp.md): the MCP server with which an agent submits and follows its runs.
- [Plugins](plugins.md): task types as entry points of a python package, e.g. the scans and fits of [sbmlsim](https://github.com/matthiaskoenig/sbmlsim).
- [Service](service.md): the HTTP API, its settings and the environment variables.
- [Cluster deployment](deployment.md): k3s with Kueue, Argo Workflows, an NFS run store, the registry Zot and BuildKit.
- [Testing](testing.md): the unit tests, the tests on a k3d cluster and a local test system on it.

## Questions and issues

If you have any questions or issues please [open an issue](https://github.com/matthiaskoenig/simrun/issues). Contributions are welcome, see [Development](development.md).

## License

`simrun` is licensed under the [MIT License](https://github.com/matthiaskoenig/simrun/blob/develop/LICENSE).

## Funding

Matthias König is supported by the Federal Ministry of Education and Research (BMBF, Germany) within the research network Systems Medicine of the Liver (**LiSyM**, grant number 031L0054) and by the German Research Foundation (DFG) within the Research Unit Programme FOR 5151 [QuaLiPerF](https://qualiperf.de) (Quantifying Liver Perfusion-Function Relationship in Complex Resection - A Systems Medicine Approach) by grant number 436883643 and by grant number 465194077 (Priority Programme SPP 2311, Subproject SimLivA). Matthias König has received funds from the EOSCsecretariat.eu which has received funding from the European Union's Horizon Programme call H2020-INFRAEOSC-05-2018-2019, grant Agreement number 831644.
