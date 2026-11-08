# simrun: simulation workflows on a k3s cluster
[![CI-CD](https://github.com/matthiaskoenig/simrun/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/matthiaskoenig/simrun/actions/workflows/ci-cd.yml)
[![Documentation](https://img.shields.io/badge/docs-simrun-3f51b5.svg)](https://matthiaskoenig.github.io/simrun)
[![Version](https://img.shields.io/pypi/v/simrun.svg)](https://pypi.org/project/simrun/)
[![Python Versions](https://img.shields.io/pypi/pyversions/simrun.svg)](https://pypi.org/project/simrun/)
[![MIT License](https://img.shields.io/pypi/l/simrun.svg)](https://opensource.org/licenses/MIT)
[![DOI](https://img.shields.io/badge/DOI-after%20the%20first%20release-lightgrey.svg)](https://matthiaskoenig.github.io/simrun/development/#pypi-and-zenodo)

`simrun` runs simulation workflows on a small cluster of servers with [k3s](https://k3s.io), [Argo Workflows](https://argoproj.github.io/workflows/) and [Kueue](https://kueue.sigs.k8s.io). People and agents submit a workflow of tasks together with the working trees of their model and libraries, uncommitted changes included, and get a run id at once; the run is queued, placed, executed and recorded without the submitter staying connected.

Features include

- **workflows** - YAML files of tasks of task types from plugins, which read the output ports of other tasks
- **split tasks** - the units of a task (the repeats of a fit, the points of a scan) spread over the idle servers and are taken back by runs of a higher priority, with results which do not depend on the split
- **priorities** - the owner of the cluster and the owner's agents first, then the other people, then their agents, with preemption by Kueue and reservations of nodes
- **learned resources** - the memory of a pod is learned from earlier runs, a pod killed for memory is retried with more
- **snapshots and environments** - the working trees are snapshotted into a shared run store, the third party dependencies are locked and built into an image only when they change
- **for people and agents** - a command line, an HTTP API and an MCP server to submit, follow, compare and cancel runs

```bash
simrun submit fit.yaml --source model=~/git/pkdb_models --source sbmlsim=~/git/sbmlsim --group hctz
simrun wait <run>
simrun compare <run> <run>
```

Without a cluster the same workflow runs on this machine, with the web app, see [local mode](https://matthiaskoenig.github.io/simrun/local/):

```bash
pip install simrun
simrun submit fit.yaml --source model=. --local
simrun outputs <run> <task> --to results
```

The documentation is available at [https://matthiaskoenig.github.io/simrun](https://matthiaskoenig.github.io/simrun): [installation](https://matthiaskoenig.github.io/simrun/installation/), [concepts](https://matthiaskoenig.github.io/simrun/concepts/), [workflows](https://matthiaskoenig.github.io/simrun/workflows/), [command line](https://matthiaskoenig.github.io/simrun/command-line/), [local mode](https://matthiaskoenig.github.io/simrun/local/), [agents and MCP](https://matthiaskoenig.github.io/simrun/mcp/), [plugins](https://matthiaskoenig.github.io/simrun/plugins/), [service](https://matthiaskoenig.github.io/simrun/service/), [cluster deployment](https://matthiaskoenig.github.io/simrun/deployment/) and [development](https://matthiaskoenig.github.io/simrun/development/).

If you have any questions or issues please [open an issue](https://github.com/matthiaskoenig/simrun/issues).

## Installation

```bash
pip install simrun
```

`simrun` needs python 3.14 or newer and runs on Linux, see [Installation](https://matthiaskoenig.github.io/simrun/installation/).

## How to cite

If you use `simrun` please cite the archived software on [Zenodo](https://zenodo.org); the DOI follows with the first release, see [CITATION.cff](CITATION.cff).

## License

`simrun` is licensed under the [MIT License](LICENSE).

## Funding

Matthias König is supported by the Federal Ministry of Education and Research (BMBF, Germany) within the research network Systems Medicine of the Liver (**LiSyM**, grant number 031L0054) and by the German Research Foundation (DFG) within the Research Unit Programme FOR 5151 [QuaLiPerF](https://qualiperf.de) (Quantifying Liver Perfusion-Function Relationship in Complex Resection - A Systems Medicine Approach) by grant number 436883643 and by grant number 465194077 (Priority Programme SPP 2311, Subproject SimLivA). Matthias König has received funds from the EOSCsecretariat.eu which has received funding from the European Union's Horizon Programme call H2020-INFRAEOSC-05-2018-2019, grant Agreement number 831644.

© 2026 Matthias König
