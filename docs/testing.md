# Testing

The tests are written with pytest and live in `tests/`. Most of them are unit tests, which run anywhere; the tests in `tests/cluster` run on a k3d cluster and are deselected by default (`-m 'not cluster'` of the `addopts` of `pyproject.toml`).

## Unit tests

```bash
pytest                                         # the unit tests, in parallel with pytest-xdist
pytest -n 0                                    # in one process, e.g. for --pdb
pytest tests/test_ledger.py                    # a single module
pytest tests/test_ledger.py::test_claim_order  # a single test
tox r -e py3.15                                # on one python in a fresh environment
tox run-parallel                               # on every supported python
```

The tests of the service run the API with an in-memory backend (`simrun.testing.FakeBackend`) which the test advances, so the controller, the submissions and the index are tested without a cluster; the tests of the environments and the snapshots call `uv` and `git`, the tests of the CWL export run `cwltool`. Continuous integration runs them on python 3.14 and 3.15 on Linux.

## Tests on a k3d cluster { #tests-on-a-k3d-cluster }

```bash
SIMRUN_K3D=1 uv run pytest -m cluster tests/cluster
SIMRUN_K3D=1 SIMRUN_K3D_KEEP=1 uv run pytest -m cluster tests/cluster  # keep the cluster
```

They need docker, about 20 GiB of free disk and the network (the images of k3s, Kueue, Argo, BuildKit, `python:3.14-slim-trixie` and the uv base image). The session downloads k3d and kubectl into `.tools/bin`, creates the k3d cluster `simrun-test` (one server with the infrastructure; three agents of 6 GiB labelled `simrun/worker=true`) with the registry `k3d-simrun-registry:5000` (`localhost:5000` from the host), writes its kubeconfig to `.tools/kubeconfig` (the contexts of the user are not touched), applies `deploy/k3d`, builds the image of the runs without sources from the working tree (`tests/cluster/Dockerfile`) and pushes it, and copies the base image of the images of environments (`ClusterConfig.base_image`, from `ghcr.io/astral-sh/uv`) into the registry with its digest.

Each test runs the service in its process with an `ArgoBackend` and the store `.tools/k3d/store`, which k3d mounts into every node:

| module | what it checks |
| --- | --- |
| `test_cluster.py` | the central assumptions of the design: a run end to end, the take-back of extra workers by a run of the same priority, the owner preempting an agent, the retry after a kill for memory, a reservation, a lost worker and the build of an image |
| `test_sources.py` | the path of a model with sources end to end: a model repository (hatchling, `uv.lock`) with task types of its own and a library (setuptools, a dependency, an uncommitted change) are snapshotted, the service resolves their environment and builds its image with BuildKit, the pods install the read-only snapshots, a split and an unsplit task run, and the merged result, the report and the learned resources are checked |
| `test_capacity.py` | a DaemonSet with requests on the agents, and the nodes planned without them |
| `test_service_image.py` | the image of the service, built from `deploy/service/Dockerfile` with a plugin, serving the API |

At the end the cluster, the registry, the store and the images are deleted (the files the pods of an environment wrote as root are given back to the user first), unless `SIMRUN_K3D_KEEP=1`; a kept cluster is used by the next session. The tests run in one worker of pytest-xdist, which shares the nodes, and take about nine minutes on a created cluster.

## A local test system { #a-local-test-system }

The k3d cluster of the cluster tests also serves as a local test system: the service runs on the host against it, and runs are submitted with the command line or the MCP server as on the real cluster. It needs docker, about 20 GiB of free disk and the network, like the tests.

```bash
uv sync --extra dev
uv run python tests/cluster/local.py up      # about two minutes, reuses a cluster which exists
```

`up` creates the cluster `simrun-test` as the session of the cluster tests does (or reuses it), builds the image of the runs from the working tree and pushes it, and writes the settings and the tokens of the service into `.tools/k3d/local`. It ends with the commands of the next two steps. Start the service in one shell:

```bash
export KUBECONFIG=$PWD/.tools/kubeconfig
export SIMRUN_CONFIG=$PWD/.tools/k3d/local/settings.yaml
uv run uvicorn --factory simrun.service.app:serve --port 8000
```

and submit from another, e.g. the `squares.yaml` of [A first run](index.md#a-first-run):

```bash
export SIMRUN_URL=http://localhost:8000
export SIMRUN_TOKEN=t-mk                       # the owner; t-mk-bot, t-alice and t-alice-bot test the priorities
export SIMRUN_STORE=$PWD/.tools/k3d/store      # the run store, mounted into every node

uv run simrun submit squares.yaml
uv run simrun wait <run>
uv run simrun status <run>
```

The service knows the task types installed in `.venv`; the runs without sources use the image built by `up`, a model submitted with `--source` gets an image of its environment built by BuildKit in the cluster. The workflows and pods are seen with `.tools/bin/kubectl --kubeconfig .tools/kubeconfig -n simrun get workflows,pods`.

After a change of the code of the pods, run `up` again, which builds a new image, and restart the service. The cluster is the one `SIMRUN_K3D_KEEP=1` keeps, so the cluster tests and the local system share it. `down` deletes the cluster, the registry, the store and the settings:

```bash
uv run python tests/cluster/local.py down
```

## Continuous integration

In continuous integration the job `cluster` runs the tests on the k3d cluster on a push to `develop`, on a release, when the workflow `CI-CD` is started by hand and on a pull request with the label `cluster`, see [Development](development.md#pull-requests).
