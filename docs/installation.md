# Installation

`simrun` needs python 3.14 or newer and runs on Linux. The same package is the client (the command line `simrun` and the MCP server `simrun-mcp`), the service and the harness of the pods (`simrun-worker`, `simrun-merge`, `simrun-exec`). Install it from [PyPI](https://pypi.org/project/simrun/) with pip:

```bash
pip install simrun
```

## With uv

With [uv](https://docs.astral.sh/uv/), install the command line as a tool:

```bash
uv tool install simrun
```

add `simrun` to a project, e.g. the one of a plugin, which locks it together with the rest of the environment:

```bash
uv add simrun
```

or run it from a checkout of the repository, which is how the MCP server of an agent is usually registered (see [Agents and MCP](mcp.md)):

```bash
git clone https://github.com/matthiaskoenig/simrun.git
uv run --project simrun simrun --help
```

## Check the installation

```bash
simrun --help
python -c "import simrun; print(simrun.__version__)"
```

The command line needs a running service, see [Command line](command-line.md); the service and the cluster are set up as described in [Cluster deployment](deployment.md).

## Optional packages

| feature | install |
| --- | --- |
| the CWL export of a run (`simrun.cwl`), validated with cwl-utils and run with cwltool, locally or on Slurm | `pip install "simrun[cwl]"` |
| the tools of development: tests, lint, types, documentation, release | `pip install "simrun[dev]"`, see [Development](development.md) |
