# Agents and MCP

`simrun-mcp` is an [MCP](https://modelcontextprotocol.io) server on stdio, a local process of an agent: it runs on the machine of the working trees, takes the snapshots of the sources the agent names and calls the service with the token of the agent. It reads `SIMRUN_URL`, `SIMRUN_TOKEN` and the optional `SIMRUN_STORE` like the [command line](command-line.md).

## Setup

Register the server with the environment of the client, for Claude Code e.g.

```bash
claude mcp add simrun --env SIMRUN_URL=http://simrun.cluster.internal \
    --env SIMRUN_TOKEN=<token of the agent> --env SIMRUN_STORE=/mnt/cluster \
    -- uv run --project ~/git/simrun simrun-mcp
```

or with an installed package, `-- simrun-mcp`. The token of an agent names its owner in the tokens file of the service; the runs of the agents of the owner of the cluster get the priority `owner`, the runs of other agents `agent`, see [Priorities and preemption](concepts.md#priorities-and-preemption).

## The tools

| tool | arguments | answer |
| --- | --- | --- |
| `submit_workflow` | `workflow_path`, `sources` (name to the path of a working tree, the model first), `group`, `single_server` | the summary of the run at once, in the state `submitting` |
| `wait_for_run` | `run_id`, `timeout_s` (600) | the summary of the run once it ended or after the timeout |
| `run_status` | `run_id` | the record: state, tasks, plans and the provenance of the sources |
| `run_progress` | `run_id` | the progress of every task: the counts of its ledger and its first and last progress lines |
| `list_runs` | `group`, `agent`, `state`, `owner` | the runs; the runs of an owner include the ones of their agents |
| `compare_runs` | `run_ids` | the provenance and the summaries of the tasks side by side |
| `cancel_run` | `run_id` | the summary of the cancelled run |
| `run_logs` | `run_id`, `task_id`, `tail_lines` (300, 0 for all) | the logs of the pods of the task |
| `run_outputs` | `run_id`, `task_id` | the output files of the task |
| `estimate_workflow` | `workflow_path`, `sources`, `single_server` | the plan and the expected duration of every task |

Paths in the answers are relative to the store, and every answer carries `store`, the local mount of the store, so an agent opens a result with `store / path`. A refusal of the service is returned as `{"error": detail, "status": code}` instead of raised, so the agent can read it; a service which cannot be reached answers the status 503, an error on the side of the client 400.

## Groups of agents

A main agent gives its subagents a group id; every subagent works on one variant of a model in its own git worktree and submits its runs with that group (`submit_workflow(..., group=...)`). The main agent lists the group, also while runs are going (`list_runs(group=...)`), compares the runs (`compare_runs`) and takes the winner from its provenance.
