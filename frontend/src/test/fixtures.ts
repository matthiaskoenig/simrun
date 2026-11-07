/** The fixtures of the tests of the executions: six runs of people and agents, three servers, and the record of a run. */
import type { Identity, RunItem, RunRecord, ServerUsage } from "../api/client";

function run(fields: Partial<RunItem> & Pick<RunItem, "id" | "state" | "owner" | "submitted_by">): RunItem {
  return {
    agent: null,
    created: "2026-11-07T13:02:00Z",
    finished: null,
    group: null,
    message: null,
    priority: "standard",
    tasks: { squares: "waiting" },
    workflow: "squares",
    ...fields,
  };
}

export const mk: Identity = { name: "mk", kind: "person", owner: "mk" };

/** Newest first, as the service sends them: two agents of mk, one of alice, a person run of each, a failed run. */
export const runs: RunItem[] = [
  run({ id: "r6", state: "running", owner: "mk", submitted_by: "mk-bot", agent: "mk-bot", priority: "agent", group: "fits", workflow: "caffeine_fit", tasks: { fit: "running", report: "waiting" } }),
  run({ id: "r5", state: "queued", owner: "mk", submitted_by: "mk-scan", agent: "mk-scan", priority: "agent", tasks: { squares: "queued" } }),
  run({ id: "r4", state: "succeeded", owner: "alice", submitted_by: "alice-bot", agent: "alice-bot", priority: "agent", finished: "2026-11-07T13:40:00Z", tasks: { squares: "done" } }),
  run({ id: "r3", state: "submitting", owner: "mk", submitted_by: "mk", priority: "owner", group: "fits", workflow: "midazolam_fit" }),
  run({ id: "r2", state: "cancelled", owner: "alice", submitted_by: "alice", finished: "2026-11-07T12:10:00Z", tasks: { squares: "skipped" } }),
  run({ id: "r1", state: "failed", owner: "mk", submitted_by: "mk", finished: "2026-11-07T12:00:00Z", message: "task squares: out of memory, 8 of 8 GiB", tasks: { squares: "failed" } }),
];

export const servers: ServerUsage[] = [
  {
    name: "node-a",
    cores: 8,
    memory_gb: 16,
    gpus: 0,
    reserved_by: null,
    pods: [
      { run_id: "r6", task_id: "fit", pod: "r6-fit-w0", phase: "Running", cores: 2, memory_gb: 4 },
      { run_id: "r5", task_id: "squares", pod: "r5-squares-w0", phase: "Running", cores: 1, memory_gb: 2.5 },
    ],
  },
  { name: "node-b", cores: 4, memory_gb: 7.5, gpus: 0, reserved_by: null, pods: [] },
  {
    name: "node-c",
    cores: 4,
    memory_gb: 8,
    gpus: 0,
    reserved_by: "r3",
    pods: [{ run_id: "r3", task_id: "squares", pod: "r3-squares-w0", phase: "Pending", cores: 4, memory_gb: 6 }],
  },
];

/** The record of a running run of one task of mk, with `fields` replaced. */
export function runRecord(fields: Partial<RunRecord> = {}): RunRecord {
  return {
    id: "r1",
    state: "running",
    agent: null,
    backend_name: null,
    created: "2026-11-07T13:02:00Z",
    finished: null,
    group: null,
    image: null,
    message: null,
    owner: "mk",
    pending_cancel: false,
    pending_logs: false,
    plans: {},
    priority: "standard",
    reserve: null,
    sources: [],
    submitted_by: "mk",
    tasks: { squares: "running" },
    workflow: { name: "squares", split: "fastest", tasks: [{ id: "squares", type: "demo.squares" }] },
    ...fields,
  };
}
