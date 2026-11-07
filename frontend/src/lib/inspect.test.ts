import { describe, expect, it } from "vitest";
import { runRecord } from "../test/fixtures";
import { metrics, resourceNote, taskTimes } from "./inspect";

describe("resourceNote", () => {
  it("names the shape of one pod without a retry of more memory", () => {
    const plan = { pods: 1, shape: { cores: 1, memory_gb: 1, processes: 1, per_worker_gb: 1, retry_memory_gb: 1, retry_per_worker_gb: 1 } };
    expect(resourceNote(plan)).toBe("Requested 1 GB and 1 core per pod for 1 process of 1 GB.");
  });

  it("has no note without a plan or a shape", () => {
    expect(resourceNote(undefined)).toBeNull();
    expect(resourceNote({ pods: 2 })).toBeNull();
    expect(resourceNote({ shape: { cores: 1 } })).toBeNull();
  });
});

describe("taskTimes", () => {
  const now = Date.parse("2026-11-07T14:00:00Z");

  it("knows no time of a running task of a run which is not built", () => {
    expect(taskTimes(runRecord({ built: null }), "running", undefined, now)).toEqual({ elapsed: null, remaining: null });
  });

  it("knows no elapsed time of a task which is over without progress lines", () => {
    const record = runRecord({ built: "2026-11-07T13:00:00Z" });
    expect(taskTimes(record, "done", { counts: null, first: null, last: null }, now)).toEqual({ elapsed: null, remaining: null });
  });

  it("has no remaining time once every unit is done", () => {
    const progress = {
      counts: { pending: 0, claimed: 0, done: 10, total: 10 },
      first: { time: "2026-11-07T13:59:00Z" },
      last: { time: "2026-11-07T13:59:50Z" },
    };
    expect(taskTimes(runRecord(), "running", progress, now)).toEqual({ elapsed: 60, remaining: null });
  });
});

describe("metrics", () => {
  it("keeps the finite numbers of a line but its unit and index", () => {
    expect(metrics({ unit: 3, index: 3, time: "x", ok: true, nan: Number.NaN, cost: 0.5 })).toEqual([{ label: "cost", value: "0.5" }]);
    expect(metrics(null)).toEqual([]);
  });
});
