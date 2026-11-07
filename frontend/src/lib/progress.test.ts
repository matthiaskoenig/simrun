import { describe, expect, it } from "vitest";
import type { TaskProgress } from "../api/client";
import { runPercent, runProgress } from "./progress";

const counts = (done: number, total: number): TaskProgress => ({
  counts: { pending: total - done, claimed: 0, done, total },
  first: null,
  last: null,
});

describe("runProgress", () => {
  it("is the mean over the tasks of their share of units, a final task whole, a task without a ledger nothing", () => {
    const tasks = { a: "running", b: "queued", plot: "waiting", prep: "done" } as const;
    const progress = { a: counts(390, 1000), b: counts(0, 250), plot: { counts: null, first: null, last: null } };
    expect(runProgress(tasks, progress)).toBeCloseTo((0.39 + 0 + 0 + 1) / 4);
    expect(runPercent(tasks, progress)).toBe(34);
  });

  it("counts the task states alone for a run card, which has no ledgers", () => {
    expect(runProgress({ a: "done", b: "skipped", c: "running", d: "failed" })).toBe(0.5);
    expect(runPercent({ a: "done", b: "running" })).toBe(50);
  });

  it("uses the ledger of a failed task", () => {
    expect(runProgress({ a: "failed" }, { a: counts(250, 1000) })).toBe(0.25);
  });

  it("reaches 100 % only once every task is final", () => {
    // every unit is done, but the task still merges and the command waits
    const tasks = { a: "running", b: "done", plot: "waiting" } as const;
    expect(runPercent({ a: "running" }, { a: counts(1000, 1000) })).toBe(99);
    expect(runPercent(tasks, { a: counts(1000, 1000), b: counts(250, 250) })).toBeLessThan(100);
    expect(runProgress({ a: "running" }, { a: counts(1000, 1000) })).toBeLessThan(1);
    expect(runPercent({ a: "done", b: "skipped" })).toBe(100);
  });

  it("is nothing for a run without tasks", () => {
    expect(runProgress({})).toBe(0);
    expect(runPercent({})).toBe(0);
  });
});
