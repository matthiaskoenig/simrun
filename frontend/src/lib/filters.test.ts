import { describe, expect, it } from "vitest";
import { mk, runs } from "../test/fixtures";
import { filterRuns, groupsOf } from "./filters";

const ids = (filtered: { id: string }[]): string[] => filtered.map((r) => r.id);

describe("filterRuns", () => {
  it("keeps every run for all", () => {
    expect(ids(filterRuns(runs, "all", mk, null))).toEqual(["r6", "r5", "r4", "r3", "r2", "r1"]);
  });

  it("keeps the runs whose owner is the owner of the identity for mine, those of its agents included", () => {
    expect(ids(filterRuns(runs, "mine", mk, null))).toEqual(["r6", "r5", "r3", "r1"]);
    expect(ids(filterRuns(runs, "mine", { name: "alice-bot", kind: "agent", owner: "alice" }, null))).toEqual(["r4", "r2"]);
  });

  it("keeps nothing for mine without an identity", () => {
    expect(filterRuns(runs, "mine", null, null)).toEqual([]);
  });

  it("keeps the runs of agents for agents", () => {
    expect(ids(filterRuns(runs, "agents", mk, null))).toEqual(["r6", "r5", "r4"]);
  });

  it("keeps the runs which are submitting, queued or running for active", () => {
    expect(ids(filterRuns(runs, "active", mk, null))).toEqual(["r6", "r5", "r3"]);
  });

  it("keeps the runs of a group, with the filter", () => {
    expect(ids(filterRuns(runs, "all", mk, "fits"))).toEqual(["r6", "r3"]);
    expect(ids(filterRuns(runs, "agents", mk, "fits"))).toEqual(["r6"]);
  });
});

describe("groupsOf", () => {
  it("names the groups of the runs once, sorted", () => {
    expect(groupsOf(runs)).toEqual(["fits"]);
    expect(groupsOf([])).toEqual([]);
  });
});
