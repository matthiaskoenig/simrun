/** The filters of the list of runs: the chips of the mockup and the group. */
import type { Identity, RunItem, RunState } from "../api/client";

/** A chip of the list of runs. */
export type RunFilter = "all" | "mine" | "agents" | "active";

/** The chips in the order of the mockup. */
export const RUN_FILTERS: { id: RunFilter; label: string }[] = [
  { id: "all", label: "All" },
  { id: "mine", label: "Mine" },
  { id: "agents", label: "Agents" },
  { id: "active", label: "Active" },
];

/** The states of a run which is not over yet. */
export const ACTIVE_STATES: ReadonlySet<RunState> = new Set<RunState>(["submitting", "queued", "running"]);

function matches(run: RunItem, filter: RunFilter, identity: Identity | null): boolean {
  switch (filter) {
    case "all":
      return true;
    case "mine":
      // the runs of the person, those of the agents acting for the person included
      return identity !== null && run.owner === identity.owner;
    case "agents":
      return run.agent !== null;
    case "active":
      return ACTIVE_STATES.has(run.state);
  }
}

/** The runs of `filter` and of `group` (null: every group), in their order. */
export function filterRuns(runs: RunItem[], filter: RunFilter, identity: Identity | null, group: string | null): RunItem[] {
  return runs.filter((run) => (group === null || run.group === group) && matches(run, filter, identity));
}

/** The groups of the runs, each once, sorted. */
export function groupsOf(runs: RunItem[]): string[] {
  const groups = new Set<string>();
  for (const run of runs) if (run.group !== null) groups.add(run.group);
  return [...groups].sort();
}
