/**
 * The progress of a run, the same for its card in the list, its run bar and
 * its nodes: the mean over its tasks of their progress, where a task which is
 * over (done or skipped) counts whole, a task with a ledger counts the share of
 * its units done, and any other task counts nothing.
 *
 * A task which is not over is never whole, even with every unit done (its
 * merge and its outputs are still to come), so that a run shows 100 % only
 * once every task is over.
 *
 * The service counts the `progress` of an active run of the list the same way
 * (`run_progress` of `simrun.service.app`), from the ledgers the list lacks.
 */
import type { TaskProgress, TaskState } from "../api/client";

/** The states of a task which count whole. */
export const FINISHED_TASK_STATES: ReadonlySet<TaskState> = new Set<TaskState>(["done", "skipped"]);

// the most a task which is not over counts
const UNFINISHED_MAX = 0.99;

/** The progress of a task: whole when it is over, else the share of the units of its ledger, if it has one. */
function taskProgress(state: TaskState, progress: TaskProgress | undefined): number {
  if (FINISHED_TASK_STATES.has(state)) return 1;
  const counts = progress?.counts;
  if (!counts || counts.total === 0) return 0;
  return Math.min(counts.done / counts.total, UNFINISHED_MAX);
}

/**
 * The progress of a run from 0 to 1, from the states of its tasks and, where
 * known (the stream of the run), their ledgers; a run without tasks has none.
 */
export function runProgress(tasks: Record<string, TaskState>, progress: Record<string, TaskProgress> = {}): number {
  const ids = Object.keys(tasks);
  if (ids.length === 0) return 0;
  const sum = ids.reduce((total, id) => total + taskProgress(tasks[id] ?? "waiting", progress[id]), 0);
  return sum / ids.length;
}

/** The progress of a run in whole percent, rounded down: 100 only once every task is over. */
export function runPercent(tasks: Record<string, TaskState>, progress: Record<string, TaskProgress> = {}): number {
  return Math.floor(runProgress(tasks, progress) * 100);
}
