/**
 * The look of the workers of a task, the same on its node and in the
 * inspector: their order and how a worker is drawn.
 *
 * A task which is over has no worker at work, whatever the last view of its
 * workers says (the stream of a cancelled run ends before its pods are gone):
 * such a worker is drawn stopped. A worker whose pod is not bound to a server
 * yet waits for one.
 */
import type { TaskState, WorkerView } from "../api/client";
import type { IconName } from "./icons";
import { spins, STATUS, type StatusStyle } from "./status";

/** The states of a task which is over. */
export const OVER_TASK_STATES: ReadonlySet<TaskState> = new Set<TaskState>(["done", "failed", "skipped"]);

// at work first (those with a server before those waiting for one), then the failed, then the done
const rank = (w: WorkerView): number => ({ running: w.node === null ? 1 : 0, failed: 2, done: 3 })[w.state];

/** The workers in the order they are shown, a sorted copy. */
export function rankWorkers(workers: readonly WorkerView[]): WorkerView[] {
  return [...workers].sort((a, b) => rank(a) - rank(b));
}

/** How a worker is drawn: the color of its bar, its icon and the pill of its state. */
export interface WorkerLook {
  bar: string;
  icon: IconName;
  spin: boolean;
  color: string;
  status: StatusStyle;
}

const BAR: Record<WorkerView["state"], string> = { running: "bg-blue-500", done: "bg-emerald-500", failed: "bg-red-500" };
const ICON_COLOR: Record<WorkerView["state"], string> = {
  running: "text-blue-600",
  done: "text-emerald-600",
  failed: "text-red-600",
};

const STOPPED: StatusStyle = { label: "stopped", pill: "bg-gray-100 text-gray-600", icon: "square" };
const PENDING: StatusStyle = { label: "pending", pill: "bg-gray-100 text-gray-500", icon: "hourglass" };

/** How the worker of a task in the state `task` is drawn. */
export function workerLook(worker: WorkerView, task: TaskState): WorkerLook {
  if (worker.state === "running" && (OVER_TASK_STATES.has(task) || worker.node === null)) {
    const status = OVER_TASK_STATES.has(task) ? STOPPED : PENDING;
    return { bar: "bg-gray-300", icon: status.icon, spin: false, color: "text-gray-400", status };
  }
  const status = STATUS[worker.state];
  return { bar: BAR[worker.state], icon: status.icon, spin: spins(status), color: ICON_COLOR[worker.state], status };
}
