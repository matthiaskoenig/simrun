/**
 * The look of the states of runs and tasks: the `STATUS` map of the mockup,
 * extended by the states it does not show (submitting, succeeded, cancelled,
 * retrying, skipped).
 */
import type { RunState, TaskState } from "../api/client";
import type { IconName } from "./icons";

/** How a state is shown: its label, the classes of its pill and its icon. */
export interface StatusStyle {
  label: string;
  pill: string;
  icon: IconName;
}

const DONE = "bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200";

export const STATUS: Record<RunState | TaskState, StatusStyle> = {
  submitting: { label: "submitting", pill: "bg-gray-100 text-gray-600", icon: "loader-circle" },
  waiting: { label: "waiting", pill: "bg-gray-100 text-gray-500", icon: "circle-dashed" },
  queued: { label: "queued", pill: "bg-slate-100 text-slate-700", icon: "hourglass" },
  running: { label: "running", pill: "bg-blue-50 text-blue-700 ring-1 ring-blue-200", icon: "loader-circle" },
  preempted: { label: "preempted", pill: "bg-amber-50 text-amber-800 ring-1 ring-amber-200", icon: "pause" },
  retrying: { label: "retrying", pill: "bg-amber-50 text-amber-800 ring-1 ring-amber-200", icon: "rotate-ccw" },
  done: { label: "done", pill: DONE, icon: "check" },
  succeeded: { label: "succeeded", pill: DONE, icon: "check" },
  failed: { label: "failed", pill: "bg-red-50 text-red-700 ring-1 ring-red-200", icon: "x" },
  cancelled: { label: "cancelled", pill: "bg-gray-100 text-gray-600", icon: "square" },
  skipped: { label: "skipped", pill: "bg-gray-100 text-gray-400", icon: "circle-dashed" },
};

/** Whether the icon of a state turns, as the loader of the mockup does. */
export function spins(style: StatusStyle): boolean {
  return style.icon === "loader-circle";
}
