/**
 * The numbers of the inspector of a task: its elapsed and remaining time, the
 * metrics of its last progress line and the resources of its plan.
 *
 * The start of a task is the time of the first line of its progress file (the
 * first unit done) and, before its first unit or for a task without progress
 * lines, the time the environment of its run was built. The time to the first
 * unit is thus left out once a task has one, which keeps the units per second
 * those of the work itself. A task which is over stops its clock at its last
 * progress line; one without lines shows no elapsed time, since its end is not
 * known (the end of its run is not its own). The remaining time is the units
 * left at the units per second since the start, unknown before the first unit.
 */
import type { RunRecord, TaskProgress, TaskState, WorkerView } from "../api/client";
import { formatCores, formatGb } from "./format";
import { OVER_TASK_STATES } from "./workers";

// the states of a task which has not begun
const NOT_STARTED: ReadonlySet<TaskState> = new Set<TaskState>(["waiting", "queued", "skipped"]);

/** The time of a progress line in ms, null without a valid one. */
function lineTime(line: Record<string, unknown> | null | undefined): number | null {
  const time = line?.time;
  if (typeof time !== "string") return null;
  const ms = Date.parse(time);
  return Number.isNaN(ms) ? null : ms;
}

function parse(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const ms = Date.parse(iso);
  return Number.isNaN(ms) ? null : ms;
}

/** The elapsed and the remaining seconds of a task, null where unknown. */
export interface TaskTimes {
  elapsed: number | null;
  remaining: number | null;
}

/** The times of a task in the state `state` at `now` (ms). */
export function taskTimes(record: RunRecord, state: TaskState, progress: TaskProgress | undefined, now: number): TaskTimes {
  const first = lineTime(progress?.first);
  if (first === null && NOT_STARTED.has(state)) return { elapsed: null, remaining: null };
  const start = first ?? parse(record.built);
  if (start === null) return { elapsed: null, remaining: null };
  if (OVER_TASK_STATES.has(state)) {
    const end = lineTime(progress?.last);
    return { elapsed: end === null ? null : Math.max(0, (end - start) / 1000), remaining: null };
  }
  const elapsed = Math.max(0, (now - start) / 1000);
  const counts = progress?.counts;
  if (!counts || counts.done === 0 || counts.done >= counts.total || elapsed < 1) return { elapsed, remaining: null };
  const rate = counts.done / elapsed;
  return { elapsed, remaining: (counts.total - counts.done) / rate };
}

/** A metric of a progress line: its field name and its value as shown. */
export interface Metric {
  label: string;
  value: string;
}

// the fields of a progress line which name a unit, not a metric
const UNIT_FIELDS = new Set(["unit", "index"]);

/** The numeric fields of the last progress line of a task, but those which name its unit. */
export function metrics(last: Record<string, unknown> | null | undefined): Metric[] {
  return Object.entries(last ?? {}).flatMap(([label, value]) =>
    typeof value === "number" && Number.isFinite(value) && !UNIT_FIELDS.has(label)
      ? [{ label, value: value.toLocaleString("en-US", { maximumSignificantDigits: 5 }) }]
      : [],
  );
}

// the memory of a process, which is often below 1 GB: two decimals
const fine = (gb: number): string => `${Math.round(gb * 100) / 100} GB`;

const num = (value: unknown): number | null => (typeof value === "number" && Number.isFinite(value) ? value : null);

/** The shape of the pods of a task as its plan has it (`PodShape` of the planner). */
export interface PlannedShape {
  pods: number;
  cores: number;
  memoryGb: number;
  processes: number;
  perWorkerGb: number | null;
  retryMemoryGb: number | null;
  retryPerWorkerGb: number | null;
}

/** The shape of the plan of a task, null without a plan or with one of another form. */
export function plannedShape(plan: unknown): PlannedShape | null {
  if (typeof plan !== "object" || plan === null) return null;
  const { pods, shape } = plan as { pods?: unknown; shape?: unknown };
  if (typeof shape !== "object" || shape === null) return null;
  const s = shape as Record<string, unknown>;
  const memoryGb = num(s.memory_gb);
  const cores = num(s.cores);
  const processes = num(s.processes);
  if (memoryGb === null || cores === null || processes === null) return null;
  return {
    pods: num(pods) ?? 1,
    cores,
    memoryGb,
    processes,
    perWorkerGb: num(s.per_worker_gb),
    retryMemoryGb: num(s.retry_memory_gb),
    retryPerWorkerGb: num(s.retry_per_worker_gb),
  };
}

/** What the plan of a task requested, one or two sentences; null without a plan. */
export function resourceNote(plan: unknown): string | null {
  const shape = plannedShape(plan);
  if (shape === null) return null;
  const { pods, cores, memoryGb, processes, perWorkerGb, retryMemoryGb, retryPerWorkerGb } = shape;
  const each = perWorkerGb === null ? "" : ` of ${fine(perWorkerGb)}`;
  const spread = pods > 1 ? `, on up to ${pods} pods` : "";
  const what = `${processes} ${processes === 1 ? "process" : "processes"}`;
  const parts = [`Requested ${formatGb(memoryGb)} and ${formatCores(cores)} ${cores === 1 ? "core" : "cores"} per pod for ${what}${each}${spread}.`];
  if (retryMemoryGb !== null && retryPerWorkerGb !== null && perWorkerGb !== null && retryPerWorkerGb > perWorkerGb) {
    parts.push(`A pod out of memory is retried with ${fine(retryPerWorkerGb)} per process and ${formatGb(retryMemoryGb)} per pod.`);
  }
  return parts.join(" ");
}

/**
 * The processes and the memory limit of a worker: those of its last report,
 * else, while its first attempt runs without one, those its plan requested
 * (a retry has the memory of the retry, its processes are not planned).
 */
export function workerShape(
  worker: WorkerView,
  shape: PlannedShape | null,
): { processes: number | null; limitGb: number | null } {
  const first = worker.attempts <= 1;
  return {
    processes: worker.processes ?? (first ? (shape?.processes ?? null) : null),
    limitGb: worker.memory_limit_gb ?? (first ? (shape?.memoryGb ?? null) : (shape?.retryMemoryGb ?? null)),
  };
}
