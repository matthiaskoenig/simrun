<!--
  A task on the canvas, `tpl-node` of the mockup in its execution mode: the
  type in small caps, the task id, its state, its settings, the units of its
  ledger with a progress bar, and the workers on their servers; a task split
  over several workers is drawn as a stack. The handles of the canvas go into
  the default slot, so that the card renders without Vue Flow in the tests.
-->
<script setup lang="ts">
import { computed } from "vue";
import type { TaskProgress, TaskSpec, TaskState, WorkerView } from "../api/client";
import type { IconName } from "../lib/icons";
import { kindOf } from "../lib/kinds";
import { spins, STATUS } from "../lib/status";
import AppIcon from "./AppIcon.vue";
import StatusPill from "./StatusPill.vue";

const props = defineProps<{
  task: TaskSpec;
  state: TaskState;
  progress: TaskProgress | null;
  workers: WorkerView[];
  selected: boolean;
}>();

const kind = computed(() => kindOf(props.task.type));

const BORDER: Partial<Record<TaskState, string>> = {
  running: "border-blue-300 shadow-blue-100 shadow-md",
  done: "border-emerald-300",
  failed: "border-red-300",
  preempted: "border-amber-300",
  retrying: "border-amber-300",
  queued: "border-gray-300",
};
const border = computed(() => BORDER[props.state] ?? "border-gray-200 border-dashed");

const BAR: Partial<Record<TaskState | WorkerView["state"], string>> = {
  running: "bg-blue-500",
  done: "bg-emerald-500",
  failed: "bg-red-500",
  preempted: "bg-amber-400",
  retrying: "bg-amber-400",
};
const barColor = (state: TaskState | WorkerView["state"]): string => BAR[state] ?? "bg-gray-300";

const WORKER_ICON: Record<WorkerView["state"], string> = {
  running: "text-blue-600",
  done: "text-emerald-600",
  failed: "text-red-600",
};

/** A setting on one line: a scalar as it is, a list of scalars joined by spaces; anything else is left out. */
function setting(value: unknown): string | null {
  if (value === null || ["string", "number", "boolean"].includes(typeof value)) return String(value);
  if (Array.isArray(value) && value.every((v) => v === null || typeof v !== "object")) return value.join(" ");
  return null;
}

const settings = computed(() =>
  Object.entries(props.task.settings ?? {})
    .flatMap(([name, value]) => {
      const text = setting(value);
      return text === null ? [] : [`${name} ${text}`];
    })
    .join(" · "),
);

const counts = computed(() => props.progress?.counts ?? null);
const number = (n: number): string => n.toLocaleString("en-US");
const percent = (frac: number): string => (Math.min(1, Math.max(0, frac)) * 100).toFixed(1) + "%";

const split = computed(() => props.workers.length > 1);

/**
 * The units a worker has done: those its reports count, and for a worker at
 * work an even share of the units done which no report counts yet (a report
 * counts the units of an attempt when the attempt ends).
 */
const unitsOf = computed(() => {
  const workers = props.workers;
  const reported = workers.reduce((sum, w) => sum + w.units_done, 0);
  const working = workers.filter((w) => w.state === "running" && w.node !== null);
  const unreported = Math.max(0, (counts.value?.done ?? 0) - reported);
  return new Map(workers.map((w) => [w.worker, w.units_done + (working.includes(w) ? unreported / working.length : 0)]));
});

/**
 * How far a worker is through its piece: the workers claim the units of one
 * ledger, so a piece is an even share of the units among the workers which
 * got a server; a worker which ended has finished its piece.
 */
function workerFrac(worker: WorkerView): number {
  if (worker.state === "done") return 1;
  const total = counts.value?.total ?? 0;
  const pieces = props.workers.filter((w) => w.node !== null).length;
  if (total === 0 || pieces === 0) return 0;
  return (unitsOf.value.get(worker.worker) ?? 0) / (total / pieces);
}

function workerTitle(worker: WorkerView): string {
  if (worker.node === null) return `${worker.worker} waits for a server`;
  const done = Math.round(unitsOf.value.get(worker.worker) ?? 0);
  const parts = [`${worker.worker} on ${worker.node}`, `${worker.units_done === done ? "" : "about "}${number(done)} units done`];
  if (worker.units_claimed > 0) parts.push(`${number(worker.units_claimed)} claimed`);
  if (worker.attempts > 1) parts.push(`attempt ${worker.attempts}`);
  if (worker.oom_killed) parts.push("killed for memory");
  return parts.join(", ");
}

// a task which is over has no worker at work, whatever the last view of its
// workers says (the stream of a cancelled run ends before its pods are gone)
const OVER: ReadonlySet<TaskState> = new Set<TaskState>(["done", "failed", "skipped"]);

/** How a worker is drawn: the color of its bar, its icon; an hourglass while it waits for a server. */
function workerLook(worker: WorkerView): { bar: string; icon: IconName; spin: boolean; color: string } {
  if (worker.state === "running" && (OVER.has(props.state) || worker.node === null)) {
    return { bar: "bg-gray-300", icon: OVER.has(props.state) ? "square" : "hourglass", spin: false, color: "text-gray-400" };
  }
  const style = STATUS[worker.state];
  return { bar: barColor(worker.state), icon: style.icon, spin: spins(style), color: WORKER_ICON[worker.state] };
}
</script>

<template>
  <div class="relative w-64" :data-task="task.id">
    <template v-if="split">
      <div data-stack class="absolute inset-0 translate-x-2 translate-y-2 rounded-xl border border-gray-200 bg-gray-50"></div>
      <div class="absolute inset-0 translate-x-1 translate-y-1 rounded-xl border border-gray-200 bg-white"></div>
    </template>
    <div
      data-card
      class="relative rounded-xl border bg-white shadow-sm"
      :class="[border, selected ? 'ring-2 ring-blue-500 ring-offset-2' : '']"
    >
      <slot />
      <div class="flex items-center gap-2.5 px-3 pt-3 pb-2">
        <span class="grid size-8 shrink-0 place-items-center rounded-lg text-white" :style="{ background: kind.color }">
          <AppIcon :name="kind.icon" class="size-4" />
        </span>
        <div class="min-w-0 flex-1">
          <div data-type class="truncate text-[10px] font-semibold tracking-wide text-gray-400 uppercase" :title="task.type">
            {{ task.type }}
          </div>
          <div data-title class="truncate text-sm font-semibold text-gray-900" :title="task.id">{{ task.id }}</div>
        </div>
        <StatusPill :status="state" small />
      </div>
      <div class="px-3 pb-3 text-xs text-gray-600">
        <div v-if="settings" data-settings class="truncate" :title="settings">{{ settings }}</div>
        <div v-else class="text-gray-400">no settings</div>

        <template v-if="counts">
          <div data-units class="mt-2 font-medium text-gray-800 tabular-nums">
            {{ number(counts.done) }} / {{ number(counts.total) }} units
          </div>
          <div data-progress class="mt-1.5 h-1.5 overflow-hidden rounded-full bg-gray-100" :title="`${counts.done}/${counts.total}`">
            <div
              class="h-full rounded-full transition-all duration-300"
              :class="barColor(state)"
              :style="{ width: percent(counts.total === 0 ? 0 : counts.done / counts.total) }"
            ></div>
          </div>
        </template>

        <div v-if="split" class="mt-2 space-y-1">
          <div
            v-for="w in workers"
            :key="w.worker"
            :data-worker="w.worker"
            class="flex items-center gap-2 text-[11px]"
            :title="workerTitle(w)"
          >
            <!-- a worker whose pod is not bound to a server yet waits for one -->
            <span v-if="w.node === null" data-node class="w-20 shrink-0 text-gray-400 italic">pending</span>
            <span v-else data-node class="truncate-start w-20 shrink-0 font-mono text-gray-500" :title="w.node"
              ><bdi>{{ w.node }}</bdi></span
            >
            <div class="h-1 flex-1 overflow-hidden rounded-full bg-gray-100">
              <div data-bar class="h-full rounded-full" :class="workerLook(w).bar" :style="{ width: percent(workerFrac(w)) }"></div>
            </div>
            <AppIcon
              :name="workerLook(w).icon"
              class="size-3"
              :class="[workerLook(w).color, workerLook(w).spin ? 'animate-spin' : '']"
            />
          </div>
        </div>
        <div v-else-if="workers.length === 1" class="mt-2 flex items-center gap-1.5 text-[11px] text-gray-500" :title="workerTitle(workers[0]!)">
          <AppIcon name="server" class="size-3 shrink-0" />
          <span data-server class="truncate-start min-w-0 font-mono"><bdi>{{ workers[0]?.node ?? "no server yet" }}</bdi></span>
        </div>
      </div>
    </div>
  </div>
</template>
