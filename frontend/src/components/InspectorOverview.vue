<!--
  The overview of a task in the inspector: the tiles of its units, elapsed and
  remaining time and the numeric fields of its last progress line
  (`lib/inspect.ts`), every worker with its server, processes and memory, and
  the resources its plan requested. The clock ticks every second while the
  task is live.
-->
<script setup lang="ts">
import { computed, onScopeDispose, ref, watch } from "vue";
import type { RunRecord, TaskProgress, TaskSpec, TaskState, WorkerView } from "../api/client";
import { formatDuration, serverLabel } from "../lib/format";
import { metrics, plannedShape, resourceNote, taskTimes, workerShape } from "../lib/inspect";
import { rankWorkers, workerLook } from "../lib/workers";
import AppIcon from "./AppIcon.vue";
import StatusPill from "./StatusPill.vue";

const props = defineProps<{
  record: RunRecord;
  task: TaskSpec;
  state: TaskState;
  progress: TaskProgress | undefined;
  workers: WorkerView[];
  live: boolean;
}>();

const now = ref(Date.now());
let timer: ReturnType<typeof setInterval> | null = null;
const stop = (): void => {
  if (timer !== null) clearInterval(timer);
  timer = null;
};
watch(
  () => props.live,
  (live) => {
    stop();
    now.value = Date.now();
    if (live) timer = setInterval(() => (now.value = Date.now()), 1000);
  },
  { immediate: true },
);
onScopeDispose(stop);

const number = (n: number): string => n.toLocaleString("en-US");

const tiles = computed(() => {
  const result: { label: string; value: string }[] = [];
  const counts = props.progress?.counts;
  if (counts) result.push({ label: "Units", value: `${number(counts.done)} / ${number(counts.total)}` });
  const times = taskTimes(props.record, props.task, props.state, props.progress, now.value);
  result.push({ label: "Elapsed", value: times.elapsed === null ? notStarted() : formatDuration(times.elapsed) });
  if (counts) result.push({ label: "Remaining", value: remaining(times.remaining, counts) });
  return [...result, ...metrics(props.progress?.last)];
});

/** Elapsed of a task without a start: its pods start on a server, or wait for one. */
function notStarted(): string {
  if (!props.live) return "-";
  return props.state === "running" || props.workers.some((w) => w.node !== null) ? "starting" : "not started";
}

/** Remaining of a split task: the estimate, and once every unit is done the merge. */
function remaining(seconds: number | null, counts: { done: number; total: number }): string {
  if (seconds !== null) return `≈ ${formatDuration(seconds)}`;
  if (props.live && counts.total > 0 && counts.done >= counts.total) return "merging";
  return "-";
}

// GiB with one decimal, as the memory of the servers strip
const gb = (value: number | null): string => (value === null ? "-" : String(Math.round(value * 10) / 10));
const shape = computed(() => plannedShape(props.record.plans[props.task.id]));

const shown = computed(() =>
  rankWorkers(props.workers).map((w) => {
    const { processes, limitGb } = workerShape(w, shape.value);
    return {
      w,
      look: workerLook(w, props.state),
      processes: `${processes ?? "-"} ${processes === 1 ? "process" : "processes"}`,
      memory: `${gb(w.peak_memory_gb)} / ${gb(limitGb)} GB`,
      note: note(w),
    };
  }),
);

function note(w: WorkerView): string | null {
  const parts: string[] = [];
  if (w.attempts > 1) parts.push(`attempt ${w.attempts}`);
  if (w.oom_killed) parts.push("killed for memory");
  if (parts.length === 0) return null;
  const text = parts.join(", ");
  return `${text[0]?.toUpperCase()}${text.slice(1)}.`;
}

const resources = computed(() => resourceNote(props.record.plans[props.task.id]));

/** Why the task failed: its part of the message of the run, `task <id> failed: ...`. */
const failure = computed(() => {
  if (props.state !== "failed" || !props.record.message) return null;
  const parts = props.record.message.split("; ");
  return parts.find((part) => part.startsWith(`task ${props.task.id} `) || part.startsWith(`task ${props.task.id}:`)) ?? null;
});
</script>

<template>
  <div class="min-h-0 flex-1 space-y-4 overflow-y-auto px-4 py-4 text-sm">
    <p v-if="failure" data-failure class="rounded-lg bg-red-50 px-3 py-2 text-xs leading-relaxed text-red-800 ring-1 ring-red-200">
      {{ failure }}
    </p>
    <div class="grid grid-cols-2 gap-2">
      <div v-for="t in tiles" :key="t.label" :data-tile="t.label" class="min-w-0 rounded-lg bg-gray-50 px-3 py-2">
        <div class="truncate text-[11px] text-gray-500" :title="t.label">{{ t.label }}</div>
        <div data-value class="truncate text-lg font-semibold tabular-nums" :title="t.value">{{ t.value }}</div>
      </div>
    </div>

    <div v-if="shown.length" data-workers>
      <div class="mb-1.5 flex items-baseline justify-between text-xs">
        <span class="font-semibold text-gray-700">Workers</span><span class="text-gray-400">peak / limit of memory</span>
      </div>
      <div class="overflow-hidden rounded-lg border border-gray-200">
        <div
          v-for="row in shown"
          :key="row.w.worker"
          :data-worker="row.w.worker"
          class="border-b border-gray-100 px-3 py-2 text-xs last:border-b-0"
        >
          <div class="flex items-center justify-between gap-2">
            <span class="truncate font-medium text-gray-800" :title="row.w.worker">{{ row.w.worker }}</span>
            <span class="flex shrink-0 items-center gap-2">
              <span data-units class="text-gray-500 tabular-nums">{{ number(row.w.units_done) }} units</span>
              <StatusPill :status="row.look.status" small />
            </span>
          </div>
          <div class="mt-0.5 flex items-center gap-3 text-gray-500 tabular-nums">
            <span class="inline-flex min-w-0 items-center gap-1">
              <AppIcon name="server" class="size-3 shrink-0" />
              <span v-if="row.w.node === null" data-node class="text-gray-400 italic">pending</span>
              <span v-else data-node class="truncate font-mono" :title="row.w.node">{{ serverLabel(row.w.node) }}</span>
            </span>
            <span class="inline-flex shrink-0 items-center gap-1">
              <AppIcon name="cpu" class="size-3" /><span data-processes>{{ row.processes }}</span>
            </span>
            <span class="inline-flex shrink-0 items-center gap-1">
              <AppIcon name="memory-stick" class="size-3" /><span data-memory>{{ row.memory }}</span>
            </span>
          </div>
          <div v-if="row.note" data-note class="mt-1 text-[11px]" :class="row.w.oom_killed ? 'text-red-700' : 'text-amber-800'">
            {{ row.note }}
          </div>
        </div>
      </div>
    </div>

    <div v-if="resources" data-resources class="rounded-lg bg-gray-50 px-3 py-2 text-[11px] leading-relaxed text-gray-600">
      <div class="mb-0.5 flex items-center gap-1 font-semibold text-gray-700">
        <AppIcon name="memory-stick" class="size-3.5" />Resources
      </div>
      {{ resources }}
    </div>
  </div>
</template>
