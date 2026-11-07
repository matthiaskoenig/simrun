<!--
  The bar of a run above its canvas, the canvas header of the mockup in its
  execution mode: the run, who submitted it, when it started, its state and
  its progress (`lib/progress.ts`); "Stop" cancels a run which is not over, after a
  confirmation in the bar itself.
-->
<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { api, ApiError, type RunRecord, type TaskProgress } from "../api/client";
import { ACTIVE_STATES } from "../lib/filters";
import { formatTime } from "../lib/format";
import { FINISHED_TASK_STATES, runPercent } from "../lib/progress";
import AppIcon from "./AppIcon.vue";
import StatusPill from "./StatusPill.vue";

const props = defineProps<{ record: RunRecord; progress: Record<string, TaskProgress> }>();

const agent = computed(() => props.record.agent !== null);
const who = computed(() => props.record.agent ?? props.record.submitted_by);

// the progress of the run as its card in the list counts it, with the ledgers of its tasks
const percent = computed(() => runPercent(props.record.tasks, props.progress));
const detail = computed(() => {
  const states = Object.values(props.record.tasks);
  const parts = [`${states.filter((s) => FINISHED_TASK_STATES.has(s)).length} of ${states.length} tasks done`];
  let done = 0;
  let total = 0;
  for (const progress of Object.values(props.progress)) {
    done += progress.counts?.done ?? 0;
    total += progress.counts?.total ?? 0;
  }
  if (total > 0) parts.push(`${done.toLocaleString("en-US")} of ${total.toLocaleString("en-US")} units`);
  return parts.join(", ");
});

const stoppable = computed(() => ACTIVE_STATES.has(props.record.state));
const confirming = ref(false);
const stopping = ref(false);
const error = ref<string | null>(null);
const confirmButton = ref<HTMLButtonElement | null>(null);

async function ask(): Promise<void> {
  error.value = null;
  confirming.value = true;
  await nextTick();
  confirmButton.value?.focus();
}

function dismiss(): void {
  if (!stopping.value) confirming.value = false;
}

async function stop(): Promise<void> {
  stopping.value = true;
  error.value = null;
  try {
    await api.cancel(props.record.id);
    confirming.value = false;
  } catch (cause) {
    error.value = cause instanceof ApiError ? cause.message : "The service cannot be reached.";
  } finally {
    stopping.value = false;
  }
}

// a run which ended meanwhile has nothing to stop
watch(stoppable, (value) => {
  if (!value) confirming.value = false;
});
</script>

<template>
  <div class="pointer-events-none absolute inset-x-3 top-3 z-10 flex items-start justify-between gap-3">
    <div
      data-run-bar
      class="pointer-events-auto flex min-w-0 items-center gap-3 rounded-lg border border-gray-200 bg-white/95 px-3 py-2 text-xs whitespace-nowrap shadow-sm"
    >
      <span data-run-id class="shrink-0 font-semibold text-gray-900">Run #{{ record.id }}</span>
      <span
        data-who
        class="inline-flex min-w-0 items-center gap-1 text-gray-600"
        :title="agent ? `${who}, an agent of ${record.owner}` : who"
      >
        <AppIcon :name="agent ? 'bot' : 'user'" class="size-3.5" />
        <span class="truncate">{{ who }}<span v-if="agent" class="text-gray-400"> for {{ record.owner }}</span></span>
      </span>
      <span data-started class="shrink-0 text-gray-500">started {{ formatTime(record.created) }}</span>
      <StatusPill :status="record.state" small />
      <span
        v-if="record.state === 'running'"
        data-percent
        class="shrink-0 text-gray-600 tabular-nums"
        :title="detail"
        >{{ percent }} %</span
      >
    </div>

    <div v-if="stoppable" class="pointer-events-auto relative flex shrink-0 items-center gap-2">
      <button
        type="button"
        data-stop
        class="inline-flex items-center gap-1.5 rounded-lg border border-red-200 bg-white px-2.5 py-1.5 text-xs font-medium whitespace-nowrap text-red-700 shadow-sm hover:bg-red-50"
        :aria-expanded="confirming"
        @click="confirming ? dismiss() : ask()"
      >
        <AppIcon name="square" class="size-3.5" />Stop
      </button>

      <template v-if="confirming">
        <!-- a click beside the confirmation dismisses it -->
        <div class="fixed inset-0 z-10" aria-hidden="true" @click="dismiss"></div>
        <div
          data-stop-confirm
          role="alertdialog"
          aria-labelledby="stop-title"
          class="absolute top-full right-0 z-20 mt-2 w-72 rounded-lg border border-gray-200 bg-white p-3 text-xs shadow-lg"
          @keydown.esc="dismiss"
        >
          <div id="stop-title" class="font-semibold text-gray-900">Stop this run?</div>
          <div class="mt-1 leading-snug text-gray-500">
            Its pods stop and the run ends as cancelled.
          </div>
          <div v-if="error" data-stop-error class="mt-2 leading-snug text-red-700">{{ error }}</div>
          <div class="mt-3 flex justify-end gap-2">
            <button
              type="button"
              class="rounded-md border border-gray-200 bg-white px-2.5 py-1 font-medium text-gray-700 hover:bg-gray-50"
              :disabled="stopping"
              @click="dismiss"
            >
              Keep running
            </button>
            <button
              ref="confirmButton"
              type="button"
              data-stop-yes
              class="rounded-md bg-red-600 px-2.5 py-1 font-medium text-white hover:bg-red-700 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-red-600 disabled:opacity-60"
              :disabled="stopping"
              @click="stop"
            >
              {{ stopping ? "Stopping" : "Stop run" }}
            </button>
          </div>
        </div>
      </template>
    </div>
  </div>
</template>
