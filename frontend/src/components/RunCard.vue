<!-- A run of the list: who submitted it, its workflow, its id, group and time, its state and its message. -->
<script setup lang="ts">
import { computed } from "vue";
import type { RunItem } from "../api/client";
import { formatDuration, formatTime, initials } from "../lib/format";
import { FINISHED_TASK_STATES, runProgress } from "../lib/progress";
import AppIcon from "./AppIcon.vue";
import StatusPill from "./StatusPill.vue";

const props = defineProps<{ run: RunItem; selected: boolean }>();
defineEmits<{ select: [id: string] }>();

const who = computed(() => props.run.agent ?? props.run.submitted_by);

// the ids of simrun are long (`20261107-002755-17eb1c`): they get a line of their own
const meta = computed(() => [props.run.group, who.value, formatTime(props.run.created)].filter((part) => part !== null).join(" · "));

// a running run shows its progress as its run bar counts it: the service
// counts it from the ledgers of its tasks, else it comes from their states alone
const tasks = computed(() => {
  const states = Object.values(props.run.tasks);
  return { total: states.length, finished: states.filter((s) => FINISHED_TASK_STATES.has(s)).length };
});
const progress = computed(() => props.run.progress ?? runProgress(props.run.tasks));
const percent = computed(() => Math.floor(progress.value * 100));

const note = computed(() => {
  if (props.run.message) return props.run.message;
  if (props.run.state === "succeeded" && props.run.finished !== null) {
    const seconds = (Date.parse(props.run.finished) - Date.parse(props.run.created)) / 1000;
    return `done in ${formatDuration(seconds)}`;
  }
  return null;
});
</script>

<template>
  <button
    type="button"
    :data-run="run.id"
    class="mb-1 block w-full rounded-lg border px-2.5 py-2 text-left"
    :class="selected ? 'border-blue-300 bg-blue-50/70' : 'border-transparent hover:bg-white'"
    :aria-current="selected ? 'true' : undefined"
    @click="$emit('select', run.id)"
  >
    <div class="flex items-center gap-2">
      <span
        data-avatar
        class="grid size-6 shrink-0 place-items-center rounded-full"
        :class="run.agent ? 'bg-violet-100 text-violet-700' : 'bg-blue-600 text-white'"
        :title="run.agent ? `${run.agent}, an agent of ${run.owner}` : run.submitted_by"
      >
        <AppIcon v-if="run.agent" name="bot" class="size-3.5" />
        <span v-else class="text-[9px] font-semibold">{{ initials(run.submitted_by) }}</span>
      </span>
      <!-- the pill stays on the line of the title, so that the lines below it have the whole width -->
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2">
          <div data-title class="min-w-0 flex-1 truncate text-sm font-medium text-gray-900">{{ run.workflow }}</div>
          <StatusPill :status="run.state" small />
        </div>
        <div data-meta class="truncate text-[11px] text-gray-500" :title="meta">{{ meta }}</div>
        <div data-id class="truncate font-mono text-[10px] text-gray-400" :title="run.id">#{{ run.id }}</div>
      </div>
    </div>
    <div
      v-if="run.state === 'running'"
      data-progress
      class="mt-1.5 h-1 overflow-hidden rounded-full bg-gray-200"
      :title="`${percent} %, ${tasks.finished} of ${tasks.total} ${tasks.total === 1 ? 'task' : 'tasks'} done`"
    >
      <div
        class="h-full rounded-full bg-blue-500 transition-all duration-300"
        :style="{ width: (progress * 100).toFixed(1) + '%' }"
      ></div>
    </div>
    <div
      v-if="note"
      data-note
      class="mt-1 text-[11px] leading-snug"
      :class="run.state === 'failed' ? 'text-red-700' : 'text-gray-500'"
    >
      {{ note }}
    </div>
  </button>
</template>
