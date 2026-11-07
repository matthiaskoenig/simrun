<!--
  The inspector on the right, the right column of the mockup in its execution
  mode: the selected task with its type, id and state, and the tabs Overview
  (its numbers, workers and resources), Outputs and Logs; with no task
  selected, the run itself. The tab stays when another task is selected.
-->
<script setup lang="ts">
import { computed, ref } from "vue";
import type { RunRecord, TaskProgress, WorkerView } from "../api/client";
import { ACTIVE_STATES } from "../lib/filters";
import { kindOf } from "../lib/kinds";
import { OVER_TASK_STATES } from "../lib/workers";
import AppIcon from "./AppIcon.vue";
import InspectorLogs from "./InspectorLogs.vue";
import InspectorOutputs from "./InspectorOutputs.vue";
import InspectorOverview from "./InspectorOverview.vue";
import InspectorRun from "./InspectorRun.vue";
import StatusPill from "./StatusPill.vue";

const props = defineProps<{
  record: RunRecord;
  progress: Record<string, TaskProgress>;
  workers: Record<string, WorkerView[]>;
  task: string | null;
}>();

type Tab = "overview" | "outputs" | "logs";
const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "outputs", label: "Outputs" },
  { id: "logs", label: "Logs" },
];
const tab = ref<Tab>("overview");

const spec = computed(() => props.record.workflow.tasks.find((t) => t.id === props.task) ?? null);
const kind = computed(() => (spec.value ? kindOf(spec.value.type) : null));
const state = computed(() => (props.task === null ? null : (props.record.tasks[props.task] ?? "waiting")));

/** Whether the task may still change: it is not over and its run is not over. */
const live = computed(
  () => state.value !== null && !OVER_TASK_STATES.has(state.value) && ACTIVE_STATES.has(props.record.state),
);
</script>

<template>
  <aside data-inspector class="flex w-[352px] shrink-0 flex-col border-l border-gray-200 bg-white">
    <template v-if="task === null">
      <header class="flex items-center gap-3 border-b border-gray-200 px-4 py-3">
        <span class="grid size-9 shrink-0 place-items-center rounded-lg bg-gray-900 text-white">
          <AppIcon name="play" class="size-4.5" />
        </span>
        <div class="min-w-0 flex-1">
          <div data-type class="text-[11px] font-semibold tracking-wide text-gray-400 uppercase">Run</div>
          <div data-title class="truncate font-semibold" :title="record.workflow.name">{{ record.workflow.name }}</div>
        </div>
        <StatusPill :status="record.state" />
      </header>
      <InspectorRun :record="record" />
    </template>

    <template v-else>
      <header class="flex items-center gap-3 border-b border-gray-200 px-4 py-3">
        <span
          class="grid size-9 shrink-0 place-items-center rounded-lg text-white"
          :class="kind ? '' : 'bg-gray-300'"
          :style="kind ? { background: kind.color } : {}"
        >
          <AppIcon :name="kind?.icon ?? 'box'" class="size-4.5" />
        </span>
        <div class="min-w-0 flex-1">
          <div data-type class="truncate text-[11px] font-semibold tracking-wide text-gray-400 uppercase" :title="spec?.type">
            {{ spec?.type ?? "task" }}
          </div>
          <div data-title class="truncate font-semibold" :title="task">{{ task }}</div>
        </div>
        <StatusPill v-if="spec && state" :status="state" />
      </header>

      <div class="flex gap-4 border-b border-gray-200 px-4 text-sm" role="tablist">
        <button
          v-for="t in TABS"
          :key="t.id"
          type="button"
          role="tab"
          :data-tab="t.id"
          :aria-selected="tab === t.id"
          class="-mb-px border-b-2 py-2"
          :class="tab === t.id ? 'border-blue-600 font-medium text-gray-900' : 'border-transparent text-gray-500 hover:text-gray-800'"
          @click="tab = t.id"
        >
          {{ t.label }}
        </button>
      </div>

      <div v-if="!spec || !state" data-missing class="p-4 text-sm text-gray-500">
        This task is not part of run #{{ record.id }}.
      </div>
      <InspectorOverview
        v-else-if="tab === 'overview'"
        :record="record"
        :task="spec"
        :state="state"
        :progress="progress[spec.id]"
        :workers="workers[spec.id] ?? []"
        :live="live"
      />
      <InspectorOutputs v-else-if="tab === 'outputs'" :run-id="record.id" :task-id="spec.id" :state="state" />
      <InspectorLogs v-else :run-id="record.id" :task-id="spec.id" :live="live" />
    </template>
  </aside>
</template>
