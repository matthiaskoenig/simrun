<!--
  A task on the canvas of the Editor, `tpl-node` of the mockup in its editor
  mode: the type icon, the type in small caps and the task id, the count of
  its errors, and the chips of the estimate (workers, memory per worker,
  duration, split). A target handle per input port of the type sits on the
  left, a source handle per output port on the right.
-->
<script setup lang="ts">
import { Handle, Position } from "@vue-flow/core";
import { computed } from "vue";
import type { TaskEstimate, TaskSpec, TaskTypeView } from "../api/client";
import { formatDuration, formatGb } from "../lib/format";
import { kindOf } from "../lib/kinds";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{
  task: TaskSpec;
  type: TaskTypeView | null;
  errors: string[];
  estimate: TaskEstimate | null;
  selected: boolean;
}>();

const kind = computed(() => kindOf(props.task.type));
const inputs = computed(() => props.type?.inputs ?? []);
const outputs = computed(() => props.type?.outputs ?? []);

/** The vertical place of a port on its side, in percent. */
const top = (index: number, count: number): string => `${((index + 1) / (count + 1)) * 100}%`;

const chips = computed(() => {
  const e = props.estimate;
  if (!e) return null;
  const pods = e.plan.pods;
  return {
    workers: pods * e.plan.shape.processes,
    memory: formatGb(e.resources.memory_per_worker_gb),
    time: e.seconds === null ? null : formatDuration(e.seconds),
    split: pods > 1 ? `${pods} pods` : null,
  };
});

const CHIP = "inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[11px]";
</script>

<template>
  <div class="relative w-64" :data-task="task.id">
    <div
      data-card
      class="relative rounded-xl border bg-white shadow-sm"
      :class="[errors.length ? 'border-red-300' : 'border-gray-200', selected ? 'ring-2 ring-blue-500 ring-offset-2' : '']"
    >
      <Handle
        v-for="(port, i) in inputs"
        :id="port"
        :key="`in-${port}`"
        type="target"
        :position="Position.Left"
        :style="{ top: top(i, inputs.length) }"
        :title="port"
      />
      <Handle
        v-for="(port, i) in outputs"
        :id="port"
        :key="`out-${port}`"
        type="source"
        :position="Position.Right"
        :style="{ top: top(i, outputs.length) }"
        :title="port"
      />
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
        <span
          v-if="errors.length"
          data-errors
          class="shrink-0 rounded-full bg-red-50 px-2 py-0.5 text-[11px] font-medium text-red-700"
          :title="errors.join('\n')"
          >{{ errors.length }} {{ errors.length === 1 ? "error" : "errors" }}</span
        >
      </div>
      <div class="px-3 pb-3 text-xs text-gray-600">
        <div v-if="inputs.length || outputs.length" class="flex justify-between gap-2 font-mono text-[10px] text-gray-400">
          <span class="truncate">{{ inputs.join(" ") }}</span>
          <span class="truncate text-right">{{ outputs.join(" ") }}</span>
        </div>
        <div v-if="chips" data-chips class="mt-2 flex flex-wrap gap-1">
          <span data-chip="workers" :class="[CHIP, 'bg-gray-100 text-gray-700']"
            ><AppIcon name="cpu" class="size-3" />{{ chips.workers }}</span
          >
          <span data-chip="memory" :class="[CHIP, 'bg-gray-100 text-gray-700']"
            ><AppIcon name="memory-stick" class="size-3" />{{ chips.memory }}</span
          >
          <span v-if="chips.time" data-chip="time" :class="[CHIP, 'bg-gray-100 text-gray-700']"
            ><AppIcon name="clock" class="size-3" />{{ chips.time }}</span
          >
          <span v-if="chips.split" data-chip="split" :class="[CHIP, 'bg-indigo-50 text-indigo-700']"
            ><AppIcon name="layers" class="size-3" />{{ chips.split }}</span
          >
        </div>
      </div>
    </div>
  </div>
</template>
