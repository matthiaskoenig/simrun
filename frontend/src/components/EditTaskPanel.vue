<!--
  The task panel of the Editor inspector: the head (kind, type and id), the id, the settings form, the
  inputs and staged ports, the resources with the estimate, the errors. Every
  edit goes up as a new workflow (`change`); a rename as `rename`, so the
  canvas can move the position of the node.
-->
<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { ResourceOverride, TaskEstimate, TaskSpec, TaskTypeView, Workflow } from "../api/client";
import { NEW_INPUT, anyInputs, disconnect, inputPorts, removeTask, renameTask } from "../lib/edit-graph";
import { formatDuration, formatGb, formatPlacement } from "../lib/format";
import { kindOf } from "../lib/kinds";
import { fieldErrors, fields } from "../lib/schema-form";
import AppIcon from "./AppIcon.vue";
import SchemaForm from "./SchemaForm.vue";

const props = defineProps<{
  workflow: Workflow;
  task: TaskSpec;
  type: TaskTypeView | null;
  estimate: TaskEstimate | null;
  errors: string[];
}>();
const emit = defineEmits<{
  change: [workflow: Workflow];
  rename: [from: string, to: string, workflow: Workflow];
  removed: [];
}>();

const INPUT = "w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm";
const kind = computed(() => kindOf(props.task.type));

function update(edit: (task: TaskSpec) => void): void {
  const next = JSON.parse(JSON.stringify(props.workflow)) as Workflow;
  const task = next.tasks.find((t) => t.id === props.task.id);
  if (!task) return;
  edit(task);
  emit("change", next);
}

// rename
const idText = ref(props.task.id);
const idProblem = ref<string | null>(null);
watch(
  () => props.task.id,
  (id) => {
    idText.value = id;
    idProblem.value = null;
  },
);
function rename(): void {
  const next = renameTask(props.workflow, props.task.id, idText.value);
  if (typeof next === "string") {
    idProblem.value = next;
    return;
  }
  idProblem.value = null;
  if (idText.value !== props.task.id) emit("rename", props.task.id, idText.value, next);
}

// settings, and the errors of the service split into those of a field and the others
const settings = computed(() => props.task.settings ?? {});
const split = computed(() => fieldErrors(props.errors, props.type ? fields(props.type.settings_schema).map((f) => f.key) : []));
function setSettings(value: Record<string, unknown>): void {
  update((task) => {
    task.settings = value;
  });
}

// inputs
const ports = computed(() => inputPorts(props.task, props.type).filter((port) => port !== NEW_INPUT));
// a command runs without the harness: it does not stage, and its resources are its settings
const command = computed(() => anyInputs(props.task.type));
function unconnect(port: string): void {
  emit("change", disconnect(props.workflow, { task: props.task.id, port }));
}
function stage(port: string, on: boolean): void {
  update((task) => {
    const rest = (task.stage ?? []).filter((p) => p !== port);
    const staged = on ? [...rest, port] : rest;
    if (staged.length) task.stage = staged;
    else delete task.stage;
  });
}

// resources
const problems = ref<Partial<Record<keyof ResourceOverride, string>>>({});
function override(key: keyof ResourceOverride, text: string): void {
  const n = Number(text);
  const empty = text.trim() === "";
  const whole = key === "workers";
  if (!empty && (Number.isNaN(n) || n <= 0 || (whole && !Number.isInteger(n)))) {
    problems.value[key] = whole ? "a positive whole number" : "a positive number";
    return;
  }
  delete problems.value[key];
  update((task) => {
    const resources: ResourceOverride = { ...task.resources };
    if (empty) delete resources[key];
    else resources[key] = n;
    task.resources = Object.keys(resources).length ? resources : null;
  });
}
function setSplit(value: string): void {
  update((task) => {
    if (value === "") delete task.split;
    else task.split = value as "fastest" | "single-server";
  });
}
const learned = computed(() => props.estimate?.resources.memory_per_worker_gb);

function remove(): void {
  emit("change", removeTask(props.workflow, props.task.id));
  emit("removed");
}
</script>

<template>
  <div>
    <div data-task-head class="flex items-center gap-3 border-b border-gray-200 px-4 py-3">
      <span class="grid size-9 shrink-0 place-items-center rounded-lg text-white" :style="{ background: kind.color }">
        <AppIcon :name="kind.icon" class="size-4.5" />
      </span>
      <div class="min-w-0 flex-1">
        <div data-head-type class="truncate text-[11px] font-semibold tracking-wide text-gray-400 uppercase">{{ task.type }}</div>
        <div data-head-title class="truncate font-semibold">{{ task.id }}</div>
      </div>
    </div>
    <div class="space-y-5 px-4 py-4 text-sm">
      <section class="space-y-3">
        <p v-if="!type" class="text-xs text-amber-700">This task type is not installed on the service.</p>
        <label class="block">
          <span class="mb-1 block text-xs text-gray-600">Id</span>
          <input v-model="idText" data-task-id :class="INPUT" class="font-mono" @change="rename" @keydown.enter="rename" />
          <span v-if="idProblem" class="mt-0.5 block text-[11px] text-red-700">{{ idProblem }}</span>
        </label>
        <ul v-if="split.other.length" data-task-errors class="space-y-1 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">
          <li v-for="error in split.other" :key="error">{{ error }}</li>
        </ul>
      </section>

      <section v-if="type" class="space-y-3">
        <h3 class="text-xs font-semibold tracking-wide text-gray-500 uppercase">Settings</h3>
        <SchemaForm :schema="type.settings_schema" :value="settings" :errors="split.byField" @update:value="setSettings" />
      </section>

      <section v-if="ports.length" class="space-y-3">
        <h3 class="text-xs font-semibold tracking-wide text-gray-500 uppercase">Inputs</h3>
        <div v-for="port in ports" :key="port" class="rounded-lg border border-gray-200 px-3 py-2" :data-input="port">
          <div class="flex items-center justify-between gap-2">
            <span class="font-mono text-xs">{{ port }}</span>
            <span v-if="task.inputs?.[port]" class="flex items-center gap-2">
              <span class="font-mono text-xs text-gray-700">{{ task.inputs[port] }}</span>
              <button type="button" :data-disconnect="port" class="text-xs text-gray-500 underline hover:text-red-700" @click="unconnect(port)">remove</button>
            </span>
            <span v-else class="text-xs text-gray-400">not connected</span>
          </div>
          <label v-if="task.inputs?.[port] && !command" class="mt-1.5 flex items-center gap-2 text-xs text-gray-600">
            <input type="checkbox" :data-stage="port" :checked="task.stage?.includes(port) ?? false" @change="(e) => stage(port, (e.target as HTMLInputElement).checked)" />
            Stage a copy in the scratch of the pod
          </label>
        </div>
      </section>

      <section class="space-y-3">
        <h3 class="flex items-center justify-between text-xs font-semibold tracking-wide text-gray-500 uppercase">
          Resources
          <span v-if="estimate && !command" class="rounded bg-emerald-50 px-1.5 py-0.5 text-[10px] font-medium tracking-normal text-emerald-700 normal-case">{{ estimate.resources.source }}</span>
        </h3>
        <div v-if="estimate" data-estimate class="rounded-lg border border-gray-200 bg-gray-50/70 p-3 text-xs leading-relaxed text-gray-600">
          <div class="flex justify-between"><span>Memory per worker</span><b class="font-semibold text-gray-900">{{ formatGb(estimate.plan.shape.per_worker_gb) }}</b></div>
          <div class="flex justify-between"><span>Duration</span><b class="font-semibold text-gray-900">{{ estimate.seconds === null ? "unknown" : formatDuration(estimate.seconds) }}</b></div>
          <div class="flex justify-between">
            <span>Placement</span>
            <b data-placement class="font-semibold text-gray-900">{{ formatPlacement(estimate.plan.pods, estimate.plan.shape.processes, estimate.plan.shape.memory_gb) }}</b>
          </div>
          <div v-if="estimate.plan.units !== null" class="mt-1.5 text-[11px] text-gray-500">{{ estimate.plan.units }} units</div>
        </div>
        <p v-else class="text-xs text-gray-400">No estimate while the workflow is not valid.</p>
        <p v-if="command" data-command-resources class="text-xs text-gray-500">A command gets the cores, memory, scratch and GPUs of its settings.</p>
        <div v-else class="grid grid-cols-2 gap-3">
          <label class="block">
            <span class="mb-1 block text-xs text-gray-600">Workers</span>
            <input data-workers type="number" min="1" step="1" placeholder="auto" :class="INPUT" :value="task.resources?.workers ?? ''" @input="(e) => override('workers', (e.target as HTMLInputElement).value)" />
            <span v-if="problems.workers" data-workers-problem class="mt-0.5 block text-[11px] text-red-700">{{ problems.workers }}</span>
          </label>
          <label class="block">
            <span class="mb-1 block text-xs text-gray-600">Memory per worker (GB)</span>
            <input
              data-memory
              type="number"
              min="0"
              step="any"
              :placeholder="learned === undefined ? 'learned' : String(Math.round(learned * 10) / 10)"
              :class="INPUT"
              :value="task.resources?.memory_per_worker_gb ?? ''"
              @input="(e) => override('memory_per_worker_gb', (e.target as HTMLInputElement).value)"
            />
            <span v-if="problems.memory_per_worker_gb" class="mt-0.5 block text-[11px] text-red-700">{{ problems.memory_per_worker_gb }}</span>
          </label>
          <label class="block">
            <span class="mb-1 block text-xs text-gray-600">Split</span>
            <select data-split :class="INPUT" :value="task.split ?? ''" @change="(e) => setSplit((e.target as HTMLSelectElement).value)">
              <option value="">inherit</option>
              <option value="fastest">fastest</option>
              <option value="single-server">single server</option>
            </select>
          </label>
        </div>
      </section>

      <button type="button" data-remove-task class="rounded-md border border-gray-300 px-3 py-1.5 text-xs text-gray-700 hover:border-red-300 hover:text-red-700" @click="remove">Remove task</button>
    </div>
  </div>
</template>
