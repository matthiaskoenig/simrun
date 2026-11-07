<!--
  The outputs of a task in the inspector: its files with their sizes, under
  their directory in the store, read again when the task changes its state.
  A task of many units writes many files: the first `MAX_FILES` are listed.
-->
<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { api, ApiError, type OutputFile, type TaskState } from "../api/client";
import { formatBytes } from "../lib/format";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{ runId: string; taskId: string; state: TaskState }>();

const MAX_FILES = 200;

const files = ref<OutputFile[] | null>(null);
const error = ref<string | null>(null);

let request = 0;
async function load(): Promise<void> {
  const own = ++request;
  try {
    const found = await api.outputs(props.runId, props.taskId);
    if (own !== request) return;
    files.value = found;
    error.value = null;
  } catch (cause) {
    if (own !== request) return;
    error.value = cause instanceof ApiError ? cause.message : "The service cannot be reached.";
  }
}

watch(
  () => [props.runId, props.taskId] as const,
  () => {
    files.value = null;
    error.value = null;
    void load();
  },
  { immediate: true },
);
watch(() => props.state, () => void load());

/** The directory all files are in, with a trailing slash; empty without files. */
const dir = computed(() => {
  const paths = files.value?.map((f) => f.path) ?? [];
  if (paths.length === 0) return "";
  let parts = (paths[0] ?? "").split("/").slice(0, -1);
  for (const path of paths) {
    const other = path.split("/").slice(0, -1);
    let n = 0;
    while (n < parts.length && n < other.length && parts[n] === other[n]) n += 1;
    parts = parts.slice(0, n);
  }
  return parts.length === 0 ? "" : `${parts.join("/")}/`;
});

// the units of a task write `0.json`, `1.json`, ...: numbers in their order
const sorted = computed(() =>
  [...(files.value ?? [])].sort((a, b) => a.path.localeCompare(b.path, "en", { numeric: true })),
);
const shown = computed(() => sorted.value.slice(0, MAX_FILES));
const more = computed(() => (files.value?.length ?? 0) - shown.value.length);
const total = computed(() => (files.value ?? []).reduce((sum, f) => sum + f.bytes, 0));
</script>

<template>
  <div class="min-h-0 flex-1 overflow-y-auto px-4 py-4 text-sm">
    <div v-if="error" data-outputs-error class="text-xs text-red-700">{{ error }}</div>
    <div v-else-if="files === null" class="inline-flex items-center gap-2 text-xs text-gray-500">
      <AppIcon name="loader-circle" class="size-3.5 animate-spin" />Reading the outputs
    </div>
    <div v-else-if="files.length === 0" data-outputs-empty class="text-xs text-gray-400 italic">Nothing written yet.</div>
    <template v-else>
      <div class="mb-2 text-xs text-gray-500">In the store of the cluster, from its root:</div>
      <div data-out-dir class="truncate-start mb-3 rounded-md bg-gray-50 px-2 py-1.5 font-mono text-[11px] text-gray-700" :title="dir">
        <bdi>{{ dir }}</bdi>
      </div>
      <div
        v-for="f in shown"
        :key="f.path"
        data-output
        :title="f.path"
        class="flex items-center gap-2 border-b border-gray-100 py-1.5 text-xs last:border-b-0"
      >
        <AppIcon name="file-text" class="size-3.5 shrink-0 text-gray-400" />
        <span data-path class="flex-1 truncate font-mono text-gray-800">{{ f.path.slice(dir.length) }}</span>
        <span data-size class="shrink-0 text-gray-400 tabular-nums">{{ formatBytes(f.bytes) }}</span>
      </div>
      <div data-outputs-total class="mt-2 text-[11px] text-gray-400 tabular-nums">
        <span v-if="more > 0" data-outputs-more>{{ more.toLocaleString("en-US") }} more, </span>{{ files.length.toLocaleString("en-US") }}
        {{ files.length === 1 ? "file" : "files" }}, {{ formatBytes(total) }}
      </div>
    </template>
  </div>
</template>
