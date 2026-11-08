<!--
  The bar of the Editor above its canvas, the canvas header of the mockup in
  its editor mode: the branch and commit of the model source, its uncommitted
  files, the machine of the session, the state of the draft, Save (Download
  YAML without a connected machine) and Run. It holds no state; the view does
  the calls.
-->
<script setup lang="ts">
import { computed } from "vue";
import type { TreeState } from "../api/client";
import { shortRun } from "../lib/format";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{
  /** The trees of the sources, the model first. */
  tree: Pick<TreeState, "branch" | "commit" | "changed_files">[];
  host: string | null;
  connected: boolean;
  valid: boolean | null;
  errorCount: number;
  dirty: boolean;
  changedOnDisk: boolean;
  saving: boolean;
  running: boolean;
  error: string | null;
  /** The run a rerun comes from, null for a session. */
  run?: string | null;
}>();

defineEmits<{
  save: [force: boolean];
  reload: [];
  run: [];
  download: [];
}>();

const model = computed(() => props.tree[0] ?? null);

// where the draft lives: the machine of the session, or the run a rerun comes from
const machine = computed(() => {
  if (props.run) return `from run #${shortRun(props.run)}`;
  if (props.connected) return props.host ?? "";
  return props.host ? `${props.host} not connected` : "not connected";
});
const changed = computed(() => model.value?.changed_files ?? []);

// what the state says, in the order of what matters most
const state = computed(() => {
  if (props.changedOnDisk && props.connected) return { text: "changed on disk", tone: "text-amber-800" };
  if (props.valid === false) return { text: `${props.errorCount} ${props.errorCount === 1 ? "error" : "errors"}`, tone: "text-red-700" };
  if (props.valid === null) return { text: "checking", tone: "text-gray-500" };
  // without a connected machine there is no file to be saved: the edits live in the browser only
  if (!props.connected) {
    if (props.dirty) return { text: "unsaved changes, download to keep them", tone: "text-amber-800" };
    return { text: "valid", tone: "text-emerald-700" };
  }
  if (props.dirty) return { text: "unsaved changes", tone: "text-gray-600" };
  return { text: "valid, saved", tone: "text-emerald-700" };
});

const button = "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm font-medium whitespace-nowrap shadow-sm";
</script>

<template>
  <!-- floats over the top of the canvas, as the run bar of the Executions -->
  <div class="pointer-events-none absolute inset-x-3 top-3 z-10 flex flex-col items-stretch gap-2">
    <div class="flex items-start justify-between gap-3">
      <div
        data-edit-bar
        class="pointer-events-auto flex min-w-0 items-center gap-2 rounded-lg border border-gray-200 bg-white/95 px-3 py-2 text-xs whitespace-nowrap shadow-sm"
      >
        <!-- one line: a long branch is cut, the separators never end a line -->
        <template v-if="model">
          <AppIcon name="git-branch" class="size-3.5 shrink-0 text-gray-500" />
          <span data-branch class="min-w-0 truncate font-mono text-gray-700" :title="model.branch ?? undefined">{{ model.branch ?? "detached" }}</span>
          <span data-commit class="shrink-0 font-mono text-gray-400">{{ model.commit.slice(0, 7) }}</span>
          <span
            v-if="changed.length"
            data-uncommitted
            class="shrink-0 rounded bg-amber-50 px-1.5 py-0.5 text-amber-800"
            :title="changed.join('\n')"
            >{{ changed.length }} uncommitted {{ changed.length === 1 ? "file" : "files" }}</span
          >
          <span class="shrink-0 text-gray-300">|</span>
        </template>
        <span class="inline-flex shrink-0 items-center gap-1.5" :class="connected || run ? 'text-gray-700' : 'text-gray-500'">
          <span v-if="!run" data-dot class="size-2 rounded-full" :class="connected ? 'bg-emerald-500' : 'bg-gray-300'"></span>
          <AppIcon v-else name="rotate-ccw" class="size-3.5 text-gray-500" />
          <span data-machine>{{ machine }}</span>
        </span>
        <span class="shrink-0 text-gray-300">|</span>
        <span data-state class="inline-flex shrink-0 items-center gap-1" :class="state.tone">
          <AppIcon v-if="state.text.startsWith('valid')" name="check" class="size-3.5" />{{ state.text }}
        </span>
        <!-- only the machine of the session can reload the file or save over it -->
        <template v-if="changedOnDisk && connected">
          <button type="button" data-reload class="font-medium text-gray-700 underline hover:text-gray-900" @click="$emit('reload')">Reload</button>
          <button type="button" data-save-anyway class="font-medium text-gray-700 underline hover:text-gray-900" @click="$emit('save', true)">
            Save anyway
          </button>
        </template>
      </div>

      <div data-actions class="pointer-events-auto flex shrink-0 items-center gap-2">
        <button
          v-if="connected"
          type="button"
          data-save
          :class="[button, 'border border-gray-200 bg-white text-gray-700 hover:bg-gray-50 disabled:opacity-60 disabled:hover:bg-white']"
          :disabled="saving || !(dirty || changedOnDisk)"
          :title="dirty || changedOnDisk ? undefined : 'Nothing to save'"
          @click="$emit('save', false)"
        >
          {{ saving ? "Saving..." : "Save" }}
        </button>
        <button
          v-else
          type="button"
          data-download
          :class="[button, 'border border-gray-200 bg-white text-gray-700 hover:bg-gray-50']"
          @click="$emit('download')"
        >
          <AppIcon name="download" class="size-4" />Download YAML
        </button>
        <button
          type="button"
          data-run
          :class="[button, 'bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-60 disabled:hover:bg-blue-600']"
          :disabled="valid === false || running"
          @click="$emit('run')"
        >
          <AppIcon name="play" class="size-4" />{{ running ? "Running..." : "Run workflow" }}
        </button>
      </div>
    </div>
    <p v-if="error" data-error class="pointer-events-auto self-start rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 shadow-sm ring-1 ring-red-200">{{ error }}</p>
    <!-- the messages of the canvas, under the bar -->
    <slot />
  </div>
</template>
