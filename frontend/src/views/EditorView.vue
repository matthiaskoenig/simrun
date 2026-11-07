<!--
  The Editor: the draft of a workflow of an edit session (`#/edit/:id`) or of a
  run (`#/runs/:id/edit`). The palette, the canvas, the inspector and the bar
  plug into the three columns; this view owns the draft and the page chrome.
-->
<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { auth } from "../auth";
import EditCanvas from "../components/EditCanvas.vue";
import EditInspector from "../components/EditInspector.vue";
import TaskPalette from "../components/TaskPalette.vue";
import TopBar from "../components/TopBar.vue";
import { addTask } from "../lib/edit-graph";
import type { TaskTypeView, Workflow } from "../api/client";
import { useEdit } from "../stores/edit";
import { useLive } from "../stores/live";

const props = defineProps<{ id: string; run?: boolean }>();

const live = useLive();
const edit = useEdit();

// the selected task, which the inspector shows
const selected = ref<string | null>(null);
// why the last connection was refused, shown over the canvas until the next edit
const refused = ref<string | null>(null);
const canvas = ref<InstanceType<typeof EditCanvas> | null>(null);

// the sources shown in the workflow panel: the trees of the session, else those of the run
const tree = computed(() => edit.session.value?.tree ?? edit.runSources.value);

function rename(from: string, to: string, workflow: Workflow): void {
  canvas.value?.rename(from, to);
  refused.value = null;
  edit.change(workflow);
  selected.value = to;
}

function add(type: TaskTypeView): void {
  if (edit.draft.value === null) return;
  const added = addTask(edit.draft.value, type);
  refused.value = null;
  edit.change(added.workflow);
  selected.value = added.id;
}

let controller: AbortController | null = null;

watch(
  () => [props.id, props.run] as const,
  ([id, run]) => {
    selected.value = null;
    controller?.abort();
    controller = null;
    if (run) void edit.openRun(id);
    else {
      controller = new AbortController();
      edit.openSession(id, controller.signal);
    }
  },
  { immediate: true },
);
onBeforeUnmount(() => controller?.abort());
</script>

<template>
  <div class="flex h-full flex-col bg-white text-gray-900">
    <TopBar
      :identity="auth.identity.value"
      :runs="live.runs.value"
      :servers="live.servers.value"
      :connected="live.connected.value"
      :workflow="edit.draft.value?.name ?? null"
      mode="editor"
      :session="run ? null : id"
    />
    <div class="flex min-h-0 flex-1">
      <aside data-palette class="w-[260px] shrink-0 border-r border-gray-200 bg-white">
        <TaskPalette :types="edit.types.value" @add="add" />
      </aside>
      <main data-canvas class="relative flex min-w-0 flex-1 flex-col bg-gray-50">
        <p v-if="edit.loadError.value" data-load-error class="m-4 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">
          {{ edit.loadError.value }}
        </p>
        <EditCanvas
          v-if="edit.draft.value"
          :key="id"
          ref="canvas"
          v-model:selected="selected"
          :workflow="edit.draft.value"
          :types="edit.types.value"
          :estimate="edit.estimate.value"
          :task-errors="edit.taskErrors.value"
          @change="(workflow) => ((refused = null), edit.change(workflow))"
          @refused="(message) => (refused = message)"
        />
        <p
          v-if="refused"
          data-refused
          class="absolute top-3 left-1/2 -translate-x-1/2 rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-800 ring-1 ring-amber-200"
        >
          {{ refused }}
        </p>
      </main>
      <aside data-inspector class="w-[352px] shrink-0 border-l border-gray-200 bg-white">
        <EditInspector
          v-if="edit.draft.value"
          v-model:selected="selected"
          v-model:group="edit.group.value"
          :workflow="edit.draft.value"
          :types="edit.types.value"
          :estimate="edit.estimate.value"
          :task-errors="edit.taskErrors.value"
          :errors="edit.errors.value"
          :tree="tree"
          @change="(workflow) => ((refused = null), edit.change(workflow))"
          @rename="rename"
        />
      </aside>
    </div>
  </div>
</template>
