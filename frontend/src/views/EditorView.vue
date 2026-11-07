<!--
  The Editor: the draft of a workflow of an edit session (`#/edit/:id`) or of a
  run (`#/runs/:id/edit`). The palette, the canvas, the inspector and the bar
  plug into the three columns; this view owns the draft and the page chrome.
-->
<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ApiError } from "../api/client";
import { auth } from "../auth";
import EditBar from "../components/EditBar.vue";
import EditCanvas from "../components/EditCanvas.vue";
import EditInspector from "../components/EditInspector.vue";
import TaskPalette from "../components/TaskPalette.vue";
import TopBar from "../components/TopBar.vue";
import { addTask } from "../lib/edit-graph";
import { fileOf, shortRun } from "../lib/format";
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

// the last part of the breadcrumb: the file of a session, or the run of a rerun
const crumb = computed(() => {
  if (props.run) return `rerun of #${shortRun(props.id)}`;
  const path = edit.session.value?.path;
  return path ? fileOf(path).name : null;
});

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

const router = useRouter();
// the request of the bar which waits, and why the last one failed
const saving = ref(false);
const running = ref(false);
const failure = ref<string | null>(null);

const errorCount = computed(() => edit.errors.value.length + Object.values(edit.taskErrors.value).reduce((n, list) => n + list.length, 0));

function describe(cause: unknown): string {
  if (cause instanceof ApiError) return cause.message;
  return cause instanceof Error ? cause.message : String(cause);
}

async function save(force: boolean): Promise<void> {
  saving.value = true;
  failure.value = null;
  try {
    await edit.save(force);
  } catch (cause) {
    failure.value = describe(cause);
  } finally {
    saving.value = false;
  }
}

async function runDraft(): Promise<void> {
  running.value = true;
  failure.value = null;
  try {
    const summary = await edit.run({});
    await router.push({ name: "run", params: { id: summary.id } });
  } catch (cause) {
    failure.value = describe(cause);
  } finally {
    running.value = false;
  }
}

async function download(): Promise<void> {
  failure.value = null;
  try {
    await edit.download();
  } catch (cause) {
    failure.value = describe(cause);
  }
}

let controller: AbortController | null = null;

watch(
  () => [props.id, props.run] as const,
  ([id, run]) => {
    selected.value = null;
    failure.value = null;
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
      :workflow="crumb"
      :crumb-title="run ? null : (edit.session.value?.path ?? null)"
      mode="editor"
      :session="run ? null : id"
    />
    <div class="flex min-h-0 flex-1">
      <aside data-palette class="w-[260px] shrink-0 border-r border-gray-200 bg-white">
        <TaskPalette :types="edit.types.value" @add="add">
          <template v-if="run" #footer>The workflow of run <span class="font-mono text-gray-700">#{{ shortRun(id) }}</span></template>
          <template v-else-if="edit.session.value" #footer>
            Saves to <span class="font-mono text-gray-700" :title="edit.session.value.path">{{ fileOf(edit.session.value.path).name }}</span> on
            {{ edit.session.value.host }}
          </template>
        </TaskPalette>
      </aside>
      <main data-canvas class="relative flex min-w-0 flex-1 flex-col bg-gray-50">
        <EditBar
          v-if="edit.draft.value"
          :tree="tree"
          :host="edit.session.value?.host ?? null"
          :connected="edit.connected.value"
          :valid="edit.valid.value"
          :error-count="errorCount"
          :dirty="edit.dirty.value"
          :changed-on-disk="edit.changedOnDisk.value"
          :saving="saving"
          :running="running"
          :error="failure"
          :run="run ? id : null"
          @save="save"
          @reload="edit.reload()"
          @run="runDraft"
          @download="download"
        >
          <p
            v-if="refused"
            data-refused
            class="pointer-events-auto self-center rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-800 shadow-sm ring-1 ring-amber-200"
          >
            {{ refused }}
          </p>
        </EditBar>
        <p v-if="edit.loadError.value" data-load-error class="m-4 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">
          {{ edit.loadError.value }}
        </p>
        <EditCanvas
          v-if="edit.draft.value"
          :key="`${id}:${edit.loaded.value}`"
          ref="canvas"
          v-model:selected="selected"
          :workflow="edit.draft.value"
          :types="edit.types.value"
          :estimate="edit.estimate.value"
          :task-errors="edit.taskErrors.value"
          @change="(workflow) => ((refused = null), edit.change(workflow))"
          @refused="(message) => (refused = message)"
        />
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
