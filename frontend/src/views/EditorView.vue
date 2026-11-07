<!--
  The Editor: the draft of a workflow of an edit session (`#/edit/:id`) or of a
  run (`#/runs/:id/edit`). The palette, the canvas, the inspector and the bar
  plug into the three columns; this view owns the draft and the page chrome.
-->
<script setup lang="ts">
import { onBeforeUnmount, watch } from "vue";
import { auth } from "../auth";
import TopBar from "../components/TopBar.vue";
import { useEdit } from "../stores/edit";
import { useLive } from "../stores/live";

const props = defineProps<{ id: string; run?: boolean }>();

const live = useLive();
const edit = useEdit();

let controller: AbortController | null = null;

watch(
  () => [props.id, props.run] as const,
  ([id, run]) => {
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
      <aside data-palette class="w-[260px] shrink-0 border-r border-gray-200 bg-white"></aside>
      <main data-canvas class="relative flex min-w-0 flex-1 flex-col bg-gray-50">
        <p v-if="edit.loadError.value" data-load-error class="m-4 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">
          {{ edit.loadError.value }}
        </p>
      </main>
      <aside data-inspector class="w-[352px] shrink-0 border-l border-gray-200 bg-white"></aside>
    </div>
  </div>
</template>
