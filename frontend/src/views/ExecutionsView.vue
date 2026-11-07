<!--
  The executions: the runs on the left, the canvas of the selected run in the
  center with its run bar above and the servers below, from the live streams
  of the service, the inspector of the run or its selected task on the right.
  The route `/runs/:id` selects a run, a click on a task of the canvas
  selects the task for the inspector.
-->
<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { auth } from "../auth";
import AppIcon from "../components/AppIcon.vue";
import Inspector from "../components/Inspector.vue";
import RunBar from "../components/RunBar.vue";
import RunCanvas from "../components/RunCanvas.vue";
import RunList from "../components/RunList.vue";
import ServerStrip from "../components/ServerStrip.vue";
import TopBar from "../components/TopBar.vue";
import { useLive } from "../stores/live";
import { useRun } from "../stores/run";

const props = defineProps<{ id?: string }>();

const router = useRouter();
const live = useLive();

const selectedId = computed(() => props.id ?? null);
const selectedRun = computed(() => live.runs.value.find((r) => r.id === selectedId.value) ?? null);
const run = useRun(selectedId);

/** The task selected on the canvas, which the inspector shows; another run clears it. */
const selectedTask = ref<string | null>(null);
watch(selectedId, () => (selectedTask.value = null));

function select(id: string): void {
  void router.push({ name: "run", params: { id } });
}
</script>

<template>
  <div class="flex h-full flex-col bg-white text-gray-900">
    <TopBar
      :identity="auth.identity.value"
      :runs="live.runs.value"
      :servers="live.servers.value"
      :connected="live.connected.value"
      :workflow="selectedRun?.workflow ?? null"
    />
    <div class="flex min-h-0 flex-1">
      <RunList :runs="live.runs.value" :selected="selectedId" :identity="auth.identity.value" @select="select" />

      <main class="relative flex min-w-0 flex-1 flex-col">
        <RunCanvas
          v-if="run.record.value"
          :key="run.record.value.id"
          v-model:selected="selectedTask"
          :record="run.record.value"
          :progress="run.progress.value"
          :workers="run.workers.value"
        >
          <RunBar :record="run.record.value" :progress="run.progress.value" />
        </RunCanvas>
        <div v-else data-canvas class="relative grid min-h-0 flex-1 place-items-center bg-gray-50">
          <div v-if="selectedId === null" class="text-center">
            <div class="text-sm font-medium text-gray-700">Select a run</div>
            <div class="mt-1 text-xs text-gray-500">Its tasks and their progress show here.</div>
          </div>
          <div v-else-if="run.notFound.value" data-not-found class="text-center">
            <div class="text-sm font-medium text-gray-700">Run not found</div>
            <div class="mt-1 text-xs text-gray-500">
              The service knows no run <span class="font-mono">#{{ selectedId }}</span>; it may have been deleted.
            </div>
          </div>
          <div v-else class="inline-flex items-center gap-2 text-xs text-gray-500">
            <AppIcon name="loader-circle" class="size-3.5 animate-spin" />Loading run
            <span class="font-mono">#{{ selectedId }}</span>
          </div>
        </div>
        <ServerStrip :servers="live.servers.value" :selected="run.notFound.value ? null : selectedId" />
      </main>

      <Inspector
        v-if="run.record.value"
        :record="run.record.value"
        :progress="run.progress.value"
        :workers="run.workers.value"
        :task="selectedTask"
      />
      <!-- the column of the inspector while the run loads, so that the canvas keeps its width -->
      <aside
        v-else-if="selectedId !== null && !run.notFound.value"
        class="w-[352px] shrink-0 border-l border-gray-200 bg-white"
        aria-hidden="true"
      ></aside>
    </div>
  </div>
</template>
