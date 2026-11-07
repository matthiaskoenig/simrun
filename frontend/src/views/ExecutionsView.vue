<!--
  The executions: the runs on the left, the selected run in the center with
  the servers below, from the live stream of the service. The route
  `/runs/:id` selects a run.
-->
<script setup lang="ts">
import { computed } from "vue";
import { useRouter } from "vue-router";
import { auth } from "../auth";
import RunList from "../components/RunList.vue";
import ServerStrip from "../components/ServerStrip.vue";
import StatusPill from "../components/StatusPill.vue";
import TopBar from "../components/TopBar.vue";
import { useLive } from "../stores/live";

const props = defineProps<{ id?: string }>();

const router = useRouter();
const live = useLive();

const selectedId = computed(() => props.id ?? null);
const selectedRun = computed(() => live.runs.value.find((r) => r.id === selectedId.value) ?? null);

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
        <!-- the canvas of the selected run with its run bar (Task 7) -->
        <div data-canvas class="relative grid min-h-0 flex-1 place-items-center bg-gray-50">
          <div v-if="selectedId === null" class="text-center">
            <div class="text-sm font-medium text-gray-700">Select a run</div>
            <div class="mt-1 text-xs text-gray-500">Its tasks and their progress show here.</div>
          </div>
          <div v-else class="flex flex-col items-center gap-1.5 text-center">
            <div class="flex items-center gap-2">
              <span class="text-sm font-semibold text-gray-900">Run #{{ selectedId }}</span>
              <StatusPill v-if="selectedRun" :status="selectedRun.state" small />
            </div>
            <div v-if="selectedRun" class="text-xs text-gray-500">{{ selectedRun.workflow }}</div>
          </div>
        </div>
        <ServerStrip :servers="live.servers.value" :selected="selectedId" />
      </main>

      <!-- the inspector of the selected task, w-[380px] (Task 8) -->
    </div>
  </div>
</template>
