<!--
  The edit sessions of the caller (`#/edit`): the files `simrun edit` has open,
  connected first. Without one, how to start it.
-->
<script setup lang="ts">
import { onMounted, onUnmounted, ref } from "vue";
import { api, type EditSession } from "../api/client";
import { auth } from "../auth";
import TopBar from "../components/TopBar.vue";
import { useLive } from "../stores/live";

const live = useLive();
const sessions = ref<EditSession[] | null>(null);
const failed = ref<string | null>(null);
let timer: ReturnType<typeof setInterval> | undefined;

async function load(): Promise<void> {
  try {
    sessions.value = await api.edits();
    failed.value = null;
  } catch (error) {
    failed.value = error instanceof Error ? error.message : String(error);
  }
}

// the list has no stream: connected dots follow a refresh every few seconds
onMounted(() => {
  void load();
  timer = setInterval(() => void load(), 5000);
});
onUnmounted(() => clearInterval(timer));
</script>

<template>
  <div class="flex h-full flex-col bg-white text-gray-900">
    <TopBar
      :identity="auth.identity.value"
      :runs="live.runs.value"
      :servers="live.servers.value"
      :connected="live.connected.value"
      :workflow="null"
      mode="editor"
    />
    <main class="min-h-0 flex-1 overflow-y-auto bg-gray-50">
      <div class="mx-auto max-w-3xl px-4 py-8">
        <h1 class="text-base font-semibold">Edit sessions</h1>
        <p class="mt-1 text-xs text-gray-500">The workflow files which <span class="font-mono">simrun edit</span> has open for you.</p>
        <p v-if="failed" data-failed class="mt-4 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">{{ failed }}</p>
        <ul v-if="sessions && sessions.length > 0" class="mt-4 divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white">
          <li v-for="s in sessions" :key="s.id" data-session>
            <a :href="`#/edit/${s.id}`" class="flex items-center gap-3 px-4 py-3 hover:bg-gray-50">
              <span
                data-connected
                class="size-2 shrink-0 rounded-full"
                :class="s.connected ? 'bg-emerald-500' : 'bg-gray-300'"
                :title="s.connected ? 'connected' : 'not connected'"
              ></span>
              <span class="min-w-0 flex-1">
                <span class="block truncate font-mono text-sm">{{ s.path }}</span>
                <span class="block text-xs text-gray-500">{{ s.host }}</span>
              </span>
            </a>
          </li>
        </ul>
        <div v-else-if="sessions" data-empty class="mt-4 rounded-lg border border-dashed border-gray-300 bg-white px-4 py-6 text-center">
          <div class="text-sm font-medium text-gray-700">No edit session</div>
          <div class="mt-2 text-xs text-gray-500">Start one on the machine with the working trees:</div>
          <code class="mt-2 inline-block rounded-md bg-gray-100 px-2 py-1 font-mono text-xs">simrun edit &lt;workflow.yaml&gt;</code>
          <div class="mt-3 text-xs text-gray-500">More in <span class="font-mono">docs/web-app.md#the-editor</span>.</div>
        </div>
      </div>
    </main>
  </div>
</template>
