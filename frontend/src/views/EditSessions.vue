<!--
  The edit sessions of the caller (`#/edit`): the files `simrun edit` has open,
  by file name with the directory dimmed, the connected ones first and the
  others under "Closed": the ones of the last day, the older ones behind
  "Show older". Without one, how to start it.
-->
<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { api, type EditSession } from "../api/client";
import { auth } from "../auth";
import TopBar from "../components/TopBar.vue";
import { fileOf, formatAgo } from "../lib/format";
import { useLive } from "../stores/live";

const live = useLive();
const sessions = ref<EditSession[] | null>(null);
const failed = ref<string | null>(null);
let timer: ReturnType<typeof setInterval> | undefined;

/** A closed session seen longer ago than this is behind "Show older". */
const RECENT_MS = 24 * 3600 * 1000;
const showOlder = ref(false);

const closed = computed(() => (sessions.value ?? []).filter((s) => !s.connected));
const older = computed(() => closed.value.filter((s) => Date.now() - Date.parse(s.seen) > RECENT_MS));

// the connected sessions, then the others under "Closed"; a section without sessions is left out
const sections = computed(() =>
  [
    { key: "open", items: (sessions.value ?? []).filter((s) => s.connected) },
    { key: "closed", items: showOlder.value ? closed.value : closed.value.filter((s) => !older.value.includes(s)) },
  ].filter((section) => section.items.length > 0 || (section.key === "closed" && older.value.length > 0)),
);

/** The host, the workflow and the state of a session, `laptop · fit · connected`. */
function meta(session: EditSession): string {
  const state = session.connected
    ? "connected"
    : session.closed
      ? `closed ${formatAgo(session.seen)}`
      : `not connected since ${formatAgo(session.seen).replace(" ago", "")}`;
  return [session.host, session.workflow.name, state].join(" · ");
}

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
        <template v-if="sessions && sessions.length > 0">
          <template v-for="section in sections" :key="section.key">
            <h2 v-if="section.key === 'closed'" data-closed-title class="mt-6 text-xs font-semibold tracking-wide text-gray-500 uppercase">Closed</h2>
            <ul
              v-if="section.items.length > 0"
              :data-section="section.key"
              class="divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white"
              :class="section.key === 'closed' ? 'mt-2' : 'mt-4'"
            >
              <li v-for="s in section.items" :key="s.id" data-session :class="s.connected ? '' : 'opacity-60'">
                <a :href="`#/edit/${s.id}`" class="flex items-center gap-3 px-4 py-3 hover:bg-gray-50">
                  <span
                    data-connected
                    class="size-2 shrink-0 rounded-full"
                    :class="s.connected ? 'bg-emerald-500' : 'bg-gray-300'"
                    :title="s.connected ? 'connected' : 'not connected'"
                  ></span>
                  <span class="min-w-0 flex-1">
                    <span class="flex min-w-0 items-baseline gap-2">
                      <span data-file class="shrink-0 font-mono text-sm font-medium text-gray-900">{{ fileOf(s.path).name }}</span>
                      <!-- truncated from the start, so the end of the directory stays -->
                      <span data-dir class="min-w-0 truncate font-mono text-xs text-gray-400 [direction:rtl]" :title="s.path"
                        ><bdi>{{ fileOf(s.path).dir }}</bdi></span
                      >
                    </span>
                    <span data-meta class="mt-0.5 block text-xs text-gray-500">{{ meta(s) }}</span>
                  </span>
                </a>
              </li>
            </ul>
            <button
              v-if="section.key === 'closed' && older.length > 0"
              data-older
              type="button"
              class="mt-2 text-xs text-gray-500 hover:text-gray-800"
              @click="showOlder = !showOlder"
            >
              {{ showOlder ? "Hide older" : `Show ${older.length} older` }}
            </button>
          </template>
        </template>
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
