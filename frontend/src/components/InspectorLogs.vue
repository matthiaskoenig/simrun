<!--
  The logs of a task in the inspector, the dark log block of the mockup: the
  last lines of every pod under its header, read `REFRESH_MS` after the last
  answer while the task is live and once more when it ends. The block stays at its bottom as
  lines come, unless the user scrolled up.
-->
<script setup lang="ts">
import { computed, nextTick, onScopeDispose, ref, watch } from "vue";
import { api, ApiError } from "../api/client";

const props = defineProps<{ runId: string; taskId: string; live: boolean }>();

const REFRESH_MS = 5000;
// how close to its bottom the block counts as at its bottom
const BOTTOM_PX = 16;

const text = ref<string | null>(null);
const error = ref<string | null>(null);
const block = ref<HTMLElement | null>(null);
let stick = true;

// one request at a time: the next is due `REFRESH_MS` after the answer to the
// last, since the service may take longer than that to read the logs of every
// pod; another task or the end of a task starts a new generation, whose
// answers alone count
let generation = 0;
let timer: ReturnType<typeof setTimeout> | null = null;

async function load(own: number): Promise<void> {
  try {
    const found = await api.logs(props.runId, props.taskId);
    if (own !== generation) return;
    text.value = found;
    error.value = null;
  } catch (cause) {
    if (own !== generation) return;
    error.value = cause instanceof ApiError ? cause.message : "The service cannot be reached.";
  }
  if (stick) {
    await nextTick();
    if (block.value) block.value.scrollTop = block.value.scrollHeight;
  }
}

/** Reads the logs, and again `REFRESH_MS` after each answer while `repeat`. */
async function poll(own: number, repeat: boolean): Promise<void> {
  await load(own);
  if (repeat && own === generation) timer = setTimeout(() => void poll(own, true), REFRESH_MS);
}

/** Ends the reads of the current generation. */
function stop(): void {
  if (timer !== null) clearTimeout(timer);
  timer = null;
  generation += 1;
}

function onScroll(): void {
  const el = block.value;
  if (el) stick = el.scrollHeight - el.scrollTop - el.clientHeight <= BOTTOM_PX;
}

// another task is read at once; the logs a task left when it ended, which
// the service keeps, are read once more
watch(
  () => [props.runId, props.taskId, props.live] as const,
  ([runId, taskId, live], old) => {
    stop();
    const own = generation;
    if (old === undefined || old[0] !== runId || old[1] !== taskId) {
      text.value = null;
      error.value = null;
      stick = true;
      void poll(own, live);
    } else if (live !== old[2]) {
      void poll(own, live);
    }
  },
  { immediate: true },
);
onScopeDispose(stop);

/** The lines of the logs, the headers of the pods marked. */
const lines = computed(() =>
  (text.value ?? "")
    .replace(/\n$/, "")
    .split("\n")
    .map((line) => ({ line, header: /^==> .* <==$/.test(line) })),
);
</script>

<template>
  <div
    ref="block"
    data-logs
    class="min-h-0 flex-1 overflow-y-auto bg-gray-950 px-3 py-3 font-mono text-[11px] leading-relaxed text-gray-300"
    @scroll="onScroll"
  >
    <pre
      v-if="text"
      data-logs-text
      class="font-mono break-words whitespace-pre-wrap"
    ><template v-for="(l, i) in lines" :key="i"><span :class="l.header ? 'text-gray-500' : ''">{{ l.line }}</span>{{ i < lines.length - 1 ? "\n" : "" }}</template></pre>
    <div v-else-if="text === '' || (text === null && !error)" class="text-gray-500">
      {{ text === null ? "reading the logs" : "no output yet" }}
    </div>
    <div v-if="error" data-logs-error class="text-red-400">{{ error }}</div>
  </div>
</template>
