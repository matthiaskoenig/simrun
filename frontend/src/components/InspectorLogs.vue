<!--
  The logs of a task in the inspector, the dark log block of the mockup: the
  last lines of every pod under its header, read `REFRESH_MS` after the last
  answer while the task is live and once more when it ends. The block stays at its bottom as
  lines come, unless the user scrolled up. The lines of Argo's executor
  (`argo=true`) are hidden unless asked for, errors and tracebacks stand out,
  and lines keep their layout, scrolled sideways instead of wrapped, unless
  "wrap lines" breaks them at the width of the panel, e.g. to read a long line
  of a narrow inspector without scrolling.
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

// the lines Argo's executor writes into the main container, e.g. `time=... level=INFO msg="..." argo=true`
const EXECUTOR = /\bargo=true\b/;
// a traceback, an exception, a line logged as an error
const ERROR = /^(Traceback |[\w.]*(Error|Exception)\b.*:|Error: )|\b(ERROR|CRITICAL)\b/;

const showExecutor = ref(false);
const wrap = ref(false);

/** Every line of the logs, the headers of the pods, the executor and the error lines marked. */
const all = computed(() =>
  (text.value ?? "")
    .replace(/\n$/, "")
    .split("\n")
    .map((line) => ({ line, header: /^==> .* <==$/.test(line), executor: EXECUTOR.test(line), error: ERROR.test(line) })),
);
const hidden = computed(() => all.value.filter((l) => l.executor).length);
const lines = computed(() => (showExecutor.value ? all.value : all.value.filter((l) => !l.executor)));

function lineClass(l: { header: boolean; executor: boolean; error: boolean }): string {
  if (l.header) return "text-gray-500";
  if (l.error) return "text-red-300";
  return l.executor ? "text-gray-600" : "";
}
</script>

<template>
  <div class="flex min-h-0 flex-1 flex-col bg-gray-950">
    <div v-if="text" data-logs-bar class="flex items-center justify-end gap-4 border-b border-gray-800 px-3 py-1.5">
      <label v-if="hidden > 0" class="flex items-center gap-1.5 text-[11px] text-gray-400">
        <input v-model="showExecutor" data-executor type="checkbox" class="size-3 accent-gray-400" />
        lines of the Argo executor ({{ hidden }})
      </label>
      <label class="flex items-center gap-1.5 text-[11px] text-gray-400">
        <input v-model="wrap" data-wrap type="checkbox" class="size-3 accent-gray-400" />
        wrap lines
      </label>
    </div>
    <div
      ref="block"
      data-logs
      class="min-h-0 flex-1 overflow-auto px-3 py-3 font-mono text-[11px] leading-relaxed text-gray-300"
      @scroll="onScroll"
    >
      <pre
        v-if="text"
        data-logs-text
        :class="['font-mono', wrap ? 'break-all whitespace-pre-wrap' : 'w-max min-w-full whitespace-pre']"
      ><template v-for="(l, i) in lines" :key="i"><span :class="lineClass(l)">{{ l.line }}</span>{{ i < lines.length - 1 ? "\n" : "" }}</template></pre>
      <div v-else-if="text === '' || (text === null && !error)" class="text-gray-500">
        {{ text === null ? "reading the logs" : "no output yet" }}
      </div>
      <div v-if="error" data-logs-error class="text-red-400">{{ error }}</div>
    </div>
  </div>
</template>
