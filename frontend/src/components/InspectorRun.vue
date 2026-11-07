<!--
  The run in the inspector while no task is selected: its message, who
  submitted it with which priority and when, its kills for memory, and its
  sources, the snapshots of the working trees with their branches, commits
  and changed files (the sources of the mockup).
-->
<script setup lang="ts">
import { computed } from "vue";
import type { RunRecord } from "../api/client";
import { formatTime } from "../lib/format";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{ record: RunRecord }>();

const failed = computed(() => props.record.state === "failed");

const details = computed(() => {
  const r = props.record;
  const who = r.agent === null ? r.submitted_by : `${r.agent} for ${r.owner}`;
  const rows: { label: string; value: string; mono?: boolean }[] = [
    { label: "Run", value: `#${r.id}`, mono: true },
    { label: "Submitted by", value: who },
    { label: "Priority", value: r.priority },
    { label: "Created", value: formatTime(r.created) },
  ];
  if (r.built) rows.push({ label: "Built", value: formatTime(r.built) });
  if (r.finished) rows.push({ label: "Finished", value: formatTime(r.finished) });
  if (r.group) rows.push({ label: "Group", value: r.group });
  if (r.image) rows.push({ label: "Image", value: r.image, mono: true });
  return rows;
});

/** The workers killed for memory, with their attempts counted from 1. */
const kills = computed(() =>
  Object.entries(props.record.oom_kills ?? {})
    .filter(([, attempts]) => attempts.length > 0)
    .map(([worker, attempts]) => {
      const numbers = attempts.map((a) => String(a + 1));
      const listed = numbers.length === 1 ? `attempt ${numbers[0]}` : `attempts ${numbers.slice(0, -1).join(", ")} and ${numbers.at(-1)}`;
      return `${worker}: ${listed}`;
    }),
);

const short = (commit: string): string => commit.slice(0, 7);
</script>

<template>
  <div data-run class="min-h-0 flex-1 space-y-4 overflow-y-auto px-4 py-4 text-sm">
    <div
      v-if="record.message"
      data-message
      class="rounded-lg px-3 py-2 text-xs leading-snug"
      :class="failed ? 'bg-red-50 text-red-700' : 'bg-gray-50 text-gray-600'"
    >
      {{ record.message }}
    </div>

    <div class="rounded-lg border border-gray-200 bg-gray-50/70 p-3 text-xs leading-relaxed text-gray-600">
      <div v-for="d in details" :key="d.label" :data-detail="d.label" class="flex justify-between gap-3">
        <span class="shrink-0">{{ d.label }}</span>
        <b
          class="min-w-0 text-gray-900"
          :class="d.mono ? 'truncate-start font-mono font-medium' : 'truncate font-semibold'"
          :title="d.value"
          ><bdi>{{ d.value }}</bdi></b
        >
      </div>
    </div>

    <div v-if="kills.length" class="space-y-1">
      <div class="text-xs font-semibold text-gray-700">Killed for memory</div>
      <div v-for="k in kills" :key="k" data-oom class="font-mono text-[11px] text-red-700">{{ k }}</div>
    </div>

    <div class="space-y-2">
      <div class="text-xs font-semibold text-gray-700">Sources, snapshot of the working trees</div>
      <div v-if="record.sources.length === 0" data-no-sources class="text-xs text-gray-400 italic">
        No sources, the workflow runs on the image alone.
      </div>
      <div v-else class="rounded-lg border border-gray-200">
        <div
          v-for="src in record.sources"
          :key="src.name"
          :data-source="src.name"
          class="border-b border-gray-100 px-3 py-2 last:border-b-0"
        >
          <div class="flex items-center justify-between gap-2 text-xs">
            <span class="truncate font-medium" :title="src.name">{{ src.name }}</span>
            <span data-branch class="inline-flex shrink-0 items-center gap-1 font-mono text-gray-600" :title="src.commit"
              ><AppIcon name="git-branch" class="size-3" />{{ src.branch ?? "detached" }}@{{ short(src.commit) }}</span
            >
          </div>
          <div v-if="src.changed_files.length" class="mt-1 space-y-0.5">
            <div
              v-for="f in src.changed_files"
              :key="f"
              data-changed
              class="flex items-center gap-1.5 font-mono text-[11px] text-amber-800"
              :title="f"
            >
              <AppIcon name="file-diff" class="size-3 shrink-0" /><span class="truncate">{{ f }}</span>
            </div>
          </div>
          <div v-else data-clean class="mt-0.5 text-[11px] text-gray-400">clean</div>
        </div>
      </div>
    </div>

    <div data-hint class="text-xs text-gray-500">Select a task on the canvas for its workers, outputs and logs.</div>
  </div>
</template>
