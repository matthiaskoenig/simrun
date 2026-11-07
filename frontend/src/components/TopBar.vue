<!-- The top bar of the mockup: the logo and where the user is, the capacity of the cluster, the user. -->
<script setup lang="ts">
import { computed } from "vue";
import type { Identity, RunItem, ServerUsage } from "../api/client";
import { formatCores, initials } from "../lib/format";
import AppIcon from "./AppIcon.vue";
import LogoMark from "./LogoMark.vue";

const props = defineProps<{
  identity: Identity | null;
  runs: RunItem[];
  servers: ServerUsage[];
  connected: boolean;
  /** The workflow of the selected run, null without one. */
  workflow: string | null;
}>();

const cluster = computed(() => ({
  used: props.servers.reduce((sum, s) => sum + s.pods.reduce((cores, pod) => cores + pod.cores, 0), 0),
  total: props.servers.reduce((sum, s) => sum + s.cores, 0),
  queued: props.runs.filter((r) => r.state === "queued").length,
}));
</script>

<template>
  <header class="flex h-12 shrink-0 items-center gap-4 border-b border-gray-200 px-4">
    <nav class="flex min-w-0 items-center gap-2" aria-label="Breadcrumbs">
      <LogoMark />
      <span class="text-sm font-semibold">simrun</span>
      <AppIcon name="chevron-right" class="size-3.5 text-gray-300" />
      <span class="text-sm" :class="workflow === null ? 'font-medium' : 'text-gray-500'">Executions</span>
      <template v-if="workflow !== null">
        <AppIcon name="chevron-right" class="size-3.5 text-gray-300" />
        <span class="truncate text-sm font-medium">{{ workflow }}</span>
      </template>
    </nav>
    <div class="ml-auto flex shrink-0 items-center gap-3">
      <span
        v-if="!connected"
        data-reconnecting
        class="inline-flex items-center gap-1 rounded-md bg-amber-50 px-2 py-1 text-xs font-medium text-amber-800 ring-1 ring-amber-200"
        title="The connection to the service dropped; the app reconnects"
        ><AppIcon name="loader-circle" class="size-3 animate-spin" />reconnecting</span
      >
      <div
        data-capacity
        class="flex items-center gap-2 rounded-md border border-gray-200 px-2.5 py-1 text-xs text-gray-600"
        title="All servers of the cluster"
      >
        <AppIcon name="server" class="size-3.5" />
        <span class="tabular-nums"
          ><b class="font-semibold text-gray-900">{{ formatCores(cluster.used) }}</b>/{{ cluster.total }} cores</span
        >
        <span class="text-gray-300">|</span>
        <span class="tabular-nums">{{ cluster.queued }} queued</span>
      </div>
      <span
        v-if="identity"
        data-avatar
        class="grid size-7 place-items-center rounded-full text-xs font-semibold"
        :class="identity.kind === 'agent' ? 'bg-violet-100 text-violet-700' : 'bg-blue-600 text-white'"
        :title="identity.kind === 'agent' ? `${identity.name}, an agent of ${identity.owner}` : identity.name"
        >{{ initials(identity.name) }}</span
      >
    </div>
  </header>
</template>
