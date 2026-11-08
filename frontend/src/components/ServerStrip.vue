<!--
  The servers below the canvas: per server the cores and memory its pods request
  against those the scheduler gives the pods of runs, those of the selected run in blue.
-->
<script setup lang="ts">
import { computed } from "vue";
import type { ServerUsage } from "../api/client";
import { formatCores, formatGb, serverLabel } from "../lib/format";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{ servers: ServerUsage[]; selected: string | null }>();

const tiles = computed(() =>
  props.servers.map((server) => ({
    server,
    mine: props.selected !== null && server.pods.some((pod) => pod.run_id === props.selected),
    cores: server.pods.reduce((sum, pod) => sum + pod.cores, 0),
    memory: server.pods.reduce((sum, pod) => sum + pod.memory_gb, 0),
    title: server.reserved_by === null ? server.name : `${server.name}: reserved by run ${server.reserved_by}`,
    segments: server.pods.map((pod) => ({
      pod,
      width: server.cores > 0 ? (pod.cores / server.cores) * 100 : 0,
      color: pod.run_id !== null && pod.run_id === props.selected ? "bg-blue-500" : "bg-slate-300",
      title: `${pod.run_id === null ? pod.pod : `#${pod.run_id} ${pod.task_id}`}: ${formatCores(pod.cores)} cores, ${formatGb(pod.memory_gb)}, ${pod.phase}`,
    })),
  })),
);
</script>

<template>
  <section class="@container shrink-0 border-t border-gray-200 bg-white px-3 pt-2 pb-3">
    <div class="mb-2 flex items-center justify-between">
      <div class="flex items-center gap-2 text-xs font-semibold text-gray-700">
        <AppIcon name="server" class="size-3.5" />Servers
      </div>
      <div data-legend class="flex items-center gap-3 text-[11px] text-gray-500">
        <span v-if="selected !== null" class="inline-flex items-center gap-1"
          ><span class="size-2.5 rounded-sm bg-blue-500"></span>run #{{ selected }}</span
        >
        <span class="inline-flex items-center gap-1"><span class="size-2.5 rounded-sm bg-slate-300"></span>other runs</span>
        <span class="inline-flex items-center gap-1"><span class="stripes size-2.5 rounded-sm"></span>reserved</span>
      </div>
    </div>
    <!-- six tiles in a row as in the mockup while a tile keeps 120 px, else four or three (the inspector narrows the canvas) -->
    <div v-if="tiles.length > 0" data-tiles class="grid grid-cols-3 gap-2 @[32rem]:grid-cols-4 @[48rem]:grid-cols-6">
      <div
        v-for="tile in tiles"
        :key="tile.server.name"
        :data-server="tile.server.name"
        :title="tile.title"
        class="min-w-0 rounded-md border px-2 py-1.5"
        :class="tile.mine ? 'border-blue-200 bg-blue-50/40' : 'border-gray-200'"
      >
        <!-- a long name pushes the capacity to a line of its own instead of being cut -->
        <div class="flex flex-wrap items-baseline justify-between gap-x-1">
          <!-- names of a cluster share their start (`k3d-simrun-test-agent-0`): the label keeps their end -->
          <span data-name class="min-w-0 truncate font-mono text-[11px] font-semibold text-gray-800">{{
            serverLabel(tile.server.name)
          }}</span>
          <span data-capacity class="shrink-0 text-[10px] text-gray-400"
            >{{ formatCores(tile.server.cores) }} c · {{ formatGb(tile.server.memory_gb) }}</span
          >
        </div>
        <div data-bar class="mt-1 flex h-2 overflow-hidden rounded-sm" :class="tile.server.reserved_by === null ? 'bg-gray-100' : 'stripes'">
          <div
            v-for="segment in tile.segments"
            :key="segment.pod.pod"
            :data-pod="segment.pod.pod"
            :title="segment.title"
            class="h-full shrink-0 border-r border-white last:border-r-0"
            :class="segment.color"
            :style="{ width: segment.width + '%' }"
          ></div>
        </div>
        <div class="mt-1 flex justify-between gap-1 text-[10px] whitespace-nowrap text-gray-500 tabular-nums">
          <span data-used-cores>{{ formatCores(tile.cores) }}/{{ formatCores(tile.server.cores) }} cores</span
          ><span data-used-memory>{{ formatGb(tile.memory) }}</span>
        </div>
      </div>
    </div>
    <div v-else class="text-xs text-gray-500">No servers reported by the cluster.</div>
  </section>
</template>
