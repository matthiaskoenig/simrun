<!-- The left column of the executions: the filter chips, the group and a card per run. -->
<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { Identity, RunItem } from "../api/client";
import { filterRuns, groupsOf, RUN_FILTERS, type RunFilter } from "../lib/filters";
import RunCard from "./RunCard.vue";

const props = defineProps<{ runs: RunItem[]; selected: string | null; identity: Identity | null }>();
defineEmits<{ select: [id: string] }>();

const filter = ref<RunFilter>("all");
const group = ref<string | null>(null);

const groups = computed(() => groupsOf(props.runs));
// a group whose runs left the list is no longer offered, nor kept
watch(groups, (names) => {
  if (group.value !== null && !names.includes(group.value)) group.value = null;
});

const visible = computed(() => filterRuns(props.runs, filter.value, props.identity, group.value));
</script>

<template>
  <aside class="flex w-64 shrink-0 flex-col border-r border-gray-200 bg-gray-50/60">
    <div class="border-b border-gray-200 p-3">
      <div class="mb-2 text-sm font-semibold">Executions</div>
      <div class="flex flex-wrap gap-1">
        <button
          v-for="f in RUN_FILTERS"
          :key="f.id"
          type="button"
          :data-filter="f.id"
          class="rounded-full border px-2.5 py-0.5 text-xs"
          :class="
            filter === f.id
              ? 'border-gray-900 bg-gray-900 text-white'
              : 'border-gray-200 bg-white text-gray-600 hover:border-gray-300'
          "
          :aria-pressed="filter === f.id"
          @click="filter = f.id"
        >
          {{ f.label }}
        </button>
      </div>
      <select
        v-if="groups.length > 0"
        v-model="group"
        name="group"
        aria-label="Group"
        class="mt-2 w-full rounded-md border border-gray-200 bg-white px-2 py-1 text-xs text-gray-700"
      >
        <option :value="null">All groups</option>
        <option v-for="name in groups" :key="name" :value="name">{{ name }}</option>
      </select>
    </div>
    <div class="flex-1 overflow-y-auto p-2">
      <RunCard
        v-for="run in visible"
        :key="run.id"
        :run="run"
        :selected="run.id === selected"
        @select="$emit('select', $event)"
      />
      <div v-if="visible.length === 0" class="px-2.5 py-2 text-xs text-gray-500">
        {{ runs.length === 0 ? "No runs yet." : "No runs match the filter." }}
      </div>
    </div>
  </aside>
</template>
