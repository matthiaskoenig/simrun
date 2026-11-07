<!--
  The workflow panel of the Editor inspector, shown with no task selected:
  name, split, reservation of a server, the group of the run (kept by the
  Editor, not in the workflow) and the sources of the session or the run.
-->
<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import type { Workflow } from "../api/client";
import { api } from "../api/client";

/** A source tree as the panel shows it. */
export interface TreeInfo {
  name: string;
  branch: string | null;
  commit: string;
  changed_files: string[];
}

const props = defineProps<{ workflow: Workflow; group: string | null; tree: TreeInfo[]; errors: string[] }>();
const emit = defineEmits<{ change: [workflow: Workflow]; "update:group": [group: string | null] }>();

const INPUT = "w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm";

const servers = ref<string[]>([]);
onMounted(() => {
  api.servers().then(
    (loaded) => (servers.value = loaded.map((s) => s.name)),
    (error: unknown) => console.error("the servers could not be loaded: %o", error),
  );
});
// a reservation of a server which is not listed (gone, or not yet loaded) stays selectable
const reservable = computed(() => {
  const current = props.workflow.reserve;
  return current && !servers.value.includes(current) ? [current, ...servers.value] : servers.value;
});

function update(edit: (workflow: Workflow) => void): void {
  const next = JSON.parse(JSON.stringify(props.workflow)) as Workflow;
  edit(next);
  emit("change", next);
}
</script>

<template>
  <div class="space-y-5 px-4 py-4 text-sm">
    <ul v-if="errors.length" class="space-y-1 rounded-md bg-red-50 px-3 py-2 text-xs text-red-800 ring-1 ring-red-200">
      <li v-for="error in errors" :key="error">{{ error }}</li>
    </ul>
    <section class="space-y-3">
      <h3 class="text-xs font-semibold tracking-wide text-gray-500 uppercase">Workflow</h3>
      <label class="block">
        <span class="mb-1 block text-xs text-gray-600">Name</span>
        <input data-name :class="INPUT" :value="workflow.name" @input="(e) => update((w) => (w.name = (e.target as HTMLInputElement).value))" />
      </label>
      <label class="block">
        <span class="mb-1 block text-xs text-gray-600">Split</span>
        <select data-split :class="INPUT" :value="workflow.split" @change="(e) => update((w) => (w.split = (e.target as HTMLSelectElement).value as Workflow['split']))">
          <option value="fastest">fastest</option>
          <option value="single-server">single server</option>
        </select>
      </label>
      <label class="block">
        <span class="mb-1 block text-xs text-gray-600">Reserve a whole server <span class="text-gray-400">(benchmarks)</span></span>
        <select data-reserve :class="INPUT" :value="workflow.reserve ?? ''" @change="(e) => update((w) => (w.reserve = (e.target as HTMLSelectElement).value || null))">
          <option value="">none</option>
          <option v-for="name in reservable" :key="name" :value="name">{{ name }}</option>
        </select>
      </label>
      <label class="block">
        <span class="mb-1 block text-xs text-gray-600">Group of the run</span>
        <input data-group :class="INPUT" :value="group ?? ''" placeholder="none" @input="(e) => emit('update:group', (e.target as HTMLInputElement).value || null)" />
      </label>
    </section>
    <section class="space-y-3">
      <h3 class="text-xs font-semibold tracking-wide text-gray-500 uppercase">Sources</h3>
      <p v-if="!tree.length" class="text-xs text-gray-400">No sources.</p>
      <div v-else class="rounded-lg border border-gray-200">
        <div v-for="src in tree" :key="src.name" data-source class="border-b border-gray-100 px-3 py-2 last:border-b-0">
          <div class="flex items-center justify-between gap-2">
            <span class="font-medium">{{ src.name }}</span>
            <span class="font-mono text-xs text-gray-600">{{ src.branch ?? "detached" }} {{ src.commit.slice(0, 7) }}</span>
          </div>
          <div v-if="src.changed_files.length" class="mt-1 space-y-0.5">
            <div v-for="file in src.changed_files" :key="file" class="truncate font-mono text-[11px] text-amber-800">{{ file }}</div>
          </div>
          <div v-else class="mt-0.5 text-[11px] text-gray-400">clean</div>
        </div>
      </div>
    </section>
  </div>
</template>
