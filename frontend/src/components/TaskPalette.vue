<!--
  The palette of the Editor, the left column of the mockup's Editor mode: a
  search and the task types of the service in groups. A click adds a task of
  the type, a drag carries its name to the canvas.
-->
<script setup lang="ts">
import { computed, ref } from "vue";
import type { TaskTypeView } from "../api/client";
import { DRAG_TYPE, paletteGroups } from "../lib/edit-graph";
import { kindOf } from "../lib/kinds";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{ types: TaskTypeView[] }>();
const emit = defineEmits<{ add: [type: TaskTypeView] }>();

const query = ref("");
const groups = computed(() => paletteGroups(props.types, query.value));

function description(type: TaskTypeView): string {
  const { description: text, title } = type.settings_schema as { description?: unknown; title?: unknown };
  if (typeof text === "string") return text;
  return typeof title === "string" ? title : "";
}

function onDragStart(event: DragEvent, type: TaskTypeView): void {
  event.dataTransfer?.setData(DRAG_TYPE, type.name);
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "copy";
}
</script>

<template>
  <div class="flex h-full flex-col bg-gray-50/60">
    <div class="border-b border-gray-200 p-3">
      <label class="flex items-center gap-2 rounded-md border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-400">
        <AppIcon name="search" class="size-4" />
        <input
          v-model="query"
          class="w-full bg-transparent text-gray-900 outline-none placeholder:text-gray-400"
          placeholder="Search tasks"
        />
      </label>
    </div>
    <div class="flex-1 overflow-y-auto p-3">
      <p v-if="groups.length === 0" data-empty class="px-1 text-xs text-gray-400">No task type matches.</p>
      <div v-for="group in groups" :key="group.title" class="mb-4">
        <div data-group-title class="mb-1.5 px-1 text-[11px] font-semibold tracking-wide text-gray-400 uppercase">
          {{ group.title }}
        </div>
        <div
          v-for="type in group.types"
          :key="type.name"
          :data-type="type.name"
          draggable="true"
          title="Drag onto the canvas, or click to add"
          class="mb-1 flex cursor-grab items-center gap-2.5 rounded-lg border border-transparent bg-white px-2 py-2 shadow-xs hover:border-gray-200 active:cursor-grabbing"
          @dragstart="onDragStart($event, type)"
          @click="emit('add', type)"
        >
          <span class="grid size-7 shrink-0 place-items-center rounded-md text-white" :style="{ background: kindOf(type.name).color }">
            <AppIcon :name="kindOf(type.name).icon" class="size-3.5" />
          </span>
          <div class="min-w-0 flex-1">
            <div class="truncate text-sm font-medium text-gray-900">{{ type.name }}</div>
            <div v-if="description(type)" class="truncate text-[11px] text-gray-500">{{ description(type) }}</div>
          </div>
          <AppIcon name="grip-vertical" class="size-3.5 text-gray-300" />
        </div>
      </div>
    </div>
  </div>
</template>
