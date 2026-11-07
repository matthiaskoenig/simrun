<!--
  The inspector of the Editor, the right column: the task panel when a task is
  selected, the workflow panel otherwise.
-->
<script setup lang="ts">
import { computed } from "vue";
import type { TaskEstimate, TaskTypeView, Workflow } from "../api/client";
import EditTaskPanel from "./EditTaskPanel.vue";
import EditWorkflowPanel, { type TreeInfo } from "./EditWorkflowPanel.vue";

const props = defineProps<{
  workflow: Workflow;
  types: TaskTypeView[];
  estimate: Record<string, TaskEstimate>;
  taskErrors: Record<string, string[]>;
  errors: string[];
  group: string | null;
  tree: TreeInfo[];
  selected: string | null;
}>();
const emit = defineEmits<{
  change: [workflow: Workflow];
  rename: [from: string, to: string, workflow: Workflow];
  "update:selected": [task: string | null];
  "update:group": [group: string | null];
}>();

const task = computed(() => props.workflow.tasks.find((t) => t.id === props.selected) ?? null);
</script>

<template>
  <div class="h-full overflow-y-auto">
    <EditTaskPanel
      v-if="task"
      :key="task.id"
      :workflow="workflow"
      :task="task"
      :type="types.find((t) => t.name === task!.type) ?? null"
      :estimate="estimate[task.id] ?? null"
      :errors="taskErrors[task.id] ?? []"
      @change="(next) => emit('change', next)"
      @rename="(from, to, next) => emit('rename', from, to, next)"
      @removed="emit('update:selected', null)"
    />
    <EditWorkflowPanel
      v-else
      :workflow="workflow"
      :group="group"
      :tree="tree"
      :errors="errors"
      @change="(next) => emit('change', next)"
      @update:group="(value) => emit('update:group', value)"
    />
  </div>
</template>
