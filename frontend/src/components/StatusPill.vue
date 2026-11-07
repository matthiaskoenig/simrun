<!-- The pill of the state of a run or a task, `tpl-pill` of the mockup, or of a look of another state (of a worker). -->
<script setup lang="ts">
import { computed } from "vue";
import type { RunState, TaskState } from "../api/client";
import { spins, STATUS, type StatusStyle } from "../lib/status";
import AppIcon from "./AppIcon.vue";

const props = defineProps<{ status: RunState | TaskState | StatusStyle; small?: boolean }>();

const style = computed(() => (typeof props.status === "string" ? STATUS[props.status] : props.status));
</script>

<template>
  <span
    data-status
    class="inline-flex shrink-0 items-center gap-1 rounded-full font-medium whitespace-nowrap"
    :class="[style.pill, small ? 'px-1.5 py-0.5 text-[10px]' : 'px-2 py-0.5 text-xs']"
  >
    <AppIcon :name="style.icon" :class="[small ? 'size-2.5' : 'size-3', spins(style) ? 'animate-spin' : '']" />{{
      style.label
    }}
  </span>
</template>
