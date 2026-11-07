<!-- The executions: the runs, the canvas of the selected run and the servers (Task 6 of the web app plan). -->
<script setup lang="ts">
import { auth } from "../auth";
import LogoMark from "../components/LogoMark.vue";

defineProps<{ id?: string }>();
</script>

<template>
  <div class="flex h-full flex-col bg-white text-gray-900">
    <header class="flex h-12 shrink-0 items-center gap-4 border-b border-gray-200 px-4">
      <div class="flex items-center gap-2">
        <LogoMark />
        <span class="text-sm font-semibold">simrun</span>
      </div>
      <span v-if="auth.identity.value" class="ml-auto text-sm text-gray-500">{{ auth.identity.value.name }}</span>
    </header>
    <main class="min-h-0 flex-1"></main>
  </div>
</template>
