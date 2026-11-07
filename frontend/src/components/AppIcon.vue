<!-- An icon of the app (`lib/icons.ts`), sized and colored by the classes of its parent, like `.ico` of the mockup. -->
<script setup lang="ts">
import { ICONS, type IconName } from "../lib/icons";

defineProps<{ name: IconName }>();
</script>

<template>
  <svg
    class="block shrink-0"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="2"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    v-html="ICONS[name]"
  ></svg>
</template>
