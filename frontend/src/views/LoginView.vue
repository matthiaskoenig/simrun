<!-- The login: the token is checked with `/me`, then the page the login interrupted opens. -->
<script setup lang="ts">
import { computed, ref } from "vue";
import { useRoute, useRouter } from "vue-router";
import { ApiError } from "../api/client";
import { auth } from "../auth";
import LogoMark from "../components/LogoMark.vue";

const route = useRoute();
const router = useRouter();

const token = ref("");
const error = ref<string | null>(null);
const busy = ref(false);
const canSubmit = computed(() => token.value.trim() !== "" && !busy.value);

/** The page to open after the login: `?next=` if it is a path of the app, else the executions. */
function next(): string {
  const value = route.query.next;
  return typeof value === "string" && value.startsWith("/") ? value : "/";
}

function describe(reason: unknown): string {
  if (reason instanceof ApiError) {
    return reason.status === 401 ? "The token was not accepted." : `The service answered with an error: ${reason.message}`;
  }
  return "The service cannot be reached.";
}

async function submit(): Promise<void> {
  if (!canSubmit.value) return;
  busy.value = true;
  error.value = null;
  try {
    await auth.login(token.value);
    await router.push(next());
  } catch (reason) {
    error.value = describe(reason);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <main class="flex h-full justify-center bg-gray-50 px-4 pt-[20vh]">
    <div class="w-full max-w-sm">
      <div class="mb-4 flex items-center justify-center gap-2">
        <LogoMark />
        <span class="text-sm font-semibold">simrun</span>
      </div>
      <form class="rounded-xl border border-gray-200 bg-white p-6 shadow-sm" @submit.prevent="submit">
        <h1 class="text-base font-semibold text-gray-900">Sign in</h1>
        <p class="mt-1 text-sm text-gray-500">Enter the token the service gave you.</p>
        <label for="token" class="mt-5 block text-xs font-medium text-gray-700">Token</label>
        <input
          id="token"
          v-model="token"
          type="password"
          autocomplete="current-password"
          spellcheck="false"
          autofocus
          class="mt-1.5 block w-full rounded-lg border border-gray-200 px-3 py-2 font-mono text-sm shadow-sm outline-none placeholder:text-gray-400 focus:border-blue-500 focus:ring-2 focus:ring-blue-100 aria-invalid:border-red-300 aria-invalid:focus:border-red-400 aria-invalid:focus:ring-red-100"
          :aria-invalid="error !== null"
          aria-describedby="token-error"
        />
        <p v-if="error" id="token-error" role="alert" class="mt-2 text-sm text-red-700">{{ error }}</p>
        <button
          type="submit"
          :disabled="!canSubmit"
          class="mt-5 w-full rounded-lg bg-blue-600 px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50"
        >
          {{ busy ? "Signing in..." : "Sign in" }}
        </button>
      </form>
    </div>
  </main>
</template>
