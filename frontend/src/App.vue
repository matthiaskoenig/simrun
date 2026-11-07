<!-- The app: the page of the route; the end of the session, a 401 anywhere, returns to the login. -->
<script setup lang="ts">
import { watch } from "vue";
import { RouterView, useRoute, useRouter } from "vue-router";
import { auth } from "./auth";
import { loginRoute } from "./router";

const route = useRoute();
const router = useRouter();

watch(auth.token, (token) => {
  if (token === null && !route.meta.public) void router.push(loginRoute(route.fullPath));
});
</script>

<template>
  <RouterView />
</template>
