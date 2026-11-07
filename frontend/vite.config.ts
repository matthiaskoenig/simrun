/// <reference types="vitest/config" />
import tailwindcss from "@tailwindcss/vite";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

// the service the dev server proxies the API to
const service = process.env.SIMRUN_URL ?? "http://localhost:8000";

// the paths of the API the app calls; the app itself lives at /ui/ with hash
// routing, so none of its routes is a path of the API
const api = ["/me", "/runs", "/events", "/servers", "/edits", "/task-types", "/workflows", "/validate", "/estimate"];

export default defineConfig({
  base: "/ui/",
  plugins: [vue(), tailwindcss()],
  server: {
    proxy: Object.fromEntries(api.map((path) => [path, { target: service, changeOrigin: true }])),
  },
  test: {
    environment: "jsdom",
    restoreMocks: true,
    unstubGlobals: true,
  },
});
