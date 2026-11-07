import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { auth } from "../auth";
import EditSessions from "./EditSessions.vue";

const base = {
  workflow: { name: "w", split: "fastest", tasks: [] },
  file_sha256: "h",
  sources: [],
  tree: [],
  created: "2026-11-07T13:00:00Z",
  seen: "2026-11-07T13:00:00Z",
  closed: false,
  owner: "mk",
};
const sessions = [
  { ...base, id: "a", host: "laptop", path: "/home/mk/squares.yaml", connected: true },
  { ...base, id: "b", host: "desktop", path: "/home/mk/fit.yaml", connected: false },
];

const stub = (data: unknown) =>
  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>().mockImplementation(async (input) =>
      String(input) === "/edits" ? new Response(JSON.stringify(data)) : new Response(new ReadableStream({ start() {} })),
    ),
  );

describe("EditSessions", () => {
  beforeEach(() => (auth.token.value = "secret"));
  afterEach(() => {
    auth.logout();
    localStorage.clear();
  });

  it("lists the sessions with path, host and a connected dot", async () => {
    stub(sessions);
    const view = mount(EditSessions);
    await flushPromises();
    const rows = view.findAll("[data-session]");
    expect(rows).toHaveLength(2);
    expect(rows[0]!.text()).toContain("/home/mk/squares.yaml");
    expect(rows[0]!.text()).toContain("laptop");
    expect(rows[0]!.get("[data-connected]").classes()).toContain("bg-emerald-500");
    expect(rows[1]!.get("[data-connected]").classes()).not.toContain("bg-emerald-500");
  });

  it("shows the command and the docs page without sessions", async () => {
    stub([]);
    const view = mount(EditSessions);
    await flushPromises();
    expect(view.text()).toContain("simrun edit <workflow.yaml>");
    expect(view.text()).toContain("docs/web-app.md#the-editor");
  });
});
