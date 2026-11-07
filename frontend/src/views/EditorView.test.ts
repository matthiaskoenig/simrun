import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createMemoryHistory } from "vue-router";
import type { EditSession, Workflow } from "../api/client";
import { auth } from "../auth";
import { createAppRouter } from "../router";
import { mk, runRecord } from "../test/fixtures";
import EditorView from "./EditorView.vue";

const sse = (event: string, data: unknown): string => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
const source = { name: "model", branch: "main", commit: "abc1234", changed_files: [], snapshot: "s1", diff_sha256: "d1", dirty: false, patch: null };
const workflow: Workflow = { name: "squares", split: "fastest", tasks: [{ id: "squares", type: "demo.squares", settings: {} }] };
const session: EditSession = {
  id: "abc",
  owner: "mk",
  host: "laptop",
  path: "/home/mk/squares.yaml",
  workflow,
  file_sha256: "h1",
  sources: [source],
  tree: [{ name: "model", branch: "main", commit: "abc1234", changed_files: [] }],
  created: "2026-11-07T13:00:00Z",
  seen: "2026-11-07T13:00:00Z",
  closed: false,
  connected: true,
} as EditSession;

const reply = (data: unknown, status = 200): Response => new Response(JSON.stringify(data), { status });
const summary = { id: "r9", state: "queued" };

// sends more events on the open stream of the session
let push: (text: string) => void = () => {};

function mockFetch(runReply: () => Response, events = "") {
  const mock = vi.fn<typeof fetch>().mockImplementation(async (input) => {
    const path = String(input);
    if (path.endsWith("/events"))
      return new Response(
        new ReadableStream({
          start: (c) => {
            push = (text) => c.enqueue(new TextEncoder().encode(text));
            push(events);
          },
        }),
      );
    if (path === "/task-types" || path === "/servers") return reply([]);
    if (path === "/validate") return reply({ ok: true, errors: [] });
    if (path === "/estimate") return reply({});
    if (path === "/runs/r1") return reply(runRecord({ workflow, group: "g1", sources: [source] }));
    if (path.endsWith("/save")) return reply({ detail: "changed on disk" }, 409);
    if (path.endsWith("/run") || path === "/runs") return runReply();
    return reply({});
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

async function open(path: string) {
  const router = createAppRouter(createMemoryHistory());
  router.push(path);
  await router.isReady();
  const view = mount(EditorView, {
    props: { id: path.split("/")[2]!, run: path.startsWith("/runs") },
    global: { plugins: [router] },
    attachTo: document.body,
  });
  await flushPromises();
  return { view, router };
}

const post = (mock: ReturnType<typeof mockFetch>, path: string) => mock.mock.calls.filter(([p, i]) => String(p) === path && i?.method === "POST");

describe("EditorView bar", () => {
  beforeEach(() => {
    auth.token.value = "secret";
    auth.identity.value = mk;
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
  });
  afterEach(() => {
    auth.logout();
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it("runs a session through its edit and navigates to the run", async () => {
    const mock = mockFetch(() => reply(summary), sse("edit", session));
    const { view, router } = await open("/edit/abc");
    await view.get("[data-run]").trigger("click");
    await flushPromises();
    const [call] = post(mock, "/edits/abc/run");
    expect(JSON.parse(String(call![1]!.body)).workflow.name).toBe("squares");
    await vi.waitFor(() => expect(router.currentRoute.value.fullPath).toBe("/runs/r9"));
    view.unmount();
  });

  it("runs a run with its sources and group", async () => {
    const mock = mockFetch(() => reply(summary));
    const { view, router } = await open("/runs/r1/edit");
    expect(view.find("[data-download]").exists()).toBe(true);
    await view.get("[data-run]").trigger("click");
    await flushPromises();
    const [call] = post(mock, "/runs");
    const body = JSON.parse(String(call![1]!.body));
    expect(body.sources).toEqual([source]);
    expect(body.group).toBe("g1");
    await vi.waitFor(() => expect(router.currentRoute.value.fullPath).toBe("/runs/r9"));
    view.unmount();
  });

  it("shows the detail of a refusal under the bar", async () => {
    mockFetch(() => reply({ detail: "task 'squares': n must be positive" }, 422), sse("edit", session));
    const { view, router } = await open("/edit/abc");
    await view.get("[data-run]").trigger("click");
    await flushPromises();
    expect(view.get("[data-error]").text()).toBe("task 'squares': n must be positive");
    expect(router.currentRoute.value.fullPath).toBe("/edit/abc");
    view.unmount();
  });

  it("offers Reload and Save anyway after a conflicting save, without overwriting", async () => {
    mockFetch(() => reply(summary), sse("edit", session));
    const { view } = await open("/edit/abc");
    expect(view.get("[data-save]").attributes("disabled")).toBeDefined();
    await view.get("[data-name]").setValue("mine");
    await view.get("[data-save]").trigger("click");
    await flushPromises();
    expect(view.get("[data-state]").text()).toBe("changed on disk");
    expect(view.find("[data-reload]").exists()).toBe(true);
    expect(view.find("[data-save-anyway]").exists()).toBe(true);
    view.unmount();
  });

  it("downloads the YAML of a run's draft as <name>.yaml", async () => {
    // the URL of the download is revoked by a timer: fake, so no timer outlives the test
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const { createObjectURL, revokeObjectURL } = URL;
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      expect(this.download).toBe("squares.yaml");
    });
    try {
      const mock = mockFetch(() => reply(summary));
      const { view } = await open("/runs/r1/edit");
      URL.createObjectURL = vi.fn(() => "blob:x");
      URL.revokeObjectURL = vi.fn();
      await view.get("[data-download]").trigger("click");
      await flushPromises();
      expect(post(mock, "/workflows/yaml")).toHaveLength(1);
      expect(click).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(30_000);
      expect(URL.revokeObjectURL).toHaveBeenCalledWith("blob:x");
      view.unmount();
    } finally {
      click.mockRestore();
      Object.assign(URL, { createObjectURL, revokeObjectURL });
      vi.useRealTimers();
    }
  });

  it("switches to Download YAML and runs from the browser when the session goes disconnected", async () => {
    const mock = mockFetch(() => reply(summary), sse("edit", session));
    const { view, router } = await open("/edit/abc");
    expect(view.find("[data-save]").exists()).toBe(true);
    push(sse("edit", { ...session, connected: false }));
    await flushPromises();
    expect(view.get("[data-machine]").text()).toBe("laptop not connected");
    expect(view.find("[data-save]").exists()).toBe(false);
    expect(view.find("[data-download]").exists()).toBe(true);
    await view.get("[data-run]").trigger("click");
    await flushPromises();
    expect(post(mock, "/edits/abc/run")).toHaveLength(0);
    const [call] = post(mock, "/runs");
    expect(JSON.parse(String(call![1]!.body)).sources).toEqual([source]);
    await vi.waitFor(() => expect(router.currentRoute.value.fullPath).toBe("/runs/r9"));
    view.unmount();
  });

  it("names the file of a session in the breadcrumb and the palette", async () => {
    mockFetch(() => reply(summary), sse("edit", session));
    const { view } = await open("/edit/abc");
    const crumb = view.get("[data-crumb]");
    expect(crumb.text()).toBe("squares.yaml");
    expect(crumb.attributes("title")).toBe("/home/mk/squares.yaml");
    expect(view.get("[data-palette-footer]").text()).toBe("Saves to squares.yaml on laptop");
    view.unmount();
  });

  it("names the run of a rerun in the breadcrumb, the bar and the palette", async () => {
    mockFetch(() => reply(summary));
    const { view } = await open("/runs/r1/edit");
    expect(view.get("[data-crumb]").text()).toBe("rerun of #r1");
    expect(view.get("[data-machine]").text()).toBe("from run #r1");
    expect(view.get("[data-palette-footer]").text()).toBe("The workflow of run #r1");
    view.unmount();
  });
});
