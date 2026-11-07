import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Workflow } from "../api/client";
import EditCanvas from "./EditCanvas.vue";

// the canvas fits the view through these; jsdom measures no nodes, so the test fires the hook itself
const fitView = vi.fn(async () => true);
const setViewport = vi.fn(async () => true);
const initialized: (() => void)[] = [];

vi.mock("@vue-flow/core", async (original) => {
  const real = await original<typeof import("@vue-flow/core")>();
  return {
    ...real,
    useVueFlow: (id: string) => ({
      ...real.useVueFlow(id),
      fitView,
      setViewport,
      onNodesInitialized: (hook: () => void) => initialized.push(hook),
      viewport: { value: { x: 0, y: 0, zoom: 1 } },
      dimensions: { value: { width: 800, height: 600 } },
      // the added task lies right of the pane
      findNode: (id: string) => (id === "b" ? { position: { x: 900, y: 200 }, dimensions: { width: 256, height: 80 } } : undefined),
    }),
  };
});

const workflow: Workflow = { name: "w", tasks: [{ id: "a", type: "gen", settings: {} }] } as Workflow;

beforeEach(() => {
  fitView.mockClear();
  setViewport.mockClear();
  initialized.length = 0;
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
});

describe("EditCanvas view", () => {
  it("fits the view on the first layout only, not on every added task", async () => {
    const wrapper = mount(EditCanvas, { props: { workflow, types: [], estimate: {}, taskErrors: {}, selected: null }, attachTo: document.body });
    await flushPromises();
    for (const hook of initialized) hook();
    expect(fitView).toHaveBeenCalledOnce();
    // the nodes are initialized again when a task is added
    await wrapper.setProps({ workflow: { ...workflow, tasks: [...workflow.tasks, { id: "b", type: "gen", settings: {} }] } as Workflow });
    for (const hook of initialized) hook();
    expect(fitView).toHaveBeenCalledOnce();
    wrapper.unmount();
  });

  it("pans an added task into the view, keeping the zoom", async () => {
    const wrapper = mount(EditCanvas, { props: { workflow, types: [], estimate: {}, taskErrors: {}, selected: null }, attachTo: document.body });
    await flushPromises();
    for (const hook of initialized) hook();
    expect(setViewport).not.toHaveBeenCalled();
    await wrapper.setProps({ workflow: { ...workflow, tasks: [...workflow.tasks, { id: "b", type: "gen", settings: {} }] } as Workflow });
    for (const hook of initialized) hook();
    expect(setViewport).toHaveBeenCalledOnce();
    expect((setViewport.mock.calls[0] as unknown[])[0]).toEqual({ x: 800 - 24 - 1156, y: 0, zoom: 1 });
    wrapper.unmount();
  });
});
