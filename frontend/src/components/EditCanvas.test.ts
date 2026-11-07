import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { TaskTypeView, Workflow } from "../api/client";
import EditCanvas from "./EditCanvas.vue";

const type = (name: string, inputs: string[], outputs: string[]): TaskTypeView => ({
  name,
  settings_schema: {},
  inputs,
  outputs,
  splittable: false,
  uses_cores: false,
  has_plan: false,
  cost_setting: null,
  hint: { gpus: 0, memory_base_gb: 0, memory_per_worker_gb: 0, scratch_gb: 0 },
  description: "",
});
const types = [type("gen", [], ["out"]), type("use", ["x", "y"], ["res"])];
const workflow: Workflow = {
  name: "w",
  tasks: [
    { id: "a", type: "gen", settings: {} },
    { id: "b", type: "use", settings: {}, inputs: { x: "a.out" } },
  ],
} as Workflow;

beforeEach(() => {
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    },
  );
});

function mountCanvas(wf: Workflow = workflow) {
  return mount(EditCanvas, {
    props: { workflow: wf, types, estimate: {}, taskErrors: { b: ["bad", "worse"] }, selected: null },
    attachTo: document.body,
  });
}

describe("EditCanvas", () => {
  it("renders a node per task and a handle per port", async () => {
    const wrapper = mountCanvas();
    await flushPromises();
    expect(wrapper.findAll("[data-task]").map((n) => n.attributes("data-task")).sort()).toEqual(["a", "b"]);
    const b = wrapper.get('[data-task="b"]');
    expect(b.findAll(".vue-flow__handle.target").map((h) => h.attributes("data-handleid"))).toEqual(["x", "y"]);
    expect(b.findAll(".vue-flow__handle.source").map((h) => h.attributes("data-handleid"))).toEqual(["res"]);
    expect(b.get("[data-errors]").text()).toContain("2");
    wrapper.unmount();
  });

  it("refuses a connection to a connected port and emits no change", async () => {
    const wrapper = mountCanvas();
    (wrapper.vm as unknown as { onConnect: (c: object) => void }).onConnect({
      source: "a",
      sourceHandle: "out",
      target: "b",
      targetHandle: "x",
    });
    expect(wrapper.emitted("refused")![0]).toEqual(["the input x of b is already connected"]);
    expect(wrapper.emitted("change")).toBeUndefined();
    wrapper.unmount();
  });

  it("emits the connected workflow", () => {
    const wrapper = mountCanvas();
    (wrapper.vm as unknown as { onConnect: (c: object) => void }).onConnect({
      source: "a",
      sourceHandle: "out",
      target: "b",
      targetHandle: "y",
    });
    const next = wrapper.emitted("change")![0]![0] as Workflow;
    expect(next.tasks[1]!.inputs).toEqual({ x: "a.out", y: "a.out" });
    wrapper.unmount();
  });

  it("removes the selected task on Delete", async () => {
    const wrapper = mountCanvas();
    await wrapper.setProps({ selected: "a" });
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Delete" }));
    const next = wrapper.emitted("change")![0]![0] as Workflow;
    expect(next.tasks.map((t) => t.id)).toEqual(["b"]);
    expect(next.tasks[0]!.inputs).toEqual({});
    expect(wrapper.emitted("update:selected")!.at(-1)).toEqual([null]);
    wrapper.unmount();
  });
});
