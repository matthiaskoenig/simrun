import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ServerUsage, TaskEstimate, TaskTypeView, Workflow } from "../api/client";
import { api } from "../api/client";
import EditInspector from "./EditInspector.vue";

const type: TaskTypeView = {
  name: "demo.squares",
  settings_schema: { properties: { n: { title: "N", type: "integer" } }, type: "object" },
  inputs: ["x"],
  outputs: ["out"],
  splittable: true,
  uses_cores: false,
  has_plan: false,
  cost_setting: null,
  hint: { gpus: 0, memory_base_gb: 0, memory_per_worker_gb: 0, scratch_gb: 0 },
};
const workflow: Workflow = {
  name: "w",
  split: "fastest",
  tasks: [
    { id: "a", type: "demo.squares", settings: {} },
    { id: "b", type: "demo.squares", settings: {}, inputs: { x: "a.out" } },
  ],
};
const estimate: Record<string, TaskEstimate> = {
  b: {
    seconds: 125,
    plan: { task_id: "b", pods: 3, units: 12, base_priority: "standard", extra_priority: "standard", shape: { cores: 4, gpus: 0, memory_gb: 8, per_worker_gb: 2, processes: 4, retry_memory_gb: 16, retry_per_worker_gb: 4, scratch_gb: 0 } },
    resources: { gpus: 0, memory_base_gb: 0, memory_per_worker_gb: 2.5, samples: 20, scratch_gb: 0, seconds_per_unit: 1, source: "learned" },
  },
};
const base = {
  workflow,
  types: [type],
  estimate,
  taskErrors: { b: ["input x is bad"] },
  errors: [],
  group: null,
  tree: [{ name: "model", branch: "main", commit: "0123456789abcdef", changed_files: ["m.xml"] }],
};

afterEach(() => vi.restoreAllMocks());

describe("EditInspector", () => {
  it("shows the estimate and the errors of the task and writes resources.workers", async () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    const text = wrapper.text();
    expect(text).toContain("learned");
    expect(text).toContain("2 min");
    expect(text).toContain("2.5 GB");
    expect(text).toContain("3 pods");
    expect(text).toContain("input x is bad");
    expect(text).toContain("a.out");
    await wrapper.get("[data-workers]").setValue("4");
    const next = wrapper.emitted("change")!.at(-1)![0] as Workflow;
    expect(next.tasks[1]!.resources).toEqual({ workers: 4 });
    expect(workflow.tasks[1]!.resources).toBeUndefined();
  });

  it("an emptied override becomes null", async () => {
    const wf = JSON.parse(JSON.stringify(workflow)) as Workflow;
    wf.tasks[1]!.resources = { workers: 4 };
    const wrapper = mount(EditInspector, { props: { ...base, workflow: wf, selected: "b" } });
    await wrapper.get("[data-workers]").setValue("");
    expect((wrapper.emitted("change")!.at(-1)![0] as Workflow).tasks[1]!.resources).toBeNull();
  });

  it("renames a task and says what refuses", async () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    await wrapper.get("[data-task-id]").setValue("Bad Id");
    await wrapper.get("[data-task-id]").trigger("change");
    expect(wrapper.text()).toContain("not valid");
    await wrapper.get("[data-task-id]").setValue("c");
    await wrapper.get("[data-task-id]").trigger("change");
    const [from, to, next] = wrapper.emitted("rename")!.at(-1)! as [string, string, Workflow];
    expect([from, to, next.tasks[1]!.id]).toEqual(["b", "c", "c"]);
  });

  it("removes a connected input and the task", async () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    await wrapper.get("[data-disconnect='x']").trigger("click");
    expect((wrapper.emitted("change")!.at(-1)![0] as Workflow).tasks[1]!.inputs).toEqual({});
    await wrapper.get("[data-remove-task]").trigger("click");
    expect((wrapper.emitted("change")!.at(-1)![0] as Workflow).tasks.map((t) => t.id)).toEqual(["a"]);
    expect(wrapper.emitted("update:selected")!.at(-1)).toEqual([null]);
  });

  it("the workflow panel writes reserve from the servers and shows the sources", async () => {
    vi.spyOn(api, "servers").mockResolvedValue([{ name: "node-1" }, { name: "node-2" }] as ServerUsage[]);
    const wrapper = mount(EditInspector, { props: { ...base, selected: null } });
    await flushPromises();
    expect(wrapper.text()).toContain("model");
    expect(wrapper.text()).toContain("0123456");
    expect(wrapper.text()).toContain("m.xml");
    await wrapper.get("[data-reserve]").setValue("node-2");
    expect((wrapper.emitted("change")!.at(-1)![0] as Workflow).reserve).toBe("node-2");
    await wrapper.get("[data-reserve]").setValue("");
    expect((wrapper.emitted("change")!.at(-1)![0] as Workflow).reserve).toBeNull();
    await wrapper.get("[data-group]").setValue("team");
    expect(wrapper.emitted("update:group")!.at(-1)).toEqual(["team"]);
  });
});
