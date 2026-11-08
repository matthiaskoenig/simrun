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
  description: "",
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
  it("lists the inputs a command reads, without staging or resource overrides", () => {
    const command: TaskTypeView = { ...type, name: "command", inputs: [], outputs: [], splittable: false };
    const withCommand: Workflow = {
      ...workflow,
      tasks: [...workflow.tasks, { id: "sum", type: "command", settings: {}, inputs: { a: "a.out", b: "b.out" } }],
    };
    const wrapper = mount(EditInspector, { props: { ...base, workflow: withCommand, types: [type, command], selected: "sum" } });
    expect(wrapper.findAll("[data-input]").map((i) => i.attributes("data-input"))).toEqual(["a", "b"]);
    expect(wrapper.find("[data-stage]").exists()).toBe(false);
    expect(wrapper.find("[data-workers]").exists()).toBe(false);
    expect(wrapper.get("[data-command-resources]").text()).toContain("cores, memory, scratch and GPUs of its settings");
  });

  it("shows the estimate and the errors of the task and writes resources.workers", async () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    const text = wrapper.text();
    expect(text).toContain("learned");
    expect(text).toContain("2 min");
    // the memory per process of the plan, not the learned one before the plan
    expect(text).toContain("Memory per worker2 GB");
    expect(text).toContain("3 pods");
    expect(text).toContain("input x is bad");
    expect(text).toContain("a.out");
    await wrapper.get("[data-workers]").setValue("4");
    const next = wrapper.emitted("change")!.at(-1)![0] as Workflow;
    expect(next.tasks[1]!.resources).toEqual({ workers: 4 });
    expect(workflow.tasks[1]!.resources).toBeUndefined();
  });

  it("refuses a fractional workers count in place", async () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    await wrapper.get("[data-workers]").setValue("2.5");
    expect(wrapper.emitted("change")).toBeUndefined();
    expect(wrapper.get("[data-workers-problem]").text()).toContain("whole number");
  });

  it("shows the own settings of the next task, not the pending text of the previous", async () => {
    const wf = JSON.parse(JSON.stringify(workflow)) as Workflow;
    const json = { ...type, settings_schema: { properties: { d: { additionalProperties: { type: "string" }, title: "D", type: "object" } }, type: "object" } };
    wf.tasks[1]!.settings = { d: { k: "v" } };
    const wrapper = mount(EditInspector, { props: { ...base, workflow: wf, types: [json], selected: "a" } });
    await wrapper.get("[data-field='d'] textarea").setValue("{bad");
    expect(wrapper.text()).toContain("not valid JSON");
    await wrapper.setProps({ selected: "b" });
    expect((wrapper.get("[data-field='d'] textarea").element as HTMLTextAreaElement).value).toContain('"k": "v"');
    expect(wrapper.text()).not.toContain("not valid JSON");
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

  it("heads a task with its kind, its type and its id as the title", () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b" } });
    const head = wrapper.get("[data-task-head]");
    expect(head.classes()).toContain("border-b");
    expect(head.get("[data-head-type]").text()).toBe("demo.squares");
    expect(head.get("[data-head-type]").classes()).toContain("uppercase");
    expect(head.get("[data-head-title]").text()).toBe("b");
    expect(head.find("svg").exists()).toBe(true);
  });

  it("shows an error of a setting under its field and the others in the panel", () => {
    const wrapper = mount(EditInspector, { props: { ...base, selected: "b", taskErrors: { b: ["n: Field required", "input x is bad"] } } });
    expect(wrapper.get("[data-field='n'] [data-field-error]").text()).toBe("Field required");
    expect(wrapper.get("[data-task-errors]").text()).toBe("input x is bad");
  });
});
