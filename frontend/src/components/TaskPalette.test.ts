import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { TaskTypeView } from "../api/client";
import TaskPalette from "./TaskPalette.vue";

const type = (name: string): TaskTypeView => ({
  name,
  settings_schema: {},
  inputs: [],
  outputs: ["out"],
  splittable: false,
  uses_cores: false,
  has_plan: false,
  cost_setting: null,
  hint: { gpus: 0, memory_base_gb: 0, memory_per_worker_gb: 0, scratch_gb: 0 },
});
const types = [type("command"), type("demo.squares"), type("sbmlsim.fit")];

describe("TaskPalette", () => {
  it("lists the groups in order with General last", () => {
    const wrapper = mount(TaskPalette, { props: { types } });
    expect(wrapper.findAll("[data-group-title]").map((g) => g.text())).toEqual(["Demo", "Sbmlsim", "General"]);
    expect(wrapper.findAll("[data-type]").map((t) => t.attributes("data-type"))).toEqual(["demo.squares", "sbmlsim.fit", "command"]);
  });

  it("filters by the search", async () => {
    const wrapper = mount(TaskPalette, { props: { types } });
    await wrapper.get("input").setValue("fit");
    expect(wrapper.findAll("[data-type]").map((t) => t.attributes("data-type"))).toEqual(["sbmlsim.fit"]);
  });

  it("emits add on a click", async () => {
    const wrapper = mount(TaskPalette, { props: { types } });
    await wrapper.get('[data-type="demo.squares"]').trigger("click");
    expect(wrapper.emitted("add")![0]![0]).toMatchObject({ name: "demo.squares" });
  });

  it("carries the type name on a drag", async () => {
    const wrapper = mount(TaskPalette, { props: { types } });
    const data: Record<string, string> = {};
    await wrapper.get('[data-type="command"]').trigger("dragstart", {
      dataTransfer: { setData: (k: string, v: string) => (data[k] = v), effectAllowed: "" },
    });
    expect(data["application/x-simrun-task-type"]).toBe("command");
  });
});
