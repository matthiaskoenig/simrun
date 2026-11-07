import { describe, expect, it } from "vitest";
import type { TaskTypeView, Workflow } from "../api/client";
import { addTask, connect, disconnect, newTaskId, paletteGroups, removeTask, renameTask } from "./edit-graph";

function type(name: string, extra: Partial<TaskTypeView> = {}): TaskTypeView {
  return {
    name,
    settings_schema: {},
    inputs: [],
    outputs: ["out"],
    splittable: false,
    uses_cores: false,
    has_plan: false,
    cost_setting: null,
    hint: { gpus: 0, memory_base_gb: 0, memory_per_worker_gb: 0, scratch_gb: 0 },
    ...extra,
  };
}

const wf = (tasks: Workflow["tasks"]): Workflow => ({ name: "w", tasks }) as Workflow;

describe("paletteGroups", () => {
  const types = [
    type("sbmlsim.fit"),
    type("command"),
    type("demo.squares", { settings_schema: { title: "Squares", description: "Sums of squares" } }),
    type("demo.cubes"),
  ];

  it("groups by the prefix before the first dot, sorted with General last", () => {
    const groups = paletteGroups(types, "");
    expect(groups.map((g) => g.title)).toEqual(["Demo", "Sbmlsim", "General"]);
    expect(groups[0]!.types.map((t) => t.name)).toEqual(["demo.cubes", "demo.squares"]);
  });

  it("filters by name, title and description, case-insensitive, and drops empty groups", () => {
    expect(paletteGroups(types, "FIT").map((g) => g.title)).toEqual(["Sbmlsim"]);
    expect(paletteGroups(types, "sums of")[0]!.types.map((t) => t.name)).toEqual(["demo.squares"]);
    expect(paletteGroups(types, "squares")[0]!.types.map((t) => t.name)).toEqual(["demo.squares"]);
    expect(paletteGroups(types, "nothing")).toEqual([]);
  });
});

describe("newTaskId and addTask", () => {
  it("takes the last part of the type name and numbers duplicates", () => {
    let w = wf([]);
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) {
      const added = addTask(w, type("demo.squares"));
      ids.push(added.id);
      w = added.workflow;
    }
    expect(ids).toEqual(["squares", "squares-2", "squares-3"]);
    expect(w.tasks.map((t) => t.type)).toEqual(["demo.squares", "demo.squares", "demo.squares"]);
  });

  it("makes a valid id of any type name and leaves the argument alone", () => {
    const w = wf([]);
    expect(newTaskId(w, "My.Fit_2")).toBe("fit-2");
    expect(newTaskId(w, "x.2d")).toBe("task-2d");
    addTask(w, type("command"));
    expect(w.tasks).toEqual([]);
  });
});

describe("connect and disconnect", () => {
  const base = (): Workflow =>
    wf([
      { id: "a", type: "t", settings: {} },
      { id: "b", type: "t", settings: {}, inputs: {} },
    ]);

  it("sets the input and refuses a second connection to a port", () => {
    const w = connect(base(), { task: "a", port: "out" }, { task: "b", port: "x" }) as Workflow;
    expect(w.tasks[1]!.inputs).toEqual({ x: "a.out" });
    expect(connect(w, { task: "a", port: "out" }, { task: "b", port: "x" })).toBe("the input x of b is already connected");
  });

  it("refuses a cycle and a self-connection", () => {
    const w = connect(base(), { task: "a", port: "out" }, { task: "b", port: "x" }) as Workflow;
    expect(connect(w, { task: "b", port: "out" }, { task: "a", port: "y" })).toBe("this connection makes a cycle");
    expect(connect(w, { task: "a", port: "out" }, { task: "a", port: "y" })).toBe("a task cannot read its own output");
  });

  it("disconnects", () => {
    const w = connect(base(), { task: "a", port: "out" }, { task: "b", port: "x" }) as Workflow;
    const d = disconnect(w, { task: "b", port: "x" });
    expect(d.tasks[1]!.inputs).toEqual({});
    expect(w.tasks[1]!.inputs).toEqual({ x: "a.out" });
  });
});

describe("removeTask and renameTask", () => {
  const chain = (): Workflow =>
    wf([
      { id: "a", type: "t", settings: {} },
      { id: "b", type: "t", settings: {}, inputs: { x: "a.out", y: "c.out" }, stage: ["x", "y"] },
      { id: "c", type: "t", settings: {} },
    ]);

  it("drops the inputs and stage entries which refer to the task", () => {
    const w = removeTask(chain(), "a");
    expect(w.tasks.map((t) => t.id)).toEqual(["b", "c"]);
    expect(w.tasks[0]!.inputs).toEqual({ y: "c.out" });
    expect(w.tasks[0]!.stage).toEqual(["y"]);
  });

  it("renames and rewrites the references", () => {
    const w = renameTask(chain(), "a", "alpha") as Workflow;
    expect(w.tasks[0]!.id).toBe("alpha");
    expect(w.tasks[1]!.inputs).toEqual({ x: "alpha.out", y: "c.out" });
  });

  it("refuses an invalid or taken id", () => {
    expect(typeof renameTask(chain(), "a", "B x")).toBe("string");
    expect(renameTask(chain(), "a", "c")).toBe("the id c is taken");
    expect(renameTask(chain(), "a", "a")).toEqual(chain());
  });
});
