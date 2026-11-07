import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import type { TaskProgress, TaskSpec, TaskState, WorkerView } from "../api/client";
import TaskNode from "./TaskNode.vue";

const squares: TaskSpec = { id: "squares", type: "demo.squares", settings: { n: 3000, sleep_s: 0.1 } };

function worker(fields: Partial<WorkerView> & Pick<WorkerView, "worker">): WorkerView {
  return {
    node: null,
    state: "running",
    attempts: 1,
    processes: 2,
    units_done: 0,
    units_claimed: 0,
    peak_memory_gb: null,
    memory_limit_gb: null,
    oom_killed: false,
    ...fields,
  };
}

function mountNode(props: {
  task?: TaskSpec;
  state?: TaskState;
  progress?: TaskProgress | null;
  workers?: WorkerView[];
  selected?: boolean;
}) {
  return mount(TaskNode, {
    props: { task: squares, state: "running", progress: null, workers: [], selected: false, ...props },
  });
}

const counts = (done: number, total: number): TaskProgress => ({
  counts: { pending: total - done, claimed: 0, done, total },
  last: null,
});

describe("TaskNode", () => {
  it("shows the type in small caps, the task id and the state", () => {
    const wrapper = mountNode({ state: "running" });
    const type = wrapper.get("[data-type]");
    expect(type.text()).toBe("demo.squares");
    expect(type.classes()).toContain("uppercase");
    expect(wrapper.get("[data-title]").text()).toBe("squares");
    expect(wrapper.get("[data-status]").text()).toBe("running");
  });

  it("shows a progress bar of the units when the task has a ledger", () => {
    const wrapper = mountNode({ progress: counts(750, 3000) });
    expect(wrapper.get("[data-units]").text()).toBe("750 / 3,000 units");
    const bar = wrapper.get("[data-progress]");
    expect(bar.attributes("title")).toBe("750/3000");
    expect(bar.get("div").attributes("style")).toContain("width: 25%");
    expect(bar.get("div").classes()).toContain("bg-blue-500");
  });

  it("shows no progress bar for a task without a ledger", () => {
    const wrapper = mountNode({ task: { id: "plot", type: "command" }, progress: { counts: null, last: null } });
    expect(wrapper.find("[data-progress]").exists()).toBe(false);
    expect(wrapper.find("[data-units]").exists()).toBe(false);
    expect(wrapper.get("[data-status]").text()).toBe("running");
    expect(mountNode({ progress: null }).find("[data-progress]").exists()).toBe(false);
  });

  it("shows a mini bar per worker of a split task with its server", () => {
    const wrapper = mountNode({
      progress: counts(1500, 3000),
      workers: [
        worker({ worker: "w0", node: "node-a", units_done: 1000 }),
        worker({ worker: "w1", node: "node-b", units_done: 500, state: "done" }),
        worker({ worker: "w2", node: null }),
      ],
    });
    const rows = wrapper.findAll("[data-worker]");
    expect(rows.map((r) => r.attributes("data-worker"))).toEqual(["w0", "w1", "w2"]);
    expect(rows.map((r) => r.get("[data-node]").text())).toEqual(["node-a", "node-b", "pending"]);
    expect(rows[1]?.get("[data-bar]").classes()).toContain("bg-emerald-500");
    expect(wrapper.find("[data-stack]").exists()).toBe(true);
  });

  it("gives the running workers the units their reports do not count yet, and none to a worker without a server", () => {
    // reports count the units of an attempt when it ends: the 600 done units are those of the running workers
    const wrapper = mountNode({
      progress: counts(600, 3000),
      workers: [
        worker({ worker: "w0", node: "node-a" }),
        worker({ worker: "w1", node: "node-b" }),
        worker({ worker: "w2", node: null }),
      ],
    });
    const width = (i: number) => wrapper.findAll("[data-worker]")[i]?.get("[data-bar]").attributes("style");
    expect(width(0)).toContain("width: 20%");
    expect(width(1)).toContain("width: 20%");
    expect(width(2)).toContain("width: 0%");
    const pending = wrapper.findAll("[data-worker]")[2]!;
    expect(pending.get("svg").classes()).not.toContain("animate-spin");
    expect(pending.attributes("title")).toBe("w2 waits for a server");
  });

  it("shows the workers of a task which is over as stopped, whatever their last state", () => {
    // the stream of a cancelled run ends before its pods are gone
    const wrapper = mountNode({
      state: "skipped",
      progress: counts(500, 3000),
      workers: [worker({ worker: "w0", node: "node-a" }), worker({ worker: "w1", node: "node-b", state: "done" })],
    });
    const [running, done] = wrapper.findAll("[data-worker]");
    expect(running?.get("[data-bar]").classes()).toContain("bg-gray-300");
    expect(running?.get("svg").classes()).not.toContain("animate-spin");
    expect(done?.get("[data-bar]").classes()).toContain("bg-emerald-500");
  });

  it("names the server of a single worker without a mini bar", () => {
    const wrapper = mountNode({ progress: counts(10, 3000), workers: [worker({ worker: "w0", node: "node-a" })] });
    expect(wrapper.findAll("[data-worker]")).toHaveLength(0);
    expect(wrapper.get("[data-server]").text()).toBe("node-a");
    expect(wrapper.find("[data-stack]").exists()).toBe(false);
  });

  it("rings the selected task and colors the border by its state", () => {
    const card = (w: ReturnType<typeof mountNode>) => w.get("[data-card]").classes();
    expect(card(mountNode({ selected: true }))).toContain("ring-blue-500");
    expect(card(mountNode({ selected: false }))).not.toContain("ring-blue-500");
    expect(card(mountNode({ state: "failed" }))).toContain("border-red-300");
    expect(card(mountNode({ state: "done" }))).toContain("border-emerald-300");
  });

  it("summarizes the settings of the task", () => {
    expect(mountNode({}).get("[data-settings]").text()).toBe("n 3000 · sleep_s 0.1");
  });
});
