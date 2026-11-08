import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { RunRecord, TaskProgress, WorkerView } from "../api/client";
import { api, ApiError } from "../api/client";
import { runRecord } from "../test/fixtures";
import Inspector from "./Inspector.vue";

const NOW = new Date("2026-11-07T14:00:00Z");

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

const record = (fields: Partial<RunRecord> = {}): RunRecord =>
  runRecord({
    id: "r7",
    built: "2026-11-07T13:50:00Z",
    workflow: {
      name: "squares",
      split: "fastest",
      tasks: [
        { id: "squares", type: "demo.squares", settings: { n: 3000 } },
        { id: "plot", type: "command", inputs: { squares: "squares.squares" } },
      ],
    },
    tasks: { squares: "running", plot: "waiting" },
    plans: {
      squares: {
        task_id: "squares",
        pods: 3,
        units: 3000,
        shape: { cores: 9, memory_gb: 5, processes: 9, per_worker_gb: 0.5, retry_memory_gb: 7.5, retry_per_worker_gb: 0.75 },
      },
    },
    ...fields,
  });

// 750 of 3000 units in the 10 min since the build of the run, the start of
// this task without inputs: 1.25 units/s, 1800 s to go
const running: TaskProgress = {
  counts: { pending: 2200, claimed: 50, done: 750, total: 3000 },
  first: { time: "2026-11-07T13:50:30+00:00", worker: "squares-w0", event: "unit_done", unit: 0 },
  last: { time: "2026-11-07T13:59:58+00:00", worker: "squares-w1", event: "unit_done", unit: 812, index: 812, cost: 1.09312, evaluations: 4200 },
};

const workers: WorkerView[] = [
  worker({ worker: "squares-w1", node: "node-b", units_done: 250, processes: 9, peak_memory_gb: 1.24, memory_limit_gb: 5, state: "done" }),
  worker({ worker: "squares-w0", node: "node-a", units_done: 500, processes: 9, peak_memory_gb: 2.5, memory_limit_gb: 5 }),
  // no report yet: the processes and the limit its plan requested
  worker({ worker: "squares-w2", node: null, processes: null }),
];

function mountInspector(props: {
  record?: RunRecord;
  progress?: Record<string, TaskProgress>;
  workers?: Record<string, WorkerView[]>;
  task?: string | null;
}) {
  return mount(Inspector, {
    props: {
      record: record(),
      progress: { squares: running, plot: { counts: null, first: null, last: null } },
      workers: { squares: workers },
      task: "squares",
      ...props,
    },
  });
}

const tile = (wrapper: ReturnType<typeof mountInspector>, label: string): string =>
  wrapper.get(`[data-tile="${label}"] [data-value]`).text();

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval", "setTimeout", "clearTimeout", "Date"] });
  vi.setSystemTime(NOW);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("Inspector of a task", () => {
  it("shows the type, the task id and its state in the header, and the three tabs", () => {
    const wrapper = mountInspector({});
    expect(wrapper.get("[data-type]").text()).toBe("demo.squares");
    expect(wrapper.get("[data-title]").text()).toBe("squares");
    expect(wrapper.get("header [data-status]").text()).toBe("running");
    expect(wrapper.findAll("[data-tab]").map((t) => t.text())).toEqual(["Overview", "Outputs", "Logs"]);
    expect(wrapper.get('[data-tab="overview"]').attributes("aria-selected")).toBe("true");
  });

  it("shows the units, the elapsed and the remaining time and the numeric fields of the last progress line", () => {
    const wrapper = mountInspector({});
    expect(tile(wrapper, "Units")).toBe("750 / 3,000");
    expect(tile(wrapper, "Elapsed")).toBe("10 min");
    expect(tile(wrapper, "Remaining")).toBe("≈ 30 min");
    expect(tile(wrapper, "cost")).toBe("1.0931");
    expect(tile(wrapper, "evaluations")).toBe("4,200");
    expect(wrapper.findAll("[data-tile]").map((t) => t.attributes("data-tile"))).toEqual([
      "Units",
      "Elapsed",
      "Remaining",
      "cost",
      "evaluations",
    ]);
  });

  it("counts the time of a task without inputs from the build of the run, also before its first unit", () => {
    const wrapper = mountInspector({
      progress: { squares: { counts: { pending: 3000, claimed: 9, done: 0, total: 3000 }, first: null, last: null } },
    });
    expect(tile(wrapper, "Units")).toBe("0 / 3,000");
    expect(tile(wrapper, "Elapsed")).toBe("10 min");
    expect(tile(wrapper, "Remaining")).toBe("-");
  });

  it("counts the time of a task with inputs from its first progress line, unknown before it", () => {
    const tasks = { squares: "done", plot: "running" } as const;
    const before = mountInspector({ record: record({ tasks }), task: "plot", progress: { plot: { counts: null, first: null, last: null } } });
    expect(tile(before, "Elapsed")).toBe("starting");
    const first = { time: "2026-11-07T13:55:00+00:00", event: "unit_done", unit: 0 };
    const after = mountInspector({
      record: record({ tasks }),
      task: "plot",
      progress: { plot: { counts: { pending: 10, claimed: 0, done: 10, total: 20 }, first, last: first } },
    });
    expect(tile(after, "Elapsed")).toBe("5 min");
    expect(tile(after, "Remaining")).toBe("≈ 5 min");
  });

  it("shows why a failed task failed, its part of the message of the run", () => {
    const message = "task plot failed: plot exited 2 (see its logs); task squares failed: squares-w0 exited 4, a configuration error (see its logs)";
    const wrapper = mountInspector({ record: record({ state: "failed", message, tasks: { squares: "failed", plot: "failed" } }) });
    expect(wrapper.get("[data-failure]").text()).toBe("task squares failed: squares-w0 exited 4, a configuration error (see its logs)");
  });

  it("names the merge of a running task once every unit is done", () => {
    const merging: TaskProgress = { ...running, counts: { pending: 0, claimed: 0, done: 3000, total: 3000 } };
    const wrapper = mountInspector({ progress: { squares: merging } });
    expect(tile(wrapper, "Remaining")).toBe("merging");
  });

  it("stops the clock of a task which is over at its last unit", () => {
    const done: TaskProgress = { ...running, counts: { pending: 0, claimed: 0, done: 3000, total: 3000 } };
    const wrapper = mountInspector({ record: record({ tasks: { squares: "done", plot: "running" } }), progress: { squares: done } });
    expect(tile(wrapper, "Elapsed")).toBe("10 min");
    expect(tile(wrapper, "Remaining")).toBe("-");
  });

  it("shows no units and no times for a task which has not started", () => {
    const wrapper = mountInspector({ task: "plot" });
    expect(wrapper.find('[data-tile="Units"]').exists()).toBe(false);
    expect(tile(wrapper, "Elapsed")).toBe("not started");
    expect(wrapper.find("[data-workers]").exists()).toBe(false);
  });

  it("lists every worker with its server, processes, peak and limit of memory and state, planned without a report", () => {
    const wrapper = mountInspector({});
    const rows = wrapper.findAll("[data-worker]");
    expect(rows.map((r) => r.attributes("data-worker"))).toEqual(["squares-w0", "squares-w2", "squares-w1"]);
    expect(rows.map((r) => r.get("[data-node]").text())).toEqual(["node-a", "pending", "node-b"]);
    expect(rows.map((r) => r.get("[data-processes]").text())).toEqual(["9 processes", "9 processes", "9 processes"]);
    expect(rows.map((r) => r.get("[data-memory]").text())).toEqual(["2.5 / 5 GB", "- / 5 GB", "1.2 / 5 GB"]);
    expect(rows.map((r) => r.get("[data-status]").text())).toEqual(["running", "pending", "done"]);
    expect(rows.map((r) => r.get("[data-units]").text())).toEqual(["500 units", "0 units", "250 units"]);
  });

  it("draws the workers of a task which is over as stopped and names a kill for memory", () => {
    const killed = worker({ worker: "squares-w0", node: "node-a", state: "failed", oom_killed: true, attempts: 2, processes: null });
    const wrapper = mountInspector({
      record: record({ state: "cancelled", tasks: { squares: "failed", plot: "skipped" } }),
      workers: { squares: [killed, worker({ worker: "squares-w1", node: "node-b" })] },
    });
    const rows = wrapper.findAll("[data-worker]");
    expect(rows.map((r) => r.get("[data-status]").text())).toEqual(["stopped", "failed"]);
    expect(rows[1]?.get("[data-note]").text()).toBe("Attempt 2, killed for memory.");
    // a retry has the memory of the retry; its processes are not planned
    expect(rows[1]?.get("[data-processes]").text()).toBe("- processes");
    expect(rows[1]?.get("[data-memory]").text()).toBe("- / 7.5 GB");
  });

  it("shows the resources planned for the task", () => {
    const wrapper = mountInspector({});
    expect(wrapper.get("[data-resources]").text()).toContain(
      "Requested 5 GB and 9 cores per pod for 9 processes of 0.5 GB, on up to 3 pods.",
    );
    expect(wrapper.get("[data-resources]").text()).toContain("A pod out of memory is retried with 0.75 GB per process and 7.5 GB per pod.");
  });

  it("lists the outputs with their sizes and store paths, numbers in their order", async () => {
    const outputs = vi.spyOn(api, "outputs").mockResolvedValue([
      { path: "runs/r7/tasks/squares/out/summary.json", bytes: 2048 },
      { path: "runs/r7/tasks/squares/out/squares/10.json", bytes: 13 },
      { path: "runs/r7/tasks/squares/out/squares/2.json", bytes: 12 },
    ]);
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="outputs"]').trigger("click");
    await flushPromises();
    expect(outputs).toHaveBeenCalledWith("r7", "squares");
    expect(wrapper.get("[data-out-dir]").text()).toBe("runs/r7/tasks/squares/out/");
    const rows = wrapper.findAll("[data-output]");
    expect(rows.map((r) => r.get("[data-path]").text())).toEqual(["squares/2.json", "squares/10.json", "summary.json"]);
    expect(rows.map((r) => r.attributes("title"))).toEqual([
      "runs/r7/tasks/squares/out/squares/2.json",
      "runs/r7/tasks/squares/out/squares/10.json",
      "runs/r7/tasks/squares/out/summary.json",
    ]);
    expect(rows.map((r) => r.get("[data-size]").text())).toEqual(["12 B", "13 B", "2 KB"]);
    expect(wrapper.get("[data-outputs-total]").text()).toBe("3 files, 2 KB");
  });

  it("lists the first 200 files of a task of many units", async () => {
    const files = Array.from({ length: 250 }, (_, i) => ({ path: `runs/r7/tasks/squares/out/squares/${i}.json`, bytes: 1024 }));
    vi.spyOn(api, "outputs").mockResolvedValue(files);
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="outputs"]').trigger("click");
    await flushPromises();
    expect(wrapper.findAll("[data-output]")).toHaveLength(200);
    expect(wrapper.get("[data-outputs-total]").text()).toBe("50 more, 250 files, 250 KB");
  });

  it("says when a task has written nothing yet", async () => {
    vi.spyOn(api, "outputs").mockResolvedValue([]);
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="outputs"]').trigger("click");
    await flushPromises();
    expect(wrapper.get("[data-outputs-empty]").text()).toBe("Nothing written yet.");
  });

  it("shows the logs in a monospace block, refreshed every 5 s while the task runs", async () => {
    const logs = vi.spyOn(api, "logs").mockResolvedValueOnce("unit 1 done\n").mockResolvedValue("unit 1 done\nunit 2 done\n");
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await flushPromises();
    const block = wrapper.get("[data-logs]");
    expect(block.classes()).toContain("font-mono");
    expect(block.text()).toBe("unit 1 done");
    expect(logs).toHaveBeenCalledWith("r7", "squares");
    await vi.advanceTimersByTimeAsync(5000);
    expect(logs).toHaveBeenCalledTimes(2);
    expect(wrapper.get("[data-logs]").text()).toBe("unit 1 done\nunit 2 done");
    wrapper.unmount();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(logs).toHaveBeenCalledTimes(2);
  });

  it("hides the lines of the Argo executor unless asked for and marks the errors", async () => {
    const text = [
      "==> squares-w0 (pod-1) <==",
      'time=2026-11-08T09:03:19Z level=INFO msg="waiting for signals" argo=true',
      "Traceback (most recent call last):",
      "ModuleNotFoundError: No module named 'simrun.plugins_builtin'",
      "unit 1 done",
    ].join("\n");
    vi.spyOn(api, "logs").mockResolvedValue(text);
    const wrapper = mountInspector({ record: record({ state: "failed", tasks: { squares: "failed", plot: "waiting" } }) });
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await flushPromises();
    expect(wrapper.get("[data-logs]").text()).not.toContain("waiting for signals");
    const red = wrapper.findAll("[data-logs-text] span.text-red-300").map((s) => s.text());
    expect(red).toEqual(["Traceback (most recent call last):", "ModuleNotFoundError: No module named 'simrun.plugins_builtin'"]);
    await wrapper.get("[data-executor]").setValue(true);
    expect(wrapper.get("[data-logs]").text()).toContain("waiting for signals");
  });

  it("keeps the layout of the lines, or wraps them at the width of the panel when asked", async () => {
    vi.spyOn(api, "logs").mockResolvedValue("a long line of the console of the run layer\n");
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await flushPromises();
    const pre = () => wrapper.get("[data-logs-text]").classes();
    expect(pre()).toContain("whitespace-pre");
    expect(pre()).toContain("w-max");
    await wrapper.get("[data-wrap]").setValue(true);
    expect(pre()).toContain("whitespace-pre-wrap");
    expect(pre()).not.toContain("w-max");
    wrapper.unmount();
  });

  it("reads the logs one request at a time, so that slow logs still show", async () => {
    const logs = vi
      .spyOn(api, "logs")
      .mockImplementation(() => new Promise((resolve) => setTimeout(() => resolve("slow\n"), 6000)));
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await vi.advanceTimersByTimeAsync(5000);
    // no second request while the first one is open
    expect(logs).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(wrapper.get("[data-logs]").text()).toBe("slow");
    // the next one 5 s after the answer
    await vi.advanceTimersByTimeAsync(4999);
    expect(logs).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(logs).toHaveBeenCalledTimes(2);
    wrapper.unmount();
  });

  it("keeps the logs at their bottom as lines come, unless the user scrolled up", async () => {
    vi.spyOn(api, "logs").mockResolvedValue("a\nb\n");
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    const block = wrapper.get("[data-logs]").element as HTMLElement;
    Object.defineProperty(block, "scrollHeight", { configurable: true, value: 1000 });
    Object.defineProperty(block, "clientHeight", { configurable: true, value: 200 });
    await flushPromises();
    await vi.advanceTimersByTimeAsync(5000);
    expect(block.scrollTop).toBe(1000);
    block.scrollTop = 300;
    await wrapper.get("[data-logs]").trigger("scroll");
    await vi.advanceTimersByTimeAsync(5000);
    expect(block.scrollTop).toBe(300);
    block.scrollTop = 800;
    await wrapper.get("[data-logs]").trigger("scroll");
    await vi.advanceTimersByTimeAsync(5000);
    expect(block.scrollTop).toBe(1000);
  });

  it("stops refreshing the logs once the task is final, after reading them once more", async () => {
    const logs = vi.spyOn(api, "logs").mockResolvedValue("done\n");
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await flushPromises();
    await wrapper.setProps({ record: record({ tasks: { squares: "done", plot: "running" } }) });
    await flushPromises();
    expect(logs).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(logs).toHaveBeenCalledTimes(2);
  });

  it("reads the logs of another task when the selection changes, and says why logs are missing", async () => {
    const logs = vi
      .spyOn(api, "logs")
      .mockResolvedValueOnce("squares\n")
      .mockRejectedValueOnce(new ApiError(409, "run r7 has no workflow"));
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await flushPromises();
    await wrapper.setProps({ task: "plot" });
    await flushPromises();
    expect(logs).toHaveBeenLastCalledWith("r7", "plot");
    expect(wrapper.find("[data-logs-text]").exists()).toBe(false);
    expect(wrapper.get("[data-logs-error]").text()).toBe("run r7 has no workflow");
  });

  it("keeps the tab when another task is selected", async () => {
    vi.spyOn(api, "logs").mockResolvedValue("");
    const wrapper = mountInspector({});
    await wrapper.get('[data-tab="logs"]').trigger("click");
    await wrapper.setProps({ task: "plot" });
    expect(wrapper.get('[data-tab="logs"]').attributes("aria-selected")).toBe("true");
    await flushPromises();
    expect(wrapper.get("[data-logs]").text()).toBe("no output yet");
  });

  it("says when the selected task is not part of the run", () => {
    const wrapper = mountInspector({ task: "gone" });
    expect(wrapper.get("[data-missing]").text()).toBe("This task is not part of run #r7.");
  });
});

describe("Inspector of a run", () => {
  it("shows the run with its state, message, priority and sources with their branches and changed files", () => {
    const wrapper = mountInspector({
      task: null,
      record: record({
        state: "failed",
        priority: "owner",
        message: "task squares: out of memory, 8 of 8 GiB",
        sources: [
          {
            name: "model",
            branch: "develop",
            commit: "3f2a91c0d4e5",
            dirty: true,
            changed_files: ["src/model.py", "README.md"],
            diff_sha256: "x",
            patch: null,
            snapshot: "model-abc",
          },
          {
            name: "sbmlsim",
            branch: null,
            commit: "9e8d7c6b5a43",
            dirty: false,
            changed_files: [],
            diff_sha256: "y",
            patch: null,
            snapshot: "sbmlsim-def",
          },
        ],
      }),
    });
    expect(wrapper.get("[data-type]").text()).toBe("Run");
    expect(wrapper.get("[data-title]").text()).toBe("squares");
    expect(wrapper.get("header [data-status]").text()).toBe("failed");
    expect(wrapper.find("[data-tab]").exists()).toBe(false);
    const message = wrapper.get("[data-message]");
    expect(message.text()).toBe("task squares: out of memory, 8 of 8 GiB");
    expect(message.classes()).toContain("text-red-700");
    expect(wrapper.get('[data-detail="Priority"]').text()).toContain("owner");
    expect(wrapper.get('[data-detail="Run"]').text()).toContain("#r7");
    const sources = wrapper.findAll("[data-source]");
    expect(sources.map((s) => s.attributes("data-source"))).toEqual(["model", "sbmlsim"]);
    expect(sources.map((s) => s.get("[data-branch]").text())).toEqual(["develop@3f2a91c", "detached@9e8d7c6"]);
    expect(sources[0]?.findAll("[data-changed]").map((f) => f.text())).toEqual(["src/model.py", "README.md"]);
    expect(sources[1]?.get("[data-clean]").text()).toBe("clean");
    expect(wrapper.get("[data-hint]").text()).toBe("Select a task on the canvas for its workers, outputs and logs.");
  });

  it("says when a run has no sources and lists its kills for memory", () => {
    const wrapper = mountInspector({ task: null, record: record({ oom_kills: { "squares-w0": [0, 1] } }) });
    expect(wrapper.get("[data-no-sources]").text()).toBe("No sources, the workflow runs on the image alone.");
    expect(wrapper.get("[data-oom]").text()).toBe("squares-w0: attempts 1 and 2");
    expect(wrapper.find("[data-message]").exists()).toBe(false);
  });
});
