import { flushPromises, mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import type { RunRecord, TaskProgress } from "../api/client";
import { api, ApiError } from "../api/client";
import { formatTime } from "../lib/format";
import { runRecord } from "../test/fixtures";
import RunBar from "./RunBar.vue";

const progress: Record<string, TaskProgress> = {
  a: { counts: { pending: 600, claimed: 10, done: 390, total: 1000 }, first: null, last: null },
  b: { counts: { pending: 0, claimed: 0, done: 0, total: 250 }, first: null, last: null },
  plot: { counts: null, first: null, last: null },
};
const tasks: RunRecord["tasks"] = { a: "running", b: "running", plot: "waiting", prep: "done" };

function mountBar(fields: Partial<RunRecord> = {}, events: Record<string, TaskProgress> = progress) {
  return mount(RunBar, { props: { record: runRecord({ tasks, ...fields }), progress: events }, attachTo: document.body });
}

describe("RunBar", () => {
  it("shows the run, who submitted it, its state and the share of its units done", () => {
    const wrapper = mountBar({ id: "20261107-002755-17eb1c" });
    expect(wrapper.get("[data-run-id]").text()).toBe("Run #17eb1c");
    expect(wrapper.get("[data-run-id]").attributes("title")).toBe("Run #20261107-002755-17eb1c");
    expect(wrapper.get("[data-who]").text()).toBe("mk");
    expect(wrapper.get("[data-started]").text()).toMatch(/^started /);
    expect(wrapper.get("[data-status]").text()).toBe("running");
    // the mean of 0.39, 0, 0 and 1 over the four tasks, as the run card counts
    expect(wrapper.get("[data-percent]").text()).toBe("34 %");
    expect(wrapper.get("[data-percent]").attributes("title")).toBe("1 of 4 tasks done, 390 of 1,250 units");
    wrapper.unmount();
  });

  it("links Rerun with changes to the Editor of the run, also for a finished run", () => {
    for (const state of ["running", "succeeded"] as const) {
      const wrapper = mountBar({ id: "r1", state });
      expect(wrapper.get("[data-rerun]").attributes("href")).toBe("#/runs/r1/edit");
      expect(wrapper.get("[data-rerun]").text()).toBe("Rerun with changes");
      wrapper.unmount();
    }
  });

  it("starts a run when its image is ready, else when it was submitted", () => {
    const created = "2026-11-07T13:02:00Z";
    const built = "2026-11-07T13:09:00Z";
    const wrapper = mountBar({ created, built });
    const started = wrapper.get("[data-started]");
    expect(started.text()).toBe(`started ${formatTime(built)}`);
    expect(started.attributes("title")).toBe(`submitted ${formatTime(created)}`);
    wrapper.unmount();
    const submitting = mountBar({ created, built: null });
    expect(submitting.get("[data-started]").text()).toBe(`started ${formatTime(created)}`);
    submitting.unmount();
  });

  it("shows no 100 % while a task is not over, even with every unit done", () => {
    const done = { counts: { pending: 0, claimed: 0, done: 1000, total: 1000 }, first: null, last: null };
    const wrapper = mountBar({ tasks: { a: "running" } }, { a: done });
    expect(wrapper.get("[data-percent]").text()).toBe("99 %");
    wrapper.unmount();
  });

  it("names the owner of an agent", () => {
    const wrapper = mountBar({ agent: "mk-bot", submitted_by: "mk-bot" });
    expect(wrapper.get("[data-who]").text()).toBe("mk-bot for mk");
    wrapper.unmount();
  });

  it("stops a run only after the confirmation in the bar", async () => {
    const cancel = vi.spyOn(api, "cancel").mockResolvedValue({} as never);
    const confirm = vi.spyOn(window, "confirm");
    const wrapper = mountBar();
    expect(wrapper.find("[data-stop-confirm]").exists()).toBe(false);

    await wrapper.get("[data-stop]").trigger("click");
    expect(wrapper.find("[data-stop-confirm]").exists()).toBe(true);
    expect(cancel).not.toHaveBeenCalled();
    await wrapper.get("[data-stop-confirm]").trigger("keydown", { key: "Escape" });
    expect(wrapper.find("[data-stop-confirm]").exists()).toBe(false);

    await wrapper.get("[data-stop]").trigger("click");
    await wrapper.get("[data-stop-yes]").trigger("click");
    await flushPromises();
    expect(cancel).toHaveBeenCalledWith("r1");
    expect(wrapper.find("[data-stop-confirm]").exists()).toBe(false);
    expect(confirm).not.toHaveBeenCalled();
    wrapper.unmount();
  });

  it("shows why a run could not be stopped", async () => {
    vi.spyOn(api, "cancel").mockRejectedValue(new ApiError(409, "run r1 has ended: succeeded"));
    const wrapper = mountBar();
    await wrapper.get("[data-stop]").trigger("click");
    await wrapper.get("[data-stop-yes]").trigger("click");
    await flushPromises();
    expect(wrapper.get("[data-stop-error]").text()).toBe("run r1 has ended: succeeded");
    wrapper.unmount();
  });

  it("offers no stop for a run which is over, and no share of units for a run which does not run", () => {
    const wrapper = mountBar({ state: "succeeded" });
    expect(wrapper.find("[data-stop]").exists()).toBe(false);
    expect(wrapper.find("[data-percent]").exists()).toBe(false);
    wrapper.unmount();
  });
});
