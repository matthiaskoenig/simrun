import { flushPromises, mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import type { RunRecord, TaskProgress } from "../api/client";
import { api, ApiError } from "../api/client";
import { runRecord } from "../test/fixtures";
import RunBar from "./RunBar.vue";

const progress: Record<string, TaskProgress> = {
  a: { counts: { pending: 600, claimed: 10, done: 390, total: 1000 }, last: null },
  b: { counts: { pending: 0, claimed: 0, done: 0, total: 250 }, last: null },
  plot: { counts: null, last: null },
};

function mountBar(fields: Partial<RunRecord> = {}) {
  return mount(RunBar, { props: { record: runRecord(fields), progress }, attachTo: document.body });
}

describe("RunBar", () => {
  it("shows the run, who submitted it, its state and the share of its units done", () => {
    const wrapper = mountBar({ id: "20261107-002755-17eb1c" });
    expect(wrapper.get("[data-run-id]").text()).toBe("Run #20261107-002755-17eb1c");
    expect(wrapper.get("[data-who]").text()).toBe("mk");
    expect(wrapper.get("[data-started]").text()).toMatch(/^started /);
    expect(wrapper.get("[data-status]").text()).toBe("running");
    expect(wrapper.get("[data-percent]").text()).toBe("31 %");
    expect(wrapper.get("[data-percent]").attributes("title")).toBe("390 of 1,250 units done");
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
