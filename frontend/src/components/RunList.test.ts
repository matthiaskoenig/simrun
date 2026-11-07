import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import { mk, runs } from "../test/fixtures";
import RunList from "./RunList.vue";

function mountList(selected: string | null = null) {
  return mount(RunList, { props: { runs, selected, identity: mk } });
}

const cardIds = (wrapper: ReturnType<typeof mountList>): string[] =>
  wrapper.findAll("[data-run]").map((card) => card.attributes("data-run") ?? "");

describe("RunList", () => {
  it("renders a card per run with its workflow, its id, its group and its state", () => {
    const wrapper = mountList();
    expect(cardIds(wrapper)).toEqual(["r6", "r5", "r4", "r3", "r2", "r1"]);
    const card = wrapper.get('[data-run="r6"]');
    expect(card.get("[data-title]").text()).toBe("caffeine_fit");
    expect(card.get("[data-meta]").text()).toMatch(/^fits · mk-bot · /);
    expect(card.get("[data-id]").text()).toBe("#r6");
    expect(wrapper.get('[data-run="r1"] [data-meta]').text()).toMatch(/^mk · /);
    expect(card.get("[data-status]").text()).toBe("running");
    expect(wrapper.get('[data-run="r4"] [data-status]').text()).toBe("succeeded");
  });

  it("shows the message of a failed run in red", () => {
    const note = mountList().get('[data-run="r1"] [data-note]');
    expect(note.text()).toBe("task squares: out of memory, 8 of 8 GiB");
    expect(note.classes()).toContain("text-red-700");
  });

  it("shows how long a finished run took", () => {
    const note = mountList().get('[data-run="r4"] [data-note]');
    expect(note.text()).toBe("done in 38 min");
    expect(note.classes()).toContain("text-gray-500");
  });

  it("marks the runs of agents with the bot and the runs of people with their initials", () => {
    const wrapper = mountList();
    expect(wrapper.find('[data-run="r6"] [data-avatar] svg').exists()).toBe(true);
    expect(wrapper.get('[data-run="r3"] [data-avatar]').text()).toBe("MK");
  });

  it("filters with the chips", async () => {
    const wrapper = mountList();
    await wrapper.get('[data-filter="agents"]').trigger("click");
    expect(cardIds(wrapper)).toEqual(["r6", "r5", "r4"]);
    expect(wrapper.get('[data-filter="agents"]').classes()).toContain("bg-gray-900");
    await wrapper.get('[data-filter="mine"]').trigger("click");
    expect(cardIds(wrapper)).toEqual(["r6", "r5", "r3", "r1"]);
    await wrapper.get('[data-filter="active"]').trigger("click");
    expect(cardIds(wrapper)).toEqual(["r6", "r5", "r3"]);
  });

  it("filters by group when the runs have groups", async () => {
    const wrapper = mountList();
    const select = wrapper.get("select");
    expect(select.findAll("option").map((o) => o.text())).toEqual(["All groups", "fits"]);
    await select.setValue("fits");
    expect(cardIds(wrapper)).toEqual(["r6", "r3"]);
  });

  it("has no group select when no run has a group", () => {
    const wrapper = mount(RunList, { props: { runs: runs.map((r) => ({ ...r, group: null })), selected: null, identity: mk } });
    expect(wrapper.find("select").exists()).toBe(false);
  });

  it("says when no run matches", async () => {
    const wrapper = mount(RunList, { props: { runs: [], selected: null, identity: mk } });
    expect(wrapper.text()).toContain("No runs");
  });

  it("emits select with the id of a card and marks the selected card", async () => {
    const wrapper = mountList("r5");
    expect(wrapper.get('[data-run="r5"]').classes()).toContain("bg-blue-50/70");
    await wrapper.get('[data-run="r4"]').trigger("click");
    expect(wrapper.emitted("select")).toEqual([["r4"]]);
  });
});
