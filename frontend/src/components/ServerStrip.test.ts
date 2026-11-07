import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import { servers } from "../test/fixtures";
import ServerStrip from "./ServerStrip.vue";

function mountStrip(selected: string | null = "r6") {
  return mount(ServerStrip, { props: { servers, selected } });
}

describe("ServerStrip", () => {
  it("renders a tile per server with its used cores and memory", () => {
    const wrapper = mountStrip();
    expect(wrapper.findAll("[data-server]").map((t) => t.attributes("data-server"))).toEqual(["node-a", "node-b", "node-c"]);
    const tile = wrapper.get('[data-server="node-a"]');
    expect(tile.get("[data-name]").text()).toBe("node-a");
    expect(tile.get("[data-capacity]").text()).toBe("8 c · 16 GB");
    expect(tile.get("[data-used-cores]").text()).toBe("3/8 cores");
    expect(tile.get("[data-used-memory]").text()).toBe("6.5 GB");
    const idle = wrapper.get('[data-server="node-b"]');
    expect(idle.get("[data-capacity]").text()).toBe("4 c · 7.5 GB");
    expect(idle.get("[data-used-cores]").text()).toBe("0/4 cores");
    expect(idle.get("[data-used-memory]").text()).toBe("0 GB");
  });

  it("shows the pods of the selected run in blue and those of other runs in gray, by their share of the cores", () => {
    const tile = mountStrip().get('[data-server="node-a"]');
    const segments = tile.findAll("[data-pod]");
    expect(segments.map((s) => s.attributes("data-pod"))).toEqual(["r6-fit-w0", "r5-squares-w0"]);
    expect(segments[0]?.classes()).toContain("bg-blue-500");
    expect(segments[0]?.attributes("style")).toContain("width: 25%");
    expect(segments[1]?.classes()).toContain("bg-slate-300");
    expect(segments[1]?.attributes("style")).toContain("width: 12.5%");
    expect(tile.classes()).toContain("border-blue-200");
    expect(mountStrip().get('[data-server="node-b"]').classes()).not.toContain("border-blue-200");
  });

  it("shows every pod in gray without a selected run", () => {
    const wrapper = mountStrip(null);
    expect(wrapper.findAll(".bg-blue-500").filter((e) => e.attributes("data-pod") !== undefined)).toEqual([]);
    expect(wrapper.text()).not.toContain("run #");
  });

  it("shows a reserved server striped, with the run holding it", () => {
    const tile = mountStrip().get('[data-server="node-c"]');
    expect(tile.get("[data-bar]").classes()).toContain("stripes");
    expect(tile.attributes("title")).toContain("reserved by run r3");
    expect(mountStrip().get('[data-server="node-a"] [data-bar]').classes()).not.toContain("stripes");
  });

  it("names the selected run in the legend, with other runs and reserved", () => {
    const legend = mountStrip().get("[data-legend]").text();
    expect(legend).toContain("run #r6");
    expect(legend).toContain("other runs");
    expect(legend).toContain("reserved");
    expect(legend).not.toContain("checkpointing");
  });

  it("says when the cluster has no servers", () => {
    expect(mount(ServerStrip, { props: { servers: [], selected: null } }).text()).toContain("No servers");
  });
});
