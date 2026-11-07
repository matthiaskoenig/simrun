import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import { mk, runs, servers } from "../test/fixtures";
import TopBar from "./TopBar.vue";

function mountBar(connected = true, workflow: string | null = null) {
  return mount(TopBar, { props: { identity: mk, runs, servers, connected, workflow } });
}

describe("TopBar", () => {
  it("shows the cores the pods use of the cores of the cluster and the queued runs", () => {
    expect(mountBar().get("[data-capacity]").text()).toBe("7/16 cores|1 queued");
  });

  it("rounds the sums of fractional cores", () => {
    // 0.1 + 0.2 cores of pods on 19.3 + 19.1 cores: not 0.30000000000000004/38.400000000000006
    const fractional = [
      { ...servers[0]!, cores: 19.3, pods: [{ ...servers[0]!.pods[0]!, cores: 0.1 }] },
      { ...servers[1]!, cores: 19.1, pods: [{ ...servers[0]!.pods[1]!, cores: 0.2 }] },
    ];
    const bar = mount(TopBar, { props: { identity: mk, runs, servers: fractional, connected: true, workflow: null } });
    expect(bar.get("[data-capacity]").text()).toBe("0.3/38.4 cores|1 queued");
  });

  it("shows the initials of the identity", () => {
    const avatar = mountBar().get("[data-avatar]");
    expect(avatar.text()).toBe("MK");
    expect(avatar.attributes("title")).toBe("mk");
  });

  it("marks a stream which reconnects", () => {
    expect(mountBar(true).find("[data-reconnecting]").exists()).toBe(false);
    expect(mountBar(false).get("[data-reconnecting]").text()).toBe("reconnecting");
  });

  it("names the workflow of the selected run in the breadcrumbs", () => {
    expect(mountBar(true, "caffeine_fit").get("nav").text()).toBe("simrunExecutionscaffeine_fit");
    expect(mountBar().get("nav").text()).toBe("simrunExecutions");
  });
});
