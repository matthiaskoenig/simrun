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
