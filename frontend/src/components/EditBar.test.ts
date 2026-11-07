import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";
import EditBar from "./EditBar.vue";

const tree = [{ name: "model", branch: "fit-cyp1a2", commit: "abcdef0123456789", changed_files: ["a.py", "b.py", "c.py"] }];

const defaults = {
  tree,
  host: "laptop" as string | null,
  connected: true,
  valid: true as boolean | null,
  errorCount: 0,
  dirty: false,
  changedOnDisk: false,
  saving: false,
  running: false,
  error: null as string | null,
  run: null as string | null,
};

function mountBar(props: Partial<typeof defaults> = {}) {
  return mount(EditBar, { props: { ...defaults, ...props } });
}

describe("EditBar", () => {
  it("shows the branch, the short commit and the uncommitted files with a title", () => {
    const bar = mountBar();
    expect(bar.get("[data-branch]").text()).toBe("fit-cyp1a2");
    expect(bar.get("[data-commit]").text()).toBe("abcdef0");
    const dirty = bar.get("[data-uncommitted]");
    expect(dirty.text()).toBe("3 uncommitted files");
    expect(dirty.attributes("title")).toBe("a.py\nb.py\nc.py");
    expect(dirty.classes()).toContain("bg-amber-50");
  });

  it("shows no badge for a clean tree", () => {
    const bar = mountBar({ tree: [{ ...tree[0]!, changed_files: [] }] });
    expect(bar.find("[data-uncommitted]").exists()).toBe(false);
  });

  it("shows the connected machine with a green dot", () => {
    const bar = mountBar();
    expect(bar.get("[data-machine]").text()).toBe("laptop");
    expect(bar.get("[data-dot]").classes()).toContain("bg-emerald-500");
  });

  it("says valid, saved when the draft is valid and clean", () => {
    expect(mountBar().get("[data-state]").text()).toBe("valid, saved");
    expect(mountBar().get("[data-state]").classes()).toContain("text-emerald-700");
  });

  it("says unsaved changes for a dirty draft", () => {
    expect(mountBar({ dirty: true }).get("[data-state]").text()).toBe("unsaved changes");
  });

  it("counts the errors in red and disables Run", () => {
    const bar = mountBar({ valid: false, errorCount: 2 });
    expect(bar.get("[data-state]").text()).toBe("2 errors");
    expect(bar.get("[data-state]").classes()).toContain("text-red-700");
    expect(bar.get("[data-run]").attributes("disabled")).toBeDefined();
    expect(mountBar({ valid: false, errorCount: 1 }).get("[data-state]").text()).toBe("1 error");
  });

  it("offers Reload and Save anyway when the file changed on disk", async () => {
    const bar = mountBar({ changedOnDisk: true, dirty: true });
    expect(bar.get("[data-state]").text()).toBe("changed on disk");
    expect(bar.get("[data-state]").classes()).toContain("text-amber-800");
    await bar.get("[data-reload]").trigger("click");
    await bar.get("[data-save-anyway]").trigger("click");
    expect(bar.emitted("reload")).toHaveLength(1);
    expect(bar.emitted("save")).toEqual([[true]]);
    expect(bar.find("[data-reload]").exists()).toBe(true);
  });

  it("saves and runs", async () => {
    const bar = mountBar({ dirty: true });
    await bar.get("[data-save]").trigger("click");
    await bar.get("[data-run]").trigger("click");
    expect(bar.emitted("save")).toEqual([[false]]);
    expect(bar.emitted("run")).toHaveLength(1);
    expect(bar.find("[data-download]").exists()).toBe(false);
  });

  it("shows Running... and disables Run while the request waits", () => {
    const bar = mountBar({ running: true });
    expect(bar.get("[data-run]").text()).toBe("Running...");
    expect(bar.get("[data-run]").attributes("disabled")).toBeDefined();
  });

  it("shows not connected in gray and Download YAML instead of Save", async () => {
    const bar = mountBar({ connected: false });
    expect(bar.get("[data-machine]").text()).toBe("laptop not connected");
    expect(bar.get("[data-dot]").classes()).not.toContain("bg-emerald-500");
    expect(bar.find("[data-save]").exists()).toBe(false);
    expect(bar.get("[data-download]").text()).toBe("Download YAML");
    await bar.get("[data-download]").trigger("click");
    expect(bar.emitted("download")).toHaveLength(1);
    // there is no file to be saved or changed
    expect(bar.get("[data-state]").text()).toBe("valid");
  });

  it("shows the error under the bar", () => {
    expect(mountBar({ error: "no such group" }).get("[data-error]").text()).toBe("no such group");
  });

  it("is the floating card of the mockup, with Save and Run as a group on the right", () => {
    const bar = mountBar();
    expect(bar.get("[data-edit-bar]").classes()).toEqual(expect.arrayContaining(["rounded-lg", "border", "shadow-sm", "bg-white/95"]));
    const actions = bar.get("[data-actions]");
    expect(actions.find("[data-save]").exists()).toBe(true);
    expect(actions.find("[data-run]").exists()).toBe(true);
    expect(bar.get("[data-edit-bar]").find("[data-run]").exists()).toBe(false);
  });

  it("says a dirty draft without a connected machine must be downloaded", () => {
    const bar = mountBar({ connected: false, dirty: true });
    expect(bar.get("[data-state]").text()).toBe("unsaved changes, download to keep them");
    expect(bar.get("[data-state]").classes()).toContain("text-amber-800");
  });

  it("offers neither Reload nor Save anyway without a connected machine", () => {
    const bar = mountBar({ connected: false, dirty: true, changedOnDisk: true });
    expect(bar.find("[data-reload]").exists()).toBe(false);
    expect(bar.find("[data-save-anyway]").exists()).toBe(false);
    expect(bar.get("[data-state]").text()).toBe("unsaved changes, download to keep them");
  });

  it("names the run a rerun comes from instead of a machine", () => {
    const bar = mountBar({ connected: false, host: null, run: "20261107-111319-5f81f7" });
    expect(bar.get("[data-machine]").text()).toBe("from run #5f81f7");
    expect(bar.find("[data-dot]").exists()).toBe(false);
  });

  it("reads Run workflow as the mockup", () => {
    expect(mountBar().get("[data-run]").text()).toBe("Run workflow");
  });
});
