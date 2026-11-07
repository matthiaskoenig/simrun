import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope } from "vue";
import type { EditSession, SourceProvenance, Workflow } from "../api/client";
import { auth } from "../auth";
import { runRecord } from "../test/fixtures";
import { useEdit } from "./edit";

const sse = (event: string, data: unknown): string => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

function body(text: string): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
    },
  });
}

const workflow = (name = "squares"): Workflow => ({
  name,
  split: "fastest",
  tasks: [{ id: "squares", type: "demo.squares", settings: { n: 4 } }],
});

const session = (fields: Partial<EditSession> = {}): EditSession => ({
  id: "abc",
  owner: "mk",
  host: "laptop",
  path: "/home/mk/squares.yaml",
  workflow: workflow(),
  file_sha256: "h1",
  sources: [],
  tree: [],
  created: "2026-11-07T13:00:00Z",
  seen: "2026-11-07T13:00:00Z",
  closed: false,
  connected: true,
  ...fields,
});

const sources: SourceProvenance[] = [
  { name: "model", branch: "main", commit: "abc", changed_files: [], snapshot: "s1", diff_sha256: "d1", dirty: false, patch: null },
];

const reply = (data: unknown, status = 200): Response => new Response(JSON.stringify(data), { status });

type Handler = (path: string, init?: RequestInit) => Response | Promise<Response>;

/** Mocks fetch: the stream of the session sends `events`, other calls go to `handler`. */
function mockFetch(events: string | (() => string), handler: Handler = () => reply({})) {
  const mock = vi.fn<typeof fetch>().mockImplementation(async (input, init) => {
    const path = String(input);
    if (path.endsWith("/events")) return new Response(body(typeof events === "string" ? events : events()));
    if (path === "/task-types") return reply([]);
    return handler(path, init);
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

const calls = (mock: ReturnType<typeof mockFetch>, path: string) => mock.mock.calls.filter(([p]) => String(p) === path);
const sent = (call: ReturnType<typeof calls>[number]): unknown => JSON.parse(String(call?.[1]?.body));

const valid: Handler = (path) => (path === "/validate" ? reply({ ok: true, errors: [] }) : reply({}));

function open() {
  const controller = new AbortController();
  const scope = effectScope();
  const edit = scope.run(() => useEdit())!;
  edit.openSession("abc", controller.signal);
  return { edit, controller };
}

describe("useEdit", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    auth.token.value = "secret";
  });
  afterEach(() => {
    vi.useRealTimers();
    auth.logout();
    localStorage.clear();
  });

  it("takes draft and base from the first event of the session", async () => {
    const mock = mockFetch(sse("edit", session()), valid);
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    expect(mock.mock.calls.map(([p]) => String(p))).toContainEqual(expect.stringMatching(/\/edits\/abc\/events$/));
    expect(edit.draft.value).toEqual(workflow());
    expect(edit.base.value).toBe("h1");
    expect(edit.dirty.value).toBe(false);
    expect(edit.connected.value).toBe(true);
    controller.abort();
  });

  it("validates once, 400 ms after the last of three quick changes", async () => {
    const mock = mockFetch(sse("edit", session()), valid);
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(500);
    const before = calls(mock, "/validate").length;
    edit.change(workflow("a"));
    await vi.advanceTimersByTimeAsync(200);
    edit.change(workflow("b"));
    await vi.advanceTimersByTimeAsync(200);
    edit.change(workflow("c"));
    expect(edit.dirty.value).toBe(true);
    await vi.advanceTimersByTimeAsync(399);
    expect(calls(mock, "/validate")).toHaveLength(before);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls(mock, "/validate")).toHaveLength(before + 1);
    expect((sent(calls(mock, "/validate").at(-1)!) as Workflow).name).toBe("c");
    controller.abort();
  });

  it("estimates a valid draft with the sources of the session, not an invalid one", async () => {
    let ok = true;
    const mock = mockFetch(sse("edit", session({ sources })), (path) =>
      path === "/validate" ? reply({ ok, errors: ok ? [] : ["bad"] }) : reply({ squares: { seconds: 3 } }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(500);
    const estimates = calls(mock, "/estimate");
    expect(estimates).toHaveLength(1);
    expect(sent(estimates[0]!)).toMatchObject({ workflow: workflow(), sources });
    expect(edit.estimate.value).toEqual({ squares: { seconds: 3 } });
    ok = false;
    edit.change(workflow("x"));
    await vi.advanceTimersByTimeAsync(500);
    expect(calls(mock, "/estimate")).toHaveLength(1);
    expect(edit.estimate.value).toEqual({});
    controller.abort();
  });

  it("puts the errors of a task in taskErrors and keeps the others", async () => {
    mockFetch(sse("edit", session()), () =>
      reply({ ok: false, errors: ["task 'squares': invalid settings: n must be positive", "duplicate name"] }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(500);
    expect(edit.taskErrors.value).toEqual({ squares: ["invalid settings: n must be positive"] });
    expect(edit.errors.value).toEqual(["duplicate name"]);
    controller.abort();
  });

  it("flags a change on disk while dirty, keeps the draft, and reload takes the disk version", async () => {
    const newer = session({ file_sha256: "h2", workflow: workflow("disk") });
    let events = sse("edit", session());
    mockFetch(() => events, valid);
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    edit.change(workflow("mine"));
    // the stream is opened again and brings the file changed on disk
    events = sse("edit", newer);
    controller.abort();
    const again = new AbortController();
    edit.openSession("abc", again.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(edit.changedOnDisk.value).toBe(true);
    expect(edit.draft.value?.name).toBe("mine");
    expect(edit.base.value).toBe("h1");
    edit.reload();
    expect(edit.draft.value?.name).toBe("disk");
    expect(edit.base.value).toBe("h2");
    expect(edit.changedOnDisk.value).toBe(false);
    expect(edit.dirty.value).toBe(false);
    again.abort();
  });

  it("takes a change on disk at once when the draft is clean", async () => {
    const newer = session({ file_sha256: "h2", workflow: workflow("disk") });
    let events = sse("edit", session());
    mockFetch(() => events, valid);
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    events = sse("edit", newer);
    controller.abort();
    const again = new AbortController();
    edit.openSession("abc", again.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(edit.draft.value?.name).toBe("disk");
    expect(edit.base.value).toBe("h2");
    expect(edit.changedOnDisk.value).toBe(false);
    again.abort();
  });

  it("saves the draft with the base, then is clean with the answered hash", async () => {
    const mock = mockFetch(sse("edit", session()), (path) =>
      path === "/edits/abc/save" ? reply(session({ file_sha256: "h3", workflow: workflow("mine") })) : reply({ ok: true, errors: [] }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    edit.change(workflow("mine"));
    await edit.save();
    const save = calls(mock, "/edits/abc/save")[0]!;
    expect(save[1]?.method).toBe("POST");
    expect(sent(save)).toEqual({ workflow: workflow("mine"), base: "h1", force: false });
    expect(edit.dirty.value).toBe(false);
    expect(edit.base.value).toBe("h3");
    controller.abort();
  });

  it("saves anyway with the hash the service has now, not the one of the last event", async () => {
    const mock = mockFetch(sse("edit", session()), (path) => {
      if (path === "/edits/abc/save") return reply(session({ file_sha256: "h4" }));
      if (path === "/edits/abc") return reply(session({ file_sha256: "h2" }));
      return reply({ ok: true, errors: [] });
    });
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    await edit.save(true);
    expect(sent(calls(mock, "/edits/abc/save")[0]!)).toMatchObject({ base: "h2", force: true });
    expect(edit.base.value).toBe("h4");
    controller.abort();
  });

  it("stays dirty when the draft changed while the save was in flight", async () => {
    let release: (r: Response) => void = () => undefined;
    mockFetch(sse("edit", session()), (path) =>
      path === "/edits/abc/save" ? new Promise<Response>((resolve) => (release = resolve)) : reply({ ok: true, errors: [] }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    edit.change(workflow("first"));
    const saving = edit.save();
    await vi.advanceTimersByTimeAsync(0);
    edit.change(workflow("second"));
    release(reply(session({ file_sha256: "h3", workflow: workflow("first") })));
    await saving;
    expect(edit.base.value).toBe("h3");
    expect(edit.dirty.value).toBe(true);
    expect(edit.draft.value?.name).toBe("second");
    controller.abort();
  });

  it("tells valid, invalid and not validated apart", async () => {
    let answer: () => Response = () => reply({ ok: true, errors: [] });
    mockFetch(sse("edit", session()), (path) => (path === "/validate" ? answer() : reply({})));
    const { edit, controller } = open();
    expect(edit.valid.value).toBeNull();
    await vi.advanceTimersByTimeAsync(500);
    expect(edit.valid.value).toBe(true);
    answer = () => reply({ ok: false, errors: ["duplicate name"] });
    edit.change(workflow("a"));
    await vi.advanceTimersByTimeAsync(500);
    expect(edit.valid.value).toBe(false);
    answer = () => reply({ detail: "boom" }, 500);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    edit.change(workflow("b"));
    await vi.advanceTimersByTimeAsync(500);
    expect(edit.valid.value).toBeNull();
    expect(edit.errors.value).toEqual(["validation failed: boom"]);
    controller.abort();
  });

  it("exposes a run which cannot be opened and ignores a slow earlier run", async () => {
    let slow: (r: Response) => void = () => undefined;
    mockFetch("", (path) => {
      if (path === "/runs/slow") return new Promise<Response>((resolve) => (slow = resolve));
      if (path === "/runs/r2") return reply(runRecord({ id: "r2", workflow: workflow("second") }));
      if (path === "/runs/gone") return reply({ detail: "no run" }, 404);
      return reply({ ok: true, errors: [] });
    });
    const edit = effectScope().run(() => useEdit())!;
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    await edit.openRun("gone");
    expect(edit.loadError.value).toBe("no run");
    expect(edit.draft.value).toBeNull();
    const first = edit.openRun("slow");
    await edit.openRun("r2");
    expect(edit.loadError.value).toBeNull();
    slow(reply(runRecord({ id: "slow", workflow: workflow("first") })));
    await first;
    expect(edit.draft.value?.name).toBe("second");
  });

  it("forgets the messages and the estimate of the previous workflow when a run opens", async () => {
    mockFetch(sse("edit", session()), (path) =>
      path === "/validate" ? reply({ ok: false, errors: ["bad"] }) : reply(runRecord({ id: "r1", workflow: workflow("ran") })),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(500);
    expect(edit.errors.value).toEqual(["bad"]);
    controller.abort();
    await edit.openRun("r1");
    expect(edit.errors.value).toEqual([]);
    expect(edit.valid.value).toBeNull();
    expect(edit.estimate.value).toEqual({});
  });

  it("marks a conflict (409) as changed on disk and keeps the draft dirty", async () => {
    mockFetch(sse("edit", session()), (path) =>
      path === "/edits/abc/save" ? reply({ detail: "conflict" }, 409) : reply({ ok: true, errors: [] }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    edit.change(workflow("mine"));
    await edit.save();
    expect(edit.changedOnDisk.value).toBe(true);
    expect(edit.dirty.value).toBe(true);
    controller.abort();
  });

  it("runs through the connected session", async () => {
    const mock = mockFetch(sse("edit", session()), (path) =>
      path === "/edits/abc/run" ? reply({ id: "r9", state: "queued" }) : reply({ ok: true, errors: [] }),
    );
    const { edit, controller } = open();
    await vi.advanceTimersByTimeAsync(0);
    const result = await edit.run({ group: "fits" });
    expect(result).toMatchObject({ id: "r9" });
    expect(sent(calls(mock, "/edits/abc/run")[0]!)).toMatchObject({ workflow: workflow(), group: "fits" });
    controller.abort();
  });

  it("opens a run: draft, group and sources from its record, a run with POST /runs", async () => {
    const record = runRecord({ id: "r1", group: "fits", workflow: workflow("ran"), sources });
    const mock = mockFetch("", (path) => {
      if (path === "/runs/r1") return reply(record);
      if (path === "/runs") return reply({ id: "r2", state: "queued" });
      return reply({ ok: true, errors: [] });
    });
    const scope = effectScope();
    const edit = scope.run(() => useEdit())!;
    await edit.openRun("r1");
    expect(edit.draft.value?.name).toBe("ran");
    expect(edit.group.value).toBe("fits");
    expect(edit.connected.value).toBe(false);
    edit.change(workflow("changed"));
    const result = await edit.run({});
    expect(result).toMatchObject({ id: "r2" });
    expect(sent(calls(mock, "/runs").find(([, i]) => i?.method === "POST")!)).toMatchObject({
      workflow: workflow("changed"),
      sources,
      group: "fits",
    });
  });
});
