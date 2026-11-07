import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, ref } from "vue";
import type { RunRecord } from "../api/client";
import { auth } from "../auth";
import { runRecord } from "../test/fixtures";
import { useRun } from "./run";

const record = (id: string, state: RunRecord["state"]): RunRecord => runRecord({ id, state });

const sse = (event: string, data: unknown): string => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;

/** A body which sends `text` and then stays open, or ends when `end`. */
function body(text: string, end = false): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
      if (end) controller.close();
    },
  });
}

describe("useRun", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    auth.token.value = "secret";
  });
  afterEach(() => {
    vi.useRealTimers();
    auth.logout();
    localStorage.clear();
  });

  it("follows the record, the progress and the workers of the run, and restarts with the id", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async (input) => {
      const id = String(input).split("/")[2] ?? "";
      const progress = { squares: { counts: { pending: 1, claimed: 0, done: 2, total: 3 }, last: null } };
      return new Response(body(sse("run", record(id, "running")) + sse("progress", progress) + sse("workers", { squares: [] })));
    });
    vi.stubGlobal("fetch", fetchMock);
    const id = ref<string | null>("r1");
    const scope = effectScope();
    const run = scope.run(() => useRun(id))!;
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/runs/r1/events");
    expect(run.record.value?.id).toBe("r1");
    expect(run.progress.value.squares?.counts?.done).toBe(2);
    expect(run.workers.value).toEqual({ squares: [] });

    id.value = "r2";
    await nextTick();
    expect(run.record.value).toBeNull();
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/runs/r2/events");
    expect(run.record.value?.id).toBe("r2");

    id.value = null;
    await nextTick();
    expect(run.record.value).toBeNull();
    expect(fetchMock.mock.calls[1]?.[1]?.signal?.aborted).toBe(true);
    scope.stop();
  });

  it("does not reopen the stream of a final run", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => new Response(body(sse("run", record("r1", "succeeded")), true)));
    vi.stubGlobal("fetch", fetchMock);
    const scope = effectScope();
    const run = scope.run(() => useRun(ref("r1")))!;
    await vi.advanceTimersByTimeAsync(30_000);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(run.record.value?.state).toBe("succeeded");
    scope.stop();
  });

  it("marks an unknown run as not found", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockImplementation(async () => new Response("unknown run", { status: 404 })));
    const id = ref<string | null>("gone");
    const scope = effectScope();
    const run = scope.run(() => useRun(id))!;
    await vi.advanceTimersByTimeAsync(0);
    expect(run.notFound.value).toBe(true);
    id.value = "r2";
    await nextTick();
    expect(run.notFound.value).toBe(false);
    scope.stop();
  });
});
