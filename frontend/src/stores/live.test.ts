import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope } from "vue";
import { auth } from "../auth";
import { runs, servers } from "../test/fixtures";
import { useLive } from "./live";

/** A body which sends `text` and stays open until the test aborts it. */
function openBody(text: string): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
    },
  });
}

const events = `event: runs\ndata: ${JSON.stringify(runs)}\n\nevent: servers\ndata: ${JSON.stringify(servers)}\n\n`;

describe("useLive", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    auth.token.value = "secret";
  });
  afterEach(() => {
    vi.useRealTimers();
    auth.logout();
    localStorage.clear();
  });

  it("follows the runs and the servers of /events, one stream for every user", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => new Response(openBody(events)));
    vi.stubGlobal("fetch", fetchMock);
    const first = effectScope();
    const second = effectScope();
    const live = first.run(() => useLive())!;
    second.run(() => useLive());
    await vi.advanceTimersByTimeAsync(0);

    expect(fetchMock).toHaveBeenCalledOnce();
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/events");
    expect(live.runs.value.map((r) => r.id)).toEqual(["r6", "r5", "r4", "r3", "r2", "r1"]);
    expect(live.servers.value.map((s) => s.name)).toEqual(["node-a", "node-b", "node-c"]);
    expect(live.connected.value).toBe(true);

    first.stop();
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(false);
    second.stop();
    expect(fetchMock.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
  });

  it("forgets the runs and the servers when the session ends", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockImplementation(async () => new Response(openBody(events))));
    const scope = effectScope();
    const live = scope.run(() => useLive())!;
    await vi.advanceTimersByTimeAsync(0);
    expect(live.runs.value).toHaveLength(6);

    auth.logout();
    expect(live.runs.value).toEqual([]);
    expect(live.servers.value).toEqual([]);
    scope.stop();
  });
});
