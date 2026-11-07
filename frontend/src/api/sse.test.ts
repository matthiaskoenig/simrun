import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { auth } from "../auth";
import { connected, initialState, parseEvents, stream, WATCHDOG_MS, type ParserState } from "./sse";

function parseAll(chunks: string[]): { event: string; data: string }[] {
  let state: ParserState = initialState();
  const events: { event: string; data: string }[] = [];
  for (const chunk of chunks) {
    const result = parseEvents(chunk, state);
    events.push(...result.events);
    state = result.state;
  }
  return events;
}

/** A body which yields nothing until the test enqueues into its controller. */
function silentBody(): { stream: ReadableStream<Uint8Array>; send: (text: string) => void } {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const stream = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
  });
  return { stream, send: (text) => controller.enqueue(new TextEncoder().encode(text)) };
}

function body(text: string): ReadableStream<Uint8Array> {
  const bytes = new TextEncoder().encode(text);
  return new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

describe("parseEvents", () => {
  it("reads two events split across three chunks, joining the data lines", () => {
    const events = parseAll([
      'event: run\ndata: {"a"',
      ': 1}\n\nevent: progress\ndata: [1,\nda',
      "ta: 2]\n\n",
    ]);
    expect(events).toEqual([
      { event: "run", data: '{"a": 1}' },
      { event: "progress", data: "[1,\n2]" },
    ]);
  });

  it("yields nothing for a comment line", () => {
    expect(parseAll([": keep-alive\n\n"])).toEqual([]);
  });

  it("reads CRLF line ends, also when a chunk ends between CR and LF", () => {
    // the CRLF after `data: [1,` is split, which is one line end and not two
    const events = parseAll(["event: runs\r\ndata: [1,\r", "\ndata: 2]\r\n\r\nevent: servers\r\ndata: {}\r\n\r\n"]);
    expect(events).toEqual([
      { event: "runs", data: "[1,\n2]" },
      { event: "servers", data: "{}" },
    ]);
  });

  it("names an event without an event line `message`", () => {
    expect(parseAll(["data: 1\n\n"])).toEqual([{ event: "message", data: "1" }]);
  });

  it("keeps an incomplete event for the next chunk", () => {
    const first = parseEvents("event: run\ndata: 1\n", initialState());
    expect(first.events).toEqual([]);
    expect(parseEvents("\n", first.state).events).toEqual([{ event: "run", data: "1" }]);
  });
});

describe("stream", () => {
  let controller: AbortController;

  beforeEach(() => {
    vi.useFakeTimers();
    controller = new AbortController();
    auth.token.value = "secret";
  });

  afterEach(() => {
    controller.abort();
    vi.useRealTimers();
    auth.logout();
  });

  it("reconnects after the body ends and stops after a 401", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response(body('event: run\ndata: {"id": "r1"}\n\n'), { status: 200 }))
      .mockResolvedValueOnce(new Response("", { status: 401 }));
    vi.stubGlobal("fetch", fetchMock);
    const onEvent = vi.fn();

    stream("/runs/r1/events", onEvent, controller.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(onEvent).toHaveBeenCalledExactlyOnceWith("run", { id: "r1" });
    expect(fetchMock).toHaveBeenCalledOnce();
    const init = fetchMock.mock.calls[0]?.[1];
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer secret");

    await vi.advanceTimersByTimeAsync(1000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(auth.token.value).toBeNull();

    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("backs off from 1 s doubling to 10 s after errors and is not connected meanwhile", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockRejectedValue(new TypeError("Failed to fetch"));
    vi.stubGlobal("fetch", fetchMock);

    stream("/events", () => {}, controller.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(connected.value).toBe(false);

    // the attempts after 1, 2, 4, 8, 10 and 10 s
    const delays = [1000, 2000, 4000, 8000, 10_000, 10_000];
    for (const [index, delay] of delays.entries()) {
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(fetchMock).toHaveBeenCalledTimes(index + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(fetchMock).toHaveBeenCalledTimes(index + 2);
    }

    controller.abort();
    await vi.advanceTimersByTimeAsync(0);
    expect(connected.value).toBe(true);
  });

  it("resets the backoff after an event", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValueOnce(new Response(body("event: servers\ndata: []\n\n"), { status: 200 }))
      .mockResolvedValue(new Response("", { status: 401 }));
    vi.stubGlobal("fetch", fetchMock);
    const onEvent = vi.fn();

    stream("/events", onEvent, controller.signal);
    await vi.advanceTimersByTimeAsync(0); // fails, waits 1 s
    await vi.advanceTimersByTimeAsync(1000); // fails, waits 2 s
    await vi.advanceTimersByTimeAsync(2000); // an event, then the end
    expect(onEvent).toHaveBeenCalledExactlyOnceWith("servers", []);
    expect(connected.value).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(1000);
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("stops on a 404 and calls onError once", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("{}", { status: 404 }));
    vi.stubGlobal("fetch", fetchMock);
    const onError = vi.fn();

    stream("/runs/unknown/events", () => {}, controller.signal, { onError });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(onError).toHaveBeenCalledExactlyOnceWith(404);
    expect(connected.value).toBe(true);
    expect(auth.token.value).toBe("secret");
  });

  it("calls onError on a 401 too", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response("", { status: 401 })));
    const onError = vi.fn();

    stream("/events", () => {}, controller.signal, { onError });
    await vi.advanceTimersByTimeAsync(0);
    expect(onError).toHaveBeenCalledExactlyOnceWith(401);
    expect(auth.token.value).toBeNull();
  });

  it("reconnects after a 5xx, a 408 and a 429", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(new Response("", { status: 503 }))
      .mockResolvedValueOnce(new Response("", { status: 408 }))
      .mockResolvedValueOnce(new Response("", { status: 429 }))
      .mockResolvedValue(new Response("", { status: 404 }));
    vi.stubGlobal("fetch", fetchMock);
    const onError = vi.fn();

    stream("/events", () => {}, controller.signal, { onError });
    await vi.advanceTimersByTimeAsync(0);
    expect(connected.value).toBe(false);
    await vi.advanceTimersByTimeAsync(1000 + 2000 + 4000);
    expect(fetchMock).toHaveBeenCalledTimes(4);
    expect(onError).toHaveBeenCalledExactlyOnceWith(404);
  });

  it("stops when the body ends and done() is true", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => new Response(body("event: run\ndata: {}\n\n")));
    vi.stubGlobal("fetch", fetchMock);

    stream("/runs/r1/events", () => {}, controller.signal, { done: () => true });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(connected.value).toBe(true);
  });

  it("reconnects when the body ends and done() is false", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => new Response(body("event: run\ndata: {}\n\n")));
    vi.stubGlobal("fetch", fetchMock);

    stream("/runs/r1/events", () => {}, controller.signal, { done: () => false });
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(connected.value).toBe(true);
    await vi.advanceTimersByTimeAsync(1000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("aborts a connection silent for 45 s and reconnects", async () => {
    expect(WATCHDOG_MS).toBe(45_000);
    // a body which never yields, as after a silent drop of the network
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async (_path, init) => {
      if (init?.signal) signals.push(init.signal);
      return new Response(silentBody().stream);
    });
    vi.stubGlobal("fetch", fetchMock);

    stream("/events", () => {}, controller.signal);
    await vi.advanceTimersByTimeAsync(WATCHDOG_MS - 1);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(connected.value).toBe(true);
    await vi.advanceTimersByTimeAsync(1);
    expect(signals[0]?.aborted).toBe(true);
    expect(controller.signal.aborted).toBe(false);
    expect(connected.value).toBe(false);
    await vi.advanceTimersByTimeAsync(1000);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("keeps a connection whose keep-alive comments arrive", async () => {
    const silent = silentBody();
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValueOnce(new Response(silent.stream));
    vi.stubGlobal("fetch", fetchMock);

    stream("/events", () => {}, controller.signal);
    for (let i = 0; i < 6; i++) {
      await vi.advanceTimersByTimeAsync(15_000);
      silent.send(": keep-alive\n\n");
    }
    await vi.advanceTimersByTimeAsync(15_000);
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(connected.value).toBe(true);
  });

  it("does not reconnect once aborted", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(async () => new Response(body(""), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    stream("/events", () => {}, controller.signal);
    await vi.advanceTimersByTimeAsync(0);
    controller.abort();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});
