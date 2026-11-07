/**
 * The event streams of the service (`text/event-stream`), read with `fetch`.
 *
 * `EventSource` cannot send the bearer token but in the URL, which the token
 * never goes into; so a stream is read with `fetch` and `parseEvents`.
 *
 * The contract of `stream`:
 *
 * - An end of the body is the service bounding a stream (`events_max_s`): the
 *   stream is opened again after the delay, unless `done()` is true then (a
 *   run stream ends once its run is final), which ends it for good. An end is
 *   not an error and does not mark the streams as reconnecting.
 * - A network error, a 5xx, a 408 or a 429 is transient: the stream is marked
 *   as reconnecting (`connected` false) and opened again after the delay.
 * - Any other 4xx is permanent (a 404 of an unknown run): the stream ends and
 *   `onError(status)` is called once; a 401 also ends the session.
 * - A connection which receives nothing, not even the `: keep-alive` comment
 *   the service sends every 15 s, for `WATCHDOG_MS` is dead (a silent drop of
 *   the network): it is aborted and handled as a network error.
 * - The delay is 1 s, doubling to 10 s, and back to 1 s after every event.
 * - Aborting `signal` ends the stream at once, also during the delay.
 */
import { computed, reactive } from "vue";
import { auth } from "../auth";
import { authorized } from "./http";

/** An event of a stream: its name (`message` without an `event:` line) and its data lines joined by `\n`. */
export interface StreamEvent {
  event: string;
  data: string;
}

/** What `parseEvents` keeps between chunks: the incomplete line and the fields of the incomplete event. */
export interface ParserState {
  buffer: string;
  event: string;
  data: string[];
}

/** The state before the first chunk of a stream. */
export function initialState(): ParserState {
  return { buffer: "", event: "", data: [] };
}

const LINE_END = /\r\n|\r|\n/;

/**
 * The events completed by `chunk`, following the rules of `text/event-stream`.
 *
 * Lines end with LF, CR or CRLF; a line starting with `:` is a comment; an
 * empty line ends an event, which is only sent if it has data; `id` and
 * `retry` are not used by the service and are ignored.
 */
export function parseEvents(chunk: string, state: ParserState): { events: StreamEvent[]; state: ParserState } {
  let buffer = state.buffer + chunk;
  let event = state.event;
  let data = [...state.data];
  const events: StreamEvent[] = [];
  for (;;) {
    const match = LINE_END.exec(buffer);
    // a CR at the end of the buffer may be the first half of a CRLF
    if (match === null || (match[0] === "\r" && match.index === buffer.length - 1)) break;
    const line = buffer.slice(0, match.index);
    buffer = buffer.slice(match.index + match[0].length);
    if (line === "") {
      if (data.length > 0) events.push({ event: event || "message", data: data.join("\n") });
      event = "";
      data = [];
      continue;
    }
    if (line.startsWith(":")) continue;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (field === "event") event = value;
    else if (field === "data") data.push(value);
  }
  return { events, state: { buffer, event, data } };
}

const INITIAL_DELAY_MS = 1000;
const MAX_DELAY_MS = 10_000;

/** How long a connection may stay silent: three times the keep-alive interval of the service, 15 s. */
export const WATCHDOG_MS = 45_000;

/** The options of `stream`. */
export interface StreamOptions {
  /** Whether the stream is complete; checked when the body ends, true ends the stream. */
  done?: () => boolean;
  /** Called once with the status of a permanent error, which ends the stream. */
  onError?: (status: number) => void;
}

/** Whether an answer with `status` (not 2xx) may succeed when the request is repeated. */
function transient(status: number): boolean {
  return status >= 500 || status === 408 || status === 429 || status < 400;
}

// the streams which failed and wait to be opened again
const reconnecting = reactive(new Set<symbol>());

/** Whether every stream is open; false while one waits to be opened again after an error. */
export const connected = computed(() => reconnecting.size === 0);

/** Resolves after `ms` or when `signal` aborts. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const done = (): void => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done);
  });
}

type Handler = (event: string, data: unknown) => void;

/** Hands an event to `onEvent`; an error of the handler is a bug of the app, not of the stream, so it is logged. */
function deliver(onEvent: Handler, { event, data }: StreamEvent): void {
  const value: unknown = JSON.parse(data);
  try {
    onEvent(event, value);
  } catch (error) {
    console.error("the handler of the event %s failed: %o", event, error);
  }
}

interface Connection {
  path: string;
  onEvent: Handler;
  signal: AbortSignal;
  /** Called once the service answered with the stream. */
  onOpen: () => void;
  /** Called before every event. */
  onMessage: () => void;
}

/** How a connection ended: the body ended, or a permanent error with its status. */
type Outcome = { kind: "end" } | { kind: "error"; status: number };

/**
 * Reads one connection of a stream to its end; a transient failure, or
 * silence for `WATCHDOG_MS`, raises.
 */
async function read({ path, onEvent, signal, onOpen, onMessage }: Connection): Promise<Outcome> {
  const watchdog = new AbortController();
  const connection = AbortSignal.any([signal, watchdog.signal]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const alive = (): void => {
    clearTimeout(timer);
    timer = setTimeout(() => watchdog.abort(new Error(`${path}: nothing received for ${WATCHDOG_MS} ms`)), WATCHDOG_MS);
  };
  alive();
  try {
    const response = await fetch(path, {
      headers: authorized(auth.token.value, { Accept: "text/event-stream" }),
      cache: "no-store",
      signal: connection,
    });
    if (!response.ok && !transient(response.status)) return { kind: "error", status: response.status };
    if (!response.ok || response.body === null) throw new Error(`${path}: ${response.status} ${response.statusText}`);
    alive();
    onOpen();
    const reader = response.body.getReader();
    // a body which is not tied to the signal (a service worker, a test) is cancelled by hand
    const cancel = (): void => void reader.cancel(connection.reason).catch(() => undefined);
    connection.addEventListener("abort", cancel, { once: true });
    try {
      const decoder = new TextDecoder();
      let state = initialState();
      for (;;) {
        const { value, done } = await reader.read();
        connection.throwIfAborted();
        if (done) return { kind: "end" };
        alive();
        const result = parseEvents(decoder.decode(value, { stream: true }), state);
        state = result.state;
        for (const event of result.events) {
          onMessage();
          deliver(onEvent, event);
        }
      }
    } finally {
      connection.removeEventListener("abort", cancel);
    }
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Reads the stream `path` of the API until `signal` aborts, `done()` is true
 * at the end of the body or a permanent error; `onEvent` gets the name and the
 * parsed JSON data of every event. See the contract at the top of the module.
 */
export function stream(path: string, onEvent: Handler, signal: AbortSignal, options: StreamOptions = {}): void {
  const id = Symbol(path);
  let delay = INITIAL_DELAY_MS;
  const connection: Connection = {
    path,
    onEvent,
    signal,
    onOpen: () => reconnecting.delete(id),
    onMessage: () => (delay = INITIAL_DELAY_MS),
  };
  const run = async (): Promise<void> => {
    while (!signal.aborted) {
      try {
        const outcome = await read(connection);
        if (outcome.kind === "error") {
          if (outcome.status === 401) auth.logout();
          options.onError?.(outcome.status);
          break;
        }
        if (options.done?.()) break;
      } catch {
        // the service is down, the network dropped, the connection was silent
        // or an event was no JSON: open it again after the delay
        if (signal.aborted) break;
        reconnecting.add(id);
      }
      await sleep(delay, signal);
      delay = Math.min(delay * 2, MAX_DELAY_MS);
    }
    reconnecting.delete(id);
  };
  void run();
}
