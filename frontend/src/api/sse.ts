/**
 * The event streams of the service (`text/event-stream`), read with `fetch`.
 *
 * `EventSource` cannot send the bearer token but in the URL, which the token
 * never goes into; so a stream is read with `fetch` and `parseEvents`. A
 * stream ends after a while on the service and is then opened again, as after
 * an error, with a backoff of 1 s doubling to 10 s, which an event resets; a
 * 401 ends the session and the stream.
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

/** Reads one connection of a stream to its end; false after a 401, an error after a failure. */
async function read({ path, onEvent, signal, onOpen, onMessage }: Connection): Promise<boolean> {
  const response = await fetch(path, {
    headers: authorized(auth.token.value, { Accept: "text/event-stream" }),
    cache: "no-store",
    signal,
  });
  if (response.status === 401) {
    auth.logout();
    return false;
  }
  if (!response.ok || response.body === null) throw new Error(`${path}: ${response.status} ${response.statusText}`);
  onOpen();
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let state = initialState();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return true;
    const result = parseEvents(decoder.decode(value, { stream: true }), state);
    state = result.state;
    for (const event of result.events) {
      onMessage();
      deliver(onEvent, event);
    }
  }
}

/**
 * Reads the stream `path` of the API until `signal` aborts, opening it again
 * after an end or an error; `onEvent` gets the name and the parsed JSON data
 * of every event.
 */
export function stream(path: string, onEvent: Handler, signal: AbortSignal): void {
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
        if (!(await read(connection))) break;
      } catch {
        // the service is down, the network dropped or an event was no JSON: open it again after the delay
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
