/**
 * The live state of the cluster: the runs and the servers of `/events`.
 *
 * Every user (a component) shares one stream, which opens with the first
 * user and ends with the last; the last state is kept meanwhile and while the
 * stream reconnects, and forgotten when the session ends.
 */
import { getCurrentScope, onScopeDispose, ref, watch, type Ref } from "vue";
import type { RunItem, ServerUsage } from "../api/client";
import { connected, stream } from "../api/sse";
import { auth } from "../auth";

/** What `useLive` gives: the runs (newest first, at most 200), the servers, and whether the streams are open. */
export interface Live {
  runs: Ref<RunItem[]>;
  servers: Ref<ServerUsage[]>;
  connected: Readonly<Ref<boolean>>;
}

const runs = ref<RunItem[]>([]);
const servers = ref<ServerUsage[]>([]);

let users = 0;
let controller: AbortController | null = null;

function onEvent(event: string, data: unknown): void {
  if (event === "runs") runs.value = data as RunItem[];
  else if (event === "servers") servers.value = data as ServerUsage[];
}

function release(): void {
  users -= 1;
  if (users === 0) {
    controller?.abort();
    controller = null;
  }
}

// the state of one session is not shown to the next
watch(
  auth.token,
  (token) => {
    if (token === null) {
      runs.value = [];
      servers.value = [];
    }
  },
  { flush: "sync" },
);

/** The live runs and servers; the stream ends when the effect scope of the caller (its component) ends. */
export function useLive(): Live {
  users += 1;
  if (controller === null) {
    controller = new AbortController();
    stream("/events", onEvent, controller.signal);
  }
  if (getCurrentScope()) onScopeDispose(release);
  return { runs, servers, connected };
}
