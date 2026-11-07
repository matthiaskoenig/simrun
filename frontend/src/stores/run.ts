/**
 * The live state of one run: its record, the progress and the workers of its
 * tasks, from `/runs/{id}/events`.
 *
 * The stream follows the id: a new id aborts the stream of the old one and
 * forgets its state, so that the canvas never shows one run with the tasks of
 * another. The service ends the stream of a final run, which is not opened
 * again; an unknown run (404) is marked as not found.
 */
import { getCurrentScope, onScopeDispose, shallowRef, watch, type Ref } from "vue";
import type { RunRecord, TaskProgress, WorkerView } from "../api/client";
import { stream } from "../api/sse";
import { ACTIVE_STATES } from "../lib/filters";

/** What `useRun` gives; every field is empty while no run is selected or before its first event. */
export interface Run {
  record: Ref<RunRecord | null>;
  progress: Ref<Record<string, TaskProgress>>;
  workers: Ref<Record<string, WorkerView[]>>;
  /** Whether the service does not know the run (deleted, or a mistyped link). */
  notFound: Ref<boolean>;
}

/** The live state of the run `id`; its stream ends with the effect scope of the caller (its component). */
export function useRun(id: Readonly<Ref<string | null>>): Run {
  // every event replaces its state as a whole: shallow refs, no deep proxies
  const record = shallowRef<RunRecord | null>(null);
  const progress = shallowRef<Record<string, TaskProgress>>({});
  const workers = shallowRef<Record<string, WorkerView[]>>({});
  const notFound = shallowRef(false);

  let controller: AbortController | null = null;

  const open = (runId: string | null): void => {
    controller?.abort();
    controller = null;
    record.value = null;
    progress.value = {};
    workers.value = {};
    notFound.value = false;
    if (runId === null) return;
    const own = new AbortController();
    controller = own;
    const onEvent = (event: string, data: unknown): void => {
      if (own.signal.aborted) return;
      if (event === "run") record.value = data as RunRecord;
      else if (event === "progress") progress.value = data as Record<string, TaskProgress>;
      else if (event === "workers") workers.value = data as Record<string, WorkerView[]>;
    };
    stream(`/runs/${encodeURIComponent(runId)}/events`, onEvent, own.signal, {
      done: () => record.value !== null && !ACTIVE_STATES.has(record.value.state),
      onError: (status) => {
        if (status === 404 && !own.signal.aborted) notFound.value = true;
      },
    });
  };

  watch(id, open, { immediate: true });
  if (getCurrentScope()) onScopeDispose(() => controller?.abort());
  return { record, progress, workers, notFound };
}
