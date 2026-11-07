/**
 * The draft of the Editor: the workflow being edited, from an edit session
 * (`/edits/{id}/events`) or from a run (`#/runs/:id/edit`), with its
 * validation and estimate.
 *
 * The draft is the browser's; the session brings the file on disk. A new hash
 * of the file replaces a clean draft and only flags a dirty one
 * (`changedOnDisk`). Every change validates (debounced) and, when valid,
 * estimates with the last snapshots of the sources.
 */
import { computed, getCurrentScope, onScopeDispose, ref, shallowRef, type ComputedRef, type Ref } from "vue";
import { ApiError, api } from "../api/client";
import type { EditSession, RunSummary, SourceProvenance, TaskEstimate, TaskTypeView, Workflow } from "../api/client";
import { stream } from "../api/sse";

/** How long after the last change the draft is validated. */
export const VALIDATE_DEBOUNCE_MS = 400;

/** How long the URL of a downloaded YAML stays valid after the click. */
const REVOKE_AFTER_MS = 30_000;

const TASK_ERROR = /^task '([^']+)': (.*)$/s;

/** What `useEdit` gives. */
export interface Edit {
  session: Ref<EditSession | null>;
  draft: Ref<Workflow | null>;
  /** The hash of the file the draft started from. */
  base: Ref<string>;
  /** The group of the run opened (`#/runs/:id/edit`), null for a session. */
  group: Ref<string | null>;
  /** Counts the times the draft took a workflow from disk or from a run; the canvas lays out again on a change. */
  loaded: Ref<number>;
  dirty: ComputedRef<boolean>;
  /** Whether the command of the session polls; false for a run. */
  connected: ComputedRef<boolean>;
  changedOnDisk: Ref<boolean>;
  /** The errors of the workflow which belong to no task. */
  errors: ComputedRef<string[]>;
  /** The errors per task, without the `task '<id>':` prefix. */
  taskErrors: ComputedRef<Record<string, string[]>>;
  estimate: Ref<Record<string, TaskEstimate>>;
  /** What the service said of the draft: null while never validated or when the validation itself failed. */
  valid: Ref<boolean | null>;
  /** Why the run could not be opened, null otherwise. */
  loadError: Ref<string | null>;
  types: Ref<TaskTypeView[]>;
  /** The sources a run was made with, empty for a session. */
  runSources: Ref<SourceProvenance[]>;
  openSession(id: string, signal: AbortSignal): void;
  openRun(id: string): Promise<void>;
  change(workflow: Workflow): void;
  /** Drops the draft for the version on disk. */
  reload(): void;
  /** Saves through the command; `force` takes the hash on disk as the base. */
  save(force?: boolean): Promise<void>;
  /** Runs the draft: through the session when it is connected, else with `POST /runs`. */
  run(options: { group?: string | null }): Promise<RunSummary>;
  /** Downloads the YAML of the draft as `<workflow name>.yaml`. */
  download(): Promise<void>;
}

function clone(workflow: Workflow): Workflow {
  return structuredClone(workflow);
}

/** The draft of the workflow being edited; its timer and stream end with the scope of the caller. */
export function useEdit(): Edit {
  const session = shallowRef<EditSession | null>(null);
  const draft = shallowRef<Workflow | null>(null);
  const base = ref("");
  const group = ref<string | null>(null);
  const loaded = ref(0);
  const isDirty = ref(false);
  const changedOnDisk = ref(false);
  const messages = ref<string[]>([]);
  const estimate = shallowRef<Record<string, TaskEstimate>>({});
  const valid = ref<boolean | null>(null);
  const loadError = ref<string | null>(null);
  const types = shallowRef<TaskTypeView[]>([]);
  // the sources a run was made with, used while there is no session
  const runSources = shallowRef<SourceProvenance[]>([]);
  let sessionId: string | null = null;
  let timer: ReturnType<typeof setTimeout> | undefined;
  // a later validation makes the answer of an earlier one stale
  let generation = 0;
  // a later open makes the answer of an earlier openRun stale
  let opening = 0;

  const dirty = computed(() => isDirty.value);
  const connected = computed(() => session.value?.connected ?? false);
  const errors = computed(() => messages.value.filter((m) => !TASK_ERROR.test(m)));
  const taskErrors = computed(() => {
    const result: Record<string, string[]> = {};
    for (const message of messages.value) {
      const match = TASK_ERROR.exec(message);
      if (match) (result[match[1]!] ??= []).push(match[2]!);
    }
    return result;
  });

  const sources = (): SourceProvenance[] => session.value?.sources ?? runSources.value;

  async function validate(): Promise<void> {
    const workflow = draft.value;
    if (workflow === null) return;
    const mine = ++generation;
    try {
      const result = await api.validate(workflow);
      if (mine !== generation) return;
      messages.value = result.errors;
      valid.value = result.ok;
      if (!result.ok) {
        estimate.value = {};
        return;
      }
    } catch (error) {
      if (mine !== generation) return;
      console.error("validate failed: %o", error);
      messages.value = [`validation failed: ${error instanceof Error ? error.message : String(error)}`];
      valid.value = null;
      estimate.value = {};
      return;
    }
    try {
      const planned = await api.estimate(workflow, sources());
      if (mine === generation) estimate.value = planned;
    } catch (error) {
      if (mine !== generation) return;
      // an estimate which fails (no snapshots yet) leaves the draft valid
      console.error("estimate failed: %o", error);
      estimate.value = {};
    }
  }

  function schedule(): void {
    clearTimeout(timer);
    generation += 1;
    timer = setTimeout(() => void validate(), VALIDATE_DEBOUNCE_MS);
  }

  function take(from: EditSession): void {
    draft.value = clone(from.workflow);
    base.value = from.file_sha256;
    loaded.value += 1;
    isDirty.value = false;
    changedOnDisk.value = false;
    schedule();
  }

  function onSession(next: EditSession): void {
    session.value = next;
    if (draft.value === null) take(next);
    else if (next.file_sha256 !== base.value) {
      if (isDirty.value) changedOnDisk.value = true;
      else take(next);
    }
  }

  function loadTypes(): void {
    api.taskTypes().then(
      (loaded) => (types.value = loaded),
      (error: unknown) => console.error("the task types could not be loaded: %o", error),
    );
  }

  function reset(): void {
    session.value = null;
    draft.value = null;
    base.value = "";
    group.value = null;
    isDirty.value = false;
    changedOnDisk.value = false;
    messages.value = [];
    valid.value = null;
    estimate.value = {};
    runSources.value = [];
    loadError.value = null;
  }

  function openSession(id: string, signal: AbortSignal): void {
    opening += 1;
    if (sessionId !== id) {
      clearTimeout(timer);
      generation += 1;
      reset();
      sessionId = id;
    }
    loadTypes();
    stream(
      `/edits/${encodeURIComponent(id)}/events`,
      (event, data) => {
        if (event === "edit" && !signal.aborted) onSession(data as EditSession);
      },
      signal,
    );
    signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
  }

  async function openRun(id: string): Promise<void> {
    clearTimeout(timer);
    sessionId = null;
    generation += 1;
    reset();
    const mine = ++opening;
    loadTypes();
    let record;
    try {
      record = await api.run(id);
    } catch (error) {
      if (mine === opening) loadError.value = error instanceof Error ? error.message : String(error);
      return;
    }
    if (mine !== opening) return;
    draft.value = clone(record.workflow);
    group.value = record.group ?? null;
    runSources.value = record.sources;
    loaded.value += 1;
    schedule();
  }

  function change(workflow: Workflow): void {
    draft.value = workflow;
    isDirty.value = true;
    schedule();
  }

  function reload(): void {
    if (session.value) take(session.value);
  }

  async function save(force = false): Promise<void> {
    const current = session.value;
    const workflow = draft.value;
    if (current === null || workflow === null) throw new Error("there is no session to save to");
    try {
      let target = base.value;
      if (force) {
        // the event with the new hash may not have arrived yet
        const fresh = await api.edit(current.id);
        session.value = fresh;
        target = fresh.file_sha256;
      }
      const saved = await api.save(current.id, workflow, target, force);
      session.value = saved;
      base.value = saved.file_sha256;
      // an edit made while the save was in flight is not on disk: the draft stays dirty
      if (draft.value === workflow) {
        isDirty.value = false;
        changedOnDisk.value = false;
      }
    } catch (error) {
      if (error instanceof ApiError && error.status === 409) {
        changedOnDisk.value = true;
        return;
      }
      throw error;
    }
  }

  async function run(options: { group?: string | null }): Promise<RunSummary> {
    const workflow = draft.value;
    if (workflow === null) throw new Error("there is no workflow to run");
    const chosen = options.group === undefined ? group.value : options.group;
    const current = session.value;
    if (current?.connected) return api.runEdit(current.id, workflow, chosen);
    return api.submit(workflow, sources(), chosen);
  }

  async function download(): Promise<void> {
    const workflow = draft.value;
    if (workflow === null) throw new Error("there is no workflow to download");
    const text = await api.workflowYaml(workflow);
    const url = URL.createObjectURL(new Blob([text], { type: "application/yaml" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = `${workflow.name}.yaml`;
    link.click();
    // a browser may begin the download after the click returns: the URL stays a while
    setTimeout(() => URL.revokeObjectURL(url), REVOKE_AFTER_MS);
  }

  if (getCurrentScope()) onScopeDispose(() => clearTimeout(timer));

  return { session, draft, base, group, loaded, dirty, connected, changedOnDisk, errors, taskErrors, estimate, valid, loadError, types, runSources, openSession, openRun, change, reload, save, run, download };
}
