/**
 * The API of the service with the token of the session; a 401 ends the session.
 *
 * The types are those of the OpenAPI document of the service, `schema.d.ts`
 * (`npm run types`).
 */
import { auth } from "../auth";
import { ApiError, request } from "./http";
import type { components } from "./schema";

export { ApiError };

type Schemas = components["schemas"];
export type EditSession = Schemas["EditSession"];
export type Identity = Schemas["Identity"];
export type OutputFile = Schemas["OutputFile"];
export type PodUsage = Schemas["PodUsage"];
export type RunItem = Schemas["RunItem"];
export type RunRecord = Schemas["RunRecord"];
export type RunState = Schemas["RunState"];
export type RunSummary = Schemas["RunSummary"];
export type ServerUsage = Schemas["ServerUsage"];
export type TaskProgress = Schemas["TaskProgress"];
export type TaskEstimate = Schemas["TaskEstimate"];
export type TaskSpec = Schemas["TaskSpec"];
export type TaskState = Schemas["TaskState"];
export type TaskTypeView = Schemas["TaskTypeView"];
export type TreeState = Schemas["TreeState"];
export type ValidationResult = Schemas["ValidationResult"];
export type SourceProvenance = Schemas["SourceProvenance"];
export type WorkerView = Schemas["WorkerView"];
export type Workflow = Schemas["Workflow"];

/** A request with the token of the session, which a 401 ends. */
async function call(path: string, init?: RequestInit): Promise<Response> {
  try {
    return await request(path, auth.token.value, init);
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) auth.logout();
    throw error;
  }
}

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await call(path, init);
  return (await response.json()) as T;
}

const segment = encodeURIComponent;

/** A POST with a JSON body. */
const post = (body: unknown): RequestInit => ({
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

/** The calls of the service the app makes. */
export const api = {
  /** Who the token of the session is. */
  me: (): Promise<Identity> => json("/me"),
  /** The record of a run. */
  run: (id: string): Promise<RunRecord> => json(`/runs/${segment(id)}`),
  /** The last `tailLines` lines of the logs of a task. */
  logs: async (id: string, task: string, tailLines = 300): Promise<string> => {
    const response = await call(`/runs/${segment(id)}/logs/${segment(task)}?tail_lines=${tailLines}`);
    return response.text();
  },
  /** The output files of a task. */
  outputs: (id: string, task: string): Promise<OutputFile[]> => json(`/runs/${segment(id)}/outputs/${segment(task)}`),
  /** Cancels a run. */
  cancel: (id: string): Promise<RunSummary> => json(`/runs/${segment(id)}/cancel`, { method: "POST" }),
  /** The task types of the service. */
  taskTypes: (): Promise<TaskTypeView[]> => json("/task-types"),
  /** The edit sessions of the caller, newest first. */
  edits: (): Promise<EditSession[]> => json("/edits"),
  /** One edit session. */
  edit: (id: string): Promise<EditSession> => json(`/edits/${segment(id)}`),
  /** Saves the workflow through the command of the session; a changed file is a 409. */
  save: (id: string, workflow: Workflow, base: string, force: boolean): Promise<EditSession> =>
    json(`/edits/${segment(id)}/save`, post({ workflow, base, force })),
  /** Runs the workflow through the command of the session, which snapshots the trees again. */
  runEdit: (id: string, workflow: Workflow, group: string | null): Promise<RunSummary> =>
    json(`/edits/${segment(id)}/run`, post({ workflow, group })),
  /** Submits a workflow with the snapshots of `sources`. */
  submit: (workflow: Workflow, sources: SourceProvenance[], group: string | null): Promise<RunSummary> =>
    json("/runs", post({ workflow, sources, group })),
  /** The errors of a workflow, none when valid. */
  validate: (workflow: Workflow): Promise<ValidationResult> => json("/validate", post(workflow)),
  /** The resources, plan and duration per task of a workflow. */
  estimate: (workflow: Workflow, sources: SourceProvenance[]): Promise<Record<string, TaskEstimate>> =>
    json("/estimate", post({ workflow, sources })),
  /** The YAML of a workflow as a save writes it. */
  workflowYaml: async (workflow: Workflow): Promise<string> => (await call("/workflows/yaml", post(workflow))).text(),
};
