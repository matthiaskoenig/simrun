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
export type Identity = Schemas["Identity"];
export type OutputFile = Schemas["OutputFile"];
export type PodUsage = Schemas["PodUsage"];
export type RunItem = Schemas["RunItem"];
export type RunRecord = Schemas["RunRecord"];
export type RunState = Schemas["RunState"];
export type RunSummary = Schemas["RunSummary"];
export type ServerUsage = Schemas["ServerUsage"];
export type TaskProgress = Schemas["TaskProgress"];
export type TaskSpec = Schemas["TaskSpec"];
export type TaskState = Schemas["TaskState"];
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
};
