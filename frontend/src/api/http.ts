/**
 * The requests of the API: the bearer token, the errors as `ApiError`.
 *
 * This module knows nothing of the session, so that the session itself can
 * check a token with it; `client.ts` adds the token of the session.
 */

/** An answer of the API other than 2xx, with its status and the `detail` of FastAPI. */
export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/** The headers of a request with the bearer token, if there is one. */
export function authorized(token: string | null, headers?: HeadersInit): Headers {
  const result = new Headers(headers);
  if (token) result.set("Authorization", `Bearer ${token}`);
  return result;
}

/** The message of an error answer: the `detail` FastAPI sends, else the status. */
async function errorMessage(response: Response): Promise<string> {
  try {
    const body: unknown = await response.json();
    if (typeof body === "object" && body !== null && "detail" in body && typeof body.detail === "string") {
      return body.detail;
    }
  } catch {
    // not JSON, the status names the error
  }
  return `${response.status} ${response.statusText}`.trim();
}

/** Fetches `path` of the API with `token`; an answer other than 2xx raises `ApiError`. */
export async function request(path: string, token: string | null, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(path, { ...init, headers: authorized(token, init.headers) });
  if (!response.ok) throw new ApiError(response.status, await errorMessage(response));
  return response;
}
