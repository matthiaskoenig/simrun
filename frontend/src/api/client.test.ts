import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { auth } from "../auth";
import { ApiError, api } from "./client";

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "Content-Type": "application/json" } });
}

describe("api", () => {
  beforeEach(() => {
    auth.token.value = "secret";
  });

  afterEach(() => {
    auth.logout();
    localStorage.clear();
  });

  it("sends the bearer token", async () => {
    const identity = { name: "mk", kind: "person", owner: "mk" };
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(json(identity));
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.me()).resolves.toEqual(identity);
    expect(fetchMock).toHaveBeenCalledOnce();
    const [path, init] = fetchMock.mock.calls[0] ?? [];
    expect(path).toBe("/me");
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer secret");
  });

  it("raises ApiError(401) on a 401 and clears the token", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(json({ detail: "a valid bearer token is required" }, 401)));

    const error = await api.me().catch((reason: unknown) => reason);
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 401, message: "a valid bearer token is required" });
    expect(auth.token.value).toBeNull();
  });

  it("keeps the token on another error", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(json({ detail: "no run r9" }, 404)));

    await expect(api.run("r9")).rejects.toMatchObject({ status: 404, message: "no run r9" });
    expect(auth.token.value).toBe("secret");
  });

  it("reads the tail of a log as text and escapes the path", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response("line 1\nline 2\n"));
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.logs("r1", "fit/a")).resolves.toBe("line 1\nline 2\n");
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/runs/r1/logs/fit%2Fa?tail_lines=300");
  });

  it("cancels a run with a POST", async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(json({ run_id: "r1", state: "cancelled" }));
    vi.stubGlobal("fetch", fetchMock);

    await api.cancel("r1");
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/runs/r1/cancel");
    expect(fetchMock.mock.calls[0]?.[1]?.method).toBe("POST");
  });
});
