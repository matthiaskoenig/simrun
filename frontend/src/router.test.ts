import { afterEach, describe, expect, it, vi } from "vitest";
import { createMemoryHistory } from "vue-router";
import { auth } from "./auth";
import { createAppRouter } from "./router";

const identity = { name: "mk", kind: "person", owner: "mk" };

describe("router", () => {
  afterEach(() => {
    auth.logout();
    localStorage.clear();
  });

  it("sends a visitor without a token to the login, remembering the page", async () => {
    const router = createAppRouter(createMemoryHistory());
    await router.push("/runs/r1");
    expect(router.currentRoute.value.name).toBe("login");
    expect(router.currentRoute.value.query.next).toBe("/runs/r1");
  });

  it("opens a page with a stored token and loads whom it identifies", async () => {
    auth.token.value = "secret";
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify(identity))));
    const router = createAppRouter(createMemoryHistory());
    await router.push("/runs/r1");
    expect(router.currentRoute.value.name).toBe("run");
    expect(router.currentRoute.value.params.id).toBe("r1");
    expect(auth.identity.value).toEqual(identity);
  });

  it("sends a visitor with a token which is no longer valid to the login", async () => {
    auth.token.value = "revoked";
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response("{}", { status: 401 })));
    const router = createAppRouter(createMemoryHistory());
    await router.push("/");
    expect(router.currentRoute.value.name).toBe("login");
    expect(auth.token.value).toBeNull();
  });

  it("opens the page when the service cannot be reached, keeping the token", async () => {
    auth.token.value = "secret";
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockRejectedValue(new TypeError("Failed to fetch")));
    const router = createAppRouter(createMemoryHistory());
    await router.push("/");
    expect(router.currentRoute.value.name).toBe("executions");
    expect(auth.token.value).toBe("secret");
  });

  it.each([
    ["/edit", "edit", {}],
    ["/edit/abc", "edit-session", { id: "abc" }],
    ["/runs/r1/edit", "run-edit", { id: "r1" }],
  ])("resolves %s to the Editor and needs the token", async (path, name, params) => {
    const router = createAppRouter(createMemoryHistory());
    await router.push(path);
    expect(router.currentRoute.value.name).toBe("login");
    expect(router.currentRoute.value.query.next).toBe(path);
    auth.token.value = "secret";
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify(identity))));
    await router.push(path);
    expect(router.currentRoute.value.name).toBe(name);
    expect(router.currentRoute.value.params).toMatchObject(params);
  });
});
