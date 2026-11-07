import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createMemoryHistory, createRouter, type Router } from "vue-router";
import { auth } from "../auth";
import LoginView from "./LoginView.vue";

async function mountLogin(path = "/login"): Promise<{ wrapper: ReturnType<typeof mount>; router: Router }> {
  const router = createRouter({
    history: createMemoryHistory(),
    routes: [
      { path: "/login", name: "login", component: LoginView },
      { path: "/", component: { render: () => null } },
      { path: "/runs/:id", component: { render: () => null } },
    ],
  });
  await router.push(path);
  const wrapper = mount(LoginView, { global: { plugins: [router] } });
  return { wrapper, router };
}

async function submit(wrapper: ReturnType<typeof mount>, token: string): Promise<void> {
  await wrapper.get("input").setValue(token);
  await wrapper.get("form").trigger("submit");
  await flushPromises();
}

describe("LoginView", () => {
  afterEach(() => {
    auth.logout();
    localStorage.clear();
  });

  it("checks the token with /me, stores it and routes to /", async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response(JSON.stringify({ name: "mk", kind: "person", owner: "mk" }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const { wrapper, router } = await mountLogin();

    await submit(wrapper, "secret");

    expect(fetchMock).toHaveBeenCalledOnce();
    const [path, init] = fetchMock.mock.calls[0] ?? [];
    expect(path).toBe("/me");
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer secret");
    expect(localStorage.getItem("simrun.token")).toBe("secret");
    expect(auth.identity.value).toEqual({ name: "mk", kind: "person", owner: "mk" });
    expect(router.currentRoute.value.path).toBe("/");
  });

  it("returns to the page the login interrupted", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({ name: "mk", kind: "person", owner: "mk" }))),
    );
    const { wrapper, router } = await mountLogin("/login?next=/runs/r1");

    await submit(wrapper, "secret");

    expect(router.currentRoute.value.path).toBe("/runs/r1");
  });

  it("shows that a token was not accepted on a 401", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockResolvedValue(new Response("{}", { status: 401 })));
    const { wrapper, router } = await mountLogin();

    await submit(wrapper, "wrong");

    expect(wrapper.text()).toContain("The token was not accepted");
    expect(localStorage.getItem("simrun.token")).toBeNull();
    expect(router.currentRoute.value.path).toBe("/login");
  });

  it("shows that the service cannot be reached", async () => {
    vi.stubGlobal("fetch", vi.fn<typeof fetch>().mockRejectedValue(new TypeError("Failed to fetch")));
    const { wrapper } = await mountLogin();

    await submit(wrapper, "secret");

    expect(wrapper.text()).toContain("The service cannot be reached");
  });
});
