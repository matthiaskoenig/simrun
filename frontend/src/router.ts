/**
 * The routes of the app, hash based (`/ui/#/runs/<id>`), so that a reload
 * never asks the service for a path of the app.
 *
 * Every route but the login needs the token of the session; a stored token is
 * checked with `/me` once, which also loads whom it identifies.
 */
import { createRouter, createWebHashHistory, type RouteLocationNormalized, type RouteLocationRaw, type Router, type RouterHistory } from "vue-router";
import { ApiError } from "./api/http";
import { auth } from "./auth";
import LoginView from "./views/LoginView.vue";

declare module "vue-router" {
  interface RouteMeta {
    /** Whether the route opens without a token. */
    public?: boolean;
  }
}

const ExecutionsView = () => import("./views/ExecutionsView.vue");

/** The login, with the page to return to. */
export function loginRoute(next: string): RouteLocationRaw {
  return next === "/" ? { name: "login" } : { name: "login", query: { next } };
}

async function guard(to: RouteLocationNormalized): Promise<true | RouteLocationRaw> {
  if (to.meta.public) return true;
  const token = auth.token.value;
  if (token === null) return loginRoute(to.fullPath);
  if (auth.identity.value === null) {
    try {
      await auth.login(token);
    } catch (error) {
      if (error instanceof ApiError && error.status === 401) {
        auth.logout();
        return loginRoute(to.fullPath);
      }
      // the service cannot be reached: the page opens and shows it
    }
  }
  return true;
}

/** The router of the app; tests pass a memory history. */
export function createAppRouter(history: RouterHistory = createWebHashHistory()): Router {
  const router = createRouter({
    history,
    routes: [
      { path: "/login", name: "login", component: LoginView, meta: { public: true } },
      { path: "/", name: "executions", component: ExecutionsView },
      { path: "/runs/:id", name: "run", component: ExecutionsView, props: true },
      { path: "/:unknown(.*)*", redirect: "/" },
    ],
  });
  router.beforeEach(guard);
  return router;
}
