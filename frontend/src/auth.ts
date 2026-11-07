/**
 * The session: the bearer token the user entered and whom it identifies.
 *
 * The token is kept in `localStorage["simrun.token"]`, so a reload keeps the
 * session; it is never put into a URL.
 */
import { ref, watch } from "vue";
import type { Identity } from "./api/client";
import { request } from "./api/http";

const STORAGE_KEY = "simrun.token";

function stored(): string | null {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    // storage disabled: the session lasts as long as the page
    return null;
  }
}

function store(value: string | null): void {
  try {
    if (value === null) localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, value);
  } catch {
    // storage disabled
  }
}

const token = ref<string | null>(stored());
const identity = ref<Identity | null>(null);

// the storage follows the token, whoever changes it
watch(token, store, { flush: "sync" });

/** Checks `value` with `/me`; a valid token starts the session, an invalid one raises `ApiError(401)`. */
async function login(value: string): Promise<Identity> {
  const candidate = value.trim();
  const response = await request("/me", candidate);
  const me = (await response.json()) as Identity;
  token.value = candidate;
  identity.value = me;
  return me;
}

/** Ends the session and forgets the token. */
function logout(): void {
  token.value = null;
  identity.value = null;
}

export const auth = { token, identity, login, logout };
