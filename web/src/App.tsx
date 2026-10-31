import { Show, createSignal, onMount } from "solid-js";
import { ApiClient, ApiError, isAbortError } from "./lib/api";
import type { AdminUser } from "./lib/models";
import { useActions } from "./lib/useRequest";
import { Dashboard } from "./Dashboard";
import { ErrorMessage } from "./common";

interface LoginResponse {
  user: { id: string; username: string };
  expires_at: number;
}

export function App() {
  const [user, setUser] = createSignal<AdminUser>();
  const [authLoading, setAuthLoading] = createSignal(true);
  const [authNotice, setAuthNotice] = createSignal("");
  const [authError, setAuthError] = createSignal<unknown>();
  const [signingOut, setSigningOut] = createSignal(false);
  const actions = useActions();
  const apiRef: { current?: ApiClient } = {};
  const expireSession = () => {
    if (!user()) return;
    apiRef.current?.unauthenticated();
    setUser(undefined);
    setAuthNotice("Your session has expired. Sign in again.");
    setAuthError(undefined);
  };
  const api = new ApiClient(expireSession);
  apiRef.current = api;

  onMount(() => {
    void actions((signal) => api.get<AdminUser>("/api/v1/auth/me", signal)).then(
      (current) => {
        api.authenticated();
        setUser(current);
      },
      (error: unknown) => {
        if (!isAbortError(error) && !(error instanceof ApiError && error.status === 401)) {
          setAuthError(error);
        }
      },
    ).finally(() => setAuthLoading(false));
  });

  const login = async (username: string, password: string) => {
    setAuthNotice("");
    setAuthError(undefined);
    try {
      const response = await actions((signal) =>
        api.post<LoginResponse>("/api/v1/auth/login", { username, password }, signal)
      );
      api.authenticated();
      setUser({ ...response.user, expires_at: response.expires_at });
    } catch (error) {
      if (!isAbortError(error)) setAuthError(error);
    }
  };

  const logout = async () => {
    api.unauthenticated();
    setUser(undefined);
    setSigningOut(true);
    setAuthNotice("");
    setAuthError(undefined);
    let failure: unknown;
    try {
      await actions((signal) => api.post<void>("/api/v1/auth/logout", undefined, signal));
    } catch (error) {
      if (!isAbortError(error)) failure = error;
    } finally {
      setSigningOut(false);
      setAuthNotice(failure ? "Signed out locally; the server did not confirm session revocation." : "");
      setAuthError(failure);
    }
  };

  return (
    <Show when={!authLoading()} fallback={<main class="login-shell"><p role="status">Checking admin session…</p></main>}>
      <Show when={signingOut()} fallback={
        <Show when={user()} keyed fallback={<Login onLogin={login} notice={authNotice()} error={authError()} />}>
          {(current) => (
            <Dashboard
              api={api}
              user={current}
              notice={authNotice()}
              onLogout={logout}
              onSessionExpired={expireSession}
            />
          )}
        </Show>
      }>
        <main class="login-shell"><p role="status">Signing out…</p></main>
      </Show>
    </Show>
  );
}

function Login(props: {
  onLogin: (username: string, password: string) => Promise<void>;
  notice: string;
  error: unknown;
}) {
  const [username, setUsername] = createSignal("");
  const [password, setPassword] = createSignal("");
  const [submitting, setSubmitting] = createSignal(false);

  const submit = async (event: SubmitEvent) => {
    event.preventDefault();
    setSubmitting(true);
    try {
      await props.onLogin(username(), password());
    } finally {
      setSubmitting(false);
      setPassword("");
    }
  };

  return (
    <main class="login-shell">
      <section class="login-card" aria-labelledby="login-title">
        <div class="brand"><span class="brand-mark" aria-hidden="true">I</span> IoT Server</div>
        <h1 id="login-title">Admin sign in</h1>
        <p class="muted">Manage devices, telemetry, commands, and runtime settings.</p>
        <Show when={props.notice}><div class="notice notice-warning" role="status">{props.notice}</div></Show>
        <Show when={props.error}><ErrorMessage error={props.error} /></Show>
        <form aria-label="Admin login" onSubmit={submit}>
          <div class="field">
            <label for="login-username">Username</label>
            <input
              id="login-username"
              name="username"
              autocomplete="username"
              required
              value={username()}
              onInput={(event) => setUsername(event.currentTarget.value)}
            />
          </div>
          <div class="field">
            <label for="login-password">Password</label>
            <input
              id="login-password"
              name="password"
              type="password"
              autocomplete="current-password"
              required
              value={password()}
              onInput={(event) => setPassword(event.currentTarget.value)}
            />
          </div>
          <button class="button button-primary" type="submit" disabled={submitting()}>
            {submitting() ? "Signing in…" : "Sign in"}
          </button>
        </form>
        <p class="form-help" aria-live="polite">{submitting() ? "Authenticating…" : "Session credentials are held only by the HttpOnly cookie."}</p>
      </section>
    </main>
  );
}
