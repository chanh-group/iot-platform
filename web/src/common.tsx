import { Show, onCleanup, onMount } from "solid-js";
import { ApiError } from "./lib/api";

export function errorText(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.code === "network_error") {
      return `${error.message} For create or credential rotation, check the current state before trying again.`;
    }
    return error.message;
  }
  return error instanceof Error ? error.message : "Something went wrong.";
}

export function ErrorMessage(props: { error: unknown }) {
  return (
    <div class="notice notice-error" role="alert">
      <strong>{errorText(props.error)}</strong>
      <Show when={props.error instanceof ApiError && props.error.requestId}>
        {(id) => <span class="request-id">Request ID: {id()}</span>}
      </Show>
    </div>
  );
}

export function TokenDialog(props: {
  title: string;
  token: string;
  onClose: () => void;
}) {
  let dialog!: HTMLDialogElement;
  onMount(() => dialog.showModal());
  onCleanup(() => {
    if (dialog.open) dialog.close();
  });
  return (
    <dialog ref={dialog} aria-labelledby="token-title" class="token-dialog" onClose={props.onClose}>
      <h2 id="token-title">{props.title}</h2>
      <p>Copy this device token now. It will not be shown again.</p>
      <label for="issued-token">Device token</label>
      <textarea id="issued-token" readonly rows={2} value={props.token} />
      <button class="button button-primary" onClick={() => dialog.close()}>Close and clear token</button>
    </dialog>
  );
}

export function formatTime(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(value);
}

export function formatValue(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "boolean" || typeof value === "number") return String(value);
  return JSON.stringify(value);
}
