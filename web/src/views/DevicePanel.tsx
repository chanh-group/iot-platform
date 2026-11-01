import { Show, createEffect, createSignal, onCleanup } from "solid-js";
import { ApiClient, isAbortError } from "../lib/api";
import type { Device } from "../lib/models";
import { useActions, useRequest } from "../lib/useRequest";
import { ErrorMessage, TokenDialog, formatTime } from "../common";

export function DevicePanel(props: {
  api: ApiClient;
  deviceId: string;
  refreshKey: number;
  reconnectKey: number;
}) {
  const actions = useActions();
  const device = useRequest(
    () => `${props.deviceId}:${props.refreshKey}:${props.reconnectKey}`,
    (signal) => props.api.get<Device>(`/api/v1/devices/${props.deviceId}`, signal),
  );
  const [name, setName] = createSignal("");
  const [metadata, setMetadata] = createSignal("{}");
  const [initialized, setInitialized] = createSignal(false);
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<unknown>();
  const [token, setToken] = createSignal("");

  createEffect(() => {
    const current = device.state().data;
    if (current && !initialized()) {
      setName(current.name);
      setMetadata(JSON.stringify(current.metadata, null, 2));
      setInitialized(true);
    }
  });
  onCleanup(() => setToken(""));

  const save = async (event: SubmitEvent) => {
    event.preventDefault();
    setError(undefined);
    let parsed: unknown;
    try {
      parsed = JSON.parse(metadata());
    } catch {
      setError(new Error("Metadata must be valid JSON."));
      return;
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      setError(new Error("Metadata must be a JSON object."));
      return;
    }
    setBusy(true);
    try {
      const updated = await actions((signal) => props.api.patch<Device>(`/api/v1/devices/${props.deviceId}`, {
        name: name(),
        metadata: parsed,
      }, signal));
      setName(updated.name);
      setMetadata(JSON.stringify(updated.metadata, null, 2));
      device.refresh();
    } catch (failure) {
      if (!isAbortError(failure)) setError(failure);
    } finally {
      setBusy(false);
    }
  };

  const toggleEnabled = async () => {
    const current = device.state().data;
    if (!current) return;
    setBusy(true);
    setError(undefined);
    try {
      await actions((signal) => props.api.patch<Device>(`/api/v1/devices/${props.deviceId}`, {
        enabled: !current.enabled,
      }, signal));
      device.refresh();
    } catch (failure) {
      if (!isAbortError(failure)) setError(failure);
    } finally {
      setBusy(false);
    }
  };

  const rotateCredential = async () => {
    if (!window.confirm("Rotate this device token? The current token will stop working.")) return;
    setBusy(true);
    setError(undefined);
    try {
      const result = await actions((signal) => props.api.post<{
        credential: { id: string; token: string; created_at: number };
      }>(`/api/v1/devices/${props.deviceId}/credentials/rotate`, undefined, signal));
      device.refresh();
      setToken(result.credential.token);
    } catch (failure) {
      if (!isAbortError(failure)) setError(failure);
    } finally {
      setBusy(false);
    }
  };

  const closeToken = () => {
    setToken("");
    window.setTimeout(() => document.getElementById("rotate-device-detail")?.focus(), 0);
  };

  return (
    <>
      <Show when={error()}><ErrorMessage error={error()} /></Show>
      <Show when={device.state().loading}><p class="loading" role="status">Loading device…</p></Show>
      <Show when={device.state().error}><ErrorMessage error={device.state().error} /></Show>
      <Show when={device.state().data}>
        {(current) => <section class="panel" aria-labelledby="device-summary-title">
          <div class="panel-head">
            <h2 id="device-summary-title">{current().name}</h2>
            <div class="inline-actions">
              <span class={`status ${current().online ? "status-online" : "status-offline"}`}>{current().online ? "Online" : "Offline"}</span>
              <button class="button" disabled={busy()} onClick={() => void toggleEnabled()}>{current().enabled ? "Disable device" : "Enable device"}</button>
              <button id="rotate-device-detail" class="button" disabled={busy()} onClick={() => void rotateCredential()}>Rotate token</button>
            </div>
          </div>
          <p class="muted">Last seen: {formatTime(current().last_seen_at)} · Credential created: {formatTime(current().credential.created_at)}</p>
          <form onSubmit={save}>
            <div class="form-grid">
              <div class="field"><label for="edit-device-name">Name</label><input id="edit-device-name" maxlength={64} required value={name()} onInput={(event) => setName(event.currentTarget.value)} /></div>
              <div class="field"><label for="edit-device-metadata">Metadata JSON</label><textarea id="edit-device-metadata" rows={4} value={metadata()} onInput={(event) => setMetadata(event.currentTarget.value)} /></div>
            </div>
            <button class="button button-primary" type="submit" disabled={busy()}>{busy() ? "Saving…" : "Save name and metadata"}</button>
          </form>
        </section>}
      </Show>
      <Show when={token()}>
        {(issued) => <TokenDialog title="New device credential" token={issued()} onClose={closeToken} />}
      </Show>
    </>
  );
}
