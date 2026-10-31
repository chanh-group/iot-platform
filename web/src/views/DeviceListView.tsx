import { For, Show, createSignal } from "solid-js";
import { ApiClient, isAbortError } from "../lib/api";
import type { Device, Page } from "../lib/models";
import { useActions, useRequest } from "../lib/useRequest";
import { ErrorMessage, formatTime } from "../common";

export function DeviceListView(props: {
  api: ApiClient;
  refreshKey: number;
  onOpen: (id: string) => void;
  onIssued: (title: string, token: string) => void;
}) {
  const [search, setSearch] = createSignal("");
  const [query, setQuery] = createSignal("");
  const [enabled, setEnabled] = createSignal("all");
  const [cursor, setCursor] = createSignal("");
  const [cursorStack, setCursorStack] = createSignal<string[]>([]);
  const [createName, setCreateName] = createSignal("");
  const [metadata, setMetadata] = createSignal("{}");
  const [createError, setCreateError] = createSignal<unknown>();
  const [busy, setBusy] = createSignal(false);
  const actions = useActions();

  const devices = useRequest(
    () => `${query()}|${enabled()}|${cursor()}|${props.refreshKey}`,
    (signal) => {
      const params = new URLSearchParams({ limit: "50" });
      if (query()) params.set("q", query());
      if (enabled() !== "all") params.set("enabled", enabled());
      if (cursor()) params.set("cursor", cursor());
      return props.api.get<Page<Device>>(`/api/v1/devices?${params}`, signal);
    },
  );

  const submitSearch = (event: SubmitEvent) => {
    event.preventDefault();
    setQuery(search().trim());
    setCursor("");
    setCursorStack([]);
  };

  const createDevice = async (event: SubmitEvent) => {
    event.preventDefault();
    setCreateError(undefined);
    let parsed: unknown;
    try {
      parsed = JSON.parse(metadata());
    } catch {
      setCreateError(new Error("Metadata must be valid JSON."));
      return;
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      setCreateError(new Error("Metadata must be a JSON object."));
      return;
    }
    setBusy(true);
    try {
      const result = await actions((signal) => props.api.post<{
        device: Device;
        credential: { id: string; token: string; created_at: number };
      }>("/api/v1/devices", { name: createName(), metadata: parsed, enabled: true }, signal));
      devices.refresh();
      setCreateName("");
      setMetadata("{}");
      props.onIssued("Device created · copy its credential", result.credential.token);
    } catch (error) {
      if (!isAbortError(error)) setCreateError(error);
    } finally {
      setBusy(false);
    }
  };

  const updateEnabled = async (device: Device) => {
    setBusy(true);
    setCreateError(undefined);
    try {
      await actions((signal) => props.api.patch<Device>(`/api/v1/devices/${device.id}`, {
        enabled: !device.enabled,
      }, signal));
      devices.refresh();
    } catch (error) {
      if (!isAbortError(error)) setCreateError(error);
    } finally {
      setBusy(false);
    }
  };

  const rotate = async (device: Device) => {
    if (!window.confirm(`Rotate credentials for ${device.name}? The current token will stop working.`)) return;
    setBusy(true);
    setCreateError(undefined);
    try {
      const result = await actions((signal) => props.api.post<{
        device_id: string;
        credential: { id: string; token: string; created_at: number };
      }>(`/api/v1/devices/${device.id}/credentials/rotate`, undefined, signal));
      devices.refresh();
      props.onIssued(`New credential · ${device.name}`, result.credential.token);
    } catch (error) {
      if (!isAbortError(error)) setCreateError(error);
    } finally {
      setBusy(false);
    }
  };

  const nextPage = () => {
    const next = devices.state().data?.next_cursor;
    if (!next) return;
    setCursorStack((stack) => [...stack, cursor()]);
    setCursor(next);
  };
  const previousPage = () => {
    const stack = cursorStack();
    if (!stack.length) return;
    setCursor(stack[stack.length - 1]);
    setCursorStack(stack.slice(0, -1));
  };

  return (
    <section aria-labelledby="devices-title">
      <div class="heading-row">
        <div><h1 id="devices-title">Devices</h1><p>Manage device metadata, access, and credentials.</p></div>
        <button class="button" onClick={devices.refresh}>Refresh list</button>
      </div>

      <Show when={createError()}><ErrorMessage error={createError()} /></Show>
      <form class="toolbar" onSubmit={submitSearch} aria-label="Filter devices">
        <div class="field">
          <label for="device-search">Search name or ID</label>
          <input id="device-search" value={search()} onInput={(event) => setSearch(event.currentTarget.value)} placeholder="At least 2 characters" />
        </div>
        <div class="field">
          <label for="device-enabled">Enabled status</label>
          <select id="device-enabled" value={enabled()} onChange={(event) => {
            setEnabled(event.currentTarget.value);
            setCursor("");
            setCursorStack([]);
          }}>
            <option value="all">All devices</option>
            <option value="true">Enabled</option>
            <option value="false">Disabled</option>
          </select>
        </div>
        <button class="button button-primary" type="submit">Search</button>
      </form>

      <div class="panel">
        <h2>Create device</h2>
        <form onSubmit={createDevice}>
          <div class="form-grid">
            <div class="field">
              <label for="new-device-name">Device name</label>
              <input id="new-device-name" required maxlength={64} value={createName()} onInput={(event) => setCreateName(event.currentTarget.value)} />
            </div>
            <div class="field">
              <label for="new-device-metadata">Metadata JSON</label>
              <textarea id="new-device-metadata" rows={2} value={metadata()} onInput={(event) => setMetadata(event.currentTarget.value)} />
            </div>
          </div>
          <p class="form-help">A new device credential is shown once after creation. If the request times out, check the list before submitting again.</p>
          <button class="button button-primary" type="submit" disabled={busy()}>{busy() ? "Saving…" : "Create device"}</button>
        </form>
      </div>

      <div class="panel">
        <h2>Device list</h2>
        <Show when={devices.state().loading}><p class="loading" role="status">Loading devices…</p></Show>
        <Show when={devices.state().error}><ErrorMessage error={devices.state().error} /></Show>
        <Show when={devices.state().data}>
          {(page) => (
            <>
              <Show when={page().items.length === 0}>
                <p class="empty-state">No devices found. Create one above to get started.</p>
              </Show>
              <Show when={page().items.length > 0}>
                <div class="table-wrap">
                  <table>
                    <thead><tr><th>Name</th><th>Connection</th><th>Enabled</th><th>Last seen</th><th>Actions</th></tr></thead>
                    <tbody>
                      <For each={page().items}>{(device) => (
                        <tr>
                          <td><button class="button button-quiet" onClick={() => props.onOpen(device.id)}>{device.name}</button><br /><small class="mono">{device.id}</small></td>
                          <td><span class={`status ${device.online ? "status-online" : "status-offline"}`}>{device.online ? "Online" : "Offline"}</span></td>
                          <td>{device.enabled ? "Enabled" : <span class="status status-disabled">Disabled</span>}</td>
                          <td>{formatTime(device.last_seen_at)}</td>
                          <td><div class="inline-actions">
                            <button class="button" disabled={busy()} onClick={() => void updateEnabled(device)}>{device.enabled ? "Disable" : "Enable"}</button>
                            <button class="button" disabled={busy()} onClick={() => void rotate(device)}>Rotate token</button>
                          </div></td>
                        </tr>
                      )}</For>
                    </tbody>
                  </table>
                </div>
              </Show>
              <div class="pagination">
                <button class="button" disabled={cursorStack().length === 0 || devices.state().loading} onClick={previousPage}>Previous</button>
                <button class="button" disabled={!page().next_cursor || devices.state().loading} onClick={nextPage}>Next</button>
              </div>
            </>
          )}
        </Show>
      </div>
    </section>
  );
}
