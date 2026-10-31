import { For, Show, createEffect, createSignal } from "solid-js";
import { ApiClient, isAbortError } from "../lib/api";
import type { Command, Page } from "../lib/models";
import { useActions, useRequest } from "../lib/useRequest";
import { ErrorMessage, formatTime } from "../common";

export function CommandsPanel(props: {
  api: ApiClient;
  deviceId: string;
  refreshKey: number;
  reconnectKey: number;
}) {
  const actions = useActions();
  const [status, setStatus] = createSignal("all");
  const [cursor, setCursor] = createSignal("");
  const [selectedId, setSelectedId] = createSignal("");
  const [name, setName] = createSignal("reboot");
  const [paramsText, setParamsText] = createSignal("{}");
  const [ttl, setTtl] = createSignal("60000");
  const [idempotencyKey, setIdempotencyKey] = createSignal("");
  const [error, setError] = createSignal<unknown>();
  const [busy, setBusy] = createSignal(false);

  const commands = useRequest(
    () => `${props.deviceId}:${status()}:${cursor()}:${props.refreshKey}:${props.reconnectKey}`,
    (signal) => {
      const query = new URLSearchParams({ limit: "50" });
      if (status() !== "all") query.set("status", status());
      if (cursor()) query.set("cursor", cursor());
      return props.api.get<Page<Command>>(`/api/v1/devices/${props.deviceId}/commands?${query}`, signal);
    },
  );
  const selected = useRequest(
    () => `${selectedId()}:${props.refreshKey}:${props.reconnectKey}`,
    (signal) => selectedId()
      ? props.api.get<Command>(`/api/v1/commands/${selectedId()}`, signal)
      : Promise.resolve(undefined),
  );

  createEffect(() => {
    const items = commands.state().data?.items;
    if (items && !items.some((item) => item.id === selectedId())) setSelectedId(items[0]?.id ?? "");
  });

  const create = async (event: SubmitEvent) => {
    event.preventDefault();
    setError(undefined);
    let params: unknown;
    try {
      params = JSON.parse(paramsText());
    } catch {
      setError(new Error("Command parameters must be valid JSON."));
      return;
    }
    if (typeof params !== "object" || params === null || Array.isArray(params)) {
      setError(new Error("Command parameters must be a JSON object."));
      return;
    }
    setBusy(true);
    try {
      const body: Record<string, unknown> = { name: name(), params };
      if (ttl()) body.ttl_ms = Number(ttl());
      if (idempotencyKey()) body.idempotency_key = idempotencyKey();
      const result = await actions((signal) => props.api.post<{
        command_id: string;
        device_id: string;
        status: string;
        expires_at: number;
      }>(`/api/v1/devices/${props.deviceId}/commands`, body, signal));
      setSelectedId(result.command_id);
      commands.refresh();
    } catch (failure) {
      if (!isAbortError(failure)) setError(failure);
    } finally {
      setBusy(false);
    }
  };

  return (
    <section class="panel" aria-labelledby="commands-title">
      <h2 id="commands-title">Commands</h2>
      <Show when={error()}><ErrorMessage error={error()} /></Show>
      <form onSubmit={create}>
        <div class="form-grid">
          <div class="field"><label for="command-name">Command name</label><input id="command-name" required maxlength={64} value={name()} onInput={(event) => setName(event.currentTarget.value)} /></div>
          <div class="field"><label for="command-ttl">TTL (milliseconds)</label><input id="command-ttl" type="number" min={1_000} value={ttl()} onInput={(event) => setTtl(event.currentTarget.value)} /></div>
          <div class="field"><label for="command-params">Parameters JSON</label><textarea id="command-params" rows={3} value={paramsText()} onInput={(event) => setParamsText(event.currentTarget.value)} /></div>
          <div class="field"><label for="command-idempotency">Idempotency key (optional)</label><input id="command-idempotency" maxlength={128} value={idempotencyKey()} onInput={(event) => setIdempotencyKey(event.currentTarget.value)} /></div>
        </div>
        <p class="form-help">A timed-out command create is not retried automatically. Check command status before creating another command.</p>
        <button class="button button-primary" type="submit" disabled={busy()}>{busy() ? "Creating…" : "Create command"}</button>
      </form>
      <div class="toolbar">
        <div class="field"><label for="command-filter">Status</label><select id="command-filter" value={status()} onChange={(event) => {
          setStatus(event.currentTarget.value);
          setCursor("");
        }}>
          <option value="all">All</option><option value="pending">Pending</option><option value="dispatched">Dispatched</option><option value="succeeded">Succeeded</option><option value="failed">Failed</option><option value="expired">Expired</option>
        </select></div>
        <button class="button" onClick={commands.refresh}>Refresh commands</button>
      </div>
      <Show when={commands.state().loading}><p class="loading" role="status">Loading commands…</p></Show>
      <Show when={commands.state().error}><ErrorMessage error={commands.state().error} /></Show>
      <Show when={commands.state().data}>
        {(page) => <>
          <Show when={page().items.length === 0}><p class="empty-state">No commands for this device.</p></Show>
          <div class="table-wrap"><table><thead><tr><th>Command</th><th>Status</th><th>Created</th><th>Result</th></tr></thead><tbody>
            <For each={page().items}>{(item) => <tr>
              <td><button class="button button-quiet mono" onClick={() => setSelectedId(item.id)}>{item.id}</button><br />{item.name}</td>
              <td>{item.status}</td><td>{formatTime(item.created_at)}</td><td>{item.error ?? (item.result === null ? "—" : JSON.stringify(item.result))}</td>
            </tr>}</For>
          </tbody></table></div>
          <button class="button" disabled={!page().next_cursor} onClick={() => setCursor(page().next_cursor ?? "")}>More commands</button>
        </>}
      </Show>
      <div class="panel">
        <h3>Selected command details</h3>
        <Show when={selectedId()} fallback={<p class="muted">Select a command to inspect its current state.</p>}>
          <Show when={selected.state().loading}><p role="status">Loading command…</p></Show>
          <Show when={selected.state().error}><ErrorMessage error={selected.state().error} /></Show>
          <Show when={selected.state().data}>
            {(item) => <dl>
              <dt>Status</dt><dd>{item().status}</dd>
              <dt>Expires</dt><dd>{formatTime(item().expires_at)}</dd>
              <dt>Attempts</dt><dd>{item().attempts}</dd>
              <dt>Result</dt><dd><pre class="json-view">{item().result === null ? "—" : JSON.stringify(item().result, null, 2)}</pre></dd>
              <dt>Error</dt><dd>{item().error ?? "—"}</dd>
            </dl>}
          </Show>
        </Show>
      </div>
    </section>
  );
}
