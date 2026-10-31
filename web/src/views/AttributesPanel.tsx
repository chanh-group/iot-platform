import { For, Show, createSignal } from "solid-js";
import { ApiClient } from "../lib/api";
import type { Attribute, Page } from "../lib/models";
import { useRequest } from "../lib/useRequest";
import { ErrorMessage, formatTime, formatValue } from "../common";

export function AttributesPanel(props: { api: ApiClient; deviceId: string; reconnectKey: number }) {
  const [cursor, setCursor] = createSignal("");
  const attributes = useRequest(
    () => `${props.deviceId}:${cursor()}:${props.reconnectKey}`,
    (signal) => {
      const params = new URLSearchParams({ limit: "200" });
      if (cursor()) params.set("cursor", cursor());
      return props.api.get<Page<Attribute>>(`/api/v1/devices/${props.deviceId}/attributes?${params}`, signal);
    },
  );

  return (
    <section class="panel" aria-labelledby="attributes-title">
      <h2 id="attributes-title">Latest attributes</h2>
      <Show when={attributes.state().loading}><p class="loading" role="status">Loading attributes…</p></Show>
      <Show when={attributes.state().error}><ErrorMessage error={attributes.state().error} /></Show>
      <Show when={attributes.state().data}>
        {(page) => <>
          <Show when={page().items.length === 0}><p class="empty-state">No reported attributes.</p></Show>
          <div class="table-wrap"><table><thead><tr><th>Key</th><th>Value</th><th>Updated</th></tr></thead><tbody>
            <For each={page().items}>{(item) => <tr><td>{item.key}</td><td>{formatValue(item.value)}</td><td>{formatTime(item.updated_at)}</td></tr>}</For>
          </tbody></table></div>
          <button class="button" disabled={!page().next_cursor || attributes.state().loading} onClick={() => setCursor(page().next_cursor ?? "")}>More attributes</button>
        </>}
      </Show>
    </section>
  );
}
