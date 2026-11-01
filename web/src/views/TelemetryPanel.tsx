import { For, Show, createEffect } from "solid-js";
import { ApiClient } from "../lib/api";
import type { RealtimeEvent } from "../lib/models";
import { useDeviceTelemetry } from "../lib/deviceTelemetry";
import { ErrorMessage, formatTime, formatValue } from "../common";
import { TelemetryChart } from "../TelemetryChart";

export function TelemetryPanel(props: {
  api: ApiClient;
  deviceId: string;
  reconnectKey: number;
  liveEvents: RealtimeEvent[];
}) {
  const telemetry = useDeviceTelemetry(
    props.api,
    props.deviceId,
    () => props.reconnectKey,
    () => props.liveEvents,
  );
  let keySelect!: HTMLSelectElement;
  createEffect(() => {
    const key = telemetry.key();
    const keys = telemetry.latestValues();
    if (keys.some((item) => item.key === key)) keySelect.value = key;
  });

  return (
    <>
      <section class="panel" aria-labelledby="latest-title">
        <div class="panel-head"><h2 id="latest-title">Latest telemetry</h2><button class="button" onClick={telemetry.latest.refresh}>Refresh</button></div>
        <Show when={telemetry.latest.state().loading}><p class="loading" role="status">Loading latest values…</p></Show>
        <Show when={telemetry.latest.state().error}><ErrorMessage error={telemetry.latest.state().error} /></Show>
        <Show when={telemetry.latest.state().data}>
          <Show when={telemetry.latestValues().length > 0} fallback={<p class="empty-state">No telemetry has been stored for this device.</p>}>
            <div class="table-wrap"><table><thead><tr><th>Key</th><th>Value</th><th>Timestamp</th></tr></thead><tbody>
              <For each={telemetry.latestValues()}>{(item) => <tr><td>{item.key}</td><td>{formatValue(item.value)}</td><td>{formatTime(item.ts)}</td></tr>}</For>
            </tbody></table></div>
          </Show>
        </Show>
      </section>

      <section class="panel" aria-labelledby="telemetry-title">
        <div class="heading-row">
          <div><h2 id="telemetry-title">Telemetry history</h2><p>History uses [from, to); live and reconnect-backfill samples overlay separately without changing that range.</p></div>
        </div>
        <div class="toolbar">
          <div class="field"><label for="history-from">From</label><input id="history-from" type="datetime-local" step="0.001" value={telemetry.from()} onInput={(event) => telemetry.changeFrom(event.currentTarget.value)} /></div>
          <div class="field"><label for="history-to">To</label><input id="history-to" type="datetime-local" step="0.001" value={telemetry.to()} onInput={(event) => telemetry.changeTo(event.currentTarget.value)} /></div>
          <div class="field"><label for="telemetry-key">Value key</label><select id="telemetry-key" ref={keySelect} value={telemetry.key()} onChange={(event) => telemetry.changeKey(event.currentTarget.value)}>
            <option value="">All values</option>
            <For each={telemetry.latestValues()}>{(item) => <option value={item.key}>{item.key}{typeof item.value === "number" ? " · numeric" : " · latest only"}</option>}</For>
          </select></div>
          <button class="button" onClick={telemetry.history.refresh}>Reload page</button>
        </div>
        <Show when={telemetry.history.state().loading}><p class="loading" role="status">Loading telemetry history…</p></Show>
        <Show when={telemetry.history.state().error}><ErrorMessage error={telemetry.history.state().error} /></Show>
        <Show when={telemetry.backfill().status === "loading"}>
          <p class="loading" role="status">Backfilling telemetry after reconnect in bounded pages…</p>
        </Show>
        <Show when={telemetry.backfill().status === "fetched" && telemetry.backfill().pages > 0}>
          <p class="notice notice-warning" role="status">
            Fetched {telemetry.backfill().rows.length} events across {telemetry.backfill().pages} pages in the event-time range. This does not prove all missed events were recovered: late events with timestamps before the selected start are excluded.
          </p>
        </Show>
        <Show when={telemetry.backfill().status === "partial"}>
          <div class="notice notice-warning" role="alert">
            Reconnect backfill query is bounded: fetched {telemetry.backfill().rows.length} events across {telemetry.backfill().pages} pages in the event-time range.
            {telemetry.backfill().nextCursor
              ? " More cursor pages remain; narrow the selected start time and retry or browse history pages."
              : " The requested start predates the 31-day limit; earlier events were not fetched."}
            {" "}Late events with timestamps before the selected start are excluded, so complete recovery cannot be established.
            <button class="button" type="button" onClick={telemetry.retryBackfill}>Retry bounded backfill</button>
          </div>
        </Show>
        <Show when={telemetry.backfill().status === "error"}>
          <ErrorMessage error={telemetry.backfill().error} />
          <button class="button" type="button" onClick={telemetry.retryBackfill}>Retry backfill</button>
        </Show>
        <Show keyed when={telemetry.chartKey()}>
          {(key) => <TelemetryChart telemetryKey={key} history={telemetry.historyRows} live={telemetry.live} />}
        </Show>
        <Show when={telemetry.key() && !telemetry.chartKey()}>
          <p class="empty-state">No numeric samples for this key on the current page or in the live stream.</p>
        </Show>
        <Show when={telemetry.history.state().data}>
          {(page) => <>
            <Show when={page().items.length === 0}><p class="empty-state">No stored telemetry on this history page.</p></Show>
            <Show when={page().items.length > 0}>
              <div class="table-wrap"><table><thead><tr><th>Timestamp</th><th>Event</th><th>Values</th></tr></thead><tbody>
                <For each={page().items}>{(row) => <tr><td>{formatTime(row.ts)}</td><td class="mono">{row.event_id}</td><td><code>{JSON.stringify(row.values)}</code></td></tr>}</For>
              </tbody></table></div>
            </Show>
            <nav class="pagination" aria-label="Telemetry history pages">
              <button class="button" disabled={!telemetry.canGoPrevious() || telemetry.history.state().loading} onClick={telemetry.previousPage}>Previous page</button>
              <span role="status">Page {telemetry.pageNumber()}</span>
              <button class="button" aria-label="Next history page" disabled={!telemetry.canGoNext() || telemetry.history.state().loading} onClick={telemetry.nextPage}>Next page</button>
            </nav>
          </>}
        </Show>
      </section>
    </>
  );
}
