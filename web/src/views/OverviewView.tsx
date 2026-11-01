import { Show } from "solid-js";
import { ApiClient } from "../lib/api";
import type { OverviewData } from "../lib/models";
import { useRequest } from "../lib/useRequest";
import { ErrorMessage, formatTime } from "../common";

export function OverviewView(props: {
  api: ApiClient;
  refreshKey: number;
  onDevices: () => void;
  onConfig: () => void;
}) {
  const overview = useRequest(
    () => props.refreshKey,
    (signal) => props.api.get<OverviewData>("/api/v1/overview", signal),
  );

  return (
    <section aria-labelledby="overview-title">
      <div class="heading-row">
        <div>
          <h1 id="overview-title" tabIndex={-1}>System overview</h1>
          <p>Live device, ingest, and command health.</p>
        </div>
        <div class="inline-actions">
          <button class="button" onClick={overview.refresh}>Refresh</button>
          <button class="button" onClick={props.onDevices}>Manage devices</button>
          <button class="button" onClick={props.onConfig}>Runtime config</button>
        </div>
      </div>
      <Show when={overview.state().loading}><p class="loading" role="status">Loading overview…</p></Show>
      <Show when={overview.state().error}><ErrorMessage error={overview.state().error} /></Show>
      <Show when={overview.state().data}>
        {(data) => (
          <>
            <Show when={!data().storage_ok}>
              <div class="notice notice-error" role="alert">
                Storage is unavailable. New telemetry may not be saved.
              </div>
            </Show>
            <div class="grid metric-grid">
              <Metric label="Devices online" value={`${data().devices.online} / ${data().devices.total}`} detail={`${data().devices.disabled} disabled`} />
              <Metric label="Telemetry events · 24h" value={data().telemetry.events_24h.toLocaleString()} detail={`Last received ${formatTime(data().telemetry.last_received_at)}`} />
              <Metric label="Commands in progress" value={`${data().commands.pending + data().commands.dispatched}`} detail={`${data().commands.pending} pending · ${data().commands.dispatched} dispatched`} />
              <Metric label="Ingest queue" value={`${data().ingest.queue_depth} / ${data().ingest.queue_capacity}`} detail={`${data().ingest.rejected_24h.toLocaleString()} rejected · 24h`} />
            </div>
            <div class="panel">
              <h2>Command outcomes · 24h</h2>
              <p>{data().commands.failed_24h} failed · {data().commands.expired_24h} expired</p>
              <p class="muted">Overview counters are refreshed from the API after reconnect.</p>
            </div>
          </>
        )}
      </Show>
    </section>
  );
}

function Metric(props: { label: string; value: string; detail: string }) {
  return (
    <article class="metric-card">
      <span class="metric-label">{props.label}</span>
      <strong class="metric-value">{props.value}</strong>
      <small>{props.detail}</small>
    </article>
  );
}
