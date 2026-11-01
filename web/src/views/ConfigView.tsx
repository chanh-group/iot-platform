import { Show } from "solid-js";
import { ApiClient, ApiError } from "../lib/api";
import { useRuntimeConfig } from "../lib/runtimeConfig";
import { ErrorMessage } from "../common";

export function ConfigView(props: { api: ApiClient; refreshKey: number }) {
  const config = useRuntimeConfig(props.api, () => props.refreshKey);

  return (
    <section aria-labelledby="config-title">
      <div class="heading-row">
        <div><h1 id="config-title" tabIndex={-1}>Runtime configuration</h1><p>Only hot-reloadable fields can be changed here.</p></div>
        <button class="button" onClick={config.config.refresh}>Refresh</button>
      </div>
      <Show when={config.error()}>
        <ErrorMessage error={config.error()} />
        <Show when={config.error() instanceof ApiError && (config.error() as ApiError).code === "restart_required"}>
          <p class="notice notice-warning">Some requested fields require a server restart and were not applied.</p>
        </Show>
        <Show when={config.error() instanceof ApiError && (config.error() as ApiError).details !== undefined}>
          <pre class="json-view">{JSON.stringify((config.error() as ApiError).details, null, 2)}</pre>
        </Show>
      </Show>
      <Show when={config.concurrentUpdate()}>
        <div class="notice notice-warning" role="alert">
          Configuration changed on the server since these edits started. Your draft was not submitted.
          <button class="button" type="button" onClick={config.discardChanges}>Discard edits and load current revision</button>
        </div>
      </Show>
      <Show when={config.config.state().loading}><p class="loading" role="status">Loading effective configuration…</p></Show>
      <Show when={config.config.state().error}><ErrorMessage error={config.config.state().error} /></Show>
      <Show when={config.config.state().data}>
        {(current) => <>
          <div class="panel">
            <h2>Hot-reloadable settings</h2>
            <p>Current revision: <strong>{current().revision}</strong></p>
            <form onSubmit={config.save}>
              <div class="form-grid">
                <div class="field"><label for="logging-level">Log level</label><select id="logging-level" value={config.level()} onChange={(event) => config.setLevel(event.currentTarget.value)}>
                  <option value="trace">Trace</option><option value="debug">Debug</option><option value="info">Info</option><option value="warn">Warn</option><option value="error">Error</option>
                </select></div>
                <div class="field"><label for="retention-days">Telemetry retention (days)</label><input id="retention-days" type="number" min={1} value={config.retentionDays()} onInput={(event) => config.setRetentionDays(Number(event.currentTarget.value))} /></div>
                <div class="field"><label for="batch-size">Telemetry batch size</label><input id="batch-size" type="number" min={1} value={config.batchSize()} onInput={(event) => config.setBatchSize(Number(event.currentTarget.value))} /></div>
                <div class="field"><label for="flush-interval">Flush interval (ms)</label><input id="flush-interval" type="number" min={1} value={config.flushInterval()} onInput={(event) => config.setFlushInterval(Number(event.currentTarget.value))} /></div>
              </div>
              <button class="button button-primary" type="submit" disabled={config.saving() || config.config.state().loading || !config.hasChanges() || config.concurrentUpdate()}>
                {config.saving() ? "Saving…" : "Save runtime settings"}
              </button>
            </form>
          </div>
          <div class="panel">
            <h2>Effective configuration (secrets redacted by server)</h2>
            <pre class="json-view">{JSON.stringify(current().config, null, 2)}</pre>
          </div>
        </>}
      </Show>
    </section>
  );
}
