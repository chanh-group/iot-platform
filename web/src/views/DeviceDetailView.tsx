import type { ApiClient } from "../lib/api";
import type { RealtimeEvent } from "../lib/models";
import type { ReconnectCheckpoint } from "../lib/ws";
import { AttributesPanel } from "./AttributesPanel";
import { CommandsPanel } from "./CommandsPanel";
import { DevicePanel } from "./DevicePanel";
import { TelemetryPanel } from "./TelemetryPanel";

export function DeviceDetailView(props: {
  api: ApiClient;
  deviceId: string;
  deviceRefreshKey: number;
  commandRefreshKey: number;
  reconnectKey: number;
  reconnectCheckpoint?: ReconnectCheckpoint;
  onTelemetryCheckpoint: (throughId: number) => void;
  liveEvents: RealtimeEvent[];
  onBack: () => void;
}) {
  return (
    <section aria-labelledby="device-title">
      <div class="heading-row">
        <div><h1 id="device-title" tabIndex={-1}>Device detail</h1><p class="mono">{props.deviceId}</p></div>
        <button class="button" onClick={props.onBack}>Back to devices</button>
      </div>
      <DevicePanel
        api={props.api}
        deviceId={props.deviceId}
        refreshKey={props.deviceRefreshKey}
        reconnectKey={props.reconnectKey}
      />
      <AttributesPanel api={props.api} deviceId={props.deviceId} reconnectKey={props.reconnectKey} />
      <TelemetryPanel
        api={props.api}
        deviceId={props.deviceId}
        reconnectKey={props.reconnectKey}
        reconnectCheckpoint={props.reconnectCheckpoint}
        onCheckpoint={props.onTelemetryCheckpoint}
        liveEvents={props.liveEvents}
      />
      <CommandsPanel
        api={props.api}
        deviceId={props.deviceId}
        refreshKey={props.commandRefreshKey}
        reconnectKey={props.reconnectKey}
      />
    </section>
  );
}
