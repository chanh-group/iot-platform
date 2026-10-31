import { Show, createEffect, createSignal, onCleanup, onMount } from "solid-js";
import { ApiClient } from "./lib/api";
import type { AdminUser, RealtimeEvent } from "./lib/models";
import { routeDashboardEvent, reconnectTarget, type DashboardView } from "./lib/dashboardEvents";
import { useActions } from "./lib/useRequest";
import { EventsSocket } from "./lib/ws";
import { TokenDialog } from "./common";
import { ConfigView } from "./views/ConfigView";
import { DeviceDetailView } from "./views/DeviceDetailView";
import { DeviceListView } from "./views/DeviceListView";
import { OverviewView } from "./views/OverviewView";

export function Dashboard(props: {
  api: ApiClient;
  user: AdminUser;
  notice: string;
  onLogout: () => Promise<void>;
  onSessionExpired: () => void;
}) {
  const [view, setView] = createSignal<DashboardView>("overview");
  const [deviceId, setDeviceId] = createSignal("");
  const [overviewRefreshKey, setOverviewRefreshKey] = createSignal(0);
  const [deviceListRefreshKey, setDeviceListRefreshKey] = createSignal(0);
  const [deviceRefreshKey, setDeviceRefreshKey] = createSignal(0);
  const [commandRefreshKey, setCommandRefreshKey] = createSignal(0);
  const [configRefreshKey, setConfigRefreshKey] = createSignal(0);
  const [reconnectKey, setReconnectKey] = createSignal(0);
  const [eventsConnected, setEventsConnected] = createSignal(false);
  const [liveEvents, setLiveEvents] = createSignal<RealtimeEvent[]>([]);
  const [issuedToken, setIssuedToken] = createSignal<{ title: string; token: string }>();
  const [systemNotice, setSystemNotice] = createSignal("");
  const actions = useActions();
  let socket!: EventsSocket;

  const channels = () => view() === "detail" && deviceId()
    ? ["overview", `device:${deviceId()}`]
    : ["overview"];

  socket = new EventsSocket({
    channels: [],
    onEvent: (event) => {
      const route = routeDashboardEvent(event, view(), deviceId());
      const telemetry = route.telemetry;
      if (telemetry) setLiveEvents((events) => [...events, telemetry].slice(-1_000));
      if (route.systemMessage) setSystemNotice(route.systemMessage);
      switch (route.refresh) {
        case "overview": setOverviewRefreshKey((value) => value + 1); break;
        case "devices": setDeviceListRefreshKey((value) => value + 1); break;
        case "device": setDeviceRefreshKey((value) => value + 1); break;
        case "commands": setCommandRefreshKey((value) => value + 1); break;
        case "config": setConfigRefreshKey((value) => value + 1); break;
        case undefined: break;
      }
    },
    onReconnect: () => {
      setLiveEvents([]);
      switch (reconnectTarget(view())) {
        case "overview": setOverviewRefreshKey((value) => value + 1); break;
        case "devices": setDeviceListRefreshKey((value) => value + 1); break;
        case "detail": setReconnectKey((value) => value + 1); break;
        case "config": setConfigRefreshKey((value) => value + 1); break;
      }
    },
    onDisconnect: () => void actions((signal) => props.api.get<AdminUser>("/api/v1/auth/me", signal)).catch(() => undefined),
    onExpired: props.onSessionExpired,
    onStatus: setEventsConnected,
  });
  createEffect(() => socket.setChannels(channels()));
  createEffect(() => {
    if (view() !== "detail") setLiveEvents([]);
  });
  createEffect(() => {
    if (view() !== "overview") return;
    const timer = setInterval(() => setOverviewRefreshKey((value) => value + 1), 15_000);
    onCleanup(() => clearInterval(timer));
  });
  onMount(() => socket.start());
  onCleanup(() => {
    socket.stop();
    setIssuedToken(undefined);
  });

  const openDevice = (id: string) => {
    setDeviceId(id);
    setLiveEvents([]);
    setView("detail");
  };

  return (
    <div class="app-shell">
      <header class="topbar">
        <div class="brand"><span class="brand-mark" aria-hidden="true">I</span> IoT Server</div>
        <div class="topbar-actions">
          <span class="user-name">{props.user.username}</span>
          <span class={`connection ${eventsConnected() ? "is-connected" : "is-disconnected"}`} role="status">
            {eventsConnected() ? "Live updates connected" : "Live updates reconnecting"}
          </span>
          <button class="button button-quiet" onClick={() => void props.onLogout()}>Log out</button>
        </div>
      </header>
      <main class="workspace">
        <nav class="view-nav" aria-label="Dashboard views">
          <NavButton label="Overview" selected={view() === "overview"} onClick={() => setView("overview")} />
          <NavButton label="Devices" selected={view() === "devices"} onClick={() => setView("devices")} />
          <Show when={view() === "detail"}>
            <NavButton label="Device detail" selected onClick={() => setView("detail")} />
          </Show>
          <NavButton label="Config" selected={view() === "config"} onClick={() => setView("config")} />
        </nav>
        <Show when={props.notice}><div class="notice notice-warning" role="status">{props.notice}</div></Show>
        <Show when={systemNotice()}><div class="notice notice-warning" role="status">{systemNotice()}</div></Show>
        <Show when={view() === "overview"}>
          <OverviewView api={props.api} refreshKey={overviewRefreshKey()} onDevices={() => setView("devices")} onConfig={() => setView("config")} />
        </Show>
        <Show when={view() === "devices"}>
          <DeviceListView
            api={props.api}
            refreshKey={deviceListRefreshKey()}
            onOpen={openDevice}
            onIssued={(title, token) => setIssuedToken({ title, token })}
          />
        </Show>
        <Show when={view() === "detail" && deviceId()} keyed>
          {(id) => (
            <DeviceDetailView
              api={props.api}
              deviceId={id}
              deviceRefreshKey={deviceRefreshKey()}
              commandRefreshKey={commandRefreshKey()}
              reconnectKey={reconnectKey()}
              liveEvents={liveEvents()}
              onBack={() => setView("devices")}
            />
          )}
        </Show>
        <Show when={view() === "config"}>
          <ConfigView api={props.api} refreshKey={configRefreshKey()} />
        </Show>
        <Show when={issuedToken()}>
          {(issued) => <TokenDialog title={issued().title} token={issued().token} onClose={() => setIssuedToken(undefined)} />}
        </Show>
      </main>
    </div>
  );
}

function NavButton(props: { label: string; selected: boolean; onClick: () => void }) {
  return (
    <button class="nav-button" aria-current={props.selected ? "page" : undefined} onClick={props.onClick}>
      {props.label}
    </button>
  );
}
