import type { RealtimeEvent } from "./models";

export type DashboardView = "overview" | "devices" | "detail" | "config";
export type RefreshTarget = "overview" | "devices" | "device" | "commands" | "config";

export interface DashboardEventRoute {
  refresh?: RefreshTarget;
  telemetry?: RealtimeEvent;
  systemMessage?: string;
}

export function routeDashboardEvent(
  event: RealtimeEvent,
  view: DashboardView,
  activeDeviceId: string,
): DashboardEventRoute {
  const route: DashboardEventRoute = {};
  if (event.type === "telemetry" && view === "detail" && event.device_id === activeDeviceId) {
    route.telemetry = event;
  }
  if (event.type === "system" && typeof event.data.message === "string") {
    route.systemMessage = event.data.message;
  }

  if (event.type === "presence") {
    if (view === "overview") route.refresh = "overview";
    else if (view === "devices") route.refresh = "devices";
    else if (view === "detail" && event.device_id === activeDeviceId) route.refresh = "device";
  } else if (event.type === "command") {
    if (view === "overview") route.refresh = "overview";
    else if (view === "detail" && event.device_id === activeDeviceId) route.refresh = "commands";
  } else if (event.type === "config" && view === "config") {
    route.refresh = "config";
  } else if (event.type === "system" && view === "overview") {
    route.refresh = "overview";
  }

  return route;
}

export function reconnectTarget(view: DashboardView): RefreshTarget | "detail" {
  return view === "detail" ? "detail" : view;
}
