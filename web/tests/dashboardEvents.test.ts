import { describe, expect, it } from "bun:test";
import { reconnectTarget, routeDashboardEvent } from "../src/lib/dashboardEvents";
import type { RealtimeEvent } from "../src/lib/models";

function event(type: string, deviceId?: string, data: Record<string, unknown> = {}): RealtimeEvent {
  return { id: `${type}-event`, type, ts: 1, device_id: deviceId, data };
}

describe("dashboard WebSocket event routing", () => {
  it("refreshes only the active feature and active detail device", () => {
    expect(routeDashboardEvent(event("presence", "device-b"), "detail", "device-a").refresh).toBeUndefined();
    expect(routeDashboardEvent(event("presence", "device-a"), "detail", "device-a").refresh).toBe("device");
    expect(routeDashboardEvent(event("command", "device-b"), "detail", "device-a").refresh).toBeUndefined();
    expect(routeDashboardEvent(event("command", "device-a"), "detail", "device-a").refresh).toBe("commands");
    expect(routeDashboardEvent(event("config"), "devices", "").refresh).toBeUndefined();
    expect(routeDashboardEvent(event("config"), "config", "").refresh).toBe("config");
  });

  it("keeps realtime telemetry scoped to the selected device and system messages separate", () => {
    expect(routeDashboardEvent(event("telemetry", "device-b"), "detail", "device-a").telemetry).toBeUndefined();
    expect(routeDashboardEvent(event("telemetry", "device-a"), "detail", "device-a").telemetry?.device_id).toBe("device-a");
    expect(routeDashboardEvent(event("system", undefined, { message: "storage" }), "detail", "device-a")).toEqual({ systemMessage: "storage" });
    expect(routeDashboardEvent(event("system", undefined, { message: "storage" }), "overview", "")).toEqual({
      refresh: "overview",
      systemMessage: "storage",
    });
  });

  it("refetches only the active view after reconnect", () => {
    expect(reconnectTarget("overview")).toBe("overview");
    expect(reconnectTarget("devices")).toBe("devices");
    expect(reconnectTarget("detail")).toBe("detail");
    expect(reconnectTarget("config")).toBe("config");
  });
});
