import { describe, expect, it } from "bun:test";
import { createRoot, createSignal } from "solid-js";
import type { ApiClient } from "../src/lib/api";
import { plotData } from "../src/lib/chart";
import type { RealtimeEvent, TelemetryRow } from "../src/lib/models";
import { useDeviceTelemetry } from "../src/lib/deviceTelemetry";

describe("device telemetry backfill lifecycle", () => {
  it("backfills old device timestamps from a committed-ID checkpoint without a time cap", async () => {
    const paths: string[] = [];
    const now = Date.now();
    const afterId = 100_000_000;
    const lateTs = now - 300_000;
    let advancedCheckpoint: number | undefined;
    const api = {
      get<T>(path: string): Promise<T> {
        paths.push(path);
        if (path.endsWith("/telemetry/latest")) return Promise.resolve({ items: [] } as T);
        if (new URL(path, "http://test").searchParams.has("after_id")) {
          return Promise.resolve({
            items: [{
              internal_id: afterId + 1,
              event_id: "late-device-ts",
              ts: lateTs,
              received_at: now,
              values: { temperature: 19 },
            }],
            next_cursor: null,
            through_id: afterId + 1,
          } as T);
        }
        return Promise.resolve({ items: [], next_cursor: null } as T);
      },
    } as unknown as ApiClient;
    let telemetry!: ReturnType<typeof useDeviceTelemetry>;
    const [checkpoint] = createSignal({ afterId });
    const dispose = createRoot((disposeRoot) => {
      telemetry = useDeviceTelemetry(
        api,
        "device-id",
        () => 1,
        () => [],
        checkpoint,
        (throughId) => { advancedCheckpoint = throughId; },
      );
      return disposeRoot;
    });

    telemetry.retryBackfill();
    await Bun.sleep(0);

    const backfillPath = paths.find((path) => {
      const query = new URL(path, "http://test").searchParams;
      return query.get("after_id") === String(afterId)
        && !query.has("from")
        && !query.has("to");
    });
    expect(backfillPath).toBeDefined();
    expect(telemetry.backfill()).toMatchObject({
      status: "fetched",
      afterId,
      throughId: afterId + 1,
      rows: [{ event_id: "late-device-ts", ts: lateTs }],
    });
    expect(lateTs).toBeLessThan(now - 60_000);
    expect(advancedCheckpoint).toBe(afterId + 1);
    dispose();
  });

  it("bounds retained backfill rows across cursor continuations and reconnects", async () => {
    const now = Date.now();
    const firstThrough = 1_001;
    const secondThrough = 1_002;
    const requests: URLSearchParams[] = [];
    const row = (event_id: string, internal_id: number): TelemetryRow => ({
      internal_id,
      event_id,
      ts: now - 60_000 + internal_id % 10_000,
      received_at: now - 1_000,
      values: { temperature: internal_id },
    });
    const liveEvent: RealtimeEvent = {
      id: "ws-live-before-reconnect",
      type: "telemetry",
      ts: now - 1_000,
      device_id: "device-id",
      data: {
        event_id: "live-before-reconnect",
        ts: now - 1_000,
        received_at: now - 1_000,
        values: { temperature: 1 },
      },
    };
    const [liveEvents, setLiveEvents] = createSignal<RealtimeEvent[]>([liveEvent]);
    const [reconnectKey] = createSignal(0);
    const [checkpoint, setCheckpoint] = createSignal({ afterId: 0 });
    const api = {
      get<T>(path: string): Promise<T> {
        if (path.endsWith("/telemetry/latest")) return Promise.resolve({ items: [] } as T);
        const query = new URL(path, "http://test").searchParams;
        if (!query.has("after_id")) return Promise.resolve({ items: [], next_cursor: null } as T);
        requests.push(query);
        if (query.has("cursor")) {
          const pageNumber = Number(query.get("cursor")?.replace("page-", ""));
          if (pageNumber < 6) {
            const firstId = (pageNumber - 1) * 200 + 1;
            return Promise.resolve({
              items: Array.from({ length: 200 }, (_, index) => {
                const id = firstId + index;
                return row(id === 1 ? "live-before-reconnect" : `stored-${id}`, id);
              }),
              next_cursor: `page-${pageNumber + 1}`,
              through_id: firstThrough,
            } as T);
          }
          return Promise.resolve({
            items: [row("page-six-final", 1_001)],
            next_cursor: null,
            through_id: firstThrough,
          } as T);
        }
        const afterId = Number(query.get("after_id"));
        if (afterId === 0) {
          const firstId = 1;
          const items = Array.from({ length: 200 }, (_, index) => {
            const id = firstId + index;
            return row(id === 1 ? "live-before-reconnect" : `stored-${id}`, id);
          });
          return Promise.resolve({
            items,
            next_cursor: "page-2",
            through_id: firstThrough,
          } as T);
        }
        if (afterId >= secondThrough) {
          const items = Array.from({ length: 150 }, (_, index) =>
            row(`round-${afterId}-${index}`, afterId + index + 1),
          );
          return Promise.resolve({
            items,
            next_cursor: null,
            through_id: afterId + items.length,
          } as T);
        }
        return Promise.resolve({
          items: [row("second-disconnect-event", 1_002)],
          next_cursor: null,
          through_id: secondThrough,
        } as T);
      },
    } as unknown as ApiClient;
    let telemetry!: ReturnType<typeof useDeviceTelemetry>;
    const dispose = createRoot((disposeRoot) => {
      telemetry = useDeviceTelemetry(
        api,
        "device-id",
        reconnectKey,
        liveEvents,
        checkpoint,
        (throughId) => setCheckpoint({ afterId: throughId }),
      );
      return disposeRoot;
    });

    await Bun.sleep(0);
    telemetry.retryBackfill();
    await Bun.sleep(0);
    expect(telemetry.backfill().status).toBe("partial");
    expect(telemetry.backfill().nextCursor).toBeDefined();
    setLiveEvents([liveEvent]);
    telemetry.retryBackfill();
    await Bun.sleep(0);
    expect(telemetry.backfill().status).toBe("fetched");
    expect(requests.some((query) => query.get("cursor") === "page-6")).toBe(true);
    expect(requests.some((query) => query.get("after_id") === String(firstThrough))).toBe(true);
    expect(telemetry.backfill().rows).toHaveLength(100);
    expect(telemetry.backfill().truncated).toBe(true);
    expect(telemetry.backfill().rows.some((item) => item.event_id === "live-before-reconnect")).toBe(false);
    expect(telemetry.backfill().rows.some((item) => item.event_id === "page-six-final")).toBe(true);
    expect(telemetry.backfill().rows.some((item) => item.event_id === "second-disconnect-event")).toBe(true);
    expect(checkpoint().afterId).toBe(secondThrough);
    expect(telemetry.live()).toHaveLength(101);
    expect(telemetry.chartLive()).toHaveLength(101);
    const [, plottedValues] = plotData([], telemetry.chartLive(), "temperature");
    expect(plottedValues).toHaveLength(101);

    for (let round = 0; round < 3; round += 1) {
      const before = checkpoint().afterId;
      telemetry.retryBackfill();
      await Bun.sleep(0);
      expect(telemetry.backfill().rows).toHaveLength(100);
      expect(telemetry.backfill().truncated).toBe(true);
      expect(checkpoint().afterId).toBe(before + 150);
    }
    dispose();
  });

});
