import { describe, expect, it } from "bun:test";
import { plotData } from "../src/lib/chart";
import type { TelemetryRow } from "../src/lib/models";

describe("telemetry plotting", () => {
  it("uses Unix seconds, separates equal timestamps within their millisecond, and filters non-numeric values", () => {
    const rows: TelemetryRow[] = [
      { internal_id: 2, event_id: "b", ts: 1_700_000_000_000, received_at: 0, values: { temperature: 22 } },
      { internal_id: 1, event_id: "a", ts: 1_700_000_000_000, received_at: 0, values: { temperature: 21 } },
      { internal_id: 3, event_id: "c", ts: 1_700_000_001_000, received_at: 0, values: { temperature: "warm" } },
    ];

    const [timestamps, values] = plotData(rows, [], "temperature");
    expect(timestamps).toHaveLength(2);
    expect(timestamps[0]).toBeGreaterThan(1_700_000_000);
    expect(timestamps[1]).toBeGreaterThan(timestamps[0]);
    expect(timestamps[1]).toBeLessThan(1_700_000_000.001);
    expect(values).toEqual([21, 22]);
  });

  it("includes finite numeric live values and skips missing/non-finite values", () => {
    const live = [
      { event_id: "live-1", ts: 4_000, received_at: 4_100, values: { humidity: 65 } },
      { event_id: "live-2", ts: 5_000, received_at: 5_100, values: { humidity: Number.NaN } },
    ];
    expect(plotData([], live, "humidity")).toEqual([[4], [65]]);
  });

  it("keeps a REST copy once when the same event is also in the live buffer", () => {
    const history: TelemetryRow[] = [
      { internal_id: 17, event_id: "same-event", ts: 10_000, received_at: 10_100, values: { temperature: 22 } },
    ];
    const live = [
      { event_id: "same-event", ts: 10_000, received_at: 10_100, values: { temperature: 99 } },
      { event_id: "next-event", ts: 11_000, received_at: 11_100, values: { temperature: 23 } },
    ];

    expect(plotData(history, live, "temperature")).toEqual([[10, 11], [22, 23]]);
  });
});
