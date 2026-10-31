import { describe, expect, it } from "bun:test";
import { mergeLatestValues } from "../src/lib/latestTelemetry";
import type { LatestValue } from "../src/lib/models";
import type { LiveTelemetry } from "../src/lib/chart";

const rest: LatestValue = {
  key: "temperature",
  value: 20,
  ts: 100,
  received_at: 200,
  event_id: "rest-winner",
};

describe("latest telemetry ordering", () => {
  it("ignores an older same-ts event even when it arrives later", () => {
    const newerFirst: LiveTelemetry = {
      event_id: "received-later",
      ts: 100,
      received_at: 250,
      values: { temperature: 25 },
    };
    const olderArrivesLast: LiveTelemetry = {
      event_id: "received-earlier",
      ts: 100,
      received_at: 225,
      values: { temperature: 22 },
    };

    const result = mergeLatestValues([], [newerFirst, olderArrivesLast]);

    expect(result.items[0]?.event_id).toBe("received-later");
    expect(result.items[0]?.value).toBe(25);
    expect(result.ambiguous).toHaveLength(0);
  });

  it("uses received_at before arrival order when equal-timestamp events differ", () => {
    const older = { event_id: "older", ts: 100, received_at: 225, values: { temperature: 22 } };
    const newer = { event_id: "newer", ts: 100, received_at: 250, values: { temperature: 25 } };

    expect(mergeLatestValues([], [newer, older]).items[0]?.value).toBe(25);
    expect(mergeLatestValues([], [older, newer]).items[0]?.value).toBe(25);
  });

  it("requests authoritative REST ordering when internal_id is needed to break a tie", () => {
    const tied: LiveTelemetry = {
      event_id: "live-tie",
      ts: rest.ts,
      received_at: rest.received_at,
      values: { temperature: 99 },
    };

    const result = mergeLatestValues([rest], [tied]);

    expect(result.items[0]).toEqual(rest);
    expect(result.ambiguous).toEqual([{
      key: "temperature",
      ts: rest.ts,
      receivedAt: rest.received_at,
      eventIds: ["live-tie", "rest-winner"],
    }]);
  });
});
