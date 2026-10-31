import type { TelemetryRow } from "./models";

export interface LiveTelemetry {
  event_id: string;
  ts: number;
  received_at?: number;
  values: Record<string, string | number | boolean>;
}

export function plotData(
  history: TelemetryRow[],
  live: LiveTelemetry[],
  key: string,
): [number[], (number | null)[]] {
  const seen = new Set<string>();
  const uniqueHistory = history.filter((row) => {
    if (seen.has(row.event_id)) return false;
    seen.add(row.event_id);
    return true;
  });
  const uniqueLive = live.filter((row) => {
    if (seen.has(row.event_id)) return false;
    seen.add(row.event_id);
    return true;
  });
  const points = [
    ...uniqueHistory.map((row) => ({
      ts: row.ts,
      order: row.internal_id,
      value: row.values[key],
    })),
    ...uniqueLive.map((row, index) => ({
      ts: row.ts,
      order: Number.MAX_SAFE_INTEGER + index,
      value: row.values[key],
    })),
  ].filter((point) => typeof point.value === "number" && Number.isFinite(point.value));
  points.sort((left, right) => left.ts - right.ts || left.order - right.order);
  const timestamps: number[] = [];
  const values: number[] = [];
  for (let start = 0; start < points.length;) {
    let end = start + 1;
    while (end < points.length && points[end].ts === points[start].ts) end += 1;
    const count = end - start;
    for (let index = start; index < end; index += 1) {
      // uPlot requires unique x values; keep same-millisecond samples within that millisecond.
      const offset = count > 1 ? ((index - start + 1) / (count + 1)) / 1_000 : 0;
      timestamps.push(points[index].ts / 1_000 + offset);
      values.push(points[index].value as number);
    }
    start = end;
  }
  return [timestamps, values];
}
