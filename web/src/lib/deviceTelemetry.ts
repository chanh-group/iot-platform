import { createEffect, createMemo, createSignal, onCleanup, type Accessor } from "solid-js";
import { ApiClient, ApiError, isAbortError } from "./api";
import type { LatestValue, Page, RealtimeEvent, TelemetryRow } from "./models";
import { useRequest } from "./useRequest";
import type { LiveTelemetry } from "./chart";
import { mergeLatestValues } from "./latestTelemetry";

function localDateTime(value: number): string {
  const date = new Date(value);
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 23);
}

function timestampFromInput(value: string): number {
  return new Date(value).getTime();
}

const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1_000;
const HISTORY_PAGE_SIZE = 200;
const BACKFILL_MAX_PAGES = 5;

interface BackfillState {
  status: "idle" | "loading" | "fetched" | "partial" | "error";
  rows: TelemetryRow[];
  pages: number;
  from?: number;
  to?: number;
  clipped: boolean;
  nextCursor?: string;
  error?: unknown;
}

export function useDeviceTelemetry(
  api: ApiClient,
  deviceId: string,
  reconnectKey: Accessor<number>,
  events: Accessor<RealtimeEvent[]>,
) {
  const [from, setFrom] = createSignal(localDateTime(Date.now() - 24 * 60 * 60 * 1_000));
  const [to, setTo] = createSignal(localDateTime(Date.now()));
  const [key, setKey] = createSignal("");
  const [keyInitialized, setKeyInitialized] = createSignal(false);
  const [cursors, setCursors] = createSignal<string[]>([""]);
  const [backfill, setBackfill] = createSignal<BackfillState>({ status: "idle", rows: [], pages: 0, clipped: false });
  const cursor = () => cursors().at(-1) ?? "";

  const latest = useRequest(
    () => `${deviceId}:${reconnectKey()}`,
    (signal) => api.get<{ items: LatestValue[] }>(`/api/v1/devices/${deviceId}/telemetry/latest`, signal),
  );
  const websocketLive = createMemo<LiveTelemetry[]>(() => events()
    .filter((event) => event.type === "telemetry" && event.device_id === deviceId)
    .flatMap((event) => {
      const payload = event.data;
      if (typeof payload.event_id !== "string" || typeof payload.values !== "object" || payload.values === null) return [];
      const values: Record<string, string | number | boolean> = {};
      for (const [name, value] of Object.entries(payload.values)) {
        if (typeof value === "string" || typeof value === "boolean" || (typeof value === "number" && Number.isFinite(value))) {
          values[name] = value;
        }
      }
      return [{
        event_id: payload.event_id,
        ts: typeof payload.ts === "number" ? payload.ts : event.ts,
        received_at: typeof payload.received_at === "number" && Number.isFinite(payload.received_at)
          ? payload.received_at
          : undefined,
        values,
      }];
    }));
  const backfilledLive = createMemo<LiveTelemetry[]>(() => backfill().rows.map((row) => ({
    event_id: row.event_id,
    ts: row.ts,
    received_at: row.received_at,
    values: row.values,
  })));
  const live = createMemo<LiveTelemetry[]>(() => [...backfilledLive(), ...websocketLive()]);
  const latestMerge = createMemo(() => mergeLatestValues(latest.state().data?.items ?? [], live()));
  const latestValues = createMemo<LatestValue[]>(() => latestMerge().items);
  const checkedTieEvents = new Map<string, Set<string>>();
  createEffect(() => {
    const groups = latestMerge().ambiguous;
    let needsRefresh = false;
    for (const group of groups) {
      const signature = `${group.key}:${group.ts}:${group.receivedAt ?? "unknown"}`;
      const checkedEvents = checkedTieEvents.get(signature) ?? new Set<string>();
      for (const eventId of group.eventIds) {
        if (!checkedEvents.has(eventId)) needsRefresh = true;
        checkedEvents.add(eventId);
      }
      checkedTieEvents.set(signature, checkedEvents);
    }
    // Keep attempts through the loading state, when the REST data temporarily disappears.
    if (needsRefresh) latest.refresh();
  });

  let previousReconnectKey = reconnectKey();
  let backfillGeneration = 0;
  let backfillController: AbortController | undefined;

  const startBackfill = () => {
    const generation = ++backfillGeneration;
    backfillController?.abort();
    backfillController = undefined;
    const requestedFrom = timestampFromInput(from());
    const toMs = Date.now();
    if (!Number.isFinite(requestedFrom)) {
      setBackfill({ status: "error", rows: [], pages: 0, clipped: false, error: new ApiError("validation_error", "Choose a valid history start time before reconnect backfill.") });
      return;
    }
    if (requestedFrom >= toMs) {
      setBackfill({ status: "error", rows: [], pages: 0, from: requestedFrom, to: toMs, clipped: false, error: new ApiError("validation_error", "Reconnect backfill start time must be before now.") });
      return;
    }

    const fromMs = Math.max(requestedFrom, toMs - MAX_RANGE_MS);
    const clipped = fromMs > requestedFrom;
    const controller = new AbortController();
    backfillController = controller;
    setBackfill({ status: "loading", rows: [], pages: 0, from: fromMs, to: toMs, clipped });

    void (async () => {
      const rows: TelemetryRow[] = [];
      const seenCursors = new Set<string>();
      let cursor: string | undefined;
      let pages = 0;
      while (pages < BACKFILL_MAX_PAGES) {
        if (controller.signal.aborted || generation !== backfillGeneration) {
          return { rows, pages, nextCursor: cursor };
        }
        const query = new URLSearchParams({ from: String(fromMs), to: String(toMs), limit: String(HISTORY_PAGE_SIZE) });
        if (cursor) query.set("cursor", cursor);
        const page = await api.get<Page<TelemetryRow>>(`/api/v1/devices/${deviceId}/telemetry?${query}`, controller.signal);
        if (controller.signal.aborted || generation !== backfillGeneration) {
          return { rows, pages, nextCursor: cursor };
        }
        rows.push(...page.items);
        pages += 1;
        cursor = page.next_cursor ?? undefined;
        if (!cursor) break;
        if (seenCursors.has(cursor)) throw new ApiError("invalid_response", "Reconnect backfill returned a repeated cursor.");
        seenCursors.add(cursor);
      }
      return { rows, pages, nextCursor: cursor };
    })().then(
      (result) => {
        if (generation !== backfillGeneration) return;
        setBackfill({
          status: result.nextCursor || clipped ? "partial" : "fetched",
          rows: result.rows,
          pages: result.pages,
          from: fromMs,
          to: toMs,
          clipped,
          nextCursor: result.nextCursor,
        });
      },
      (error: unknown) => {
        if (generation === backfillGeneration && !isAbortError(error)) {
          setBackfill({ status: "error", rows: [], pages: 0, from: fromMs, to: toMs, clipped, error });
        }
      },
    );
  };

  createEffect(() => {
    const currentReconnectKey = reconnectKey();
    if (currentReconnectKey === previousReconnectKey) return;
    previousReconnectKey = currentReconnectKey;
    startBackfill();
  });
  onCleanup(() => {
    backfillGeneration += 1;
    backfillController?.abort();
  });
  const history = useRequest(
    () => `${deviceId}:${from()}:${to()}:${key()}:${cursor()}:${reconnectKey()}`,
    (signal): Promise<Page<TelemetryRow>> => {
      const fromMs = timestampFromInput(from());
      const toMs = timestampFromInput(to());
      if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs >= toMs || toMs - fromMs > MAX_RANGE_MS) {
        throw new ApiError("validation_error", "Choose a valid range of 31 days or less.");
      }
      const params = new URLSearchParams({
        from: String(fromMs),
        to: String(toMs),
        limit: String(HISTORY_PAGE_SIZE),
      });
      if (key()) params.set("keys", key());
      if (cursor()) params.set("cursor", cursor());
      return api.get<Page<TelemetryRow>>(`/api/v1/devices/${deviceId}/telemetry?${params}`, signal);
    },
  );
  const historyRows = createMemo(() => history.state().data?.items ?? []);
  const chartKey = createMemo(() => {
    const selected = key();
    if (!selected) return undefined;
    const hasHistory = historyRows().some((row) =>
      typeof row.values[selected] === "number" && Number.isFinite(row.values[selected])
    );
    const hasLive = live().some((event) =>
      typeof event.values[selected] === "number" && Number.isFinite(event.values[selected])
    );
    return hasHistory || hasLive ? selected : undefined;
  });

  createEffect(() => {
    const items = latestValues();
    if (items.length && !keyInitialized()) {
      setKey(items.find((item) => typeof item.value === "number")?.key ?? items[0].key);
      setKeyInitialized(true);
      if (cursors().length > 1) setCursors([""]);
    }
  });

  return {
    from,
    to,
    key,
    live,
    latest,
    latestValues,
    history,
    historyRows,
    chartKey,
    backfill,
    retryBackfill: startBackfill,
    pageNumber: () => cursors().length,
    canGoPrevious: () => cursors().length > 1,
    canGoNext: () => {
      const next = history.state().data?.next_cursor;
      return Boolean(next && !cursors().includes(next));
    },
    changeFrom: (value: string) => {
      setFrom(value);
      setCursors([""]);
    },
    changeTo: (value: string) => {
      setTo(value);
      setCursors([""]);
    },
    changeKey: (value: string) => {
      setKey(value);
      setKeyInitialized(true);
      setCursors([""]);
    },
    nextPage: () => {
      const next = history.state().data?.next_cursor;
      if (next && !cursors().includes(next)) setCursors((current) => [...current, next]);
    },
    previousPage: () => {
      if (cursors().length > 1) setCursors((current) => current.slice(0, -1));
    },
  };
}
