import { createEffect, createMemo, createSignal, onCleanup, type Accessor } from "solid-js";
import { ApiClient, ApiError, isAbortError } from "./api";
import type { LatestValue, Page, RealtimeEvent, TelemetryRow } from "./models";
import { useRequest } from "./useRequest";
import type { LiveTelemetry } from "./chart";
import type { ReconnectCheckpoint } from "./ws";
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
const BACKFILL_MAX_ROWS = 100;

function keepRecentRows(rows: TelemetryRow[]): boolean {
  if (rows.length <= BACKFILL_MAX_ROWS) return false;
  rows.sort((left, right) => left.internal_id - right.internal_id);
  rows.splice(0, rows.length - BACKFILL_MAX_ROWS);
  return true;
}

type BackfillRequest =
  { afterId: number; throughId?: number; cursor?: string };

interface BackfillState {
  status: "idle" | "loading" | "fetched" | "partial" | "error";
  rows: TelemetryRow[];
  pages: number;
  afterId?: number;
  throughId?: number;
  truncated?: boolean;
  nextCursor?: string;
  error?: unknown;
}

interface TelemetryHistoryPage extends Page<TelemetryRow> {
  through_id?: number;
}

export function useDeviceTelemetry(
  api: ApiClient,
  deviceId: string,
  reconnectKey: Accessor<number>,
  events: Accessor<RealtimeEvent[]>,
  reconnectCheckpoint: Accessor<ReconnectCheckpoint | undefined> = () => undefined,
  onCheckpoint: (throughId: number) => void = () => {},
) {
  const [from, setFrom] = createSignal(localDateTime(Date.now() - 24 * 60 * 60 * 1_000));
  const [to, setTo] = createSignal(localDateTime(Date.now()));
  const [key, setKey] = createSignal("");
  const [keyInitialized, setKeyInitialized] = createSignal(false);
  const [cursors, setCursors] = createSignal<string[]>([""]);
  const [backfill, setBackfill] = createSignal<BackfillState>({ status: "idle", rows: [], pages: 0 });
  const cursor = () => cursors().at(-1) ?? "";

  const latest = useRequest(
    () => `${deviceId}:${reconnectKey()}`,
    (signal) => api.get<{ items: LatestValue[] }>(`/api/v1/devices/${deviceId}/telemetry/latest`, signal),
  );
  const live = (): LiveTelemetry[] => {
    const seen = new Set<string>();
    const backfilled = backfill().rows.map((row) => ({
      event_id: row.event_id,
      ts: row.ts,
      received_at: row.received_at,
      values: row.values,
    }));
    const websocket = events()
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
      });
    return [...backfilled, ...websocket].filter((event) => {
      if (seen.has(event.event_id)) return false;
      seen.add(event.event_id);
      return true;
    });
  };
  const latestMerge = createMemo(() => mergeLatestValues(latest.state().data?.items ?? [], live()));
  const latestValues = createMemo<LatestValue[]>(() => latestMerge().items);
  const checkedTieEvents = new Map<string, Set<string>>();
  createEffect(() => {
    const requestState = latest.state();
    // Refresh clears the REST data temporarily; retain tie checks until its response settles.
    if (requestState.loading) return;
    const groups = latestMerge().ambiguous;
    const activeSignatures = new Set(groups.map((group) => `${group.key}\0${group.ts}\0${group.receivedAt ?? "unknown"}`));
    for (const signature of checkedTieEvents.keys()) {
      if (!activeSignatures.has(signature)) checkedTieEvents.delete(signature);
    }
    if (!requestState.data) return;

    let needsRefresh = false;
    for (const group of groups) {
      const signature = `${group.key}\0${group.ts}\0${group.receivedAt ?? "unknown"}`;
      const checkedEvents = checkedTieEvents.get(signature) ?? new Set<string>();
      if (group.eventIds.some((eventId) => !checkedEvents.has(eventId))) needsRefresh = true;
      checkedTieEvents.set(signature, new Set(group.eventIds));
    }
    if (needsRefresh) latest.refresh();
  });

  let previousReconnectKey = reconnectKey();
  let backfillGeneration = 0;
  let backfillController: AbortController | undefined;
  let catchUpAfterCursor = false;

  const startBackfill = (request: BackfillRequest) => {
    const generation = ++backfillGeneration;
    backfillController?.abort();
    backfillController = undefined;
    const previous = backfill();
    if (!Number.isSafeInteger(request.afterId) || request.afterId < 0) {
      setBackfill({ ...previous, status: "error", error: new ApiError("validation_error", "Reconnect commit checkpoint is invalid.") });
      return;
    }
    const controller = new AbortController();
    backfillController = controller;
    setBackfill({
      ...previous,
      status: "loading",
      afterId: request.afterId,
      throughId: request.throughId,
      nextCursor: request.cursor,
      error: undefined,
    });

    void (async () => {
      const rows: TelemetryRow[] = [];
      const seenCursors = new Set<string>();
      let cursor = request.cursor;
      if (cursor) seenCursors.add(cursor);
      let throughId = request.throughId;
      let truncated = false;
      let pages = 0;
      while (pages < BACKFILL_MAX_PAGES) {
        if (controller.signal.aborted || generation !== backfillGeneration) {
          return { rows, pages, nextCursor: cursor, throughId, truncated };
        }
        const query = new URLSearchParams({ after_id: String(request.afterId), limit: String(HISTORY_PAGE_SIZE) });
        if (cursor) query.set("cursor", cursor);
        const page = await api.get<TelemetryHistoryPage>(`/api/v1/devices/${deviceId}/telemetry?${query}`, controller.signal);
        if (controller.signal.aborted || generation !== backfillGeneration) {
          return { rows, pages, nextCursor: cursor, throughId, truncated };
        }
        if (typeof page.through_id !== "number"
          || !Number.isSafeInteger(page.through_id)
          || page.through_id < request.afterId
          || (throughId !== undefined && throughId !== page.through_id)) {
          throw new ApiError("invalid_response", "Reconnect backfill returned an invalid committed checkpoint.");
        }
        throughId = page.through_id;
        rows.push(...page.items);
        const pageTruncated = keepRecentRows(rows);
        truncated = truncated || pageTruncated;
        pages += 1;
        cursor = page.next_cursor ?? undefined;
        if (!cursor) break;
        if (seenCursors.has(cursor)) throw new ApiError("invalid_response", "Reconnect backfill returned a repeated cursor.");
        seenCursors.add(cursor);
      }
      return { rows, pages, nextCursor: cursor, throughId, truncated };
    })().then(
      (result) => {
        if (generation !== backfillGeneration) return;
        backfillController = undefined;
        const mergedRows = new Map(
          backfill().rows.map((row) => [row.event_id, row]),
        );
        for (const row of result.rows) {
          mergedRows.set(row.event_id, row);
        }
        const retainedRows = [...mergedRows.values()];
        const mergedTruncated = keepRecentRows(retainedRows);
        const didTruncate = result.truncated || mergedTruncated;
        const complete = !result.nextCursor;
        setBackfill({
          status: result.nextCursor ? "partial" : "fetched",
          rows: retainedRows.sort((left, right) =>
            left.ts - right.ts || left.internal_id - right.internal_id,
          ),
          pages: previous.pages + result.pages,
          afterId: request.afterId,
          throughId: result.throughId,
          truncated: previous.truncated || didTruncate,
          nextCursor: result.nextCursor,
        });
        if (complete && result.throughId !== undefined) {
          if (catchUpAfterCursor) {
            catchUpAfterCursor = false;
            startBackfill({ afterId: result.throughId });
          } else {
            onCheckpoint(result.throughId);
          }
        }
      },
      (error: unknown) => {
        if (generation === backfillGeneration && !isAbortError(error)) {
          backfillController = undefined;
          setBackfill({ ...backfill(), status: "error", error });
        }
      },
    );
  };

  createEffect(() => {
    const currentReconnectKey = reconnectKey();
    if (currentReconnectKey === previousReconnectKey) return;
    previousReconnectKey = currentReconnectKey;
    const previous = backfill();
    if (backfillController) {
      catchUpAfterCursor = true;
      return;
    }
    if (previous.nextCursor && previous.afterId !== undefined) {
      catchUpAfterCursor = true;
      startBackfill({
        afterId: previous.afterId,
        throughId: previous.throughId,
        cursor: previous.nextCursor,
      });
    } else {
      startBackfill({ afterId: reconnectCheckpoint()?.afterId ?? 0 });
    }
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
  const chartLive = () => {
    const fromMs = timestampFromInput(from());
    const toMs = timestampFromInput(to());
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs >= toMs) return [];
    return live().filter((event) => event.ts >= fromMs && event.ts < toMs);
  };
  const chartKey = createMemo(() => {
    const selected = key();
    if (!selected) return undefined;
    const hasHistory = historyRows().some((row) =>
      typeof row.values[selected] === "number" && Number.isFinite(row.values[selected])
    );
    const hasLive = chartLive().some((event) =>
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
    chartLive,
    latest,
    latestValues,
    history,
    historyRows,
    chartKey,
    backfill,
    retryBackfill: () => {
      const previous = backfill();
      if (previous.nextCursor && previous.afterId !== undefined) {
        catchUpAfterCursor = true;
        startBackfill({
          afterId: previous.afterId,
          throughId: previous.throughId,
          cursor: previous.nextCursor,
        });
      } else {
        startBackfill({ afterId: previous.throughId ?? reconnectCheckpoint()?.afterId ?? 0 });
      }
    },
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
