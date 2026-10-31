import { createEffect, createSignal, type Accessor } from "solid-js";
import { ApiClient, ApiError, isAbortError } from "./api";
import type { ConfigResponse } from "./models";
import { useActions, useRequest } from "./useRequest";

interface RuntimeDraft {
  level: string;
  retentionDays: number;
  batchSize: number;
  flushIntervalMs: number;
}

export function useRuntimeConfig(api: ApiClient, refreshKey: Accessor<number>) {
  const config = useRequest(
    refreshKey,
    (signal) => api.get<ConfigResponse>("/api/v1/config", signal),
  );
  const actions = useActions();
  const [level, setLevel] = createSignal("info");
  const [retentionDays, setRetentionDays] = createSignal(30);
  const [batchSize, setBatchSize] = createSignal(500);
  const [flushInterval, setFlushInterval] = createSignal(1_000);
  const [draft, setDraft] = createSignal<RuntimeDraft>();
  const [draftRevision, setDraftRevision] = createSignal<number>();
  const [saving, setSaving] = createSignal(false);
  const [error, setError] = createSignal<unknown>();

  const values = (): RuntimeDraft => ({
    level: level(),
    retentionDays: retentionDays(),
    batchSize: batchSize(),
    flushIntervalMs: flushInterval(),
  });
  const hasChanges = () => {
    const original = draft();
    const current = values();
    return Boolean(original && (
      original.level !== current.level ||
      original.retentionDays !== current.retentionDays ||
      original.batchSize !== current.batchSize ||
      original.flushIntervalMs !== current.flushIntervalMs
    ));
  };
  const concurrentUpdate = () => {
    const current = config.state().data;
    const revision = draftRevision();
    return Boolean(hasChanges() && current && revision !== undefined && current.revision !== revision);
  };
  const applyConfig = (current: ConfigResponse) => {
    const next = {
      level: current.config.logging.level,
      retentionDays: current.config.telemetry.retention_days,
      batchSize: current.config.telemetry.batch_size,
      flushIntervalMs: current.config.telemetry.flush_interval_ms,
    };
    setLevel(next.level);
    setRetentionDays(next.retentionDays);
    setBatchSize(next.batchSize);
    setFlushInterval(next.flushIntervalMs);
    setDraft(next);
    setDraftRevision(current.revision);
    setError(undefined);
  };

  createEffect(() => {
    const current = config.state().data;
    const original = draft();
    if (current && (!original || (!hasChanges() && current.revision !== draftRevision()))) {
      applyConfig(current);
    }
  });

  const save = async (event: SubmitEvent) => {
    event.preventDefault();
    const current = config.state().data;
    const original = draft();
    const expectedRevision = draftRevision();
    if (!current || !original || expectedRevision === undefined || !hasChanges() || concurrentUpdate()) return;

    const next = values();
    const settings: {
      logging?: { level: string };
      telemetry?: { retention_days?: number; batch_size?: number; flush_interval_ms?: number };
    } = {};
    if (next.level !== original.level) settings.logging = { level: next.level };
    const telemetry: { retention_days?: number; batch_size?: number; flush_interval_ms?: number } = {};
    if (next.retentionDays !== original.retentionDays) telemetry.retention_days = next.retentionDays;
    if (next.batchSize !== original.batchSize) telemetry.batch_size = next.batchSize;
    if (next.flushIntervalMs !== original.flushIntervalMs) telemetry.flush_interval_ms = next.flushIntervalMs;
    if (Object.keys(telemetry).length) settings.telemetry = telemetry;

    setSaving(true);
    setError(undefined);
    try {
      const updated = await actions((signal) => api.patch<ConfigResponse>('/api/v1/config/runtime', {
        expected_revision: expectedRevision,
        settings,
      }, signal));
      setDraft(next);
      setDraftRevision(updated.revision);
      config.refresh();
    } catch (failure) {
      if (!isAbortError(failure)) {
        setError(failure);
        if (failure instanceof ApiError && (failure.status === 409 || failure.details !== undefined)) config.refresh();
      }
    } finally {
      setSaving(false);
    }
  };

  const discardChanges = () => {
    const current = config.state().data;
    if (current) applyConfig(current);
    else config.refresh();
  };

  return {
    config,
    level,
    setLevel,
    retentionDays,
    setRetentionDays,
    batchSize,
    setBatchSize,
    flushInterval,
    setFlushInterval,
    saving,
    error,
    hasChanges,
    concurrentUpdate,
    save,
    discardChanges,
  };
}
