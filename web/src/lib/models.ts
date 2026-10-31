export interface AdminUser {
  id: string;
  username: string;
  expires_at: number;
}

export interface Device {
  id: string;
  name: string;
  enabled: boolean;
  online: boolean;
  metadata: Record<string, unknown>;
  created_at: number;
  updated_at: number;
  last_seen_at: number | null;
  credential: { id: string; created_at: number };
}

export interface Page<T> {
  items: T[];
  next_cursor: string | null;
}

export interface OverviewData {
  devices: { total: number; online: number; disabled: number };
  telemetry: { events_24h: number; last_received_at: number | null };
  commands: {
    pending: number;
    dispatched: number;
    failed_24h: number;
    expired_24h: number;
  };
  ingest: { queue_depth: number; queue_capacity: number; rejected_24h: number };
  storage_ok: boolean;
}

export interface Attribute {
  key: string;
  value: string | number | boolean;
  device_ts: number | null;
  updated_at: number;
}

export interface LatestValue {
  key: string;
  value: string | number | boolean;
  ts: number;
  received_at: number;
  event_id: string;
}

export interface TelemetryRow {
  internal_id: number;
  event_id: string;
  ts: number;
  received_at: number;
  values: Record<string, string | number | boolean>;
}

export type CommandStatus = "pending" | "dispatched" | "succeeded" | "failed" | "expired";

export interface Command {
  id: string;
  device_id: string;
  name: string;
  params: Record<string, unknown>;
  status: CommandStatus;
  created_at: number;
  expires_at: number;
  dispatched_at: number | null;
  completed_at: number | null;
  result: unknown;
  error: string | null;
  attempts: number;
}

export interface ConfigResponse {
  revision: number;
  config: {
    logging: { level: string };
    telemetry: { retention_days: number; batch_size: number; flush_interval_ms: number };
    [section: string]: unknown;
  };
  restart_required: string[];
}

export interface RealtimeEvent {
  id: string;
  type: string;
  ts: number;
  device_id?: string;
  data: Record<string, unknown>;
}
