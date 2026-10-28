CREATE TABLE users (
    id            TEXT PRIMARY KEY,
    username      TEXT NOT NULL COLLATE NOCASE UNIQUE,
    password_hash TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
) STRICT;

CREATE TABLE sessions (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash   BLOB NOT NULL UNIQUE CHECK (length(token_hash) = 32),
    created_at   INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,
    revoked_at   INTEGER,
    remote_ip    TEXT,
    user_agent   TEXT,
    CHECK (expires_at > created_at)
) STRICT;

CREATE INDEX sessions_user_idx ON sessions(user_id);
CREATE INDEX sessions_expiry_idx ON sessions(expires_at);

CREATE TABLE devices (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL COLLATE NOCASE UNIQUE,
    metadata_json TEXT NOT NULL DEFAULT '{}'
                  CHECK (json_valid(metadata_json) AND json_type(metadata_json) = 'object'),
    enabled       INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    last_seen_at  INTEGER
) STRICT;

CREATE INDEX devices_created_idx ON devices(created_at DESC, id DESC);
CREATE INDEX devices_enabled_idx ON devices(enabled);

CREATE TABLE device_credentials (
    id           TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    token_hash   BLOB NOT NULL UNIQUE CHECK (length(token_hash) = 32),
    status       TEXT NOT NULL CHECK (status IN ('active', 'revoked')),
    created_at   INTEGER NOT NULL,
    revoked_at   INTEGER,
    rotated_from TEXT REFERENCES device_credentials(id)
) STRICT;

CREATE UNIQUE INDEX device_credentials_active_uq
    ON device_credentials(device_id) WHERE status = 'active';
CREATE INDEX device_credentials_device_idx ON device_credentials(device_id);

CREATE TABLE telemetry_events (
    internal_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id      TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    event_id       TEXT NOT NULL CHECK (length(event_id) BETWEEN 1 AND 128),
    ts             INTEGER NOT NULL CHECK (ts > 0),
    received_at    INTEGER NOT NULL CHECK (received_at > 0),
    values_json    TEXT NOT NULL
                   CHECK (json_valid(values_json) AND json_type(values_json) = 'object'),
    payload_sha256 BLOB NOT NULL CHECK (length(payload_sha256) = 32),
    UNIQUE (device_id, event_id)
) STRICT;

CREATE INDEX telemetry_device_ts_idx
    ON telemetry_events(device_id, ts, internal_id);
CREATE INDEX telemetry_received_idx ON telemetry_events(received_at);

CREATE TABLE device_attributes (
    device_id  TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    key        TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 64),
    value_json TEXT NOT NULL
               CHECK (json_valid(value_json)
                      AND json_type(value_json) IN ('true', 'false', 'integer', 'real', 'text')),
    device_ts  INTEGER,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (device_id, key)
) STRICT;

CREATE TABLE commands (
    id              TEXT PRIMARY KEY,
    device_id       TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    params_json     TEXT NOT NULL
                    CHECK (json_valid(params_json) AND json_type(params_json) = 'object'),
    status          TEXT NOT NULL
                    CHECK (status IN ('pending', 'dispatched', 'succeeded', 'failed', 'expired')),
    created_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL,
    next_attempt_at INTEGER NOT NULL,
    dispatched_at   INTEGER,
    completed_at    INTEGER,
    result_json     TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    error           TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_attempt_at INTEGER,
    idempotency_key TEXT,
    request_sha256  BLOB CHECK (request_sha256 IS NULL OR length(request_sha256) = 32),
    late_ack_at     INTEGER,
    late_ack_json   TEXT CHECK (late_ack_json IS NULL OR json_valid(late_ack_json)),
    CHECK (expires_at > created_at),
    CHECK ((idempotency_key IS NULL) = (request_sha256 IS NULL)),
    UNIQUE (device_id, idempotency_key)
) STRICT;

CREATE INDEX commands_device_created_idx
    ON commands(device_id, created_at DESC, id DESC);
CREATE INDEX commands_due_idx
    ON commands(status, next_attempt_at, expires_at);
CREATE INDEX commands_status_expiry_idx ON commands(status, expires_at);

CREATE TABLE audit_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at INTEGER NOT NULL,
    actor_type  TEXT NOT NULL CHECK (actor_type IN ('admin', 'device', 'system')),
    actor_id    TEXT,
    action      TEXT NOT NULL,
    target_type TEXT,
    target_id   TEXT,
    request_id  TEXT,
    remote_ip   TEXT,
    details_json TEXT
                 CHECK (details_json IS NULL
                        OR (json_valid(details_json) AND json_type(details_json) = 'object'))
) STRICT;

CREATE INDEX audit_occurred_idx ON audit_events(occurred_at DESC, id DESC);
CREATE INDEX audit_target_idx ON audit_events(target_type, target_id, occurred_at DESC);
