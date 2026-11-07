# HTTP API

Base path: `/api/v1`. This is an operator/developer quick reference; the full contract, including every field and validation rule, is [02 — Contract kỹ thuật](plans/02-contracts.md) §4. The server does not accept telemetry over HTTP; devices publish over MQTT.

## Authentication and CSRF

Admin sessions use the `iot_session` cookie. `POST /api/v1/auth/login` with `{"username":"admin","password":"..."}` sets the cookie; `POST /api/v1/auth/logout` revokes it. `GET /api/v1/auth/me` returns the current admin and expiry.

Every authenticated mutation (`POST`, `PATCH`, `DELETE`) also requires:

- `Content-Type: application/json`;
- `X-IoT-CSRF: 1`;
- if an `Origin` header is present, its host must match the request `Host`.

Use a cookie jar for the examples:

```sh
curl -sS -c cookies.txt -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"..."}' \
  http://127.0.0.1:8080/api/v1/auth/login

curl -sS -b cookies.txt -H 'Content-Type: application/json' \
  -H 'X-IoT-CSRF: 1' \
  -d '{"name":"sensor-01"}' \
  http://127.0.0.1:8080/api/v1/devices
```

A wrong username/password returns `401 invalid_credentials`; repeated failures return `429 rate_limited` with `Retry-After`. Device tokens cannot be used for the admin API. If `cookie_secure = true`, the cookie is only sent over HTTPS.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process liveness; no database check, no auth |
| `GET` | `/readyz` | Readiness of database, MQTT, ingest and command dispatcher; no auth |
| `POST` | `/api/v1/auth/login` | Create an admin session |
| `POST` | `/api/v1/auth/logout` | Revoke the current session |
| `GET` | `/api/v1/auth/me` | Current admin and session expiry |
| `GET` | `/api/v1/devices` | List devices (`limit`, `cursor`, `q`, `enabled`) |
| `POST` | `/api/v1/devices` | Create a device; returns its token once |
| `GET` | `/api/v1/devices/{id}` | Read one device |
| `PATCH` | `/api/v1/devices/{id}` | Rename, replace metadata, enable/disable |
| `POST` | `/api/v1/devices/{id}/credentials/rotate` | Revoke the old token and return a new one |
| `GET` | `/api/v1/devices/{id}/telemetry` | Telemetry history by `from`/`to` or reconnect checkpoint `after_id` |
| `GET` | `/api/v1/devices/{id}/telemetry/latest` | Latest value per key |
| `GET` | `/api/v1/devices/{id}/attributes` | Latest device attributes |
| `POST` | `/api/v1/devices/{id}/commands` | Queue a command |
| `GET` | `/api/v1/devices/{id}/commands` | List commands (`limit`, `cursor`, `status`) |
| `GET` | `/api/v1/commands/{id}` | Read one command and its result |
| `GET` | `/api/v1/config` | Effective, redacted configuration |
| `PATCH` | `/api/v1/config/runtime` | Update hot-reloadable settings |
| `GET` | `/api/v1/overview` | Dashboard counters |
| `GET` | `/metrics` | Prometheus metrics; disabled by default, Bearer token only |
| `GET` | `/api/v1/events` | WebSocket upgrade |
| `GET` | `/` and assets | Embedded dashboard when `http.dashboard = true` |

Lists use opaque cursors, not offsets. Devices/commands default to `limit=50` (max 200); telemetry defaults to 100 (max 1 000); attributes default to 50 (max 200). Telemetry `from`/`to` must be an increasing range of at most 31 days. For reconnect backfill, use `after_id` instead of `from`/`to` and advance only to the returned `through_id`.

## Device credentials

`POST /api/v1/devices` and `POST /api/v1/devices/{id}/credentials/rotate` return the device token exactly once:

```json
{"device": {"id": "019c...", "name": "sensor-01", "enabled": true},
 "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}}
```

Rotation returns `device_id` instead of the full `device` object:

```json
{"device_id": "019c...",
 "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}}
```

The token is `iot_` plus 43 base64url characters. Store it in the device's secret store; the database keeps only a SHA-256 hash and the API never shows it again. Rotation commits the new credential and revokes the old one in one transaction, then kicks the connected session. If the response is lost after commit, rotate again to obtain a new known token; the previous token is already invalid.

## Commands

Create a command with `name`, optional `params`, `ttl_ms`, and optional `idempotency_key`:

```json
{
  "name": "reboot",
  "params": {"delay_s": 5},
  "ttl_ms": 60000,
  "idempotency_key": "retry-2026-10-28-01"
}
```

The response is `202` with `command_id`, `status: "pending"`, and `Location: /api/v1/commands/{id}`. Reusing an idempotency key with the same body returns the existing command with `Idempotent-Replayed: true`; a different body returns `409 idempotency_conflict`. A disabled device returns `409 device_disabled`; a full per-device queue returns `409 command_queue_full`. Command status is one of `pending`, `dispatched`, `succeeded`, `failed`, `expired`.

## Runtime configuration

`PATCH /api/v1/config/runtime` accepts `expected_revision` and a `settings` object. Only these fields are hot-reloadable:

- `logging.level`
- `telemetry.retention_days`
- `telemetry.batch_size`
- `telemetry.flush_interval_ms`

A stale revision returns `409 revision_conflict`. A valid field that needs a restart returns `422 restart_required` and does not write the file. Invalid values return `400 validation_error` and also do not write. The response is the same redacted shape as `GET /api/v1/config` with the new revision. Changing `runtime.toml` on disk is only read at startup.

## WebSocket

Connect to `GET /api/v1/events` with the session cookie and a matching `Origin`. The server sends:

```json
{"type": "hello", "server_time": 1793149200000, "max_subscriptions": 8}
```

Client messages (maximum 4 096 bytes):

```json
{"action": "subscribe", "channels": ["overview", "device:019c..."], "request_id": 17}
{"action": "unsubscribe", "channels": ["device:019c..."]}
{"action": "ping"}
```

Channels are `overview` and `device:{uuid}`, at most 8 per connection. `subscribe` may include an integer `request_id`; the matching `subscribed` message contains the channels accepted for that request. `ping` returns `pong`. Events are invalidations, not a durable stream: telemetry is sent only after commit, commands carry `state_version`, and clients refetch REST resources after reconnect. The server pings every 30 seconds and closes a connection that does not respond for 60 seconds (`1001`). A slow consumer is closed with `1013 slow_consumer`; reconnect and backfill with `after_id`/`through_id`. Logout or session expiry closes WebSocket connections with `4401`.

Event types are `telemetry`, `command`, `presence`, `reported_status`, `config`, and `system`. The envelope is:

```json
{"type": "telemetry", "id": "019c...", "ts": 1793149200123, "device_id": "019c...", "data": {}}
```

## Errors

Errors use one envelope and always return `X-Request-ID`:

```json
{
  "code": "validation_error",
  "message": "invalid field",
  "request_id": "019c...",
  "details": [{"field": "name", "reason": "must not be empty"}]
}
```

Common codes: `validation_error`, `invalid_json`, `invalid_cursor`, `unauthenticated`, `invalid_credentials`, `csrf_failed`, `not_found`, `device_not_found`, `command_not_found`, `method_not_allowed`, `device_name_conflict`, `idempotency_conflict`, `revision_conflict`, `device_disabled`, `command_queue_full`, `payload_too_large`, `unsupported_media_type`, `restart_required`, `rate_limited`, `internal_error`, `config_write_failed`, `not_ready`, `request_capacity_exceeded`, and `storage_unavailable`. The complete status/code table is in [02 §4.10](plans/02-contracts.md).

## Metrics

`/metrics` returns `404` unless `http.metrics = true` and `http.metrics_bearer_token_env` names an environment variable containing a token of at least 32 bytes. It accepts only `Authorization: Bearer <token>`, never the admin cookie. Do not expose it without a reverse proxy, mTLS, or a network policy that limits access to the monitoring system. The exported metric names and semantics are listed in [02 §4.7](plans/02-contracts.md).
