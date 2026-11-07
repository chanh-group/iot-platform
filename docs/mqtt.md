# MQTT guide

The broker is embedded in `iot-server`; no external MQTT broker is required. The full wire contract is [02 — Contract kỹ thuật](plans/02-contracts.md) §2. This guide is the client/operator view.

## Connection

| Setting | Value |
| --- | --- |
| Protocol | MQTT 3.1.1, protocol level `4` |
| Listen | `[mqtt] listen`, default `127.0.0.1:1883` |
| `username` | Device ID (UUID v7) |
| `password` | Device token (`iot_...`) |
| `client_id` | Must equal the device ID |
| `clean_session` | `0` or `1`; offline session expires after `max_session_expiry_ms` (default 1 hour) |
| TLS | Not built in; terminate MQTT TLS at a proxy if traffic leaves the host |

The default listener is loopback-only. Plaintext MQTT is not safe on an untrusted network even when it runs inside Docker.

Connection failures deliberately do not reveal whether the device is unknown, disabled, or the token is wrong:

| Result | Meaning |
| --- | --- |
| CONNACK `0x04` | Bad username/password; also used for unknown, disabled, or revoked devices and for `client_id != username` |
| CONNACK `0x05` | MQTT 3.1 (protocol level 3) is not supported |
| CONNACK `0x87` | MQTT 5 (protocol level 5) is not supported |
| Connection refused | Server connection cap reached, or listener unavailable |

MQTT over WebSocket, `$share`, topic aliases, and MQTT 5 properties are not supported. `$SYS` is not available to devices.

## Topics and permissions

ACL is deny-by-default. A device can only use its own `devices/{self}/...` namespace:

| Topic | Device publish | Device subscribe | Stored | Notes |
| --- | --- | --- | --- | --- |
| `devices/{id}/telemetry` | Yes | No | Yes | Ingest; application ACK after commit |
| `devices/{id}/telemetry/ack` | No | Yes | No | Server publishes QoS 1, non-retained |
| `devices/{id}/attributes` | Yes | No | Latest only | No application ACK |
| `devices/{id}/commands` | No | Yes | No | Server publishes QoS 1, non-retained |
| `devices/{id}/commands/ack` | Yes | No | Command update | Execution result |
| `devices/{id}/status` | Yes | Yes | No | Retained/LWT signal; server presence is authoritative |
| `devices/{id}/data/#` | Yes | Yes | No | Free-form pub/sub; retained allowed within caps |

Publishing must use a concrete topic, without `+` or `#`. Subscriptions may use wildcards, but the resolved filter must stay inside `devices/{self}/`. Do not subscribe to `devices/{self}` or `devices/{self}/#`; they would expose server-only topics. Cross-device access, `$SYS`, and `$share` are rejected.

ACL denial behavior:

- Denied `PUBLISH`: the message is not routed and the broker closes the connection without a PUBACK.
- Denied `SUBSCRIBE`: SUBACK failure `0x80`; the connection stays open.
- Retained publish to `telemetry`, `attributes`, or a server-only topic is rejected; telemetry receives a `retained_not_allowed` application ACK only when its `event_id` is valid.
- Exceeding the retained-message or retained-byte cap closes the connection and increments `iot_retained_cap_rejected_total`.

## Telemetry

Publish to `devices/{id}/telemetry` with QoS 0 or 1 (QoS 1 recommended):

```json
{
  "event_id": "evt-019c2f7e-01",
  "ts": 1793149200000,
  "values": {
    "temperature": 27.5,
    "humidity": 65,
    "relay": true
  }
}
```

- `event_id` is required for every telemetry message, including QoS 0. It is 1–128 characters from `[A-Za-z0-9._:-]` and must be stable across retries.
- `ts` is optional. If absent, the server uses `received_at`. Timestamps outside the configured past/future skew are rejected.
- `values` is a flat object with 1–128 keys. Values are finite numbers, booleans, or strings up to 4 096 bytes; null, arrays, and nested objects are invalid.
- The payload is limited by `telemetry.max_payload_bytes` (default 65 536 bytes) and the MQTT packet limit.
- Deduplication is by `(device_id, event_id)` using a SHA-256 hash of the raw payload. Retries must send byte-identical payloads. The same ID with different bytes returns `event_id_conflict`.
- Deduplication only covers records still inside telemetry retention. After retention deletes the old row, the old ID can be accepted again.

The server publishes the application ACK to `devices/{id}/telemetry/ack`, QoS 1, non-retained:

```json
{
  "event_id": "evt-019c2f7e-01",
  "status": "stored",
  "received_at": 1793149200123,
  "code": null
}
```

| Status | Meaning | ACK after commit? |
| --- | --- | --- |
| `stored` | New event committed | Yes |
| `duplicate` | Same event ID and same payload hash already exist | Yes |
| `rejected` | Invalid event; `code` explains why | No write |
| `retryable` | Temporary failure; retry the same event | No write |

Common `code` values: `missing_event_id`, `invalid_event_id`, `invalid_payload`, `payload_too_large`, `invalid_timestamp`, `event_id_conflict`, `retained_not_allowed`, `unknown_device`, `queue_full`, and `storage_unavailable`. `queue_full` and `storage_unavailable` are retryable; retry with the same `event_id` and backoff.

The application ACK queue is bounded. If it fills, ACKs are dropped and `iot_telemetry_ack_dropped_total` increases; the device sees no ACK and must retry. A retry of an already committed event returns `duplicate`. If the device is not subscribed to the ACK topic, ACKs are lost; retry after reconnect. QoS 0 delivery still receives an ACK when the server can process it, but QoS 0 must not be treated as durable.

## Attributes

Publish to `devices/{id}/attributes` with QoS 0 or 1, non-retained:

```json
{
  "ts": 1793149200000,
  "values": {
    "firmware": "1.4.2",
    "interval_s": 30
  }
}
```

Only the latest value per key is stored; there is no history in the MVP. `ts` is optional and stored as the device timestamp. Invalid payloads are dropped with a metric; there is no application ACK topic for attributes, and a QoS 1 PUBACK is only a protocol acknowledgement.

## Commands

Create a command through the HTTP API, then subscribe to `devices/{id}/commands`. The server publishes QoS 1, non-retained:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "name": "reboot",
  "params": {"delay_s": 5},
  "created_at": 1793149200000,
  "expires_at": 1793149260000
}
```

Acknowledge on `devices/{id}/commands/ack`, QoS 0 or 1:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "status": "succeeded",
  "result": {"uptime_s": 42},
  "error": null
}
```

- `status` is `succeeded` or `failed`; `result` is any JSON value up to 16 384 bytes; `error` is at most 512 characters.
- Commands do not depend on the broker session queue. A command for an offline device stays `pending` in SQLite and is published after reconnect if it has not expired.
- Re-subscribing to the exact `devices/{id}/commands` topic wakes the dispatcher and can resend a command whose earlier publish raced the subscription. Deduplicate by `command_id`.
- Duplicate ACKs do not change a terminal state. An ACK after `expires_at` is stored for diagnostics, but the command remains `expired`.
- A device may have at most `commands.max_pending_per_device` commands in `pending` or `dispatched`; further creates return `409 command_queue_full`.

## Status and LWT

Publish a retained status and configure the same topic as the last will:

```json
{"status": "offline"}
```

`devices/{id}/status` accepts `online` or `offline` and an optional `ts`. It is an additional signal; actual presence is decided by the authenticated MQTT session. The server does not change presence because of this payload. The recommended LWT is a retained `{"status":"offline"}` on the device status topic so the dashboard can show the reported state.

## Free-form data

`devices/{id}/data/#` accepts arbitrary payloads within the packet limit, with QoS and retain chosen by the device. It is not stored in SQLite. Retained messages count toward the retained caps.

## Limits

| Limit | Default |
| --- | --- |
| Connections per server | 1 000 |
| Client ID length | 64 bytes |
| Topic levels | 16 |
| Subscriptions per client | 8 |
| Inflight messages per client | 32 |
| Queued messages per client | 256 |
| Session expiry after disconnect | 1 hour |
| Retained messages | 10 000 |
| Retained bytes | 16 MiB |
| MQTT packet size | 262 144 bytes |

These are configurable in `[mqtt]` in `conf.toml` (see [02 §5](plans/02-contracts.md)). Exceeding a limit can reject the connection, return SUBACK failure, or close the connection; it is not an application ACK and is not retried by the broker.

## Client examples

The commands below use `mosquitto_pub`/`mosquitto_sub` only as examples; the project does not require those tools.

```sh
export DEVICE_ID='019c2f7e-7a1b-7c3d-8e9f-0123456789ab'
export TOKEN='iot_...'

mosquitto_sub -h 127.0.0.1 -p 1883 \
  -i "$DEVICE_ID" -u "$DEVICE_ID" -P "$TOKEN" \
  -t "devices/$DEVICE_ID/telemetry/ack" -q 1

mosquitto_pub -h 127.0.0.1 -p 1883 \
  -i "$DEVICE_ID" -u "$DEVICE_ID" -P "$TOKEN" \
  -t "devices/$DEVICE_ID/telemetry" -q 1 \
  -m '{"event_id":"evt-001","values":{"temperature":27.5}}'
```

For a command client, subscribe to `devices/$DEVICE_ID/commands`, execute the command, then publish the ACK with the same `command_id`.
