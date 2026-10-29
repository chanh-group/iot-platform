# 02 — Contract kỹ thuật

- Trạng thái: **CHỐT cho MVP**. Mọi thay đổi breaking phải sửa tài liệu này trước khi sửa code.
- Phạm vi: MQTT, SQLite, HTTP/WebSocket, `conf.toml`/`runtime.toml`, CLI, mã lỗi và test vector.
- Quy ước: thời gian là Unix milliseconds UTC (`i64`); ID là UUID v7 chữ thường có gạch nối; JSON dùng UTF-8, không có comment/trailing comma.

## 1. Quy ước chung

### 1.1. Giới hạn nội dung

| Đối tượng | Giới hạn |
| --- | --- |
| HTTP body | `http.max_body_bytes`, mặc định 262 144 byte |
| HTTP request timeout | `http.request_timeout_ms`, mặc định 30 000 ms |
| Telemetry payload | `telemetry.max_payload_bytes`, mặc định 65 536 byte |
| Telemetry values | tối đa `telemetry.max_keys` = 128 key; string tối đa 4 096 byte |
| Command params | 16 384 byte JSON object |
| Device metadata | 8 192 byte JSON object, depth ≤ 8 |
| WebSocket message từ client | 4 096 byte |
| MQTT packet | `mqtt.max_packet_bytes`, mặc định 262 144 byte |
| MQTT topic | tối đa 16 level; không giới hạn byte riêng ngoài packet |

Chuỗi vượt giới hạn bị từ chối, không cắt ngầm. JSON number phải hữu hạn; `NaN`/`Infinity` không hợp lệ.

### 1.2. Lỗi HTTP

Mọi lỗi JSON có cùng envelope, không có stack trace:

```json
{
  "code": "validation_error",
  "message": "trường không hợp lệ",
  "request_id": "019c2f7e-...",
  "details": [{"field": "name", "reason": "must not be empty"}]
}
```

- `details` là object hoặc array tùy mã lỗi, có thể vắng.
- `request_id` lấy từ header `X-Request-ID` nếu khớp `^[A-Za-z0-9._-]{8,64}$`; nếu không, server sinh UUID v7. Luôn trả lại header `X-Request-ID`.
- Không trả secret, SQL, đường dẫn tuyệt đối của DB, hay lỗi nội bộ thô.

### 1.3. Auth và CSRF

- Admin dùng cookie `iot_session`; token 32 byte base64url, DB chỉ lưu SHA-256.
- Cookie: `HttpOnly; SameSite=Strict; Path=/; Max-Age=<session_ttl_ms/1000>`; thêm `Secure` khi `[http] cookie_secure = true`.
- Mutation (POST/PATCH/DELETE) yêu cầu:
  - `Content-Type: application/json`;
  - header `X-IoT-CSRF: 1`;
  - nếu có `Origin`, host của Origin phải khớp header `Host`.
- Không bật CORS. Request không có cookie hợp lệ trả `401 unauthenticated`.
- WebSocket kiểm tra cookie và `Origin` host khớp `Host` trước khi upgrade.
- Device token **không** dùng được cho HTTP admin.

## 2. Contract MQTT

### 2.1. Protocol và kết nối

- Chỉ chấp nhận **MQTT 3.1.1**, protocol level `4`.
- Protocol level `3` (MQTT 3.1): RMQTT trả CONNACK `0x05`; client phải coi là bị từ chối protocol. Đây là hành vi đã xác nhận của broker, không tự map sang `0x01`.
- Protocol level `5` (MQTT 5): RMQTT trả CONNACK `0x87`; client phải coi là bị từ chối protocol. Không tự map sang `0x84`.
- `allow_anonymous` chỉ được `false`. Cấu hình `true` làm startup fail.
- `username` = `device_id`; `password` = device token; `client_id` = `device_id`.
- `client_id` khác `username`: RMQTT trả CONNACK `0x04`; không phân biệt với credential sai ra bên ngoài.
- Unknown device, device disabled, credential revoked, sai token: CONNACK `0x04` (bad username/password), không phân biệt lý do.
- Token đúng nhưng device vừa bị disable/rotate: từ chối; kết nối đang mở bị kick.
- `clean_session` 0 và 1 đều được phép. Session offline tối đa `max_session_expiry_ms` (mặc định 1 giờ), sau đó xóa.
- Không hỗ trợ MQTT over WebSocket, `$share`, topic alias, MQTT 5 properties.
- `$SYS/#`: từ chối subscribe/publish từ thiết bị.

### 2.2. Danh tính và token

- Device ID là UUID v7 canonical; topic và username phải khớp chính xác.
- Token format: `iot_` + 43 ký tự base64url (32 byte ngẫu nhiên), tổng 47 ký tự.
- Token chỉ hiển thị một lần khi tạo/rotate. DB lưu `SHA-256(token)` dạng BLOB 32 byte.
- Mỗi device có đúng một credential `active`; rotate tạo credential mới và revoke credential cũ trong cùng transaction.
- Không tin device ID trong payload. ID luôn lấy từ principal + topic; nếu payload có `device_id` thì bỏ qua.

### 2.3. Topic

| Topic | Device publish | Device subscribe | Lưu SQL | Ghi chú |
| --- | --- | --- | --- | --- |
| `devices/{id}/telemetry` | Có | Không | Có | Ingest, ACK sau commit |
| `devices/{id}/telemetry/ack` | Không | Có | Không | Server publish QoS 1, non-retained |
| `devices/{id}/attributes` | Có | Không | Có (latest) | Không có ACK ứng dụng |
| `devices/{id}/commands` | Không | Có | Không | Server publish QoS 1, non-retained |
| `devices/{id}/commands/ack` | Có | Không | Có (cập nhật command) | Kết quả thực thi |
| `devices/{id}/status` | Có | Có | Không | Retained/LWT là tín hiệu, không quyết định online |
| `devices/{id}/data/#` | Có | Có | Không | Pub/sub thường, retained được phép |

Quy tắc topic:

- `{id}` phải là UUID v7 canonical của chính principal.
- Publish phải là topic cụ thể, không chứa `+` hoặc `#`.
- Subscribe được dùng wildcard, nhưng filter đã resolve phải nằm trong prefix `devices/{self}/`.
- `devices/{self}/data/#` cho phép topic con tùy ý, tối đa 16 level; không được rỗng.
- Không cho subscribe `devices/{self}` hoặc `devices/{self}/#` (mở rộng sang telemetry/commands là sai).
- Không cross-device, không `$SYS`, không `$share`.

### 2.4. ACL

Mặc định deny. Bảng dưới là toàn bộ quyền của một device:

| Hành động | Topic/filter được phép |
| --- | --- |
| PUBLISH | `devices/{self}/telemetry` |
| PUBLISH | `devices/{self}/attributes` |
| PUBLISH | `devices/{self}/commands/ack` |
| PUBLISH | `devices/{self}/status` |
| PUBLISH | `devices/{self}/data/{...}` (không wildcard) |
| SUBSCRIBE | `devices/{self}/telemetry/ack` |
| SUBSCRIBE | `devices/{self}/commands` |
| SUBSCRIBE | `devices/{self}/status` |
| SUBSCRIBE | `devices/{self}/data/#` hoặc filter con của nó |

- ACL kiểm tra tại PUBLISH và SUBSCRIBE. RMQTT 0.24 không có hook từ chối lúc giao message (`MessageDelivered` chỉ thay payload), nên quyền đọc được chốt bằng filter đã duyệt: mọi filter được allow đều nằm trong `devices/{self}/`, wildcard không thể match topic của thiết bị khác.
- Publish vào topic server-only (`telemetry/ack`, `commands`) bị từ chối.
- Publish retained vào `telemetry` hoặc `attributes`: message **bị từ chối ở broker ACL trước retained storage**, không ingest/xử lý; với telemetry có `event_id` hợp lệ, server gửi ACK `rejected/retained_not_allowed`. Publish retained vào topic server-only (`telemetry/ack`, `commands`, `commands/ack`) bị ACL từ chối.
- Publish ACL deny: message không được route, RMQTT đóng connection và không gửi PUBACK; không ghi telemetry.
- Subscribe ACL deny: RMQTT trả SUBACK failure `0x80`, giữ connection; không message nào của filter bị giao.
- RMQTT gọi `MessagePublish` **trước** `MessagePublishCheckAcl`. `MessagePublish` chỉ parse/log tối thiểu, không enqueue/ghi SQL; ingest chỉ được thực hiện sau khi `MessagePublishCheckAcl` cho phép.
- Không có API cấp quyền cross-device trong MVP.

### 2.5. Retained, session và tài nguyên

| Giới hạn | Giá trị mặc định | Hành vi khi vượt |
| --- | --- | --- |
| Retained messages | 10 000 | Hook ghi metric/log rồi RMQTT đóng connection; không có PUBACK |
| Retained bytes | 16 MiB | Hook ghi metric/log rồi RMQTT đóng connection; không có PUBACK |
| Session expiry sau disconnect | 1 giờ | Session bị xóa; client phải clean session/reconnect |
| Subscriptions/client | 8 | SUBACK failure cho subscription vượt |
| Inflight/client | 32 | Broker backpressure theo QoS |
| Queued messages/client | 256 | Drop message mới theo policy broker; metric |
| Connections toàn server | 1 000 | Từ chối connection mới |
| Client ID length | 64 | Từ chối connect |
| Topic levels | 16 | Từ chối publish/subscribe |

- Retained chỉ hữu ích cho `status` và `data/#`; retained trên topic nghiệp vụ bị xử lý như §2.4.
- Retained của cùng topic thay thế bản cũ; payload rỗng xóa retained.
- Vượt retained cap là broker-level disconnect, không phải application ACK/retry response. Hook product phải increment metric có reason `retained_cap` và log có topic/size đã redact payload trước khi trả reject.
- Product dùng `BudgetedRetainStorage` thay `DefaultRetainStorage` (no-op): kiểm tra cap và cập nhật counter trước khi set; replace trừ byte cũ, payload rỗng xóa topic và trừ toàn bộ byte; thao tác phải serialize để tránh race.
- Retained/session không bền qua process restart. Sau restart, mọi device coi là offline đến khi reconnect.
- Session offline vẫn có thể nhận message QoS 1 trong giới hạn queue; command không dựa vào queue này (xem §2.8).

### 2.6. Telemetry

Publish:

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

Quy tắc:

| Trường | Quy tắc |
| --- | --- |
| `event_id` | Bắt buộc; 1..128 ký tự `[A-Za-z0-9._:-]`; giữ nguyên khi retry |
| `ts` | Tùy chọn; Unix ms; thiếu thì dùng `received_at`; không được lệch quá khứ `max_past_skew_ms` hoặc tương lai `max_future_skew_ms` |
| `values` | Bắt buộc; object phẳng; 1..128 key |
| key | 1..64 ký tự `[A-Za-z0-9_.:-]`, không bắt đầu bằng `$` |
| value | number hữu hạn, boolean, hoặc string ≤ 4 096 byte; **không** null/object/array |
| payload | JSON object, UTF-8, không trailing data; ≤ `max_payload_bytes` |

- `event_id` bắt buộc cho mọi telemetry trong MVP, kể cả QoS 0.
- `received_at` do server ghi; client không gửi được.
- Dedup theo `(device_id, event_id)`; hash `SHA-256` của **raw payload bytes** (sau khi broker nhận). Retry phải gửi byte-identical; nếu cùng ID nhưng hash khác → `event_id_conflict`.
- Dedup chỉ còn hiệu lực khi bản ghi còn trong retention. Bản ghi đã bị retention xóa thì ID cũ có thể được chấp nhận lại.

ACK topic `devices/{id}/telemetry/ack`, QoS 1, non-retained:

```json
{
  "event_id": "evt-019c2f7e-01",
  "status": "stored",
  "received_at": 1793149200123,
  "code": null
}
```

| `status` | Ý nghĩa | ACK sau commit? |
| --- | --- | --- |
| `stored` | Đã insert mới | Có |
| `duplicate` | Đã có bản ghi cùng ID + cùng hash | Có |
| `rejected` | Dữ liệu sai; `code` cho biết lý do | Không (không ghi) |
| `retryable` | Tạm thời không ghi được; `code` là `queue_full`/`storage_unavailable` | Không |

- `stored`/`duplicate`: chỉ publish ACK sau khi transaction commit.
- `rejected`: publish ngay sau validate; không ghi SQL.
- Nếu validation không có `event_id` hợp lệ để echo, ACK `rejected` dùng `event_id: null`; không echo chuỗi event ID sai định dạng.
- Application ACK dùng hàng đợi nội bộ bounded 256; khi đầy, drop ACK + increment `telemetry_ack_dropped_total`, không tạo task mới. Device retry; dedup trả `duplicate` nếu event đã commit.
- `retryable`: publish khi queue đầy hoặc DB lỗi; node retry cùng `event_id` với exponential backoff + jitter.
- Nếu device không subscribe ACK, ACK bị mất; node retry và nhận `duplicate`.
- QoS 0 vẫn nhận ACK nếu server xử lý được, nhưng node không được coi QoS 0 là bền vững.

Mã `code` cho telemetry:

| Code | Status | Khi nào |
| --- | --- | --- |
| `missing_event_id` | rejected | Thiếu/rỗng `event_id` |
| `invalid_event_id` | rejected | Sai ký tự/độ dài |
| `invalid_payload` | rejected | JSON sai, values sai kiểu, quá key/string |
| `payload_too_large` | rejected | Vượt `max_payload_bytes` |
| `invalid_timestamp` | rejected | `ts` sai kiểu hoặc lệch cửa sổ cho phép |
| `event_id_conflict` | rejected | Cùng ID, khác hash |
| `retained_not_allowed` | rejected | Publish retained vào topic telemetry |
| `unknown_device` | rejected | Không resolve được device (thường đã bị auth chặn) |
| `queue_full` | retryable | Ingest queue đầy |
| `storage_unavailable` | retryable | DB lỗi/timeout/disk full |

### 2.7. Attributes

Topic `devices/{id}/attributes`, QoS 0/1, không retained:

```json
{
  "ts": 1793149200000,
  "values": {
    "firmware": "1.4.2",
    "interval_s": 30
  }
}
```

- `values` cùng quy tắc key/value như telemetry; tối đa 128 key.
- Payload ≤ `telemetry.max_payload_bytes`; vượt → drop + metric.
- `ts` tùy chọn, lưu thành `device_ts`; `updated_at` do server ghi.
- Chỉ lưu **giá trị mới nhất** theo `(device_id, key)`; không có history trong MVP.
- Không có topic ACK; lỗi payload bị drop + metric. QoS 1 chỉ xác nhận giao thức.

### 2.8. Command

Server publish `devices/{id}/commands`, QoS 1, non-retained:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "name": "reboot",
  "params": {"delay_s": 5},
  "created_at": 1793149200000,
  "expires_at": 1793149260000
}
```

Device ACK `devices/{id}/commands/ack`, QoS 0/1:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "status": "succeeded",
  "result": {"uptime_s": 42},
  "error": null
}
```

- `status` chỉ `succeeded` hoặc `failed`.
- `result` là JSON bất kỳ ≤ 16 384 byte; `error` ≤ 512 ký tự; vượt giới hạn → ACK bị bỏ qua + log.
- Server kiểm tra `device_id` của principal khớp command; ACK lạc device bị bỏ qua + log.
- ACK trùng không đổi terminal state.
- ACK sau `expires_at` được lưu vào `late_ack_at`/`late_ack_json` để chẩn đoán; command vẫn `expired`.
- Command ID do server sinh (UUID v7); node dedup theo `command_id`.
- Lệnh không retained; không gửi lại lệnh đã hết hạn.
- Device offline: command `pending` trong SQL; khi reconnect và còn hạn, dispatcher publish. Không phụ thuộc session queue của broker.
- Tối đa `commands.max_pending_per_device` command ở trạng thái `pending`/`dispatched`; vượt → HTTP `409 command_queue_full`.

### 2.9. Status và data

`devices/{id}/status`, retained được phép:

```json
{"status": "online", "ts": 1793149200000}
```

- `status` chỉ `online`/`offline`; `ts` tùy chọn.
- Đây là tín hiệu bổ sung (LWT). Online thực tế do session đã auth quyết định.
- Server không đổi presence vì payload này; có thể broadcast `reported_status` cho dashboard.
- LWT nên đặt retained `{"status":"offline"}`.

`devices/{id}/data/#`: payload tùy ý ≤ packet limit; không lưu SQL; QoS/retain theo broker. Retained phải nằm trong cap §2.5.

### 2.10. Server principal và giao nhận

- Server publish command/ACK bằng `FromType::System` với id `$server`; không phải MQTT login, không qua ACL device.
- Command/ACK dùng QoS 1 để có ít nhất một lần giao trong session; node phải dedup.
- Thứ tự publish trên cùng topic/session do broker bảo đảm theo QoS; application không cam kết ordering giữa các device.
- Khi rotate/disable: commit SQL → kick connection. Nếu kick lỗi, retry có giới hạn, metric + readiness false; token cũ đã không thể reconnect.
- Shutdown: dừng accept, kick toàn bộ session đang mở, chờ ngắn rồi abort listener. `abort` một mình chỉ nhả port, **không** đóng session; phải kick trước.
- Duplicate `client_id`: broker đóng connection cũ trước khi nhận connection mới; presence dùng generation để sự kiện disconnect cũ không ghi đè trạng thái mới.

## 3. Contract SQLite

### 3.1. Kết nối và pragma

Mỗi connection trong pool chạy:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA wal_autocheckpoint = 1000;
```

- `synchronous=FULL` bắt buộc cho cả Phase 0 và product MVP: application ACK `stored` chỉ được gửi sau commit, nên commit phải sống qua power loss/hard reboot. `NORMAL` có thể rollback transaction đã commit (WAL chưa fsync), chỉ được xem xét sau này như chế độ performance riêng kèm ADR hạ cam kết durability.

- Pool mặc định 5 connection; telemetry chỉ ghi qua một worker.
- Database file phải nằm trong `server.data_dir`; nếu không, startup fail. Điều này cho phép một lock duy nhất bảo vệ DB + runtime file.
- Không dùng `:memory:` trong production. Test dùng file tạm.
- Migration chạy trước khi mở HTTP/MQTT listener; lỗi migration → exit code 4.
- SQLx quản lý bảng `_sqlx_migrations`; đây là `schema_migrations` logic của SPEC. Không tạo bảng trùng.
- Không tự downgrade schema. Migration mới chỉ thêm/sửa theo hướng tương thích; migration phá hủy phải có ghi chú backup.
- `synchronous=FULL` là bắt buộc: chỉ trả application ACK `stored` sau commit bền vững qua application crash và power loss. Không cung cấp cấu hình hạ xuống `NORMAL` trong MVP.
- SQLite file được tạo trước khi migration chạy. Nếu migration lỗi, application không tự xóa file rỗng/partial: operator đọc lỗi, restore nếu có dữ liệu, hoặc chỉ xóa file khi đã xác nhận nó chưa từng chứa dữ liệu cần giữ rồi chạy lại `migrate`.

### 3.2. DDL — `migrations/sqlite/0001_init.sql`

```sql
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
```

Không có bảng `schema_migrations` riêng, không có bảng projection latest, không có bảng lịch sử attribute/command.

### 3.3. Bất biến và transaction

| Nghiệp vụ | Transaction/bất biến |
| --- | --- |
| Tạo device | Insert `devices` + `device_credentials(active)` + audit trong một transaction; trả token sau commit |
| Rotate credential | `BEGIN IMMEDIATE`; revoke active cũ; insert active mới; audit; commit; sau đó kick connection |
| Disable device | Update `enabled=0` + audit; commit; kick; token vẫn bị auth từ chối |
| Telemetry | Batch `BEGIN IMMEDIATE`; insert từng event; unique conflict → đọc hash cũ; commit; ACK sau commit |
| Attribute | Upsert từng key trong một transaction ngắn; không history |
| Tạo command | Insert command + audit trong một transaction; kiểm tra queue limit trong cùng transaction |
| Dispatch | Publish broker; update `dispatched_at`, `attempts`, `next_attempt_at` chỉ khi command còn `pending`/`dispatched` |
| ACK command | Update chỉ khi status `pending`/`dispatched`; terminal không đổi; ACK muộn ghi `late_ack_*` |
| Config update | Ghi file tạm + fsync + rename; chỉ swap RAM sau khi ghi thành công; audit sau swap (hoặc trong transaction audit riêng) |

- `BEGIN IMMEDIATE` chỉ dùng cho telemetry batch và các mutation cần khóa ghi ngay; transaction phải ngắn, không chờ network/broker trong transaction.
- Không giữ transaction khi publish MQTT/HTTP response.
- `internal_id` là cursor ổn định; không dùng `ts` đơn lẻ làm khóa.
- `event_id` unique theo device; `payload_sha256` là SHA-256 raw payload.
- `commands.next_attempt_at` cho phép dispatcher query due mà không scan toàn bảng.
- `idempotency_key` unique theo device; NULL được phép lặp.

### 3.4. Retention và cleanup

| Dữ liệu | Chính sách MVP |
| --- | --- |
| `telemetry_events` | Xóa theo `received_at < now - retention_days`; batch 5 000; chạy mỗi giờ; nghỉ giữa batch |
| `sessions` | Xóa session hết hạn hoặc revoked quá 7 ngày; mỗi giờ |
| `commands` | Giữ vô thời hạn (kể cả expired) |
| `audit_events` | Giữ vô thời hạn |
| `device_attributes` | Giữ latest vô thời hạn |
| `device_credentials` | Giữ credential revoked để audit; không xóa |

- Retention không chạy trong transaction dài; mỗi batch một transaction.
- Xóa telemetry không tự `VACUUM`; dung lượng file có thể không giảm. Tài liệu vận hành ghi rõ backup/`VACUUM` offline.
- Khi disk full/DB lỗi: worker dừng ACK thành công, retry backoff, readiness false.
- `last_seen_at` chỉ ghi khi connect/disconnect (không ghi mỗi telemetry) để giảm write amplification.

### 3.5. Truy vấn chuẩn

Telemetry history:

```sql
SELECT internal_id, event_id, ts, received_at, values_json
FROM telemetry_events
WHERE device_id = ?
  AND (ts, internal_id) > (?, ?)
  AND ts >= ? AND ts < ?
ORDER BY ts, internal_id
LIMIT ?;
```

- `from`/`to` là `[from, to)`; cursor `(ts, internal_id)`; limit 1..1000.
- Filter key dùng `json_extract(values_json, '$."' || ? || '"') IS NOT NULL`; key đã validate nên không có quote.
- Latest theo key: subquery `ORDER BY ts DESC, received_at DESC, internal_id DESC LIMIT 1` cho từng key; chấp nhận chậm hơn khi dữ liệu lớn, chỉ thêm projection sau benchmark.
- Danh sách command: `ORDER BY created_at DESC, id DESC` với cursor tương tự.

### 3.6. Audit

| Action | Actor | Target | Details (đã redact) |
| --- | --- | --- | --- |
| `admin.create` | system | user | `username` |
| `auth.login` | admin | user | `username` |
| `auth.logout` | admin | session | không có |
| `device.create` | admin | device | `name`, `enabled` |
| `device.update` | admin | device | danh sách field đổi; `enabled` from/to |
| `device.enable` / `device.disable` | admin | device | `enabled` |
| `credential.rotate` | admin | device | `old_credential_id`, `new_credential_id` |
| `command.create` | admin | command | `name`, `expires_at` |
| `config.update` | admin | config | `old_revision`, `new_revision`, danh sách path đổi |

- Không ghi token, password hash, params command, metadata values, payload telemetry.
- `request_id`, `remote_ip`, `occurred_at` lấy từ request.
- Device/credential/command mutation: audit trong cùng transaction với mutation; audit lỗi → rollback mutation.
- Riêng `config.update`: file đã rename + RAM đã swap; audit lỗi chỉ log + metric, không rollback file (tránh file/RAM lệch).
- Login thất bại: chỉ log có rate limit, không ghi audit (tránh flood).
- MQTT connect/disconnect/telemetry không ghi audit; chỉ metric/log.

## 4. Contract HTTP API

### 4.1. Quy ước

- Base path: `/api/v1`. Mọi response JSON trừ static assets/health text/metrics.
- Không có HTTP telemetry ingestion trong MVP; thiết bị gửi qua MQTT.
- Thời gian: integer Unix ms.
- `POST` tạo mới trả `201` nếu tạo tài nguyên đồng bộ; `202` cho command.
- Danh sách dùng cursor opaque base64url của JSON `{"v":1,...}`; cursor sai → `400 invalid_cursor`.
- `limit` mặc định 50, tối đa 200 cho devices/commands; telemetry mặc định 100, tối đa 1 000.
- Không hỗ trợ `offset`.
- Response lỗi theo §1.2; `Content-Type: application/json; charset=utf-8`.
- Request body tối đa `http.max_body_bytes`; vượt → `413 payload_too_large`.
- `Content-Type` sai → `415 unsupported_media_type`.
- Endpoint không tồn tại dưới `/api` → `404 not_found`; method sai → `405 method_not_allowed`.

### 4.2. Auth

#### `POST /api/v1/auth/login`

```json
{"username": "admin", "password": "..."}
```

- 200:

```json
{"user": {"id": "019c...", "username": "admin"}, "expires_at": 1793235600000}
```

- Set-Cookie: `iot_session=<token>; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400[; Secure]`.
- Sai user/password/disabled: `401 invalid_credentials` (cùng message).
- Rate limit: `auth.login_max_attempts` lần thất bại trong `auth.login_window_ms` theo cặp IP + username → `429 rate_limited`, header `Retry-After` (giây).
- Không audit password; audit login success (không ghi token).

#### `POST /api/v1/auth/logout`

- 204, thu hồi session hiện tại, xóa cookie, đóng các WebSocket thuộc session. Yêu cầu CSRF header như mutation khác.

#### `GET /api/v1/auth/me`

- 200 `{"id":"...","username":"admin","expires_at":...}`.
- Dùng cho dashboard kiểm tra session.

Mọi endpoint sau yêu cầu session admin, trừ health và static. `sessions.last_seen_at` được cập nhật tối đa một lần mỗi 5 phút cho mỗi session để tránh write amplification.

### 4.3. Devices

#### `GET /api/v1/devices`

Query: `limit`, `cursor`, `q` (tùy chọn, 2..64 ký tự, tìm trong name/id), `enabled` (`true`/`false`, tùy chọn).

```json
{
  "items": [{
    "id": "019c...",
    "name": "sensor-01",
    "enabled": true,
    "online": true,
    "metadata": {},
    "created_at": 1793149200000,
    "updated_at": 1793149200000,
    "last_seen_at": 1793149200000,
    "credential": {"id": "019c...", "created_at": 1793149200000}
  }],
  "next_cursor": null
}
```

- `online` lấy từ presence registry (RAM); sau restart false.
- `credential` là active credential summary; không có hash/token.
- Sort `created_at DESC, id DESC`.

#### `POST /api/v1/devices`

```json
{"name": "sensor-01", "metadata": {"room": "A1"}, "enabled": true}
```

- 201:

```json
{
  "device": { "...": "như item trên, credential summary" },
  "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}
}
```

- `name`: 1..64 ký tự Unicode, trim, không control char; unique case-insensitive (ASCII).
- `metadata`: object ≤ 8 192 byte, depth ≤ 8; mặc định `{}`.
- `enabled`: mặc định true.
- Token chỉ xuất hiện trong response này; không log.
- Trùng tên → `409 device_name_conflict`.

#### `GET /api/v1/devices/{id}`

- 200 device item; 404 `device_not_found`.

#### `PATCH /api/v1/devices/{id}`

```json
{"name": "sensor-02", "metadata": {}, "enabled": false}
```

- Partial; ít nhất một trường; unknown field → 400.
- `metadata` khi có mặt **thay thế toàn bộ** object, không merge.
- Disable: commit rồi kick; 200 device mới.
- Enable không tự kết nối lại.
- 404/409 như trên.

#### `POST /api/v1/devices/{id}/credentials/rotate`

- 200:

```json
{"device_id": "019c...", "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}}
```

- Revoke active cũ + tạo active mới + audit trong transaction; commit; kick connection cũ.
- Token cũ không reconnect được; token mới chỉ hiện một lần.
- Device không tồn tại → 404; device disabled vẫn rotate được (admin có thể chuẩn bị trước khi enable).

### 4.4. Telemetry và attributes

#### `GET /api/v1/devices/{id}/telemetry`

Query bắt buộc: `from`, `to` (ms, `from < to`, range ≤ 31 ngày). Tùy chọn: `limit` 1..1000 (mặc định 100), `cursor`, `keys` (comma-separated, tối đa 16 key).

```json
{
  "items": [{
    "internal_id": 42,
    "event_id": "evt-019c...",
    "ts": 1793149200000,
    "received_at": 1793149200123,
    "values": {"temperature": 27.5}
  }],
  "next_cursor": "eyJ2IjoxLCJ0cyI6..."
}
```

- Sort `(ts, internal_id)` tăng dần.
- `keys` lọc values trả về; event không có key nào vẫn có thể xuất hiện với `values:{}`? **Không**: chỉ trả event có ít nhất một key khớp.
- `next_cursor` null khi hết.
- 404 device; 400 `validation_error`/`invalid_cursor`.

#### `GET /api/v1/devices/{id}/telemetry/latest`

Query tùy chọn `keys` (tối đa 16).

```json
{
  "items": [{
    "key": "temperature",
    "value": 27.5,
    "ts": 1793149200000,
    "received_at": 1793149200123,
    "event_id": "evt-019c..."
  }]
}
```

- Latest theo `(ts, received_at, internal_id)`; sort key tăng dần.
- Không có dữ liệu → `items: []`.

#### `GET /api/v1/devices/{id}/attributes`

```json
{
  "items": [{
    "key": "firmware",
    "value": "1.4.2",
    "device_ts": 1793149200000,
    "updated_at": 1793149200123
  }]
}
```

- Chỉ đọc; admin không ghi attribute trong MVP.

### 4.5. Commands

#### `POST /api/v1/devices/{id}/commands`

```json
{
  "name": "reboot",
  "params": {"delay_s": 5},
  "ttl_ms": 60000,
  "idempotency_key": "retry-2026-10-28-01"
}
```

- `name`: 1..64, `^[a-z][a-z0-9_.-]{0,63}$`.
- `params`: object ≤ 16 384 byte; mặc định `{}`.
- `ttl_ms`: 1 000..`commands.max_ttl_ms`; mặc định `commands.default_ttl_ms`.
- `idempotency_key`: tùy chọn, 1..128 printable ASCII; unique theo device.
- 202:

```json
{
  "command_id": "019c...",
  "device_id": "019c...",
  "status": "pending",
  "expires_at": 1793149260000
}
```

- Header `Location: /api/v1/commands/{command_id}`.
- Cùng key + cùng raw body hash → trả lại command cũ, 202, header `Idempotent-Replayed: true`.
- Cùng key + khác body hash → `409 idempotency_conflict`.
- Device disabled → `409 device_disabled`; queue đầy → `409 command_queue_full` (`details.limit`).
- 404 device.

#### `GET /api/v1/commands/{id}`

```json
{
  "id": "019c...",
  "device_id": "019c...",
  "name": "reboot",
  "params": {"delay_s": 5},
  "status": "dispatched",
  "created_at": 1793149200000,
  "expires_at": 1793149260000,
  "dispatched_at": 1793149200500,
  "completed_at": null,
  "result": null,
  "error": null,
  "attempts": 1
}
```

- `status` ∈ `pending|dispatched|succeeded|failed|expired`.
- 404 `command_not_found`.

#### `GET /api/v1/devices/{id}/commands`

Query: `limit` (mặc định 50, max 200), `cursor`, `status` (một giá trị). Sort `created_at DESC, id DESC`. Response `{items:[Command], next_cursor}`.

### 4.6. Runtime config

#### `GET /api/v1/config`

```json
{
  "revision": 3,
  "config": {
    "config_version": 1,
    "server": {"name": "my-iot-server", "data_dir": "./data", "shutdown_timeout_ms": 10000},
    "http": {"listen": "127.0.0.1:8080", "dashboard": true, "cookie_secure": false, "...": "..."},
    "mqtt": {"listen": "127.0.0.1:1883", "...": "..."},
    "database": {"url": "sqlite://<redacted>", "max_connections": 5, "...": "..."},
    "telemetry": {"...": "..."},
    "commands": {"...": "..."},
    "runtime": {"settings_path": "./data/runtime.toml"},
    "logging": {"level": "info", "format": "json"}
  },
  "restart_required": []
}
```

- Trả **cấu hình hiệu lực** sau khi merge runtime, đã redact.
- `database.url`/`url_env` giá trị thật không bao giờ trả; `url_env` chỉ trả tên biến.
- `restart_required` liệt kê field đã cố đổi nhưng cần restart (chỉ có trong response 422; GET luôn `[]`).

#### `PATCH /api/v1/config/runtime`

```json
{
  "expected_revision": 3,
  "settings": {
    "logging": {"level": "debug"},
    "telemetry": {"retention_days": 14, "batch_size": 200, "flush_interval_ms": 500}
  }
}
```

- Chỉ cho phép: `logging.level`, `telemetry.retention_days`, `telemetry.batch_size`, `telemetry.flush_interval_ms`.
- Field không tồn tại trong schema config → `400 validation_error`.
- Field hợp lệ nhưng không hot-reload (kể cả `http.listen`, `database`, `mqtt`, `auth`) → `422 restart_required`, `details.restart_required` là danh sách path; **không ghi file**.
- `settings` phải là object không rỗng.
- `expected_revision` khác revision hiện tại → `409 revision_conflict`, `details.current_revision`.
- Validate toàn bộ snapshot merge; lỗi → 400, không ghi.
- Ghi `runtime.toml` bằng temp file cùng thư mục + `fsync` + atomic rename; lỗi ghi → `500 config_write_failed`, config cũ giữ nguyên.
- 200 trả body như GET với revision mới.
- Nếu settings không đổi gì: 200, revision giữ nguyên, không ghi file.
- Update được serialize bằng mutex; check revision ở trong lock.

### 4.7. Overview và metrics

#### `GET /api/v1/overview`

```json
{
  "devices": {"total": 10, "online": 7, "disabled": 1},
  "telemetry": {"events_24h": 12345, "last_received_at": 1793149200123},
  "commands": {"pending": 2, "dispatched": 1, "failed_24h": 0, "expired_24h": 1},
  "ingest": {"queue_depth": 0, "queue_capacity": 4096, "rejected_24h": 0},
  "storage_ok": true
}
```

- Dùng cho dashboard; số liệu có thể lệch nhẹ so với DB do cache/broadcast.
- `storage_ok` false nếu lần ghi gần nhất lỗi hoặc readiness DB false.

#### `GET /metrics`

- Mặc định tắt (`[http] metrics = false` → 404).
- Khi bật, **không** dùng session cookie: bắt buộc `http.metrics_bearer_token_env` trỏ tới environment variable có token ngẫu nhiên tối thiểu 32 byte. Yêu cầu `Authorization: Bearer <token>`; thiếu/sai trả 401. Token không bao giờ xuất hiện trong API, log hay `GET /config`.
- Production đặt endpoint sau reverse proxy/mTLS hoặc network policy chỉ cho Prometheus. Không bật metrics nếu không có một trong các lớp mạng đó.
- Tên metric tối thiểu: `iot_mqtt_connections_active`, `iot_ingest_accepted_total`, `iot_ingest_rejected_total{reason}`, `iot_telemetry_queue_depth`, `iot_telemetry_commit_seconds` (histogram), `iot_telemetry_commit_errors_total`, `iot_commands_pending`, `iot_commands_expired_total`, `iot_ws_clients`, `iot_ws_dropped_total`.
- Không dùng device ID làm label.

### 4.8. WebSocket

`GET /api/v1/events` (Upgrade). Yêu cầu cookie + Origin hợp lệ.

Server gửi ngay:

```json
{"type": "hello", "server_time": 1793149200000, "max_subscriptions": 8}
```

Client gửi (tối đa 4 096 byte):

```json
{"action": "subscribe", "channels": ["overview", "device:019c..."]}
{"action": "unsubscribe", "channels": ["device:019c..."]}
{"action": "ping"}
```

- Channel hợp lệ: `overview`, `device:{uuid}`. Tối đa 8 channel/kết nối.
- `subscribe` trả `{"type":"subscribed","channels":[...]}`; lỗi `{"type":"error","code":"...","message":"..."}`.
- `ping` trả `{"type":"pong","ts":...}`.
- Server ping WebSocket protocol mỗi 30 giây; không pong trong 60 giây → đóng 1001.
- Queue mỗi client `http.ws_queue_capacity` (256); đầy → đóng 1013 `slow_consumer`; client reconnect và REST refetch.
- Tối đa `http.max_ws_connections` kết nối; vượt → HTTP 503 trước upgrade.
- Không replay; không đảm bảo thứ tự giữa các loại event.

Envelope event:

```json
{"type": "telemetry", "id": "019c...", "ts": 1793149200123, "device_id": "019c...", "data": {}}
```

| Type | Channel | Data |
| --- | --- | --- |
| `telemetry` | `device:{id}` | `{event_id, ts, received_at, values}` của event đã commit |
| `command` | `device:{id}`, `overview` | `{command_id, device_id, status, completed_at, error}` |
| `presence` | `overview`, `device:{id}` | `{online: true/false, last_seen_at}` |
| `reported_status` | `device:{id}` | `{status: "online"/"offline", ts}` từ topic status/LWT |
| `config` | `overview` | `{revision}` |
| `system` | `overview` | `{kind: "storage_unavailable"/"queue_full", severity: "warning"/"error", message}` |

- Telemetry chỉ broadcast sau commit.
- `system.message` là thông báo chung, không chứa SQL/đường dẫn/secret.
- Session hết hạn hoặc logout: server đóng WS thuộc session với code 4401; client reconnect sẽ nhận 401.

### 4.9. Health, readiness, static

#### `GET /healthz`

- 200 `{"status":"ok"}` nếu process còn sống; không kiểm tra DB; không auth; không lộ config.

#### `GET /readyz`

- 200 `{"status":"ready","checks":{"database":"ok","mqtt":"ok","ingest":"ok"}}`.
- 503 `{"status":"not_ready","checks":{"database":"error","mqtt":"ok","ingest":"degraded"}}`.
- `database`: `SELECT 1` timeout 1s.
- `mqtt`: listener task còn sống.
- `ingest`: worker task còn sống và không trong trạng thái storage-error kéo dài.
- Không trả chi tiết lỗi/secret.

#### Static dashboard

- `GET /` trả `index.html`; asset `/assets/*` từ bundle nhúng.
- Fallback mọi path không bắt đầu `/api` về `index.html` (SPA).
- `index.html`: `Cache-Control: no-store`; asset có hash: `public, max-age=31536000, immutable`.
- `[http] dashboard = false`: `/` và asset trả 404; API vẫn chạy.
- Thiếu `web/dist` trong debug: trả trang hướng dẫn build, không panic. Release thiếu asset là lỗi build.

### 4.10. Bảng trạng thái và mã lỗi HTTP

| HTTP | Code | Khi nào |
| --- | --- | --- |
| 400 | `validation_error` | Field sai/thiếu, range sai |
| 400 | `invalid_json` | Body không parse được |
| 400 | `invalid_cursor` | Cursor hỏng/không khớp |
| 401 | `unauthenticated` | Không có/session hết hạn |
| 401 | `invalid_credentials` | Login sai |
| 403 | `csrf_failed` | Thiếu header/Origin sai |
| 403 | `forbidden` | Không đủ quyền (dự phòng) |
| 404 | `not_found` | Route/resource chung |
| 404 | `device_not_found` | Device ID không tồn tại |
| 404 | `command_not_found` | Command ID không tồn tại |
| 405 | `method_not_allowed` | Method sai |
| 409 | `device_name_conflict` | Tên device trùng |
| 409 | `idempotency_conflict` | Cùng key, khác body |
| 409 | `revision_conflict` | `expected_revision` cũ |
| 409 | `device_disabled` | Tạo command cho device disabled |
| 409 | `command_queue_full` | Vượt pending/dispatched/device |
| 413 | `payload_too_large` | Body vượt giới hạn |
| 415 | `unsupported_media_type` | Content-Type sai |
| 422 | `restart_required` | Đổi field không hot-reload |
| 429 | `rate_limited` | Login quá nhiều |
| 500 | `internal_error` | Lỗi không mong đợi |
| 500 | `config_write_failed` | Ghi runtime.toml lỗi |
| 503 | `not_ready` | Readiness false |
| 503 | `storage_unavailable` | DB/disk lỗi khi xử lý request |

## 5. Contract cấu hình

### 5.1. `conf.toml` đầy đủ

```toml
config_version = 1

[server]
name = "my-iot-server"
data_dir = "./data"
shutdown_timeout_ms = 10000

[http]
listen = "127.0.0.1:8080"
dashboard = true
max_body_bytes = 262144
cookie_secure = false
request_timeout_ms = 30000
max_ws_connections = 100
ws_queue_capacity = 256
metrics = false
# Khi metrics = true, bắt buộc đặt tên biến môi trường; không ghi token trong TOML.
# metrics_bearer_token_env = "IOT_METRICS_TOKEN"

[mqtt]
listen = "127.0.0.1:1883"
allow_anonymous = false
max_connections = 1000
max_packet_bytes = 262144
max_inflight_per_client = 32
max_queued_messages_per_client = 256
max_subscriptions_per_client = 8
max_session_expiry_ms = 3600000
max_client_id_len = 64
max_topic_levels = 16
max_retained_messages = 10000
max_retained_bytes = 16777216

[database]
url = "sqlite://./data/iot.db"
max_connections = 5
connect_timeout_ms = 5000
busy_timeout_ms = 5000
# Hoặc thay `url` bằng:
# url_env = "IOT_DATABASE_URL"

[telemetry]
queue_capacity = 4096
batch_size = 500
max_batch_bytes = 8388608
flush_interval_ms = 1000
retention_days = 30
max_future_skew_ms = 300000
max_past_skew_ms = 31536000000
max_payload_bytes = 65536
max_keys = 128
max_string_bytes = 4096

[commands]
default_ttl_ms = 60000
max_ttl_ms = 86400000
max_pending_per_device = 100
retry_initial_ms = 1000
retry_max_ms = 30000
dispatch_interval_ms = 500

[auth]
session_ttl_ms = 86400000
login_max_attempts = 5
login_window_ms = 300000
password_min_length = 12

[runtime]
settings_path = "./data/runtime.toml"

[logging]
level = "info"
format = "json"
```

### 5.2. Kiểu, range, mặc định

| Field | Kiểu | Range/mặc định | Ghi chú |
| --- | --- | --- | --- |
| `config_version` | integer | bắt buộc `1` | Khác → startup fail |
| `server.name` | string | 1..64, default `iot-server` | Dùng trong log/metric |
| `server.data_dir` | path | default `./data` | Tạo nếu thiếu; chứa lock, DB, runtime |
| `server.shutdown_timeout_ms` | integer | 1 000..60 000, default 10 000 | Deadline drain |
| `http.listen` | SocketAddr | default `127.0.0.1:8080` | Chỉ IP, không DNS |
| `http.dashboard` | bool | default true | Tắt static |
| `http.max_body_bytes` | integer | 1 024..10 485 760, default 262 144 | |
| `http.cookie_secure` | bool | default false | true khi HTTPS proxy |
| `http.request_timeout_ms` | integer | 1 000..120 000, default 30 000 | |
| `http.max_ws_connections` | integer | 1..10 000, default 100 | |
| `http.ws_queue_capacity` | integer | 16..4 096, default 256 | |
| `http.metrics` | bool | default false | Bật `/metrics` (bắt buộc Bearer token riêng) |
| `http.metrics_bearer_token_env` | string/env name | none | Bắt buộc khi `metrics=true`; token ≥32 byte, chỉ dùng Bearer cho `/metrics` |
| `mqtt.listen` | SocketAddr | default `127.0.0.1:1883` | Khác `http.listen` |
| `mqtt.allow_anonymous` | bool | **phải false** | true → config error |
| `mqtt.max_connections` | integer | 1..100 000, default 1 000 | |
| `mqtt.max_packet_bytes` | integer | 1 024..16 777 216, default 262 144 | |
| `mqtt.max_inflight_per_client` | integer | 1..65 535, default 32 | |
| `mqtt.max_queued_messages_per_client` | integer | 1..65 535, default 256 | |
| `mqtt.max_subscriptions_per_client` | integer | 1..1 024, default 8 | |
| `mqtt.max_session_expiry_ms` | integer | 60 000..3 600 000, default 3 600 000 | Tối đa 1 giờ |
| `mqtt.max_client_id_len` | integer | 36..256, default 64 | ≥ 36 |
| `mqtt.max_topic_levels` | integer | 1..32, default 16 | |
| `mqtt.max_retained_messages` | integer | 0..10 000, default 10 000 | 0 = cấm retained |
| `mqtt.max_retained_bytes` | integer | 0..16 777 216, default 16 777 216 | 0 = cấm retained |
| `database.url` / `url_env` | string | đúng một trong hai | MVP chỉ `sqlite://` |
| `database.max_connections` | integer | 1..64, default 5 | |
| `database.connect_timeout_ms` | integer | 100..60 000, default 5 000 | SQLite không có connect timeout riêng; MVP map sang SQLx pool acquire timeout |
| `database.busy_timeout_ms` | integer | 100..60 000, default 5 000 | SQLite |
| `telemetry.queue_capacity` | integer | 16..65 536, default 4 096 | |
| `telemetry.batch_size` | integer | 1..2 000, default 500 | `<= queue_capacity` |
| `telemetry.max_batch_bytes` | integer | 65 536..67 108 864, default 8 388 608 | Flush sớm theo byte |
| `telemetry.flush_interval_ms` | integer | 10..60 000, default 1 000 | |
| `telemetry.retention_days` | integer | 1..3 650, default 30 | |
| `telemetry.max_future_skew_ms` | integer | 0..86 400 000, default 300 000 | |
| `telemetry.max_past_skew_ms` | integer | 0..31 536 000 000, default 31 536 000 000 (365 ngày) | |
| `telemetry.max_payload_bytes` | integer | 256..1 048 576, default 65 536 | `<= mqtt.max_packet_bytes` |
| `telemetry.max_keys` | integer | 1..1 024, default 128 | |
| `telemetry.max_string_bytes` | integer | 1..65 536, default 4 096 | |
| `commands.default_ttl_ms` | integer | 1 000..`max_ttl_ms`, default 60 000 | |
| `commands.max_ttl_ms` | integer | 60 000..604 800 000, default 86 400 000 | |
| `commands.max_pending_per_device` | integer | 1..10 000, default 100 | pending + dispatched |
| `commands.retry_initial_ms` | integer | 100..60 000, default 1 000 | |
| `commands.retry_max_ms` | integer | 1 000..300 000, default 30 000 | `>= retry_initial_ms` |
| `commands.dispatch_interval_ms` | integer | 100..10 000, default 500 | |
| `auth.session_ttl_ms` | integer | 300 000..604 800 000, default 86 400 000 | |
| `auth.login_max_attempts` | integer | 1..100, default 5 | |
| `auth.login_window_ms` | integer | 1 000..3 600 000, default 300 000 | |
| `auth.password_min_length` | integer | 8..128, default 12 | |
| `runtime.settings_path` | path | default `./data/runtime.toml` | Phải nằm trong `data_dir` |
| `logging.level` | enum | `trace/debug/info/warn/error`, default `info` | Hot |
| `logging.format` | enum | `json/text`, default `json` | Restart |

### 5.3. Quy tắc load và validate

- Parse bằng Serde + `toml` với `deny_unknown_fields`; field lạ → lỗi nêu path.
- `url` và `url_env` loại trừ nhau. `url_env` đọc env; thiếu env → lỗi. Không fallback.
- `http.metrics=true` yêu cầu `http.metrics_bearer_token_env`; env thiếu hoặc token ngắn hơn 32 byte → lỗi startup. `metrics_bearer_token_env` bị redact ở mọi response config.
- Không interpolate biến trong chuỗi TOML.
- Path tương đối resolve theo thư mục chứa `conf.toml` (canonical), không theo cwd. So sánh "nằm trong data_dir" bằng normalize lexical; không yêu cầu path tồn tại lúc `check`.
- `data_dir`: tạo khi `run`/`migrate`/`admin create`; `check` không tạo.
- SQLite path: bỏ prefix `sqlite://`, resolve theo base; phải nằm trong `data_dir`; không query string, không `:memory:`. File được tạo nếu thiếu (`create_if_missing`).
- `runtime.settings_path` phải nằm trong `data_dir` sau resolve; parent được tạo khi `run`/PATCH.
- `database.url_env` khi resolve vẫn phải là SQLite trong MVP; `postgres`/`mysql` → `unsupported_database` (Phase 5).
- `http.listen != mqtt.listen`.
- `telemetry.batch_size <= telemetry.queue_capacity`; `default_ttl_ms <= max_ttl_ms`; `retry_initial_ms <= retry_max_ms`; `max_payload_bytes <= max_packet_bytes`.
- `mqtt.max_client_id_len >= 36`.
- `max_retained_bytes == 0` hoặc `max_retained_messages == 0` → cấm retained (test dùng).
- Validate toàn bộ trước khi mở DB/listener; lỗi in tất cả path + lý do, exit 3.
- `check` không sửa DB/file; chỉ đọc config + env + kiểm tra path syntax/quyền đọc.
- `run`/`migrate`/`admin create` lấy exclusive lock `<data_dir>/.lock`; không được thì exit 5. Lock giữ đến khi process thoát.
- Không watch file; sửa `conf.toml` cần restart.

### 5.4. `runtime.toml`

```toml
revision = 3

[logging]
level = "debug"

[telemetry]
retention_days = 14
batch_size = 200
flush_interval_ms = 500
```

- Chỉ chứa field hot; `runtime` struct dùng `Option` để preserve override cũ.
- `revision` bắt buộc nếu file tồn tại; integer ≥ 0; file thiếu → revision 0, không override.
- File tồn tại nhưng parse/validate lỗi → startup fail (exit 3), không bỏ qua.
- Không chứa secret, không chứa `database`, `mqtt`, `http`, `server`, `auth`.
- Ghi atomic: temp cùng thư mục → fsync file → rename → fsync thư mục. Lỗi giữ file cũ.
- Revision tăng 1 mỗi lần ghi; không reuse/giảm.
- PATCH validate **toàn bộ** snapshot sau merge, không chỉ field thay đổi.

### 5.5. Precedence

`default` → `conf.toml` → `runtime.toml` (chỉ field hot). Env chỉ dùng cho `database.url_env` và secret file của CLI; không có override chung.

## 6. Contract CLI

```bash
iot-server check   --config conf.toml
iot-server migrate --config conf.toml
iot-server admin create --config conf.toml [--username admin] [--password-file /path]
iot-server run     --config conf.toml
iot-server --version
```

| Lệnh | Hành vi | Exit |
| --- | --- | --- |
| `check` | Validate config + env; không tạo DB/file | 0 hợp lệ, 3 lỗi config |
| `migrate` | Lock, mở/tạo DB, chạy migration, thoát | 0, 3 config, 4 mở/tạo DB hoặc migration, 5 lock |
| `admin create` | Lock, migrate? **Không**; yêu cầu schema đã có; tạo admin duy nhất | 0, 3, 5, 6 đã có admin |
| `run` | Lock, migrate, start HTTP/MQTT/worker | 0 graceful, 1 runtime, 3, 4, 5 |
| Clap usage | Thiếu/sai tham số | 2 |

- `admin create`:
  - password từ `--password-file` (đọc trim newline) hoặc prompt 2 lần bằng `rpassword`; không nhận argv password.
  - username mặc định `admin`, regex `^[a-z0-9_.-]{3,32}$`.
  - Nếu bảng `users` đã có bất kỳ row nào → exit 6, không sửa.
  - Hash Argon2id; insert audit `admin.create` actor system.
  - In `created admin '<username>'`; không in password/hash.
- `migrate` không tạo admin, không start listener; idempotent.
- `run` tự migrate trước listener; nếu chưa có admin vẫn chạy nhưng login luôn fail (không tạo mặc định).
- `--config` là global option, đặt trước/sau subcommand đều được; default `./conf.toml`.
- Log JSON ra stdout; lỗi CLI/config ra stderr; SIGTERM/SIGINT graceful; signal thứ hai exit ngay.
- Không có flag override cấu hình ngoài `--config`, `--username`, `--password-file`.

## 7. Test vector bắt buộc

### 7.1. Telemetry

| # | Input | Kỳ vọng |
| --- | --- | --- |
| T1 | `{"event_id":"evt-1","ts":1793149200000,"values":{"temperature":27.5,"humidity":65,"relay":true}}` | 1 row; ACK `stored` |
| T2 | Thiếu `event_id` | Không row; ACK `rejected/missing_event_id`, `event_id: null` |
| T3 | `values:{"x":null}` | Không row; ACK `rejected/invalid_payload`, field `values.x` |
| T4 | `values:{"x":{"a":1}}` | Không row; `invalid_payload` |
| T5 | String 4 097 byte | Không row; `invalid_payload` |
| T6 | 129 key | Không row; `invalid_payload` |
| T7 | `ts = now + 300001` | Không row; `invalid_timestamp` |
| T8 | Thiếu `ts` | Row có `ts = received_at` |
| T9 | Gửi lại T1 byte-identical | Vẫn 1 row; ACK `duplicate` |
| T10 | T1 với `temperature:28` | Vẫn 1 row cũ; ACK `rejected/event_id_conflict` |
| T11 | Publish retained T1 | Không row; ACK `rejected/retained_not_allowed` |
| T12 | Publish `devices/B/telemetry` bằng token A | Không row; ACL deny; không giao cho B |
| T13 | Subscribe `devices/+/telemetry` | SUBACK failure/deny |
| T14 | Subscribe `devices/{B}/commands` | Deny |
| T15 | Publish `devices/{A}/status` retained `{"status":"offline"}` | Subscriber mới nhận retained; presence A không đổi nếu đang online |
| T16 | Publish 10 001 retained topic khác nhau | Retained thứ 10 001 bị từ chối; metric cap |
| T17 | Event ID trùng nhưng bản cũ đã bị retention xóa | Insert mới thành công (dedup không vĩnh viễn) |

### 7.2. Command

| # | Input | Kỳ vọng |
| --- | --- | --- |
| C1 | Create command cho device online | 202, `pending`; dispatcher publish QoS1; `dispatched` |
| C2 | Device offline | `pending`; không publish; reconnect → publish nếu còn hạn |
| C3 | ACK `succeeded` đúng device/ID | `succeeded`, `completed_at` set |
| C4 | ACK lặp | Không đổi terminal; 1 kết quả |
| C5 | ACK sai device | Bỏ qua + log; command không đổi |
| C6 | ACK sau `expires_at` | Command `expired`; `late_ack_*` ghi nhận |
| C7 | Create cùng idempotency key + body | 202 cùng `command_id`; `Idempotent-Replayed: true` |
| C8 | Cùng key khác body | 409 `idempotency_conflict` |
| C9 | 101 command pending/device | 409 `command_queue_full` |
| C10 | Publish lại sau reconnect | Cùng command ID; node dedup |

### 7.3. Config/HTTP

| # | Input | Kỳ vọng |
| --- | --- | --- |
| H1 | PATCH runtime đúng revision, field hot | 200, revision +1; restart giữ thay đổi |
| H2 | PATCH revision cũ | 409, không ghi file |
| H3 | PATCH `http.listen` | 422 `restart_required`; không ghi |
| H4 | File runtime không ghi được | 500; config RAM cũ |
| H5 | Login sai 5 lần | 429 + `Retry-After` |
| H6 | Mutation thiếu `X-IoT-CSRF` | 403 `csrf_failed` |
| H7 | Rotate token khi device online | Kết nối cũ đóng; token cũ CONNACK 0x04; token mới OK |
| H8 | `GET /config` | Không chứa password/token/URL thật |
| H9 | Cursor sai | 400 `invalid_cursor` |
| H10 | Telemetry `from >= to` | 400 |

## 8. Truy vết SPEC

| SPEC | Contract |
| --- | --- |
| §5.1 giao thức | §2.1 |
| §5.2 danh tính/ACL | §2.2–§2.5 |
| §5.3 payload telemetry | §2.6 |
| §5.4 bền vững/backpressure | §2.6, §3.3 |
| §6 HTTP/dashboard | §4 |
| §7 command | §2.8, §4.5 |
| §8 storage | §3 |
| §9 cấu hình | §5 |
| §10 vận hành/bảo mật | §1.3, §4.2, §4.8–§4.9 |
| §11 CLI/đóng gói | §6 |
| §13 tiêu chí A01–A19 | `03-delivery-plan.md` |

## 9. Kết quả Phase 0 đã chốt

Phase 0 PASS ngày 2026-10-28; bằng chứng đầy đủ trong `spike-rmqtt.md`. Các điểm dưới đây là contract cuối cùng, không còn là câu hỏi mở:

1. Protocol level 3 bị từ chối bằng CONNACK `0x05`; level 5 bằng CONNACK `0x87`; `client_id != username` bằng `0x04`.
2. Session expiry `clean_session=0` hoạt động với TTL ngắn trong test; production cấu hình tối đa 1 giờ (`max_session_expiry_ms`).
3. Duplicate `client_id` đóng connection cũ trước connection mới; session id chứa `create_time` dùng được làm generation.
4. Retained cap dùng `BudgetedRetainStorage`; vượt cap đóng connection, không PUBACK, metric `retained_cap`; replace/clear cập nhật counter đúng.
5. `entry.publish` QoS 1 và `entry.kick` hoạt động; shutdown phải kick session trước khi abort listener.
6. Publish ACL deny đóng connection; subscribe ACL deny trả SUBACK `0x80` và giữ connection; cả hai không route message.
7. `MessagePublish` chạy trước `MessagePublishCheckAcl`; không ingest trong `MessagePublish`.
8. Abort listener nhả port và không accept connection mới nhưng không đóng session; wrapper chịu trách nhiệm kick.

Nếu implementation Phase 1 phát hiện hành vi khác, dừng và sửa tài liệu này + ADR trước khi tiếp tục, không tự điều chỉnh ngầm.
