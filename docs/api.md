# HTTP API

Base path: `/api/v1`. Đây là bản tham chiếu nhanh cho operator/developer; contract đầy đủ, gồm mọi field và quy tắc validate, nằm trong [02 — Contract kỹ thuật](plans/02-contracts.md) §4. Server không nhận telemetry qua HTTP; thiết bị publish qua MQTT.

## Xác thực và CSRF

Admin session dùng cookie `iot_session`. `POST /api/v1/auth/login` với `{"username":"admin","password":"..."}` set cookie; `POST /api/v1/auth/logout` thu hồi session. `GET /api/v1/auth/me` trả admin hiện tại và thời hạn session.

Mọi mutation đã xác thực (`POST`, `PATCH`, `DELETE`) còn yêu cầu:

- `Content-Type: application/json`;
- header `X-IoT-CSRF: 1`;
- nếu có header `Origin`, host của Origin phải khớp `Host` của request.

Dùng cookie jar cho ví dụ:

```sh
curl -sS -c cookies.txt -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"..."}' \
  http://127.0.0.1:8080/api/v1/auth/login

curl -sS -b cookies.txt -H 'Content-Type: application/json' \
  -H 'X-IoT-CSRF: 1' \
  -d '{"name":"sensor-01"}' \
  http://127.0.0.1:8080/api/v1/devices
```

Sai username/password trả `401 invalid_credentials`; sai nhiều lần trả `429 rate_limited` kèm `Retry-After`. Device token không dùng được cho admin API. Nếu `cookie_secure = true`, cookie chỉ được gửi qua HTTPS.

## Endpoint

| Method | Path | Mục đích |
| --- | --- | --- |
| `GET` | `/healthz` | Process còn sống; không kiểm tra database, không auth |
| `GET` | `/readyz` | Readiness của database, MQTT, ingest và command dispatcher; không auth |
| `POST` | `/api/v1/auth/login` | Tạo admin session |
| `POST` | `/api/v1/auth/logout` | Thu hồi session hiện tại |
| `GET` | `/api/v1/auth/me` | Admin hiện tại và thời hạn session |
| `GET` | `/api/v1/devices` | Liệt kê device (`limit`, `cursor`, `q`, `enabled`) |
| `POST` | `/api/v1/devices` | Tạo device; trả token đúng một lần |
| `GET` | `/api/v1/devices/{id}` | Đọc một device |
| `PATCH` | `/api/v1/devices/{id}` | Đổi tên, thay metadata, enable/disable |
| `POST` | `/api/v1/devices/{id}/credentials/rotate` | Thu hồi token cũ và trả token mới |
| `GET` | `/api/v1/devices/{id}/telemetry` | Lịch sử telemetry theo `from`/`to` hoặc checkpoint reconnect `after_id` |
| `GET` | `/api/v1/devices/{id}/telemetry/latest` | Giá trị mới nhất theo key |
| `GET` | `/api/v1/devices/{id}/attributes` | Attribute mới nhất của device |
| `POST` | `/api/v1/devices/{id}/commands` | Đưa command vào queue |
| `GET` | `/api/v1/devices/{id}/commands` | Liệt kê command (`limit`, `cursor`, `status`) |
| `GET` | `/api/v1/commands/{id}` | Đọc một command và kết quả |
| `GET` | `/api/v1/config` | Cấu hình hiệu lực đã redact |
| `PATCH` | `/api/v1/config/runtime` | Cập nhật setting hot-reload |
| `GET` | `/api/v1/overview` | Counter cho dashboard |
| `GET` | `/metrics` | Prometheus metric; mặc định tắt, chỉ Bearer token |
| `GET` | `/api/v1/events` | WebSocket upgrade |
| `GET` | `/` và asset | Dashboard nhúng khi `http.dashboard = true` |

Danh sách dùng cursor opaque, không dùng offset. Device/command mặc định `limit=50` (tối đa 200); telemetry mặc định 100 (tối đa 1 000); attribute mặc định 50 (tối đa 200). `from`/`to` của telemetry phải là range tăng dần, tối đa 31 ngày. Để backfill sau reconnect, dùng `after_id` thay cho `from`/`to` và chỉ tiến tới `through_id` được trả về.

## Credential thiết bị

`POST /api/v1/devices` và `POST /api/v1/devices/{id}/credentials/rotate` trả token thiết bị đúng một lần:

```json
{"device": {"id": "019c...", "name": "sensor-01", "enabled": true},
 "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}}
```

Rotate trả `device_id` thay vì object `device` đầy đủ:

```json
{"device_id": "019c...",
 "credential": {"id": "019c...", "token": "iot_...", "created_at": 1793149200000}}
```

Token là `iot_` cộng 43 ký tự base64url. Lưu token trong secret store của thiết bị; database chỉ giữ hash SHA-256 và API không bao giờ hiển thị lại token. Rotate commit credential mới và thu hồi credential cũ trong cùng một transaction, rồi kick session đang kết nối. Nếu response bị mất sau commit, rotate lần nữa để lấy token mới đã biết; token trước đó đã bị vô hiệu.

## Command

Tạo command với `name`, `params` tùy chọn, `ttl_ms` và `idempotency_key` tùy chọn:

```json
{
  "name": "reboot",
  "params": {"delay_s": 5},
  "ttl_ms": 60000,
  "idempotency_key": "retry-2026-10-28-01"
}
```

Response là `202` với `command_id`, `status: "pending"` và `Location: /api/v1/commands/{id}`. Dùng lại idempotency key với cùng body trả command cũ kèm `Idempotent-Replayed: true`; body khác trả `409 idempotency_conflict`. Device disabled trả `409 device_disabled`; queue theo device đầy trả `409 command_queue_full`. Trạng thái command là một trong `pending`, `dispatched`, `succeeded`, `failed`, `expired`.

## Runtime configuration

`PATCH /api/v1/config/runtime` nhận `expected_revision` và object `settings`. Chỉ các field sau hot-reload được:

- `logging.level`
- `telemetry.retention_days`
- `telemetry.batch_size`
- `telemetry.flush_interval_ms`

Revision cũ trả `409 revision_conflict`. Field hợp lệ nhưng cần restart trả `422 restart_required` và không ghi file. Giá trị không hợp lệ trả `400 validation_error` và cũng không ghi file. Response có cùng shape đã redact như `GET /api/v1/config` với revision mới. Sửa `runtime.toml` trên đĩa chỉ được đọc lúc khởi động.

## WebSocket

Kết nối tới `GET /api/v1/events` với cookie session và `Origin` khớp. Server gửi ngay:

```json
{"type": "hello", "server_time": 1793149200000, "max_subscriptions": 8}
```

Message từ client (tối đa 4 096 byte):

```json
{"action": "subscribe", "channels": ["overview", "device:019c..."], "request_id": 17}
{"action": "unsubscribe", "channels": ["device:019c..."]}
{"action": "ping"}
```

Channel là `overview` và `device:{uuid}`, tối đa 8 channel mỗi connection. `subscribe` có thể gửi `request_id` dạng integer; message `subscribed` khớp sẽ chứa các channel được chấp nhận cho request đó. `ping` trả `pong`. Event chỉ là invalidation, không phải stream bền vững: telemetry chỉ được gửi sau commit, command mang `state_version`, và client phải refetch REST resource sau reconnect. Server ping mỗi 30 giây và đóng connection không phản hồi trong 60 giây (`1001`). Slow consumer bị đóng với `1013 slow_consumer`; hãy reconnect và backfill bằng `after_id`/`through_id`. Logout hoặc session hết hạn đóng WebSocket với `4401`.

Các loại event là `telemetry`, `command`, `presence`, `reported_status`, `config` và `system`. Envelope:

```json
{"type": "telemetry", "id": "019c...", "ts": 1793149200123, "device_id": "019c...", "data": {}}
```

## Lỗi

Lỗi dùng chung một envelope và luôn trả `X-Request-ID`:

```json
{
  "code": "validation_error",
  "message": "invalid field",
  "request_id": "019c...",
  "details": [{"field": "name", "reason": "must not be empty"}]
}
```

Các code thường gặp: `validation_error`, `invalid_json`, `invalid_cursor`, `unauthenticated`, `invalid_credentials`, `csrf_failed`, `not_found`, `device_not_found`, `command_not_found`, `method_not_allowed`, `device_name_conflict`, `idempotency_conflict`, `revision_conflict`, `device_disabled`, `command_queue_full`, `payload_too_large`, `unsupported_media_type`, `restart_required`, `rate_limited`, `internal_error`, `config_write_failed`, `not_ready`, `request_capacity_exceeded` và `storage_unavailable`. Bảng status/code đầy đủ nằm trong [02 §4.10](plans/02-contracts.md).

## Metrics

`/metrics` trả `404` trừ khi `http.metrics = true` và `http.metrics_bearer_token_env` trỏ tới biến environment chứa token tối thiểu 32 byte. Endpoint chỉ nhận `Authorization: Bearer <token>`, không bao giờ dùng cookie admin. Không expose endpoint nếu không có reverse proxy, mTLS hoặc network policy giới hạn cho hệ thống monitoring. Tên metric và ngữ nghĩa được liệt kê trong [02 §4.7](plans/02-contracts.md).
