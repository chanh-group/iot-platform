# Hướng dẫn MQTT

Broker được nhúng trong `iot-server`; không cần broker MQTT bên ngoài. Contract đầy đủ là [02 — Contract kỹ thuật](plans/02-contracts.md) §2. Tài liệu này là góc nhìn của client/operator.

## Kết nối

| Setting | Giá trị |
| --- | --- |
| Protocol | MQTT 3.1.1, protocol level `4` |
| Listen | `[mqtt] listen`, mặc định `127.0.0.1:1883` |
| `username` | Device ID (UUID v7) |
| `password` | Device token (`iot_...`) |
| `client_id` | Phải bằng device ID |
| `clean_session` | `0` hoặc `1`; session offline hết hạn sau `max_session_expiry_ms` (mặc định 1 giờ) |
| TLS | Không tích hợp; terminate MQTT TLS ở proxy nếu traffic ra khỏi host |

Listener mặc định chỉ bind loopback. MQTT plaintext không an toàn trên mạng không tin cậy, kể cả khi chạy trong Docker.

Lỗi kết nối cố ý không tiết lộ device không tồn tại, đã disable hay token sai:

| Kết quả | Ý nghĩa |
| --- | --- |
| CONNACK `0x04` | Sai username/password; cũng dùng cho device không tồn tại, đã disable hoặc đã revoke, và cho `client_id != username` |
| CONNACK `0x05` | MQTT 3.1 (protocol level 3) không được hỗ trợ |
| CONNACK `0x03` | Broker/session tạm thời unavailable; retry với exponential backoff + jitter có giới hạn, chỉ một CONNECT đang chạy cho mỗi `client_id` |
| CONNACK `0x87` | MQTT 5 (protocol level 5) không được hỗ trợ |
| Connection bị từ chối | Đã đạt connection cap của server, hoặc listener không khả dụng |

MQTT over WebSocket, `$share`, topic alias và MQTT 5 property không được hỗ trợ. Device không được dùng `$SYS`.

## Topic và quyền

ACL mặc định deny. Device chỉ dùng được namespace `devices/{self}/...` của chính nó:

| Topic | Device publish | Device subscribe | Lưu | Ghi chú |
| --- | --- | --- | --- | --- |
| `devices/{id}/telemetry` | Có | Không | Có | Ingest; application ACK sau commit |
| `devices/{id}/telemetry/ack` | Không | Có | Không | Server publish QoS 1, non-retained |
| `devices/{id}/attributes` | Có | Không | Chỉ latest | Không có application ACK |
| `devices/{id}/commands` | Không | Có | Không | Server publish QoS 1, non-retained |
| `devices/{id}/commands/ack` | Có | Không | Cập nhật command | Kết quả thực thi |
| `devices/{id}/status` | Có | Có | Không | Tín hiệu retained/LWT; presence của server là nguồn chính |
| `devices/{id}/data/#` | Có | Có | Không | Pub/sub tự do; retained được phép trong giới hạn |

Publish phải dùng topic cụ thể, không chứa `+` hoặc `#`. Subscribe được dùng wildcard, nhưng filter đã resolve phải nằm trong `devices/{self}/`. Không subscribe `devices/{self}` hoặc `devices/{self}/#`; chúng sẽ lộ topic chỉ dành cho server. Cross-device, `$SYS` và `$share` bị từ chối.

Hành vi khi ACL từ chối:

- `PUBLISH` bị từ chối: message không được route và broker đóng connection mà không gửi PUBACK.
- `SUBSCRIBE` bị từ chối: SUBACK failure `0x80`; connection vẫn mở.
- Publish retained vào `telemetry`, `attributes` hoặc topic chỉ dành cho server bị từ chối; telemetry nhận application ACK `retained_not_allowed` chỉ khi `event_id` hợp lệ.
- Vượt retained-message hoặc retained-byte cap đóng connection và tăng `iot_retained_cap_rejected_total`.

## Telemetry

Publish tới `devices/{id}/telemetry` với QoS 0 hoặc 1 (khuyến nghị QoS 1):

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

- `event_id` bắt buộc cho mọi telemetry, kể cả QoS 0. Chuỗi dài 1–128 ký tự thuộc `[A-Za-z0-9._:-]` và phải giữ nguyên khi retry.
- `ts` tùy chọn. Nếu thiếu, server dùng `received_at`. Timestamp lệch quá cửa sổ past/future đã cấu hình bị từ chối.
- `values` là object phẳng với 1–128 key. Value là number hữu hạn, boolean hoặc string tối đa 4 096 byte; null, array và object lồng nhau không hợp lệ.
- Payload giới hạn bởi `telemetry.max_payload_bytes` (mặc định 65 536 byte) và packet limit của MQTT.
- Dedup theo `(device_id, event_id)` bằng hash SHA-256 của raw payload. Retry phải gửi payload giống hệt từng byte. Cùng ID nhưng khác byte trả `event_id_conflict`.
- Dedup chỉ có hiệu lực khi bản ghi còn trong retention telemetry. Sau khi retention xóa row cũ, ID cũ có thể được chấp nhận lại.

Server publish application ACK tới `devices/{id}/telemetry/ack`, QoS 1, non-retained:

```json
{
  "event_id": "evt-019c2f7e-01",
  "status": "stored",
  "received_at": 1793149200123,
  "code": null
}
```

| Status | Ý nghĩa | ACK sau commit? |
| --- | --- | --- |
| `stored` | Event mới đã commit | Có |
| `duplicate` | Cùng event ID và payload hash đã tồn tại | Có |
| `rejected` | Event không hợp lệ; `code` cho biết lý do | Không ghi |
| `retryable` | Lỗi tạm thời; retry cùng event | Không ghi |

Các `code` thường gặp: `missing_event_id`, `invalid_event_id`, `invalid_payload`, `payload_too_large`, `invalid_timestamp`, `event_id_conflict`, `retained_not_allowed`, `unknown_device`, `queue_full` và `storage_unavailable`. `queue_full` và `storage_unavailable` là retryable; retry với cùng `event_id` và backoff.

Application ACK queue có giới hạn. Nếu đầy, ACK bị drop và `iot_telemetry_ack_dropped_total` tăng; device không thấy ACK và phải retry. Retry event đã commit trả `duplicate`. Nếu device không subscribe topic ACK, ACK bị mất; retry sau reconnect. QoS 0 vẫn nhận ACK khi server xử lý được, nhưng không được coi QoS 0 là bền vững.

## Attribute

Publish tới `devices/{id}/attributes` với QoS 0 hoặc 1, non-retained:

```json
{
  "ts": 1793149200000,
  "values": {
    "firmware": "1.4.2",
    "interval_s": 30
  }
}
```

Chỉ giá trị mới nhất theo key được lưu; MVP không có history. `ts` tùy chọn và được lưu làm device timestamp. Payload không hợp lệ bị drop kèm metric; không có topic application ACK cho attribute, và PUBACK QoS 1 chỉ là xác nhận giao thức.

## Command

Tạo command qua HTTP API, rồi subscribe `devices/{id}/commands`. Server publish QoS 1, non-retained:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "name": "reboot",
  "params": {"delay_s": 5},
  "created_at": 1793149200000,
  "expires_at": 1793149260000
}
```

ACK trên `devices/{id}/commands/ack`, QoS 0 hoặc 1:

```json
{
  "command_id": "019c2f7e-7a1b-7c3d-8e9f-0123456789ab",
  "status": "succeeded",
  "result": {"uptime_s": 42},
  "error": null
}
```

- `status` là `succeeded` hoặc `failed`; `result` là JSON bất kỳ tối đa 16 384 byte; `error` tối đa 512 ký tự.
- Command không phụ thuộc session queue của broker. Command cho device offline vẫn `pending` trong SQLite và được publish sau reconnect nếu chưa hết hạn.
- Subscribe lại đúng topic `devices/{id}/commands` đánh thức dispatcher và có thể resend command có lần publish trước chạy trước subscription. Dedup theo `command_id`.
- ACK trùng không đổi terminal state. ACK sau `expires_at` được lưu để chẩn đoán, nhưng command vẫn `expired`.
- Mỗi device có tối đa `commands.max_pending_per_device` command ở trạng thái `pending` hoặc `dispatched`; tạo thêm trả `409 command_queue_full`.

## Status và LWT

Publish status retained và cấu hình cùng topic làm last will:

```json
{"status": "offline"}
```

`devices/{id}/status` nhận `online` hoặc `offline` và `ts` tùy chọn. Đây là tín hiệu bổ sung; presence thật do MQTT session đã xác thực quyết định. Server không đổi presence vì payload này. LWT khuyến nghị là `{"status":"offline"}` retained trên topic status của device để dashboard hiển thị trạng thái được báo.

## Data tự do

`devices/{id}/data/#` nhận payload tùy ý trong packet limit, với QoS và retain do device chọn. Dữ liệu không được lưu vào SQLite. Retained message tính vào retained cap.

## Giới hạn

| Giới hạn | Mặc định |
| --- | --- |
| Connection mỗi server | 1 000 |
| Độ dài client ID | 64 byte |
| Số level topic | 16 |
| Subscription mỗi client | 8 |
| Inflight message mỗi client | 32 |
| Message trong queue mỗi client | 256 |
| Session expiry sau disconnect | 1 giờ |
| Retained message | 10 000 |
| Retained byte | 16 MiB |
| Kích thước packet MQTT | 262 144 byte |

Có thể cấu hình trong `[mqtt]` của `conf.toml` (xem [02 §5](plans/02-contracts.md)). Vượt giới hạn có thể từ chối connection, trả SUBACK failure hoặc đóng connection; đây không phải application ACK và broker không tự retry.

## Ví dụ client

Các lệnh dưới dùng `mosquitto_pub`/`mosquitto_sub` chỉ làm ví dụ; dự án không yêu cầu các công cụ này.

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

Với client command, subscribe `devices/$DEVICE_ID/commands`, thực thi command, rồi publish ACK với cùng `command_id`.
