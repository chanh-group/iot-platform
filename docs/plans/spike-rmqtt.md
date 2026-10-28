# Spike RMQTT 0.24.0 — Phase 0

- Ngày chạy: 2026-10-28
- Môi trường: Linux, `rustc 1.98.0`, `cargo 1.98.0`, Rust edition 2024
- Repo: `ThingsBoardRust`, commit gốc `0bc1bbc` + working tree Phase 0
- Phạm vi: crate throwaway trong `spikes/`; **không** merge vào `src/`, không phải code sản phẩm
- Kết luận: **PASS — bắt đầu Phase 1**

Sáu cổng bắt buộc trong `docs/plans/README.md` đều đạt. Các khác biệt contract được ghi đầy đủ ở §5; không có cổng nào fail.

## 1. Cách chạy lại

```sh
cd spikes

cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all

# Từng cổng
cargo test -p rmqtt-spike --test auth_hooks
cargo test -p rmqtt-spike --test protocol_reject
cargo test -p rmqtt-spike --test acl_cross_device
cargo test -p rmqtt-spike --test server_publish_kick
cargo test -p rmqtt-spike --test retained
cargo test -p rmqtt-spike --test session_expiry
cargo test -p rmqtt-spike --test shutdown
cargo test -p rmqtt-spike --test smoke_qos_lwt

# All-in-one
cargo run -p allinone-spike
```

Kết quả đã chạy:

| Lệnh | Kết quả |
| --- | --- |
| `cargo fmt --all --check` | pass, không output |
| `cargo clippy --all-targets --all-features -- -D warnings` | pass, không warning |
| `cargo test --all` | 13 test pass, 0 fail |
| `cargo run -p allinone-spike` | self-check pass |

## 2. Bảng pass/fail

| # | Cổng | Test | Kết quả | Bằng chứng chính | Pass |
| --- | --- | --- | --- | --- | --- |
| 1 | Hook auth/connect/connected/disconnected + subscribe/publish ACL + publish | `cargo test -p rmqtt-spike --test auth_hooks` | 1 passed | Hook thấy `client_id`, `username`, `proto_ver=4`, `clean_session`, `session_id`, `created_at`; sai token và `client_id != username` bị từ chối, không tạo session mới | ✅ |
| 2 | Hook thấy protocol/topic/QoS/retain; từ chối MQTT 3.1/5 | `cargo test -p rmqtt-spike --test protocol_reject` | 1 passed | Level 3 → CONNACK `0x05`; level 5 → CONNACK `0x87`; không session nào được tạo | ✅ |
| 3 | ACL cross-device, deny không route/không ingest | `cargo test -p rmqtt-spike --test acl_cross_device` | 1 passed | SUBACK `0x80` cho topic B; publish A → B bị đóng kết nối, B không nhận PUBLISH; `MessagePublish` chạy trước `MessagePublishCheckAcl` | ✅ |
| 4 | `entry.publish` QoS 1 + `entry.kick` + duplicate client ID | `cargo test -p rmqtt-spike --test server_publish_kick` | 1 passed | Server publish tới A đúng topic/payload/QoS 1; client ID trùng thay connection cũ; `kick` đóng session; rotate token cũ bị từ chối, token mới kết nối được | ✅ |
| 5 | Retained: set/replace/clear/cap/restart | `cargo test -p rmqtt-spike --test retained` | 1 passed | `status` retained được subscriber mới nhận; replace đổi byte; payload rỗng xóa; cap 1 topic/8 byte từ chối publish mới (không PUBACK); restart cùng port không còn retained | ✅ |
| 6 | Session expiry cho `clean_session=0` | `cargo test -p rmqtt-spike --test session_expiry` | 1 passed | Reconnect trong 2 s: `session_present=true`, subscription còn, command được giao; sau expiry: `session_present=false` | ✅ |
| 7 | `MqttServer::run()` spawn/abort, nhả port, không accept mới | `cargo test -p rmqtt-spike --test shutdown` | 1 passed | Sau abort, connect tới port cũ fail; bind lại đúng port thành công; không panic khi còn client/session | ✅ |
| 8 | SQLx 0.9 migrate + SQLite WAL/FULL + Axum asset/WS + RMQTT cùng process | `cargo run -p allinone-spike` | pass | `journal=wal synchronous=2 fk=1 migrations=1 devices=1 ws=ping mqtt_payload=from-server` | ✅ |
| 9 | Smoke QoS 0/1/2, wildcard, LWT | `cargo test -p rmqtt-spike --test smoke_qos_lwt` | 2 passed | QoS 0/1/2 giao đúng; `devices/A/data/#` nhận đủ; LWT retained `offline` được subscriber mới nhận | ✅ |

Test unit bổ sung trong `spikes/rmqtt/src/lib.rs`:

- `acl_publish_matrix`
- `acl_subscribe_matrix`
- `ingest_only_live_business_topics`

Cả 3 pass.

## 3. Bằng chứng chi tiết

### 3.1. Hook log thực tế

Chạy `RUST_LOG=info cargo run -p rmqtt-spike`, kết nối MQTT 3.1.1 bằng device A và publish `devices/A/status`:

```text
INFO rmqtt_spike] hook connect client_id=019c0e1e-0000-7000-8000-00000000000a username=Some("019c0e1e-0000-7000-8000-00000000000a") proto=4 (MQTT 3.1.1) clean_session=true
INFO rmqtt_spike] hook authenticate client_id=019c0e1e-0000-7000-8000-00000000000a username=Some("019c0e1e-0000-7000-8000-00000000000a") proto=4 (MQTT 3.1.1) password_ok=true
INFO rmqtt_spike] hook connected client_id=019c0e1e-0000-7000-8000-00000000000a session_id=0@127.0.0.1:18883/127.0.0.1:36770/019c0e1e-0000-7000-8000-00000000000a/019c0e1e-0000-7000-8000-00000000000a/1793204349789 session_present=false
INFO rmqtt_spike] hook publish client_id=Some("019c0e1e-0000-7000-8000-00000000000a") from=Custom topic=devices/019c0e1e-0000-7000-8000-00000000000a/status qos=0 retain=false
INFO rmqtt_spike] hook publish_acl client_id=019c0e1e-0000-7000-8000-00000000000a topic=devices/019c0e1e-0000-7000-8000-00000000000a/status qos=0 retain=false allowed=true ingest=false
INFO rmqtt_spike] hook disconnected client_id=019c0e1e-0000-7000-8000-00000000000a session_id=0@127.0.0.1:18883/127.0.0.1:36770/019c0e1e-0000-7000-8000-00000000000a/019c0e1e-0000-7000-8000-00000000000a/1793204349789 reason=Reasons([ConnectRemoteClose])
```

Session ID chứa node/listener, địa chỉ client, `client_id`, `username` và `create_time`; `create_time` đổi theo từng connection nên dùng được làm connection generation.

### 3.2. CONNACK trên wire

Probe TCP thô, không qua thư viện client:

```text
MQTT 3.1.1, token đúng: 20 02 00 00   (CONNACK, session_present=0, code=0)
MQTT 3.1   (level 3):    20 02 00 05   (refused, code=5 NotAuthorized)
MQTT 5     (level 5):    20 03 00 87 00 (v5 CONNACK, reason=0x87 NotAuthorized)
```

Đây là khác biệt contract quan trọng nhất; xem §5.

### 3.3. ACL trên wire

```text
SUBACK devices/B/commands -> 90 03 00 01 80   (failure cho filter B)
SUBACK devices/A/commands -> 90 03 00 02 01   (success QoS 1 cho filter A)
QoS 1 publish devices/B/data/attack -> socket đóng, không có PUBACK
```

Log tương ứng:

```text
hook subscribe_acl ... filter=devices/.../commands allowed=false
hook subscribe_acl ... filter=devices/.../commands allowed=true
hook publish ... topic=devices/.../data/attack qos=1 retain=false
hook publish_acl ... topic=devices/.../data/attack qos=1 retain=false allowed=false ingest=false
hook disconnected ... reason=Reasons([MqttError(PublishAckReason(NotAuthorized, "PublishRefused"))])
```

Kết luận: publish ACL deny đóng connection; subscribe ACL deny trả SUBACK failure nhưng giữ connection. Message bị deny không được route và không tạo ingest event.

Retained cap probe (cùng broker, `max_retained_messages=1`, `max_retained_bytes=8`):

```text
first retained PUBACK            40020001
over-cap retained publish        <closed>, không PUBACK
```

### 3.4. API RMQTT đã dùng

Đăng ký hook:

```rust
let register = scx.extends.hook_mgr().register();
for typ in [
    Type::ClientConnect,
    Type::ClientAuthenticate,
    Type::ClientConnected,
    Type::ClientDisconnected,
    Type::ClientSubscribeCheckAcl,
    Type::MessagePublishCheckAcl,
    Type::MessagePublish,
] {
    register.add_priority(typ, 10, Box::new(hooks.clone())).await;
}
register.start().await;
```

Server publish QoS 1:

```rust
let id = Id::new(self.scx.node.id(), 0, None, None, client_id.into(), None);
let from_id = Id::new(self.scx.node.id(), 0, None, None, "$server".into(), None);
let publish = Publish::new(
    Box::new(CodecPublish {
        dup: false,
        retain: false,
        qos: QoS::AtLeastOnce,
        topic: topic.into(),
        packet_id: None,
        payload: Bytes::copy_from_slice(payload),
        properties: None,
    }),
    None, None, None,
);
self.scx.extends.shared().await
    .entry(id)
    .publish(From::from_system(from_id), publish)
    .await
```

Kick session:

```rust
let id = Id::new(self.scx.node.id(), 0, None, None, client_id.into(), None);
let mut entry = self.scx.extends.shared().await.entry(id);
entry.kick(true, true, true).await?;
```

Retained storage có cap:

```rust
// DefaultRetainStorage::enable() == false; set/get là no-op.
self.inner.set_with_timeout(topic, retain, expiry_interval).await?;
self.inner.get_message(topic_filter).await
```

`BudgetedRetainStorage` giữ map topic → byte, kiểm tra `max_messages`/`max_bytes`, trừ byte cũ khi replace và xóa khi payload rỗng.

Lifecycle:

```rust
let task = tokio::spawn(async move {
    MqttServer::new(scx).listener(listener).build().run().await
});
// shutdown
task.abort();
```

### 3.5. All-in-one

```text
$ cargo run -p allinone-spike
allinone ok: http=127.0.0.1:34079 mqtt=127.0.0.1:34659 journal=wal synchronous=2 fk=1 migrations=1 devices=1 ws=ping mqtt_payload=from-server
```

Kiểm tra trong cùng process:

- SQLx migrate chạy 1 migration (`devices`).
- `PRAGMA journal_mode=wal`, `PRAGMA synchronous=2` (FULL), `foreign_keys=1`.
- Axum phục vụ asset nhúng `index.html`.
- WebSocket `/ws` echo `ping`.
- RMQTT nhận client MQTT 3.1.1 và server publish command QoS 1 tới client.

## 4. Kết quả test đầy đủ

```text
allinone_spike: 1 passed
rmqtt_spike unit: 3 passed
acl_cross_device: 1 passed
auth_hooks: 1 passed
protocol_reject: 1 passed
retained: 1 passed
server_publish_kick: 1 passed
session_expiry: 1 passed
shutdown: 1 passed
smoke_qos_lwt: 2 passed
```

Tổng: **13 passed, 0 failed**. Không có test ignored.

## 5. Khác biệt contract và giới hạn đã biết

| # | Contract (`docs/plans/02-contracts.md`) | Thực tế RMQTT 0.24 | Xử lý |
| --- | --- | --- | --- |
| 1 | §2.1: MQTT 3.1 (level 3) → CONNACK `0x01` | `0x05` (NotAuthorized) | **Đã chốt:** contract dùng mã RMQTT thực tế `0x05`; không fork/patch. Gate chỉ yêu cầu “từ chối”, nên không fail Phase 0. |
| 2 | §2.1: MQTT 5 → CONNACK `0x84` nếu mã hóa được, nếu không đóng kết nối | v5 CONNACK `0x87` (NotAuthorized) | **Đã chốt:** contract dùng `0x87`; không fork/patch. |
| 3 | §2.1: `client_id != username` → CONNACK `0x02` | `0x04` (BadUserNamePassword) | **Đã chốt:** contract dùng `0x04`; không phân biệt với credential sai. |
| 4 | Không quy định thứ tự `MessagePublish` và ACL | `MessagePublish` chạy **trước** `MessagePublishCheckAcl` | Không đặt ingest trong `MessagePublish`; ingest quyết định trong/after `MessagePublishCheckAcl` (spike đã làm). Ghi rõ trong contract/Phase 1. |
| 5 | §2.5: retained vượt cap → từ chối retained mới; metric + log | `RetainStorage::set` trả lỗi; RMQTT đóng session, không PUBACK; wrapper chưa phát metric/log | **Đã chốt:** contract quy định product hook phải ghi metric/log `retained_cap`, sau đó chấp nhận disconnect/no PUBACK; client phải reconnect với backoff. |
| 6 | §2.5: shutdown/abort không accept connection mới, không panic | `task.abort()` đóng listener và nhả port; **không** tự đóng các session đang mở | **Đã chốt:** shutdown wrapper kick session trước abort listener. |
| 7 | §2.4: ACL deny — contract chỉ nói publish bị đóng kết nối | Subscribe deny trả SUBACK failure và giữ connection; publish deny đóng connection | Hành vi publish khớp contract; bổ sung câu cho subscribe nếu muốn chốt. |
| 8 | §2.5: session expiry mặc định 1 giờ | Cơ chế chứng minh với TTL 2 s; listener production đặt `3600 s`, cap `3600 s` | Test dùng TTL ngắn theo yêu cầu; Phase 1 dùng config thật, không hard-code. |
| 9 | §2.4: ACL kiểm tra cả lúc broker giao message | `MessageDelivered` chỉ thay payload, không có kết quả allow/deny | **Đã chốt:** chặn ở SUBSCRIBE; filter allow luôn nằm trong `devices/{self}/` nên delivery không thể vượt prefix thiết bị. |

Không phải khác biệt, nhưng **chưa kiểm chứng end-to-end** trong Phase 0 (đã cấu hình trong `Builder`, sẽ test ở Phase 1/A19):

- `max_connections=1000`, `max_subscriptions=8`, `max_inflight=32`, `max_mqueue_len=256`, `max_packet_size=256 KiB`, `max_clientid_len=64`, `max_topic_levels=16`.
- `$SYS`/`$share` mới được test ở ACL unit, chưa qua hook end-to-end.
- Chưa test tải/đồng thời nhiều client; chưa test TLS (ngoài MVP, dùng reverse proxy).

## 6. Quyết định

**PASS — bắt đầu Phase 1.**

Lý do:

- Cả 6 cổng bắt buộc trong `docs/plans/README.md` đạt.
- Không có gate nào fail; không có trạng thái “gần pass”.
- Retained cap/replacement/clear/restart và session expiry có test tái lập.
- Shutdown không panic, không accept connection mới, nhả port.
- All-in-one SQLx + SQLite WAL/FULL + Axum + RMQTT chạy trong một process.

Các quyết định bắt buộc trước MQTT auth đã được chốt trong contract: mã CONNACK thực tế, retained-cap disconnect/metric/log và kick trước abort. Việc còn lại cho Phase 1/A19 là đưa các limit `max_*` vào test, thay vì chỉ cấu hình.

Rủi ro còn lại:

- RMQTT 0.x có thể đổi API; pin `=0.24.0` và cô lập trong adapter MQTT.
- Không có graceful stop tích hợp; phải tự quản lý session khi shutdown.
- Retained vượt cap đóng client có thể gây reconnect storm nếu device không backoff; cần metric/log và hướng dẫn client.
- `DefaultRetainStorage` không dùng được trực tiếp; wrapper `BudgetedRetainStorage` hiện là code spike, phải viết lại cẩn thận trong product (đặc biệt counter khi replace/clear và race giữa các publish retained).
