# TODO — Ngày 1: Phase 0 / RMQTT technical spike

Mục tiêu hôm nay: quyết định có thể dùng RMQTT 0.24.0 cho MVP hay không. Đây là code thử nghiệm; **không** đưa spike vào `src/` hay merge như code sản phẩm.

Nguồn sự thật: `plans/01-stack-and-architecture.md` (ADR-001), `plans/02-contracts.md` (§2 MQTT), `plans/03-delivery-plan.md` (§3).

## Kết quả phải có trước khi kết thúc ngày

- [x] `spikes/rmqtt/` build và chạy được với `rmqtt = "=0.24.0"`, feature `retain`.
- [x] `docs/plans/spike-rmqtt.md` có bảng pass/fail, command chạy lại được, log/API snippet và khác biệt so với contract.
- [x] Có kết luận rõ: **PASS — bắt đầu Phase 1**, hoặc **FAIL — dừng và mở ADR broker mới**.

Không có trạng thái “gần pass”: thiếu bất cứ gate nào thì Phase 0 fail.

## Checklist thực hiện

### 1. Chuẩn bị spike

- [x] Tạo crate throwaway `spikes/rmqtt`; pin exact version và commit `Cargo.lock` của spike.
- [x] Cấu hình listener loopback trên port ngẫu nhiên hoặc `18883`, không dùng port production `1883`.
- [x] Bật log đủ verbose để thấy client ID, username, session ID, protocol level, topic, QoS và retain.
- [x] Tạo hai device giả A/B với credential hard-code **chỉ trong spike**.
- [x] Viết một test/client nhỏ bằng `rumqttc` để tái lập từng case; không thử bằng MQTT Explorer thủ công như evidence duy nhất.

### 2. Hooks, xác thực và protocol

- [x] Đăng ký và chạy được hook: authenticate, connect, connected, disconnected, subscribe ACL, publish ACL và publish.
- [x] Xác nhận hook nhận đúng `client_id`, username, password, session ID, protocol level, topic, QoS, retain.
- [x] MQTT 3.1.1 (level 4), `client_id == username`, token đúng: kết nối thành công.
- [x] MQTT 3.1 (level 3) trả `0x05`, MQTT 5 (level 5) trả `0x87`, `client_id != username` trả `0x04`; tất cả bị từ chối và không còn session mới.

### 3. ACL và giao nhận

- [x] Implement ACL tối thiểu theo `plans/02-contracts.md` §2.4: deny mặc định, chỉ topic của chính device.
- [x] Device A không publish được `devices/B/telemetry`.
- [x] Device A không subscribe được `devices/B/commands` hoặc `devices/+/telemetry`.
- [x] Deny không route message và không tạo ingest event; ghi chính xác RMQTT chọn disconnect hay drop.
- [x] Server principal publish QoS 1 đến device A đang online qua `entry.publish`.
- [x] `entry.kick` đóng đúng session sau rotate/disable giả lập.
- [x] Duplicate client ID thay connection cũ; log được thứ tự hook/session ID để dùng làm connection generation.

### 4. Broker state

- [x] Retained `devices/A/status` được subscriber mới nhận.
- [x] Retained replacement và zero-length clear hoạt động.
- [x] Cap retained theo số topic và byte: hook ghi metric/log `retained_cap`, RMQTT đóng connection không PUBACK; counter không âm khi replace/clear.
- [x] Restart spike không giữ retained state (đúng scope MVP).
- [x] `clean_session=0` reconnect hoạt động; session expiry tối đa 1 giờ được chứng minh hoặc có wrapper cụ thể, test được.
- [x] Smoke QoS 0/1/2, wildcard hợp lệ, LWT và session reconnect pass.

### 5. Lifecycle và all-in-one feasibility

- [x] `MqttServer::run()` chạy trong task; abort nhả port/không accept mới nhưng không đóng session; graceful shutdown kick session trước abort.
- [x] Không panic khi shutdown trong lúc có client/session.
- [x] Spike riêng `spikes/allinone/`: SQLx 0.9 migration + SQLite WAL với `synchronous=FULL`, Axum asset/WS echo và RMQTT cùng một process.

## Mẫu evidence trong `docs/plans/spike-rmqtt.md`

| Gate | Command/test | Kết quả | Bằng chứng / API thực tế | Pass |
| --- | --- | --- | --- | --- |
| Auth + hooks | `cargo test auth_hooks` | … | hook type, fields | ✅/❌ |
| MQTT 3.1/5 reject | `cargo test protocol_reject` | … | CONNACK/close | ✅/❌ |
| ACL A → B | `cargo test acl_cross_device` | … | drop/disconnect | ✅/❌ |
| Server publish / kick | `cargo test server_publish_kick` | … | method signature | ✅/❌ |
| Retain + cap | `cargo test retained` | … | replacement/clear/cap | ✅/❌ |
| Session expiry | `cargo test session_expiry` | … | RMQTT API/wrapper | ✅/❌ |
| Lifecycle | `cargo test shutdown` | … | port closed/no panic | ✅/❌ |
| All-in-one | `cargo run -p allinone-spike` | … | endpoint + DB | ✅/❌ |

## Definition of done / quyết định

Chỉ đánh dấu **PASS** nếu toàn bộ mục bên dưới đúng:

- [x] Sáu gate trong `plans/README.md` đều pass.
- [x] MQTT 3.1 và MQTT 5 bị từ chối; ACL cross-device không giao message.
- [x] Retained cap/replacement/clear/restart và session expiry có evidence tái lập được.
- [x] Shutdown không panic và không accept connection sau stop.
- [x] Không có khác biệt contract chưa được ghi trong báo cáo.

Nếu một mục fail: không bắt đầu Phase 1/2, không tự viết broker MQTT. Ghi failure, mở ADR mới và chỉ thử broker nhúng hoàn chỉnh khác sau khi kế hoạch được cập nhật.
