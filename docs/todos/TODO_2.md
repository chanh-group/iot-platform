# TODO — Ngày 3 / Phase 2: MQTT và telemetry
TODO dựa trên contract hiện tại; mọi khác biệt RMQTT phải cập nhật contract/ADR trước khi code.

Mục tiêu: hoàn thành luồng **device → MQTT 3.1.1 → validate → SQLite commit → application ACK → REST/WebSocket** với ACL kín và tài nguyên hữu hạn. Phase này đạt A03, A05–A07 và matrix A19; A04 phải pass ở unit + integration. **Chưa làm** device CRUD/rotate credential, command, runtime PATCH, dashboard Svelte, Docker hay crash/shutdown gate đầy đủ — chúng thuộc Phase 3/4.

Nguồn sự thật:

- `docs/plans/01-stack-and-architecture.md` — RMQTT 0.24.0, kiến trúc adapter/core, bounded channel và ADR.
- `docs/plans/02-contracts.md` — MQTT §2, SQLite §3, HTTP/WS §4, test vector §7, kết quả spike §9.
- `docs/plans/03-delivery-plan.md` §5, §9 — task P2-01..P2-13, acceptance mapping và harness.
- `docs/plans/spike-rmqtt.md` — API RMQTT đã xác nhận; chỉ dùng làm tham chiếu API, không merge code spike.

## 0. Điều kiện vào

- [x] Phase 0 PASS ngày 2026-10-28; RMQTT `=0.24.0`, feature `retain`, chạy library mode trong cùng process.
- [x] Phase 1 PASS ngày 2026-10-29; 63 test pass, 0 fail; config, SQLite WAL/FULL, admin/session, health/readiness, supervisor và harness đã sẵn sàng.
- [x] Đọc lại `02` §2, §3.3–§3.5, §4.4, §4.8, §7.1 và §9 trước khi code.
- [x] Giữ chính xác các kết quả spike đã chốt:
  - MQTT level 3 bị từ chối CONNACK `0x05`; MQTT 5 là `0x87`; `client_id != username` là `0x04`.
  - Publish ACL deny đóng connection, không PUBACK/route/ingest; subscribe deny trả SUBACK `0x80`, giữ connection.
  - `MessagePublish` chạy trước `MessagePublishCheckAcl`; tuyệt đối không enqueue/ghi SQL trong hook `MessagePublish`.
  - `entry.publish` dùng principal `$server`, QoS 1; shutdown về sau phải kick session trước khi abort listener.
  - Retained cap dùng `BudgetedRetainStorage`; replace/clear phải cập nhật byte/topic đúng, vượt cap ghi metric/log `retained_cap` rồi RMQTT đóng connection.
- [ ] Không đổi broker/version/feature, protocol MQTT, SQLite durability hoặc contract nếu chưa cập nhật ADR/contract trước.

## 1. MQTT adapter và lifecycle — P2-01

- [x] Thêm `rmqtt = "=0.24.0"`, `default-features = false`, feature `retain`; không bật `plugin`, `grpc`, `tls`, `ws`, `msgstore`.
- [x] Tạo `src/mqtt/{mod,server,config}.rs`; mọi type/API RMQTT chỉ ở `src/mqtt/`, không rò vào `core`.
- [x] Build `ServerContext` và listener từ cấu hình: bind address, anonymous=false, max connections/packet/inflight/queue/subscriptions, client ID length, topic levels, session expiry và QoS 2.
- [x] Spawn `MqttServer::run()` dưới supervisor; listener chết → readiness false và shutdown có kiểm soát.
- [x] Chuẩn bị interface `DeviceChannel` tối thiểu cho server publish/kick/is_online; chưa làm command dispatcher trong Phase 2.
- [x] Test boot: MQTT listener thật nhận connection trên port test, `/readyz` phản ánh task MQTT; không dùng port cố định.

## 2. Auth thiết bị và presence — P2-02, P2-04

- [x] Bổ sung repository lookup thiết bị/credential active: device enabled, SHA-256 token BLOB và so sánh constant-time; không log token/hash/password.
- [x] Hook `ClientConnect` chỉ chấp nhận protocol level 4; từ chối 3/5 bằng mã RMQTT đã chốt. Ghi chú: `ClientConnect` không đặt được CONNACK trong RMQTT 0.24, nên gate nằm ở `ClientAuthenticate` (`NotAuthorized` → `0x05`/`0x87`).
- [x] Hook `ClientAuthenticate`: bắt buộc `username = client_id = UUID v7 canonical`; sai/missing token, unknown/disabled device hoặc credential revoked đều trả `0x04`, không lộ nguyên nhân.
- [x] Không tin device ID trong payload; principal xác thực là nguồn danh tính duy nhất.
- [x] Tạo `src/core/presence.rs` và `src/mqtt/presence.rs`: ghi online/offline trong RAM, connection generation lấy từ session id/create time; disconnect cũ không được ghi đè connection mới khi duplicate client ID.
- [x] Chỉ cập nhật `devices.last_seen_at` trực tiếp trong hook connect/disconnect để tránh write amplification. Đây là low-volume MVP; DB chậm làm chậm hook/CONNACK của client đó nhưng không chặn listener hay client khác. Lỗi ghi chỉ log + metric, không từ chối connect và không làm readiness false. `shortcut: last_seen_at ghi inline trong hook, chuyển sang presence channel nếu connect/CONNACK p95 hoặc hook backlog xấu dưới tải.`
- [x] Test `tests/mqtt_auth.rs`: token đúng/sai, unknown, disabled, revoked, client-id mismatch, MQTT 3.1/5 reject, duplicate client ID và generation.
- [ ] Đánh dấu A03 chỉ pass khi sai token và device disable đều bị từ chối, không thể ghi telemetry. — phần auth đã pass (refused, 0 session/presence); vế "không có row telemetry" chờ ingest ở mục 4.

## 3. ACL deny mặc định — P2-03

- [x] Viết hàm thuần `src/core/acl.rs`, table test toàn bộ matrix `02` §2.3–§2.4; adapter `src/mqtt/acl.rs` chỉ map type RMQTT vào hàm này.
- [x] Publish chỉ cho phép topic cụ thể của principal: `telemetry`, `attributes`, `commands/ack`, `status`, `data/{...}`; không wildcard, không `$SYS`, `$share`, server-only topic hoặc device khác.
- [x] Subscribe chỉ cho phép `telemetry/ack`, `commands`, `status`, `data/#`/filter con thuộc prefix chính device; cấm `devices/{self}`, `devices/{self}/#` và mở rộng sang topic nghiệp vụ khác.
- [x] Wildcard không mở quyền chéo thiết bị: RMQTT 0.24 không có hook deny lúc giao message (`MessageDelivered` chỉ thay payload), nên enforcement nằm ở SUBSCRIBE; mọi filter allow đều nằm trong `devices/{self}/`. Đã ghi vào `02` §2.4 và `spike-rmqtt.md` §5 #9.
- [x] Publish bị deny không route/không broadcast (không có SQL/ingest để gọi ở phase này); ingest ở §4 phải nối sau `MessagePublishCheckAcl`, không đặt trong `MessagePublish`.
- [x] Test unit matrix và `tests/mqtt_acl.rs`: T12, T13, T14, cross-device wildcard, `$SYS`, `$share`, server-only topics, publish filter có `+/#` (gửi raw vì rumqttc chặn wildcard ở client). T12 kiểm "không giao cho B"; "không row telemetry" chờ pipeline §4.
- [x] A04 mức Phase 2 chỉ pass khi ACL unit và integration chứng minh device A không thể đọc/ghi dữ liệu B, kể cả wildcard. — đã pass; cổng A04 đầy đủ vẫn chốt ở Phase 4.

## 4. Telemetry validate và bounded ingest — P2-05, P2-06

- [x] Tạo domain payload/validator trong `src/core/telemetry/{mod,payload,validate}.rs`; parse JSON object UTF-8 không trailing data.
- [x] Validate: `event_id` 1..128 `[A-Za-z0-9._:-]`; `values` 1..max_keys, key 1..64 hợp lệ/không `$`; value chỉ finite number/bool/string trong limit; payload và timestamp đúng policy.
- [x] Ghi `received_at` tại server; khi thiếu `ts`, dùng đúng `received_at`; payload gửi device ID bị bỏ qua.
- [x] Tính SHA-256 trên raw payload bytes nhận từ broker; retry chỉ duplicate khi byte-identical.
- [x] Bounded ingest queue có capacity `telemetry.queue_capacity`; MQTT hook dùng `try_send`, không `unbounded_channel`, không chờ/block hook. Queue đầy → metric + ACK `retryable/queue_full`, không ACK thành công. ACK publisher có queue bounded 256; queue ACK đầy drop ACK + metric, device retry; reason labels được exporter bổ sung ở P2-13.
- [x] Worker duy nhất gom batch theo `batch_size`, `max_batch_bytes`, `flush_interval_ms`; `BEGIN IMMEDIATE`, transaction ngắn, không giữ transaction khi publish ACK/WS.
- [x] Repository telemetry insert `(device_id,event_id)`; unique conflict đọc hash cũ: hash trùng → `duplicate`, khác → `rejected/event_id_conflict`; ACK `stored`/`duplicate` chỉ sau commit.
- [x] DB timeout/disk/full lỗi → readiness false, retry/backoff, `retryable/storage_unavailable`, RAM vẫn hữu hạn. Không làm test disk-full/crash gate đầy đủ ở đây (Phase 4), nhưng phải có path xử lý không panic.
- [x] Test T1–T10 trong `tests/telemetry_flow.rs` và validator/unit tests: row/ACK/status/code/received_at/raw-hash; rejected không tạo row.

## 5. Application ACK, attributes và retained — P2-07, P2-08, P2-09

- [ ] `src/mqtt/server_publish.rs`: ACK telemetry tới `devices/{id}/telemetry/ack`, principal `$server`, QoS 1, non-retained; payload không chứa secret/raw telemetry.
- [ ] ACK `rejected` sau validation và `retryable` khi xác định được event ID; không tạo ACK thành công trước commit. Device không subscribe ACK thì retry được duplicate.
- [ ] Reject retained telemetry: không enqueue/SQL, ACK `rejected/retained_not_allowed` khi event ID hợp lệ (T11).
- [ ] Attributes: validate cùng giới hạn values/payload, chỉ non-retained; upsert latest per `(device_id,key)` trong transaction ngắn; lỗi drop + metric, không application ACK/history.
- [ ] Viết lại `BudgetedRetainStorage` trong product, không copy nguyên spike: serialize update map topic→bytes, trừ giá trị cũ khi replace, trừ toàn bộ khi payload rỗng clear, không âm/race.
- [ ] Dùng retained storage này thay default no-op; cap 0 cấm retained. Vượt cap: metric/log redact payload, reason `retained_cap`, để RMQTT đóng connection/no PUBACK.
- [ ] Test retained: T15 status retained không đổi presence; clear/replace; cap T16 với limit test nhỏ; restart không giữ retained. Test attributes: latest/upsert và invalid payload không ghi.

## 6. Telemetry REST, events WebSocket và metrics — P2-10, P2-11, P2-13

- [ ] `GET /api/v1/devices/{id}/telemetry`: auth admin, `from`/`to` bắt buộc `[from,to)`, range ≤31 ngày, limit/cursor opaque đúng contract; filter `keys` tối đa 16 và chỉ trả event có ít nhất một key khớp.
- [ ] `GET /api/v1/devices/{id}/telemetry/latest`: latest xác định theo `(ts, received_at, internal_id)`, sort key tăng; không thêm projection/cache trước benchmark.
- [ ] Test `tests/telemetry_api.rs`: history ordering/cursor, key filter, latest tie-break, missing device, cursor/range invalid (H9/H10), không lộ SQL/error nội bộ.
- [ ] Tạo EventBus broadcast best-effort và WS `/api/v1/events`: cookie + Origin validation có sẵn, gửi `hello`, subscribe/unsubscribe/ping theo contract; telemetry chỉ publish event **sau SQL commit**.
- [ ] Bounded broadcast/per-client WS queues; lag/full đóng 1013. Chỉ cần telemetry/hello/subscription trong Phase 2; command/config/system đầy đủ thuộc Phase 3/4.
- [ ] Test `tests/ws_telemetry.rs`: subscribe device đúng nhận committed event, event không phát trước/không có khi validation/commit fail, subscribe sai bị lỗi, reconnect dùng REST history để bù.
- [ ] Hoàn thiện `/metrics` theo contract: mặc định 404; khi bật chỉ Bearer token env riêng (≥32 byte), không cookie/session, token không lộ config/log. Có connection active, ingest accepted/rejected reason, queue depth, commit latency/errors; không label device ID.
- [ ] Test `/metrics` auth/tắt/bật và metric telemetry chính; không thêm Prometheus dependency ngoài stack đã chốt nếu registry skeleton hiện có đáp ứng được.

## 7. Protocol suite và acceptance — P2-12

- [ ] Mở rộng `tests/common/mod.rs`: helper tạo device/credential qua fixture/repository tạm, `rumqttc` MQTT client, subscribe/publish/wait ACK và WS client; mỗi test data dir/HTTP/MQTT port riêng, retry race port tối đa 3 lần.
- [ ] `tests/mqtt_protocol.rs` chạy matrix A19: QoS 0/1/2, wildcard hợp lệ, retained, LWT, clean-session reconnect/expiry, duplicate client ID, MQTT 3.1/MQTT 5 reject.
- [ ] Matrix phải kiểm chứng các limit `max_*` đã cấu hình (packet, connection/subscription/inflight/queue/client-id/topic-level) ở mức có thể tái lập; không chỉ tin builder setup từ spike.
- [ ] A05: telemetry hợp lệ commit SQLite, ACK `stored`, REST và WS thấy dữ liệu đã commit.
- [ ] A06: retry byte-identical cùng event ID giữ một row, ACK `duplicate`.
- [ ] A07: cùng event ID/raw payload khác giữ row cũ, ACK `rejected/event_id_conflict`.
- [ ] Test retained/session state rỗng sau restart; persistence telemetry vẫn là SQL. Không claim broker session/retained survive restart.

## 8. Kiểm tra cuối Phase 2

Chạy bắt buộc:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

Checklist gate:

- [ ] A03 pass: token sai/device disabled không kết nối hay ghi telemetry.
- [ ] A05, A06, A07 pass; không ACK `stored`/`duplicate` trước commit.
- [ ] A04 pass ở unit + integration, gồm wildcard/cross-device.
- [ ] A19 matrix MVP pass: QoS 0/1/2, wildcard, retain/LWT, session reconnect/expiry, duplicate client ID, MQTT 3.1/5 reject.
- [ ] Retained cap hoạt động; restart retained rỗng; counter replace/clear chính xác.
- [ ] Queue full không làm RAM tăng vô hạn, không ghi/ACK thành công giả, có metric `ingest_rejected_total{reason="queue_full"}`.
- [ ] `/readyz` phản ánh MQTT listener/telemetry worker/DB; lỗi không lộ secret/payload/DB URL.
- [ ] `cargo fmt`, clippy `-D warnings`, toàn bộ test sạch; commit `Cargo.lock` nếu dependency Phase 2 được thêm.

## 9. Không làm trong Phase 2

- Device CRUD HTTP, create/disable/credential rotate và audit mutation đầy đủ (Phase 3). Fixture/test có thể tạo device trực tiếp qua repository; không tạo API tạm.
- Command state machine, dispatcher, command ACK (Phase 3).
- Runtime config PATCH, revision/atomic write và reload hot fields (Phase 3).
- Dashboard Svelte/uPlot, asset embed release hoàn chỉnh (Phase 3).
- Crash recovery kill -9, disk-full test, graceful MQTT drain/kick shutdown gate, security-suite tổng thể, Docker/backup/load/release (Phase 4).
- PostgreSQL/MySQL, MQTT 5, integrated TLS, cross-device pub/sub, aggregation/projection telemetry (sau MVP hoặc Phase 5).

## 10. Commit

- [ ] Commit nhỏ, mỗi commit build/test được; gợi ý: `feat(mqtt): adapter auth and acl`, `feat(telemetry): ingest dedup and ack`, `feat(http): telemetry history and events`, `test: phase 2 protocol suite`.
- [ ] Không commit `target/`, `data/`, `web/dist/`, log hoặc token/credential fixture thật.
- [ ] Nếu một contract/RMQTT behavior khác spike: dừng task liên quan, cập nhật `02` và ADR trước; không tự điều chỉnh ngầm trong code.
