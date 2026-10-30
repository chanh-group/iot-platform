# TODO — Ngày 4 / Phase 3: Quản trị, command, runtime config, dashboard

TODO dựa trên contract hiện tại; không dùng checklist này để tự thay đổi contract hoặc stack.

Mục tiêu: admin vận hành được MVP qua dashboard; **HTTP → SQL commit → command MQTT → ACK → REST/WS** có vòng đời đúng; rotate/disable ngắt kết nối cũ; runtime PATCH lưu bền và áp dụng nóng an toàn. Phase này chốt **A10–A14**, nối dashboard vào luồng A05 và hoàn thiện client reconnect/backfill của **A15**; gate A15 đầy đủ cùng crash/shutdown/release vẫn thuộc Phase 4.

Nguồn sự thật:

- `docs/SPEC.md` — phạm vi MVP, §5–§10 và acceptance §13.
- `docs/plans/01-stack-and-architecture.md` — stack/version, ADR, hướng phụ thuộc, bounded queues và failure policy.
- `docs/plans/02-contracts.md` — §1, §2.8–§2.10, §3.3–§3.6, §4.3–§4.9, §5 và test vector C1–C10/H1–H8.
- `docs/plans/03-delivery-plan.md` — §6 P3-01..P3-12, §9 test mapping và definition of done.
- `docs/plans/spike-rmqtt.md` — tham chiếu API server publish/kick; không copy code throwaway vào sản phẩm.
- `docs/RUST_STYLE_GUIDE.md` và `AGENTS.md` — chuẩn code và lệnh kiểm tra bắt buộc.
- `docs/todos/TODO_2.md` — hồ sơ gate Phase 2 và các giới hạn evidence đã ghi nhận.

## 0. Điều kiện vào và quy tắc thực thi

- [x] Phase 2 §8 PASS; inflight saturation được thêm ở `261601d`; sửa presence poison/readiness đã commit ở `d338dd7`.
- [x] Chạy lại baseline fmt/Clippy/test trước khi bắt đầu; kiểm tra git status, giữ nguyên công việc chưa commit của người khác. Evidence 2026-10-30: `git status --short --branch` sạch trên `master`; `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings` và `cargo test --all` đều PASS.
- [ ] Đọc lại contract liên quan trước mỗi task; cập nhật `02` trước khi đổi hành vi, thêm ADR trong `01` nếu đổi version/broker/frontend/TLS. Không lấy các dòng trạng thái cũ trong `AGENTS.md`/báo cáo spike làm bằng chứng phủ định gate đã có trong `TODO_2.md`.
- [ ] Dùng mã lỗi theo `02` §4.6: field không tồn tại → **400 `validation_error`**; field tồn tại nhưng không hot-reload → **422 `restart_required`**. Gate tóm tắt trong `03` §6 ghi field lạ 422 không khớp contract; không viết test/code theo dòng tóm tắt đó.
- [ ] Giữ MQTT 3.1.1, SQLite WAL/FULL, một telemetry writer và queue hữu hạn. Không thêm generic framework, projection/cache hoặc dependency ngoài `01` nếu chưa có lý do được ghi nhận.
- [ ] Các đường dẫn mới dưới đây là gợi ý theo delivery plan; tái sử dụng module/helper hiện có, không tạo file/trait chỉ để đủ tên trong checklist.
- [ ] Triển khai audit cùng từng mutation từ đầu; P3-07 ở mục 7 là bước rà soát tổng thể, không phải cho phép trì hoãn audit đến cuối.

## 1. Device admin và attributes REST — P3-01

- [x] Nối domain/repository/HTTP trong `src/core/device.rs`, `src/storage/sqlite/devices.rs`, `src/http/devices.rs`; giữ lookup auth Phase 2 hoạt động. Dùng bảng và index đã có trong migration; không sửa migration đã áp dụng để thêm tính năng. Evidence: `tests/device_api.rs` và MQTT auth tests.
- [x] `GET /api/v1/devices`: `limit` mặc định 50, 1..200; cursor opaque, sort `(created_at DESC, id DESC)`; `q` 2..64 ký tự tìm name/id và filter `enabled` đúng contract. Cursor sai/không khớp truy vấn → `400 invalid_cursor`; không dùng offset. Evidence: `tests/device_api.rs` kiểm tra filter, cursor không khớp và phân trang cùng timestamp theo ID giảm dần.
- [x] `POST /api/v1/devices`: UUID v7 canonical; name trim, 1..64 ký tự Unicode, không control char, unique ASCII case-insensitive; metadata object ≤8 192 byte, depth ≤8, mặc định `{}`; enabled mặc định true. Evidence: `device_create_patch_validate_and_audit` và validator unit test.
- [x] Tạo device + credential active + audit trong một transaction ngắn; chỉ trả 201 và token sau commit. Token `iot_` + 43 ký tự base64url từ 32 byte ngẫu nhiên; DB chỉ lưu SHA-256 BLOB, đúng một credential active/device. Evidence: `device_create_patch_validate_and_audit`; audit failure rollback trong `device_audit_failures_roll_back_create_and_patch`.
- [x] `GET /api/v1/devices/{id}` và list trả active credential summary, không hash/token; online lấy từ presence RAM, không từ status/LWT. Lỗi presence được xử lý, không fallback im lặng thành offline. Evidence: `device_reads_use_live_presence_and_bound_pagination` và `http_presence_failure_stops_mqtt_listener`.
- [x] `PATCH /api/v1/devices/{id}` partial có ít nhất một field; unknown field → 400; metadata thay toàn bộ, không merge; tên trùng → `409 device_name_conflict`, device thiếu → `404 device_not_found`. Evidence: `device_create_patch_validate_and_audit`.
- [x] Disable update + audit → commit → kick theo mục 2; enable không tự reconnect. Nếu cả hai kick path thất bại, áp dụng ngoại lệ fail-closed A20 (không cam kết đóng socket tức thì hoặc thu hồi packet đã queue/in-flight). Không thêm DELETE device vì contract MVP không có endpoint này. Evidence: `tests/device_api.rs::disabling_a_device_kicks_its_live_session_and_blocks_reconnect`; barrier/timeout/fallback/concurrency: `src/mqtt/server.rs::disable_waits_for_every_pre_authorized_handshake_before_commit_and_kick`, `http_timeout_does_not_cancel_a_committed_kick_and_acl_blocks_telemetry`, `both_failed_kicks_fence_a_live_mqtt_session_in_both_directions`, `queued_jobs_for_one_device_do_not_block_an_unrelated_device`; supervisor thoát lỗi tại `src/runtime/supervisor.rs::run`.
- [x] Bổ sung `GET /api/v1/devices/{id}/attributes`: admin chỉ đọc latest key/value/device_ts/updated_at; không thêm API ghi attributes. Evidence: `attributes_are_latest_per_key_and_invalid_or_retained_updates_are_dropped` gửi attributes qua MQTT và kiểm tra REST, timestamp, phân trang >128 key, auth, device thiếu và lỗi storage.
- [x] `tests/device_api.rs`: auth/CSRF, create/get/list/PATCH, boundary validation, unknown/empty PATCH, metadata replace/depth/size, name conflict, pagination/filter, summary redaction và audit failure rollback. Test attributes đọc dữ liệu thực đã ingest. Evidence: `device_create_patch_validate_and_audit`, `device_audit_failures_roll_back_create_and_patch`, `device_reads_use_live_presence_and_bound_pagination`, `pagination_orders_equal_timestamps_by_descending_id`; kick barrier/timeout/fallback evidence ở `src/mqtt/server.rs`; attributes evidence ở `tests/attributes_retained.rs`.

## 2. DeviceChannel, rotate và kick — P3-02

- [x] Implement adapter `DeviceChannel` cho RMQTT: server publish principal `$server`, QoS 1 non-retained, kick session và online check; không để type RMQTT rò vào core. Tái sử dụng đường server publish đã có, không tạo MQTT client nội bộ. Evidence: `src/mqtt/server.rs::device_channel_publishes_server_qos1_non_retained_and_reads_presence`; real kick/token evidence `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token`.
- [x] Nối `PresenceError`/connection limiter failure với readiness và supervisor hiện có; không dùng dữ liệu mutex đã poison. Kiểm tra mọi caller nếu cần điều chỉnh port `DeviceChannel::is_online` để biểu diễn lỗi. Evidence: `src/mqtt/server.rs::http_presence_failure_stops_mqtt_listener`, `presence_failure_stops_real_rmqtt_listener`; `src/mqtt/presence.rs::poisoned_limiter_fails_closed_and_signals_shutdown`, `fatal_failure_keeps_device_fences_until_shutdown` (fail-closed + concurrent re-enable). Port không đổi.
- [x] `POST /api/v1/devices/{id}/credentials/rotate`: `BEGIN IMMEDIATE` → revoke active cũ → insert active mới/`rotated_from` → audit → commit → kick. Device disabled vẫn rotate được; không tồn tại → 404. Evidence: `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token`, `rotates_disabled_device_without_enabling_it`, `missing_device_returns_not_found`.
- [x] Token mới chỉ xuất hiện một lần trong response rotate; token cũ bị từ chối reconnect bằng CONNACK `0x04`. Không trả thành công mutation nếu transaction chưa commit. Evidence: `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token`, `audit_failure_rolls_back_rotation_and_preserves_old_credential`.
- [x] Disable/rotate không giữ transaction khi chờ broker. Kick lỗi phải retry có giới hạn, metric + readiness false theo `02` §2.10; không rollback credential đã commit hoặc ngầm coi kick thất bại là thành công. Evidence: `src/mqtt/server.rs::rotate_timeout_keeps_kick_running_and_retry_recovers_one_time_token`, `failed_rotate_kicks_leave_sql_enabled_but_fence_the_live_session`.
- [x] Test A14/H7: real-MQTT integration tests in `tests/credential_rotate.rs` verify kick, old-token rejection, new-token connect, disabled rotation, and audit rollback; `src/mqtt/server.rs::concurrent_rotations_return_tokens_in_serialized_commit_order`, `rotating_an_unrelated_device_does_not_wait_for_a_blocked_kick`, `rotate_waits_for_pre_authorized_handshake_before_commit_and_kick`, `rotate_timeout_keeps_kick_running_and_retry_recovers_one_time_token`, and `failed_rotate_kicks_leave_sql_enabled_but_fence_the_live_session` cover ordering, cross-device independence, handshake race, cancellation, kick failure, retry, and readiness.
- [x] Test disable online tương tự: kick connection, từ chối reconnect và không nhận thêm telemetry từ session đã bị ngắt. Evidence: `tests/device_api.rs::disabling_a_device_kicks_its_live_session_and_blocks_reconnect`; ACL-after-timeout `src/mqtt/server.rs::http_timeout_does_not_cancel_a_committed_kick_and_acl_blocks_telemetry`.
- [ ] A20 process-level PASS remains unverified: the listed tests run the broker in-process and do not prove child-process exit.

## 3. Command domain, SQL và REST — P3-03, P3-05

- [x] Domain state enum `pending|dispatched|succeeded|failed|expired`; domain types/error và repository port tối thiểu trong `src/core/command/`, SQLite adapter trong `src/storage/sqlite/commands.rs`. Evidence: `core::command::state::tests::parses_only_contract_command_states`, repository port và SQLite adapter được dùng bởi các handler command.
- [x] Validate create: name `^[a-z][a-z0-9_.-]{0,63}$`; params object ≤16 384 byte, mặc định `{}`; TTL 1 000..`commands.max_ttl_ms`, mặc định `default_ttl_ms`; idempotency key tùy chọn 1..128 printable ASCII. Evidence: `core::command::create::tests` và `tests/command_api.rs::create_validates_fields_and_device_state`.
- [x] Lưu hash **raw HTTP body bytes**, không hash JSON đã serialize/chuẩn hóa. Key unique theo device: cùng key + raw hash → command cũ, 202 + `Idempotent-Replayed: true`; khác hash → `409 idempotency_conflict`. Lookup/conflict precedes current TTL validation in the same immediate transaction. Evidence: `tests/command_api.rs::idempotency_uses_raw_request_bytes_and_does_not_audit_replays`, `idempotency_replay_precedes_current_ttl_validation`.
- [x] Kiểm tra device enabled, số command `pending` + `dispatched`, insert command và audit trong cùng transaction; dùng `BEGIN IMMEDIATE` để serialize kiểm tra limit với insert. Device thiếu → 404; disabled → `409 device_disabled`; quá limit → `409 command_queue_full`, `details.limit`. Evidence: `tests/command_api.rs::concurrent_creates_do_not_exceed_pending_queue_limit`, `create_validates_fields_and_device_state`, `audit_failure_rolls_back_command_and_returns_storage_error`.
- [x] `POST /api/v1/devices/{id}/commands`: chỉ trả 202 sau commit, có `Location: /api/v1/commands/{id}` và body đúng `02` §4.5. Evidence: `tests/command_api.rs::create_commits_command_and_redacted_audit_before_returning_202`.
- [x] `GET /api/v1/commands/{id}` và `GET /api/v1/devices/{id}/commands`: đúng field/status, 404, filter status, limit 50/max 200, cursor và sort `(created_at DESC, id DESC)`. Evidence: `tests/command_api.rs::command_list_paginates_filters_and_detail_is_scoped_to_contract_fields`.
- [ ] ACK/dispatcher state writes for `result`, `error`, and late-ACK columns remain in Section 4. Command creation initializes `attempts`, `next_attempt_at`, timestamps and idempotency using the existing schema; no new command schema/table was added.
- [x] `tests/command_api.rs`: malformed/oversize input, auth/CSRF, cursor, raw-byte conflict including semantically equal JSON with different bytes, replay after config TTL max drops, concurrent create versus queue cap (including a dispatched row), replay without duplicate command/audit, audit rollback, pagination/filter and redaction; domain validation/state unit tests also pass.

## 4. Dispatcher, command ACK, expiry và recovery — P3-04

- [ ] Dispatcher query due commands theo index `commands_due_idx`, đọc theo batch hữu hạn; chỉ publish khi device online/enabled và command còn hạn. Offline giữ pending trong SQL, không dựa vào broker offline queue để bảo đảm command delivery.
- [ ] Publish payload `command_id`, name, params, created_at, expires_at tới `devices/{id}/commands`, `$server`, QoS 1, non-retained. Không sinh ID mới khi resend/reconnect/restart.
- [ ] Sau publish, update dispatched_at/attempts/last_attempt_at/next_attempt_at chỉ khi command còn `pending`/`dispatched`; không ghi đè terminal nếu ACK tới trước update dispatch. Không giữ SQL transaction khi publish.
- [ ] Retry/backoff dùng `commands.retry_initial_ms`, `retry_max_ms`, tick `dispatch_interval_ms`; expiry chuyển command quá hạn sang `expired`, không gửi lại lệnh hết hạn. Recheck TTL khi dispatch, không chỉ ở thời điểm query.
- [ ] Nối `commands/ack` sau ACL allow; topic/principal quyết định device ID, không tin ID tự khai trong payload. Retained ACK vẫn bị deny như Phase 2.
- [ ] Bounded command ACK queue capacity 4 096; hook `try_send`, không chờ SQL. Queue đầy → drop + metric, không tăng RAM/task vô hạn; command resend cho phép device ACK lại.
- [ ] Parse ACK: command ID đúng, status chỉ succeeded/failed; result JSON bất kỳ ≤16 384 byte, error ≤512 ký tự; invalid/unknown/cross-device ACK bị bỏ qua + log đã redact, không đổi command.
- [ ] Update ACK có điều kiện theo device/status; ACK lặp không đổi terminal/result/completed_at. ACK sau expires_at ghi `late_ack_at`/`late_ack_json` và giữ command expired, kể cả expiry job chưa kịp chạy.
- [ ] Khởi động lại dispatcher từ SQL cho pending/dispatched chưa hết hạn; không dùng danh sách command RAM làm nguồn sự thật. Dispatcher task chết/panic → readiness/shutdown có kiểm soát; nối cancellation và chờ task kết thúc khi shutdown.
- [ ] `tests/command_offline.rs`, `tests/command_ack.rs` và domain tests: C1–C6/C10, A10/A11, duplicate/failed ACK, ACK sai device, late ACK, reconnect/resend cùng ID, restart recovery từ SQL, race ACK/dispatch và ACK/expiry, queue-full và publish/storage failure không tạo terminal giả. Inject clock hoặc TTL ngắn, không sleep dài.

## 5. Runtime config snapshot và PATCH atomic — P3-06

- [ ] Mở rộng `src/config/runtime.rs` với snapshot nhất quán + revision dùng `arc-swap` đã chốt; giữ startup load/validate/redaction và data-dir lock Phase 1. Update serialize bằng mutex; kiểm tra expected_revision bên trong lock.
- [ ] `GET /api/v1/config` trả cấu hình hiệu lực đã merge runtime, revision, `restart_required: []`; che database URL thật, token/password/hash và metrics secret/env name theo policy redaction hiện có.
- [ ] `PATCH /api/v1/config/runtime` chỉ cho `logging.level`, `telemetry.retention_days`, `telemetry.batch_size`, `telemetry.flush_interval_ms`; settings object không rỗng; validate toàn bộ snapshot merge và các quan hệ range trước khi ghi.
- [ ] Field lạ → 400; field tồn tại nhưng không hot → 422 + `details.restart_required`; revision cũ → 409 + `details.current_revision`. Mọi request không hợp lệ giữ nguyên RAM/file/revision; settings không làm đổi snapshot → 200, không tăng revision/không ghi file.
- [ ] Preserve các override hot cũ; file runtime chỉ chứa revision và field hot, không chứa secret hoặc cấu hình cần restart. Revision tăng một lần cho mỗi update có thay đổi.
- [ ] Chuẩn bị mọi phần có thể thất bại trước khi publish snapshot: file tạm cùng thư mục → fsync file → atomic rename → fsync thư mục; RAM chỉ swap sau bước ghi thành công, không có bước apply có thể thất bại sau swap.
- [ ] Làm rõ trong `02` trước khi code nếu cần policy riêng cho lỗi sau rename (đặc biệt fsync thư mục): không claim file cũ còn nguyên nếu rename đã thành công; file/RAM phải có quy tắc nhất quán và test failure injection cho từng bước.
- [ ] Lỗi ghi trả `500 config_write_failed`; kiểm chứng đường lỗi giữ cấu hình cũ theo contract, dọn đúng file tạm do chính thao tác tạo; không xóa file của update khác.
- [ ] Áp dụng nóng thực sự: reload logging filter; telemetry worker lấy batch_size/flush_interval từ snapshot ở batch tiếp theo; retention job lấy retention_days ở lượt tiếp theo. Không chỉ đổi GET/config mà worker vẫn giữ tham số startup.
- [ ] Nối telemetry retention theo `02` §3.4 nếu chưa có: xóa theo received_at, batch 5 000, chạy mỗi giờ, nghỉ giữa batch, không transaction dài/VACUUM tự động. Không xóa command/audit/revoked credentials; giữ session cleanup hiện có.
- [ ] Audit config sau file/RAM update; audit lỗi chỉ log + metric, không rollback file/snapshot đã publish. Broadcast config revision sau update thành công.
- [ ] `tests/runtime_config.rs`: A12/A13, H1–H4/H8, no-op, unknown vs restart-required, concurrent PATCH cùng revision chỉ một update thắng, preserve override, restart giữ revision/settings, lỗi ghi/rename/sync, runtime file hỏng startup fail. Test chứng minh worker/log/retention áp dụng snapshot mới; test T17 cho dedup sau retention.

## 6. Overview, WebSocket events và metrics — P3-08

- [ ] `GET /api/v1/overview`: devices total/online/disabled; telemetry events_24h/last_received_at; command pending/dispatched/failed_24h/expired_24h; ingest queue_depth/queue_capacity/rejected_24h; storage_ok theo `02` §4.7. Không lấy lifetime counter giả làm số liệu 24 giờ.
- [ ] Mở rộng EventBus/WS hiện có cho command, presence, config, system; thêm reported_status từ status/LWT theo `02` §4.8. Không tạo generic event framework hoặc replay store.
- [ ] Route đúng channel: command/presence → overview và device tương ứng; config/system → overview; telemetry/reported_status → device. Một connection subscribe cả hai channel chỉ nhận một bản event phù hợp.
- [ ] Giữ telemetry chỉ sau commit; command/config event phản ánh thay đổi đã thành công; presence dùng generation, status/LWT không quyết định online. `system.message` chung, không SQL/path/payload/secret.
- [ ] Giữ cookie + Origin trước upgrade, max connection/subscription/message, bounded broadcast/per-client queue, deadline mọi đường gửi, lag/full close 1013, session expiry/logout close 4401.
- [ ] Nối metric command pending/expired và lỗi ACK/kick/audit mới theo hành vi thực; không device ID label. Giữ auth Bearer riêng cho `/metrics`.
- [ ] `tests/ws_events.rs` + overview/metrics tests: đúng envelope/channel/filter, event sau commit, rollback không phát thành công, presence generation, config revision, system redaction, overview/storage degraded. Chạy lại slow-consumer/reconnect/session tests Phase 2.

## 7. Audit và bảo mật mutation — P3-07

- [ ] Dùng bảng `audit_events` hiện có, domain/repository tối thiểu; action/actor/target/details theo `02` §3.6 cho device.create/update/enable/disable, credential.rotate, command.create, config.update.
- [ ] Device/credential/command mutation và audit cùng transaction: audit lỗi rollback toàn bộ. Không thêm audit cho idempotent replay như một command mới.
- [ ] Config update có thứ tự file → snapshot → audit; audit lỗi không rollback update đã thành công. Request ID, actor, remote_ip và occurred_at lấy từ context được server kiểm soát.
- [ ] Không ghi token/password/hash, command params, metadata values, telemetry payload vào log/audit; details chỉ field/path/ID được phép. Token chỉ xuất hiện trong response create/rotate đã xác thực, không trong GET/list/WS/config.
- [ ] `tests/audit.rs`: action/context/details đúng, secret sentinel không xuất hiện trong audit/log/GET/WS, mutation rollback khi audit insert lỗi; config audit failure không làm file/RAM lệch. Không thêm endpoint audit chưa có trong contract.

## 8. Dashboard và WebSocket client — P3-09, P3-10, P3-11

- [ ] Tạo `web/` Svelte **5.57.3**, Vite **8.3.5**, TypeScript **7.0.2** strict, uPlot **1.6.32**, các tool còn lại theo `01` §2; pin exact Svelte/Vite, commit `web/package-lock.json` và `.nvmrc` Node 24. Không đổi version nếu chưa có ADR.
- [ ] SPA dùng tab state overview/devices/device detail/config; reload về overview. Không SvelteKit/SSR, UI framework, router/state library hoặc chart library khác.
- [ ] API client same-origin cookie; JSON mutation có `X-IoT-CSRF: 1`; xử lý error envelope/request_id, 401 về login, loading/empty/error/disabled states. Không lưu session/device token trong localStorage hoặc log console.
- [ ] Login/logout/me; overview hiển thị online, số liệu command/ingest, cảnh báo storage unavailable. Có label input, điều khiển được bằng bàn phím và thông báo lỗi dễ đọc; không bỏ qua accessibility warnings.
- [ ] Device list search/filter/pagination, create/get/edit metadata/name, enable/disable/rotate. Token mới chỉ hiện trong kết quả thao tác và bị xóa khỏi state khi đóng; cảnh báo không thể xem lại, không tự gửi lại rotate/create chỉ vì timeout.
- [ ] Device detail: trạng thái kết nối/last_seen, attributes latest, telemetry latest/history theo khoảng và key; command create/list/detail/status/result. Không thêm dashboard builder hoặc API quản trị attributes.
- [ ] uPlot chỉ vẽ numeric values, đổi Unix ms sang đơn vị trục thời gian phù hợp; boolean/string vẫn hiển thị ở latest/detail. Xử lý timestamp trùng, dữ liệu rỗng, pagination và range ≤31 ngày; không tự aggregation/downsampling ngoài contract.
- [ ] WS client subscribe/unsubscribe theo tab/device, reconnect có backoff, không mở socket/timer vô hạn; reconnect REST refetch/backfill telemetry và command/overview/config cần thiết. Không coi WS có replay; chống event trùng hoặc response cũ ghi đè device/range mới.
- [ ] Hủy request/subscription/timer/socket/chart khi đổi view/logout/unmount; không gửi command đã hết TTL hoặc đổi command ID để xử lý resend phía device.
- [ ] `npm run check` chạy svelte-check; `npm run build` tái lập được từ `npm ci`. Logic TS không trivial có unit test/assert-based self-check chạy được; không thêm test framework chỉ cho một helper nếu công cụ hiện có đáp ứng.
- [ ] Evidence frontend: kiểm tra login → tạo device → MQTT telemetry → chart/latest → command/ACK → disable/rotate → runtime PATCH → reconnect REST backfill, gồm lỗi storage/session hết hạn. Ghi lại cách chạy và kết quả; manual UI không thay thế integration test Rust/MQTT.

## 9. Asset embed và build — P3-12

- [ ] `src/http/assets.rs`/router dùng rust-embed + mime_guess theo stack; build frontend trước Rust release. Debug thiếu dist trả hướng dẫn build, release thiếu dist làm build fail rõ ràng.
- [ ] `/` và fallback path không thuộc API trả index; asset thật đúng MIME; asset không tồn tại không giả làm JS thành công. Giữ API 404/405 JSON và health/metrics không bị SPA nuốt.
- [ ] `index.html` no-store; asset tên có hash cache immutable 1 năm. Dashboard=false tắt static nhưng API/MQTT vẫn hoạt động.
- [ ] `tests/assets.rs`: index/fallback/MIME/cache/asset thiếu/dashboard=false/API isolation; smoke binary release phục vụ bundle mà không cần Node runtime hoặc thư mục source.
- [ ] Kiểm tra build order/CI frontend nếu đã có workflow; không commit `web/dist`, `node_modules` hoặc thêm Docker/release pipeline Phase 4 vào task này.

## 10. Kiểm tra cuối Phase 3

Chạy ở repo root:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
git diff --check
```

Khi `web/` đã được thêm, chạy tại `web/`:

```sh
npm ci
npm run check
npm run build
```

Sau build frontend, ở repo root:

```sh
cargo build --release --locked
```

Checklist gate (ghi tên test/evidence thực tế khi tick):

- [ ] A10: command tạo lúc offline còn pending bền trong SQL; reconnect nhận lệnh chưa hết hạn.
- [ ] A11: resend/reconnect giữ command ID; ACK trùng/cross-device/muộn không làm sai terminal; restart recovery đúng.
- [ ] A12: runtime PATCH đúng revision lưu bền, có hiệu lực nóng và restart giữ giá trị/revision.
- [ ] A13: revision cũ/schema/range/restart-required/lỗi ghi từ chối nhất quán, không cập nhật nửa file/RAM.
- [x] A14: rotate online kick connection cũ, token cũ refused, token mới OK; disable online cũng kick và chặn reconnect. Evidence: `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token`; `tests/device_api.rs::disabling_a_device_kicks_its_live_session_and_blocks_reconnect`.
- [ ] Dashboard hoàn thành toàn bộ luồng mục 8; numeric chart/REST backfill và WS reconnect có evidence. A15 regression Phase 2 giữ xanh; gate slow-client/release đầy đủ ở Phase 4 không được claim chỉ từ thao tác tay.
- [ ] Audit đúng transaction/redaction; metrics/config/log/WS không lộ secret; error response không trả SQL/DB URL/stack trace.
- [ ] Task dispatcher mới được supervise/cancel; readiness không bị báo healthy giả khi task trọng yếu lỗi; queue ACK command và WS hữu hạn.
- [ ] A03–A07 và A19 Phase 2 không regression; không thay durability, broker/session/retained contract.
- [ ] Rust fmt/Clippy/tests, frontend check/build, release asset smoke và diff check sạch; task không có evidence vẫn để mở. Không đổi test để che lỗi kết nối không ổn định từng ghi nhận ở Phase 2; lưu reproduction nếu tái xuất hiện.
- [ ] Chỉ kết luận Phase 3 PASS khi toàn bộ gate trên đạt; chưa claim MVP/release hoặc A01–A17 đầy đủ.

## 11. Rà Rust style và giữ thay đổi tối thiểu

- [ ] Áp dụng style guide trong ngữ cảnh Cargo application này: giữ layout modular monolith và Tokio đã chốt; không áp dụng máy móc quy tắc riêng `[chrome]`/`[Fuchsia]` để cấm async, đổi test framework hoặc chia crate.
- [ ] Logic nằm ở leaf module, mod/lib chủ yếu khai báo/re-export; đặt tên theo trách nhiệm. Không tạo wrapper/trait/generic chỉ để tổ chức code; repository/DeviceChannel phục vụ boundary thực tế.
- [ ] Import theo module, tránh glob trừ ngoại lệ inline test; ưu tiên gọi free function bằng tên module, type import rõ, trait local nếu chỉ dùng cục bộ. Dùng rustfmt mặc định, không tắt lint/format để né lỗi.
- [ ] Ưu tiên borrow `&str`/`&[T]`/`&T`, tránh deep clone; chỉ Arc khi cần share ownership giữa task. Dùng named fields cho dữ liệu nhiều thành phần, enum cho state thay vì bool khó hiểu.
- [ ] `Result`/`Option` biểu diễn lỗi đúng boundary; không unwrap/expect runtime trừ invariant đã chứng minh + comment. Không truy cập dữ liệu poisoned mutex, không dùng catch_unwind làm cơ chế giữ tính đúng; lock không giữ qua await nếu không cần.
- [ ] Public API có doc/error behavior hữu ích, visibility nhỏ nhất đủ dùng; derive trait phù hợp, Default cho options, must_use khi cần. Không blanket thêm non_exhaustive/generics chỉ vì application có pub type.
- [ ] Validation tại HTTP/MQTT boundary, SQL bind parameters, transaction ngắn; không network/broker await trong transaction. Assertion test giải thích invariant, lỗi mới có test tái hiện, comment chỉ giải thích vì sao; shortcut ghi rõ giới hạn và lúc nâng cấp.

## 12. Không làm trong Phase 3

- Crash kill -9/disk-full gate tổng thể, graceful MQTT kick-all/drain/shutdown gate, Docker/Compose/backup/restore/load benchmark, alert/SBOM/scan/signature/release: Phase 4.
- PostgreSQL/MySQL, MQTT 5, integrated TLS, cross-device ACL, clustering/HA/multi-tenancy, OTA/rule engine: ngoài Phase 3/MVP theo plans.
- UI framework/router/SSR, dashboard builder, aggregation/downsampling, latest projection/cache, command cancellation/DELETE device/API audit mới: không có trong contract MVP.
- Không coi broker session/retained hoặc WebSocket là storage bền vững; không quảng bá exactly-once hay throughput chưa benchmark.

## 13. Commit và hồ sơ bàn giao

- [ ] Commit nhỏ, mỗi commit build/test được; gợi ý: `feat(device): admin and credential rotation`, `feat(command): lifecycle and dispatcher`, `feat(config): atomic runtime updates`, `feat(http): overview and events`, `feat(web): dashboard and embedded assets`.
- [ ] Commit Cargo.lock khi dependency thay đổi, web/package-lock.json và .nvmrc; không commit dist/target/node_modules/data/log/token thật. Không chỉnh migration đã chạy để né migration mới.
- [ ] Rà mọi caller/test/fixture/config/export khi đổi signature hoặc snapshot; giữ regression suite Phase 1/2. Nếu có migration mới, ghi backup/rollback theo delivery plan.
- [ ] Cập nhật checkbox bằng evidence thực, ghi rõ giới hạn và việc chuyển Phase 4; đối chiếu tài liệu trạng thái hiện hành khi bàn giao, không sửa hồ sơ PASS lịch sử để che phần chưa kiểm chứng.
