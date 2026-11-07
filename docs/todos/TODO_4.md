# TODO — Ngày 5 / Phase 4: Tin cậy, bảo mật, shutdown, Docker, backup

TODO dựa trên contract hiện tại; không dùng checklist này để tự thay đổi contract hoặc stack.

Mục tiêu: hoàn thiện A01, A04, A08, A09, A15–A17 và A19; đóng gói Docker single-binary; backup/restore SQLite đã test; tài liệu vận hành và release checklist đầy đủ. Phase này **chốt MVP** — khi tất cả gate pass, tiến hành release.

Nguồn sự thật:

- `docs/SPEC.md` — §5–§10, §13 acceptance A01–A17/A19/A20.
- `docs/plans/01-stack-and-architecture.md` — stack/version, ADR, bounded queues, failure policy.
- `docs/plans/02-contracts.md` — §2.10, §3, §4, §5, test vector C1–C10/H1–H8.
- `docs/plans/03-delivery-plan.md` — §7 P4-01..P4-12, §9 test mapping, §10 CI, §11 Docker/vận hành, §12 Definition of Done.
- `docs/RUST_STYLE_GUIDE.md` và `AGENTS.md` — chuẩn code và lệnh kiểm tra bắt buộc.
- `docs/todos/TODO_3.md` — hồ sơ gate Phase 3, evidence đã ghi nhận và các giới hạn đã chấp nhận.

## 0. Điều kiện vào và quy tắc thực thi

- [x] Phase 3 gate đầu vào cho A10–A14, dashboard, runtime PATCH, credential rotate, audit/redaction và strict TypeScript đã có evidence tên test cụ thể trong `TODO_3.md §10`; đã đọc `TODO_3.md §10`. Không claim Phase 3 PASS vì hồ sơ đó vẫn còn các mục bàn giao mở.
- [x] Chạy lại baseline trước khi bắt đầu:
  ```sh
  cargo fmt --all --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test --all
  git diff --check
  # tại web/:
  bun install --frozen-lockfile
  bun run --bun typecheck
  bun run --bun test
  bun run --bun build
  ```
  Tất cả phải PASS. Ghi evidence (số lượng test, timestamp) trước khi tick.

  Evidence 2026-11-01T23:35:52+07:00: Rust toolchain 1.98.0 — `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (242 passed, 0 failed across 30 suites) và `git diff --check` PASS. Web — `bun install --frozen-lockfile` (Bun 1.4.4), `bun run --bun typecheck`, `bun run --bun test` (25 passed, 165 assertions, 0 failed) và `bun run --bun build` PASS. Không có dependency hay hành vi sản phẩm nào thay đổi.
- [ ] Đọc lại contract liên quan trước mỗi task; cập nhật `02` trước khi đổi hành vi; thêm ADR trong `01` nếu đổi version/image/TLS/backup strategy.
- [ ] Không thêm dependency ngoài `01` nếu chưa ghi ADR và lý do.
- [ ] Mọi test crash/kill dùng binary thật + hook `cfg(debug_assertions)` — không bật trong `--release`.
- [ ] Không làm PostgreSQL/MySQL (Phase 5). Không thêm cluster/HA/multi-tenancy.

## 1. Rà soát bounded resources — P4-01

- [x] Kiểm tra toàn repo: mọi channel/queue/WS/MQTT/HTTP phải có giới hạn kích thước. Đã đối chiếu với contract `02` §2.10 và stack `01`: ingest queue `telemetry.queue_capacity`; command ACK queue 4 096; dispatcher wake queue 4 096; WS queue theo `http.ws_queue_capacity` và tổng connection theo `http.max_ws_connections`; MQTT inflight/queued messages theo cấu hình RMQTT; HTTP body theo `http.max_body_bytes` và in-flight theo `http.max_concurrent_requests`.
- [x] Thêm metric cho điểm reject/drop: ingest queue, ACK queue, dispatcher wake, WS/broadcast, retained cap, attribute queue và HTTP request capacity đều có counter tương ứng; không có `unbounded_channel` trong `src/`.
- [x] Ghi audit checklist và test cho giới hạn HTTP; không thêm `unbounded_channel` mới.
- [x] Evidence: `cargo test --all` PASS (245 passed, 0 failed); unit `http::middleware::tests::http_request_budget_rejects_when_full` và integration `config_validation::http_concurrent_request_limit_is_bounded` PASS. `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `git diff --check` PASS. Thay đổi HTTP thêm `http.max_concurrent_requests` mặc định 256, reject `503 request_capacity_exceeded` và metric `iot_http_rejected_total`.

## 2. DB failure, disk full và backpressure — P4-02

- [x] Kiểm tra `src/core/telemetry/ingest.rs` và pool: lỗi DB hạ readiness, không phát `stored` trước commit; retry exponential có trần cấu hình `telemetry.retry_initial_ms`/`telemetry.retry_max_ms`. Thêm validation `retry_initial_ms <= retry_max_ms`, mặc định 100/5 000 ms.
- [x] Disk full: `tests/storage_failure.rs::sqlite_full_never_panics_or_publishes_stored_and_recovers` giới hạn SQLite thật bằng `PRAGMA max_page_count`; worker không panic/không phát `stored`, readiness false, sau khi mở rộng giới hạn thì ghi thành công và readiness hồi phục.
- [x] Queue đầy: `tests/storage_failure.rs::saturated_ingest_queue_stays_bounded_and_exports_rejection_metric` xác nhận reject queue, metric `ingest_rejected_total{reason="queue_full"}`, queue depth không vượt capacity 16 và readiness degraded.
- [x] Phục hồi sau lỗi DB tạm thời: `tests/storage_failure.rs::database_failure_never_publishes_stored_and_recovers` dùng trigger SQLite, xác nhận `retryable/storage_unavailable`, readiness 503 rồi `stored` và readiness 200 sau khi bỏ lỗi.
- [x] Evidence 2026-11-01: `tests/storage_failure.rs` 3/3 pass; `tests/config_validation.rs::telemetry_retry_backoff_must_be_ordered` pass. Full `cargo test --all` 250 passed, 0 failed across 31 suites; fmt, Clippy `-D warnings` và `git diff --check` PASS.
- [x] Đây là A09; đã đối chiếu acceptance criteria `SPEC.md §13 A09`.

## 3. Crash recovery — P4-03

- [x] Implement hook `IOT_TEST_PAUSE_BEFORE_COMMIT` và `IOT_TEST_PAUSE_AFTER_COMMIT` trong telemetry worker chỉ compile khi `#[cfg(debug_assertions)]`. Hook chờ signal/file sentinel để resume; không dùng trong release.
- [x] Test: kill -9 trước commit → restart → thiết bị retry → không mất dữ liệu, không tạo bản trùng trong retention window.
- [x] Test: kill -9 sau commit nhưng trước ACK → restart → ACK không đến → thiết bị retry cùng event_id → nhận `duplicate` ACK, không tạo thêm bản ghi.
- [x] Test: kill -9 sau commit + ACK → restart → không mất dữ liệu; command pending phục hồi từ SQL.
- [x] Dùng binary thật (`cargo build`, không `--release`) với `IOT_TEST_PAUSE_*` env; spawn child, send SIGKILL, restart, verify.
- [x] Evidence 2026-11-02T00:15:12+07:00: `tests/crash_recovery.rs::kill_before_commit_recovers_without_losing_or_duplicating_event`, `::kill_after_commit_before_ack_retries_as_duplicate`, `::kill_after_ack_preserves_telemetry_and_recovers_pending_command` pass. `cargo build`, `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (253 passed, 0 failed across 31 suites) và `git diff --check` PASS.
- [x] Đây là A08.

## 4. Graceful shutdown — P4-04

- [x] Mở rộng `src/runtime/shutdown.rs` và `src/runtime/supervisor.rs` để xử lý đầy đủ:
  1. Nhận SIGTERM → readiness false → ngừng accept request HTTP mới.
  2. Drain queue telemetry và commit trong deadline `shutdown_timeout_ms`.
  3. Chặn MQTT handshake mới, drain application ACK, kick session đang kết nối.
  4. Đóng HTTP listener, MQTT listener, DB pool.
  5. Release data-dir lock.
  6. Exit code 0 khi sạch; log số lượng item còn chờ nếu hết deadline.
- [x] Không ACK item chưa commit trước khi drain hoàn tất.
- [x] Tín hiệu thứ hai (SIGTERM/SIGINT trong khi đang shutdown) → exit ngay; không panic.
- [x] Test: SIGTERM khi có request in-flight → request hoàn tất (nếu trong deadline) hoặc trả 503; lock được release; exit 0. Test: SIGTERM khi ingest queue không rỗng → drain/commit → exit. Test: deadline hết → log và exit.
- [x] Evidence 2026-11-02T07:30:11+07:00: `tests/shutdown_http.rs::sigterm_drains_in_flight_request_and_releases_lock`, `::sigterm_commits_queued_telemetry_then_kicks_mqtt_and_releases_lock`, `::shutdown_deadline_logs_uncommitted_telemetry_and_exits_nonzero`, `::second_signal_exits_immediately_without_graceful_cleanup`, và `mqtt::presence::tests::shutdown_fences_new_sessions_and_waits_for_existing_handshakes` pass. `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all` (256 passed, 0 failed across 31 suites) và `git diff --check` PASS.
- [x] Đây là A16.

## 5. Security suite — P4-05

- [x] Tổng hợp và chạy lại bộ test bảo mật Phase 2/3; không có regression:
  - ACL wildcard/cross-device: `tests/mqtt_acl.rs::subscribe_outside_own_prefix_is_refused_and_keeps_the_connection`, `::cross_device_publish_is_denied_not_routed_and_disconnects`.
  - Rotate/disable: `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token`, `tests/device_api.rs::disabling_a_device_kicks_its_live_session_and_blocks_reconnect`.
  - CSRF: `tests/auth.rs::csrf_content_type_and_origin_are_enforced`, `tests/credential_rotate.rs::rotation_requires_admin_session_and_csrf`, `tests/device_api.rs::device_create_patch_validate_and_audit`.
  - Cookie/token exposure: `tests/auth.rs::login_me_logout_roundtrip_and_cookie_flags`, `::secure_cookie_when_configured`; `tests/audit.rs::audit_context_details_transactions_and_redaction_follow_contract` extracts the admin `iot_session` token from `Set-Cookie` and checks it is absent from GET/list/WS/config responses, all captured WebSocket messages (including `hello`/`subscribed`), logs, and audit rows.
  - Rate limit: `tests/auth.rs::login_rate_limit_returns_retry_after`.
  - Redaction: `tests/audit.rs::audit_context_details_transactions_and_redaction_follow_contract` checks captured logs, audit rows and secret-bearing outputs (including the admin session token); `src/http/api_error.rs::tests::internal_error_does_not_leak_detail` checks SQL/DB path details are not returned.
  - Device token boundary: `tests/auth.rs::device_token_cannot_authenticate_http_admin_routes` rejects the token as either session cookie or Bearer credential.
- [x] Không tạo `tests/security.rs`: các suite hiện hữu được liệt kê theo scenario ở trên.
- [x] Không thêm endpoint mới trong Phase 4; chỉ kiểm tra endpoint đã có.
- [x] Evidence 2026-11-02T08:51:32+07:00: `tests/audit.rs::audit_context_details_transactions_and_redaction_follow_contract` checks the admin session token is absent from GET/list/WS/config responses, including initial `hello`/`subscribed` messages, and from captured logs/audit rows. The 29 security-suite tests and `cargo test --all` (257 passed, 0 failed) pass; `cargo fmt --all --check`, Clippy, and `git diff --check` PASS.

## 6. WebSocket slow client và A15 release gate — P4-06

- [x] WebSocket handler (`src/http/events.rs`): per-client queue full → close `1013 Try Again Later`, warning log + metric; publish queue vẫn non-blocking, client nhanh khác tiếp tục nhận event.
- [x] Mỗi lần mở device, trước khi subscribe, client chụp SQLite AUTOINCREMENT commit high-water mark bằng `after_id=0&limit=1`; chỉ ACK có `device:{id}` mới chạy catch-up từ baseline. ACK được ghép bằng `request_id`, channel đã xác nhận được tích lũy và ACK cũ của lần mở trước bị bỏ qua. Re-open chụp baseline mới, tránh quét event tích lũy khi rời trang. Reconnect dùng `after_id`/`through_id` theo commit order, không lấy từ browser clock, WS delivery hay `received_at`; migration `0002` thêm index `(device_id, internal_id)`. Batch chưa commit không thể đẩy mốc vượt qua nó, cursor giữ high-water cố định và mode này không có giới hạn 31 ngày. Browser chỉ giữ 100 backfill rows; history API pagination lấy lại phần bị loại. Commands/overview re-fetch snapshot REST hiện tại.
- [x] Test slow consumer: `tests/ws_telemetry.rs::slow_websocket_is_closed_and_releases_its_connection_slot` — close 1013, metric tăng, client khác nhận event trong lúc flood và slot được giải phóng để kết nối thay thế.
- [x] Test reconnect: `tests/ws_telemetry.rs::subscribed_clients_receive_only_committed_telemetry_and_refetch_after_reconnect` — REST quét theo commit ID, telemetry đến muộn vẫn được trả, commands/overview phản ánh state lúc offline. `tests/telemetry_api.rs::commit_checkpoint_does_not_advance_past_an_uncommitted_batch` giữ transaction qua request backfill; `::starting_checkpoint_skips_large_old_history_and_recovers_a_new_event` tạo 1.001 event cũ rồi lấy event mới từ baseline; `history_pages_are_stable_and_key_filter_projects_matching_values` kiểm tra đổi riêng `to` trả `invalid_cursor`. `web/tests/ws.test.ts` trì hoãn ACK overview/device, ACK cũ cùng device và đổi channel khi reconnect đang chờ; `web/tests/ui-smoke.test.ts` kiểm tra event giữa ACK, catch-up theo ACK device và re-open sau 1.001 event. `web/tests/deviceTelemetry.test.ts` backfill nhiều lượt vượt cap, deduplicate và giữ live point; `ui-smoke` xác nhận mở rộng range refetch history.
- [x] Test session logout: cùng test trên xác nhận close code 4401.
- [x] Đây là A15 release gate; full evidence gồm slow-client, reconnect backfill và logout close.
- [x] Evidence 2026-11-03T00:01:36+07:00: Rust format, Clippy, `cargo test --all` (261 passed); web typecheck, tests (29 passed, including repeated backfills beyond the 100-row cap and range refetch), and build pass. `git diff --check` passes.

## 7. Dockerfile multi-stage — P4-07

- [x] Tạo `deploy/Dockerfile` multi-stage:
  1. Stage 1 `oven/bun:1.4.2-alpine` (hoặc bookworm): `COPY web/ .` → `bun install --frozen-lockfile && bun run --bun build`.
  2. Stage 2 `rust:1.98-bookworm` (hoặc slim): copy web/dist từ stage 1 + Cargo source; `cargo build --release --locked`.
  3. Stage 3 `debian:bookworm-slim`: copy binary từ stage 2; cài `ca-certificates`, `curl` (healthcheck), `util-linux` (`flock` cho backup lock); tạo user non-root `iot` UID 10001; không copy source/target/node_modules.
- [x] Binary path `/usr/local/bin/iot-server`; workdir `/app`; config `/app/conf.toml`; data `/app/data`.
- [x] `ENTRYPOINT ["/usr/local/bin/iot-server"]`; `CMD ["run", "--config", "/app/conf.toml"]`; `STOPSIGNAL SIGTERM`.
- [x] `USER 10001:10001` (user `iot`); không chạy root; binary mode `0755`. Yêu cầu “file không có setuid” được áp dụng cho toàn filesystem trong image: Dockerfile xóa setuid/setgid trên mọi regular file và kiểm tra fail-build nếu còn sót.
- [x] Tạo `.dockerignore`: loại trừ `target/`, `web/node_modules/`, `web/dist/`, `data/`, log, `.git`, `spikes/`.
- [x] Smoke image: `docker build -f deploy/Dockerfile -t iot-server:local .` PASS; image `--version` → `iot-server 0.1.0`; user `10001:10001`; `/healthz` và `/readyz` trả 200; stop SIGTERM → exit 0. Secret sentinel scan sạch, `/app/conf.toml` và file app/config mẫu không có trong image. Build-time assertion và scan runtime xác nhận không còn regular file setuid/setgid.
- [x] Evidence 2026-11-05T06:19:42+07:00: `sg docker -c 'docker build -f deploy/Dockerfile -t iot-server:local .'` PASS; image ID `sha256:df210d2b78da2a9827dc07341bdd30ef15c740acbcfd22c738b38aba5b3a08dd` (159050684 bytes). `docker run --rm iot-server:local --version`, UID check, `/healthz`, `/readyz`, SIGTERM (exit 0), exact sample-secret scan, and whole-image setuid/setgid scan all PASS. `git diff --check` PASS.
- [x] Evidence 2026-11-06T06:48:39+07:00: rebuilt runtime image including `util-linux` for `flock`; A17 Docker smoke PASS. Build-time whole-filesystem setuid/setgid assertion also passed.

## 8. Docker Compose SQLite và config mẫu — P4-08

- [x] Tạo `deploy/compose.sqlite.yml`:
  - Service `iot-server` dùng image build local hoặc registry.
  - Ports `8080:8080` và `1883:1883` (bind localhost hoặc note bảo mật).
  - Volume `./data:/app/data`; config mount `./conf.docker.toml:/app/conf.toml:ro`.
  - Healthcheck `curl -fsS http://127.0.0.1:8080/healthz`.
  - `stop_grace_period: 20s`, `init: true`.
- [x] Tạo `deploy/conf.docker.toml`: bind `0.0.0.0`, `data_dir=/app/data`, `database.url=sqlite:///app/data/iot.db`, `cookie_secure=false` ghi chú rõ chỉ khi local.
- [x] Data bind mount first-run: `deploy/prepare-data.sh` tạo directory UID/GID 10001; từ chối tự đổi owner của thư mục không rỗng. Hướng dẫn ở `deploy/README.md`; smoke test bắt đầu với đường dẫn chưa tồn tại.
- [x] Ghi chú cảnh báo: ports chỉ bind localhost; không publish MQTT plaintext ra Internet nếu không có TLS proxy.
- [x] Test A17: `tests/docker_smoke.sh` chạy ignored integration test `tests/docker_smoke.rs::compose_recreate_preserves_data_and_live_interfaces`; seed qua admin CLI + HTTP/MQTT khi server đang chạy, recreate, rồi xác nhận device/config/history qua API và dùng lại credential qua MQTT.
- [x] Evidence 2026-11-05T23:12:35+07:00: `sg docker -c 'sh tests/docker_smoke.sh'` PASS từ data dir chưa tồn tại; cleanup xác nhận data dir và temp dir đã bị xóa. Test non-empty/wrong-owner xác nhận prepare script từ chối trước khi gọi Docker (`:Z`), giữ nguyên fixture. `cargo fmt --all --check`, Clippy, `cargo test --all` (262 passed, 1 ignored Docker integration test chạy riêng), Compose config, shell syntax và `git diff --check` PASS. Fixture cũ `/tmp/opencode/.tmp9Gql2t` không tồn tại.

## 9. Backup/restore SQLite — P4-09

- [x] Tạo `deploy/backup.sh`: xác nhận có container trong Compose project hiện tại; stop gửi SIGTERM và đợi `stop_grace_period`; kiểm tra container đã stopped; helper container đọc bind mount bằng root và giữ exclusive `.lock` suốt thời gian copy để chặn process/restart khác. Ghi DB, WAL/SHM nếu còn, `runtime.toml` và SHA-256 manifest. Không tự restart service; destination đã tồn tại thì từ chối.
- [x] Ghi rõ: **không copy riêng `.db` khi app đang chạy WAL** — backup thiếu có thể bỏ mất commit còn trong WAL; document cảnh báo.
- [x] Test restore/failure: `tests/backup_restore.rs::stopped_sqlite_backup_restores_device_telemetry_and_runtime_revision` gọi chính `deploy/backup.sh`; fake Docker adapter chạy nguyên shell-copy command trên fixture filesystem. Inject `cp` ghi dở rồi exit lỗi để xác nhận không publish backup; run thành công tạo backup, test restore chính output đó vào data dir mới, chạy binary `iot-server run`, đăng nhập và xác nhận device, telemetry, runtime revision qua API.
- [x] Tạo `docs/operations.md` với hướng dẫn backup/restore, verify checksum, WAL và monitoring disk; README deploy trỏ đến hướng dẫn.
- [x] Backup regression: A17 smoke test dùng data dir UID/GID 10001, mode 0750 do `prepare-data.sh` tạo và gọi backup script từ user chạy test; kiểm tra project mismatch/lock held bị từ chối, backup hợp lệ có mode 0700/0600, owner là operator và checksum đúng.
- [x] Evidence 2026-11-06T06:57:02+07:00: `tests/backup_restore.rs` PASS sau khi inject `cp` ghi partial rồi trả lỗi (không có backup được publish); success path restore đúng thư mục output của `backup.sh` và verify device, telemetry, runtime revision qua API. `sg docker -c 'sh tests/docker_smoke.sh'` PASS với data dir thật do `prepare-data.sh` tạo, backup từ operator; project mismatch/lock held bị từ chối, checksum và mode/owner riêng tư PASS. `cargo fmt --all --check`, Clippy, `cargo test --all` (263 passed, 1 ignored Docker test chạy riêng), `sh -n deploy/backup.sh`, `git diff --check` PASS.

## 10. Load test baseline — P4-10

- [x] Thiết lập test tải tối thiểu: `tests/load/run.sh` chạy 1 000 MQTT client đồng thời, telemetry QoS 1/event_id và ghi PUBACK, application ACK rate, latency p95, commit count.
- [x] Ghi hardware, SQLite/WAL, message, QoS, connection count, publish-request target, p95, peak RSS và disk growth/phút tại `docs/reports/load-baseline-2026-11-07.md`. Baseline 4 vCPU/8 GiB: 59 999 yêu cầu publish được tạo trong cửa sổ (thiếu 1 so với mục tiêu 60 000; runner không ghi riêng nhịp trễ), 59 999 PUBACK/commit sau drain, 999.983 yêu cầu/s; application ACK 51.201% (29 279 ACK dropped) là tín hiệu lỗi ở tải này, không phải throughput promise.
- [x] Không commit throughput làm cam kết release; report giới hạn baseline cục bộ và cảnh báo application ACK drop.
- [x] Định nghĩa `deploy/alerts/prometheus.rules.yml`: scrape/readiness failure và probe metric missing, filesystem còn ≤20% (80% full), ingest queue full, ingest rejection, dropped application ACK, SQLite commit error, backup failure và freshness.
- [x] `tests/alerts.sh` chạy Prometheus `promtool check rules` và `promtool test rules`; đủ firing/non-firing cases — PASS.
- [x] Restore drill chạy `deploy/backup.sh`, inject copy failure, tạo backup thành công, làm hỏng DB đích, restore artifact, start server và assert admin/device/telemetry/runtime revision qua API; log tại `docs/reports/restore-drill-2026-11-07.md`.
- [x] Evidence: `tests/load/` + report baseline; `deploy/alerts/` rules + promtool tests; restore drill log. `sh tests/alerts.sh` PASS.
- [x] Evidence 2026-11-07: 60 s load baseline trên Ryzen 7 H 255 / WD NVMe Btrfs, cgroup 4 vCPU/8 GiB; 59 999 yêu cầu publish được tạo trong cửa sổ (999.983 yêu cầu/s; thiếu 1 so với mục tiêu), PUBACK/commit sau drain, ACK 51.201% (29 279 dropped), ACK p95 515.627 ms chỉ trên ACK nhận được, CONNACK p95 1 510.652 ms, peak presence hooks 711, peak RSS 136 008 KiB, SQLite growth 22 731 624 bytes. `tests/backup_restore.rs` PASS after DB corruption/restore; Prometheus rules and firing/non-firing cases PASS. Results and constraints are recorded in the linked reports.

## 11. Tài liệu — P4-11

- [x] `README.md`: quick start Docker/SQLite, yêu cầu phần cứng tối thiểu, cảnh báo TLS, trạng thái Phase 4. Không có CI workflow nên không thêm badge.
- [x] `docs/operations.md`: deploy Docker/Compose, config mẫu/env, log, shutdown, backup/restore, retention, monitoring, alert/runbook và troubleshooting.
- [x] API reference `docs/api.md`: endpoint, auth/CSRF, pagination, telemetry/command/config, WebSocket subscribe/unsubscribe, error envelope; dẫn contract `02`.
- [x] MQTT guide `docs/mqtt.md`: topic, QoS, ACL, application ACK, command flow/ACK, LWT, status/data, limits và ví dụ client.
- [x] Troubleshooting: exit 3/4/5/6, disk full, DB lỗi, MQTT refuse/disconnect, login và dashboard; mỗi mục có hành động cụ thể.
- [x] Không còn placeholder rỗng; README và các doc vận hành liên kết chéo tới contract `02`.
- [x] Evidence 2026-11-07: review README, `docs/operations.md`, `docs/api.md`, `docs/mqtt.md`; toàn bộ tài liệu dự án do repo viết (README, operations, API, MQTT, hai report, `tests/load/README.md`, `web/README.md`) đã chuyển sang tiếng Việt. `cargo fmt --all --check`, Clippy `-D warnings`, `cargo test --all` và `git diff --check` PASS. Commit P4-11.

## 12. Release checklist, SBOM và scan — P4-12

- [ ] Tạo `docs/release-checklist.md` với các bước:
  1. Baseline: `cargo fmt --check`, `clippy -D warnings`, `cargo test --all`, frontend typecheck/test/build, `cargo build --release --locked`.
  2. `cargo deny check`; `cargo audit`; không có advisory critical/high chưa có exception còn hạn.
  3. Docker build; smoke image.
  4. A01–A17/A19/A20 gate pass — ghi tên test và timestamp.
  5. Backup/restore drill pass.
  6. Alert/runbook review.
  7. Tạo SBOM (`cargo sbom` hoặc `syft`).
  8. Quét dependency/OS image (`trivy` hoặc tương đương); không có critical/high unmitigated.
  9. Ký image và tạo provenance (cosign hoặc tương đương).
  10. Tag semver, push image, tạo release artifact.
- [ ] Implement CI workflow release (GitHub Actions hoặc tương đương) chạy khi tag semver: build, test, SBOM, scan, sign, publish image và artifact Linux x86_64.
- [ ] Không có item TODO/P0/P1 trong `src/` khi release; backlog chỉ chứa mục sau MVP.
- [ ] Evidence: `docs/release-checklist.md` commit; CI workflow file commit; ít nhất một dry-run (không push thật) pass.

## 13. Kiểm tra cuối Phase 4

Chạy ở repo root:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
cargo test --release --locked
git diff --check
```

Tại `web/`:

```sh
bun install --frozen-lockfile
bun run --bun typecheck
bun run --bun test
bun run --bun build
```

Sau build frontend, ở repo root:

```sh
cargo build --release --locked
```

Docker:

```sh
docker build -t iot-server:local .
docker run --rm iot-server:local --version
```

Checklist gate — ghi tên test và timestamp thực tế khi tick:

- [ ] A01: `tests/e2e_boot.rs` (hoặc tương đương) pass — binary mới với SQLite: migrate, dashboard/API và MQTT sẵn sàng.
- [ ] A04 đầy đủ: `tests/mqtt_acl.rs` wildcard/cross-device và `tests/security.rs` ACL regression pass.
- [x] A08: `tests/crash_recovery.rs` — kill trước commit, sau commit trước ACK, sau ACK — tất cả pass (evidence §3).
- [ ] A09: `tests/storage_failure.rs` — DB lỗi (readiness false, không ACK), disk full, queue đầy (bounded RAM, metric), phục hồi — pass.
- [x] A15 release gate: `tests/ws_telemetry.rs::slow_websocket_is_closed_and_releases_its_connection_slot`, `::subscribed_clients_receive_only_committed_telemetry_and_refetch_after_reconnect` — slow consumer close 1013, commit-order reconnect backfill, logout close 4401 — pass (evidence §6, 2026-11-03T00:01:36+07:00).
- [x] A16: `tests/shutdown_http.rs` — SIGTERM drain, không ACK trước commit, kick session, lock release, exit 0 — pass (evidence §4).
- [x] A17: `tests/docker_smoke.sh` / `tests/docker_smoke.rs::compose_recreate_preserves_data_and_live_interfaces` — Docker recreate cùng volume giữ device, credential, telemetry, runtime revision; API và MQTT credential được dùng sau restart; fixture được dọn sau test — pass (evidence §8, 2026-11-05T23:12:35+07:00).
- [ ] A19 regression: `tests/mqtt_protocol.rs` — QoS/wildcard/retain/LWT/session/dup client/version matrix — pass.
- [ ] Bounded resources: không còn `unbounded_channel` nghiệp vụ; audit checklist P4-01 hoàn tất.
- [ ] Security: `tests/security.rs` tất cả scenario pass; không regression từ Phase 2/3.
- [ ] Backup/restore drill: `deploy/backup.sh` + restore test pass; `docs/operations.md` có hướng dẫn.
- [ ] Load test baseline: báo cáo có hardware/số liệu; alert rules commit; restore drill log.
- [ ] Tài liệu: README, operations, API, MQTT, troubleshooting — không còn placeholder.
- [ ] Release checklist: `docs/release-checklist.md` commit; CI workflow file; `cargo deny check` + `cargo audit` sạch; SBOM + scan dry-run pass.
- [ ] `cargo fmt --check`, `clippy -D warnings`, `cargo test --all`, strict TypeScript, Docker smoke — tất cả PASS.
- [ ] Không còn TODO/P0/P1 trong `src/`; backlog Phase 5 ghi rõ.

## 14. Rà Rust style và giữ thay đổi tối thiểu

- [ ] Chạy `cargo fmt --all --check` và `cargo clippy --all-targets --all-features -- -D warnings` sau mỗi task; không merge khi còn warning.
- [ ] Không thêm `unwrap`/`expect` trên path runtime trừ invariant đã chứng minh có comment.
- [ ] Không thêm dependency mới ngoài `01` nếu chưa ghi ADR; không tạo trait/module chỉ để đủ tên checklist.
- [ ] Mỗi test dùng data dir riêng; không dùng chung DB; cleanup temp sau test.
- [ ] Hook crash test chỉ compile trong `debug_assertions`; không bật trong release.

## 15. Không làm trong Phase 4

- PostgreSQL/MySQL (Phase 5).
- MQTT 5, topic alias, `$share`, MQTT over WebSocket.
- TLS MQTT listener tích hợp (nếu chưa có ADR; TCP proxy là đủ cho MVP với tài liệu).
- Rule engine, dynamic plugin, export data, read-only account.
- Cluster, HA, multi-tenancy, billing.
- Tính năng dashboard mới ngoài những gì Phase 3 đã ship.
- Thay đổi schema migration đã áp dụng.

## 16. Commit và hồ sơ bàn giao

- [ ] Commit theo nhóm task; mỗi commit build/test được.
- [ ] Message gợi ý: `feat(reliability): bounded resources audit`, `feat(crash): crash recovery hooks`, `feat(shutdown): graceful drain and kick`, `feat(security): security suite`, `feat(ws): slow client and A15 gate`, `build(docker): multi-stage Dockerfile`, `docs: operations and release checklist`, `release: MVP v0.1.0`.
- [ ] Không commit `target/`, `data/`, `web/dist/`, `web/node_modules/`, log, secret.
- [ ] Hồ sơ bàn giao Phase 4: ghi vào phần này sau khi hoàn tất — timestamp chạy, số test pass, Docker image digest, SBOM artifact, kết quả load test và restore drill.

---

> **Phase 4 PASS** khi tất cả gate ở §13 tick, không còn placeholder doc, release checklist dry-run pass, và không còn item nào trong `src/` có tag TODO/P0/P1.
