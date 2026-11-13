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
- [ ] Không làm PostgreSQL (Phase 5). Không thêm cluster/HA/multi-tenancy.

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
  3. Stage 3 `ubuntu:24.04` (sau `apt-get upgrade`): copy binary từ stage 2; cài `ca-certificates`, `curl` (healthcheck), `util-linux` (`flock` cho backup lock); tạo user non-root `iot` UID 10001; không copy source/target/node_modules.
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

- [x] Tạo `docs/release-checklist.md` với các bước:
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
- [x] Implement CI workflow release (GitHub Actions hoặc tương đương) chạy khi tag semver: build, test, SBOM, scan, sign, publish image và artifact Linux x86_64. Workflow `.github/workflows/release.yml` xác minh tag khớp `Cargo.toml`/`web/package.json`, chạy frontend + Rust gate, `cargo deny`/`cargo audit`, push image staging, tạo SBOM source/image, gate toàn bộ HIGH/CRITICAL bằng `deploy/check-trivy-report.py` (chỉ chấp nhận report image đúng cấu trúc, có result `os-pkgs` khớp OS; exception có owner/expiry trong `deploy/trivy-exceptions.json`; entry thiếu field, hết hạn hoặc không khớp finding bị từ chối), ký keyless bằng cosign, verify digest + checksum bundle trước promote, chỉ promote tag version/major-minor/`latest` sau đó, verify tag trỏ đúng digest đã ký, và tạo GitHub Release kèm binary/SBOM/scan/checksum.
- [x] Không có item TODO/P0/P1 trong `src/` khi release; backlog chỉ chứa mục sau MVP. `grep -rnE 'TODO|FIXME|P0|P1' src/` không có kết quả; backlog Phase 5 nằm trong `docs/plans/03-delivery-plan.md §8`.
- [x] Evidence 2026-11-07T11:46:28+07:00–11:48:27+07:00: `deploy/release-dry-run.sh` PASS, không push registry/tag/release. 279 test pass, 0 fail (gồm `tests/release_dry_run.rs` và 13 test `tests/trivy_gate.rs` cho exception hợp lệ/thiếu owner/hết hạn/unapproved/stale, report rỗng/thiếu metadata/sai ArtifactType/Results sai kiểu/result rỗng/thiếu os-pkgs/os-pkgs sai OS/finding sai kiểu); frontend typecheck/test/build pass; `cargo deny check` advisories/bans/licenses/sources ok; `cargo audit` 0 vulnerability (2 unmaintained warning transitive qua `rmqtt` đã ghi exception trong checklist); runtime image Ubuntu 24.04 sau `apt-get upgrade`, UID 10001, `--version` = `iot-server 0.1.0`; `tests/docker_smoke.sh` (A17, compose recreate) PASS; Trivy gate chỉ chấp nhận report image hợp lệ (`ArtifactType=container_image`, `ArtifactName`, `Metadata.OS.Family`, mỗi result có `Target`/`Class`/`Type`, có result `os-pkgs` khớp OS), 0 HIGH/CRITICAL, `deploy/trivy-exceptions.json` rỗng; SBOM source 853 package, image 110 package; `SHA256SUMS` được ký/verify bằng khóa cosign tạm; `actionlint` 1.7.12, `sh -n`, `git diff --check` PASS. Script dry-run từ chối `OUT_DIR` đã tồn tại và không xóa dữ liệu. Thay đổi kèm theo: `deny.toml` (`unmaintained = workspace`, license `CDLA-Permissive-2.0`, private crate ignore) và `Cargo.toml` `publish = false` để `cargo deny` sạch.

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
docker build -f deploy/Dockerfile -t iot-server:local .
docker run --rm iot-server:local --version
```

Checklist gate — ghi tên test và timestamp thực tế khi tick:

- [x] A01: bộ tương đương `tests/migration.rs`, `tests/http_shell.rs`, `tests/assets.rs`, `tests/mqtt_boot.rs` pass — migrate, dashboard/API và MQTT readiness trên SQLite.
- [x] A04 đầy đủ: `tests/mqtt_acl.rs` wildcard/cross-device pass; các regression auth/CSRF/rotate/redaction được chạy trong `tests/auth.rs`, `tests/credential_rotate.rs`, `tests/device_api.rs`, `tests/audit.rs`. Không tạo `tests/security.rs`; các scenario được giữ tại suite sở hữu chúng (§5).
- [x] A08: `tests/crash_recovery.rs` — kill trước commit, sau commit trước ACK, sau ACK — tất cả pass (evidence §3).
- [x] A09: `tests/storage_failure.rs` — DB lỗi/readiness/không ACK thành công, SQLite full, queue bounded + metric và phục hồi — pass.
- [x] A15 release gate: `tests/ws_telemetry.rs::slow_websocket_is_closed_and_releases_its_connection_slot`, `::subscribed_clients_receive_only_committed_telemetry_and_refetch_after_reconnect` — slow consumer close 1013, commit-order reconnect backfill, logout close 4401 — pass (evidence §6, 2026-11-03T00:01:36+07:00).
- [x] A16: `tests/shutdown_http.rs` — SIGTERM drain, không ACK trước commit, kick session, lock release, exit 0 — pass (evidence §4).
- [x] A17: `tests/docker_smoke.sh` / `tests/docker_smoke.rs::compose_recreate_preserves_data_and_live_interfaces` — Docker recreate cùng volume giữ device, credential, telemetry, runtime revision; API và MQTT credential được dùng sau restart; fixture được dọn sau test — pass (evidence §8, 2026-11-05T23:12:35+07:00).
- [x] A19 regression: `tests/mqtt_protocol.rs` QoS/wildcard/retain/LWT/session/limits và `tests/mqtt_auth.rs` duplicate client/protocol-version matrix — pass.
- [x] Bounded resources: không có `unbounded_channel` trong `src/`; queue/channel limits và P4-01 audit được kiểm tra.
- [x] Security: toàn bộ scenario trong `tests/mqtt_acl.rs`, `tests/auth.rs`, `tests/credential_rotate.rs`, `tests/device_api.rs`, `tests/audit.rs` pass; không cần file tổng hợp `tests/security.rs` (§5).
- [x] Backup/restore drill: `deploy/backup.sh`, `tests/backup_restore.rs`, `docs/operations.md` và `docs/reports/restore-drill-2026-11-07.md` pass/evidence ghi nhận.
- [x] Load test baseline: `docs/reports/load-baseline-2026-11-07.md` có hardware/số liệu; alert rules và restore drill log đã ghi nhận (§10).
- [x] Tài liệu: README, operations, API, MQTT, troubleshooting đã review và không còn placeholder (§11).
- [x] Release checklist: `docs/release-checklist.md`, `.github/workflows/release.yml`, `cargo deny check`, `cargo audit`, SBOM, Trivy gate và release dry-run pass theo evidence §12. Audit không có vulnerability; 2 unmaintained advisory transitive qua RMQTT có exception đã ghi.
- [x] `cargo fmt --all --check`, Clippy `-D warnings`, `cargo test --all`, release tests/build, strict TypeScript, frontend tests/build và Docker smoke — PASS (evidence hiện tại và §7–§12).
- [x] Không còn TODO/P0/P1 trong `src/`; backlog Phase 5 được ghi rõ (§12).

Evidence rerun 2026-11-07T12:53:00+07:00: `cargo fmt --all --check`, Clippy, `cargo test --all` (279 passed; 2 ignored: Docker Engine integration và manual load), `cargo test --release --locked` pass (debug-only crash/race hooks và manual Docker/load tests bị cfg/ignore như thiết kế), `cargo build --release --locked`, `git diff --check`. Bun 1.4.4 frozen install, typecheck, 29 tests/195 assertions và build vào `/tmp/opencode/todo4-section13-final-dist` pass tại 12:30. Release suite từng lộ xung đột port; test config hiện bind MQTT trên `127.0.0.1:0`, suite release rerun pass. Docker build dùng `-f deploy/Dockerfile`; Docker/Compose smoke, SBOM/scan và dry-run evidence gần nhất nằm ở §7, §8 và §12; không push registry/tag/release.

## 14. Rà Rust style và giữ thay đổi tối thiểu

- [x] `cargo fmt --all --check` và `cargo clippy --all-targets --all-features -- -D warnings` pass cho thay đổi hiện tại; tiếp tục chạy sau mỗi task, không merge khi còn warning.
- [x] Rà path runtime: Clippy bổ sung `unwrap_used`/`expect_used` chỉ báo `src/observability/logging.rs::prepare_reload`, invariant subscriber toàn cục đã có comment và tồn tại trước Phase 4. Những `unwrap`/`expect` thêm trong diff Phase 4 thuộc test; tiếp tục không thêm trên runtime trừ invariant có chứng minh và comment.
- [x] So sánh manifest với trước Phase 4 (`4043139`): không thêm dependency; `Cargo.toml` chỉ thêm `publish = false`, `web/package.json` không đổi. Không thêm trait/module chỉ để đủ tên checklist; tiếp tục áp dụng quy tắc dependency/ADR cho thay đổi sau.
- [x] Rà fixture: `TestApp` và các test process dùng `TempDir` riêng, test SQLite in-memory dùng pool riêng; runner load dùng thư mục riêng và dọn cả khi lệnh test thất bại. `tests/load_runner.rs` kiểm tra hai lượt song song và đường lỗi. Tiếp tục giữ cách ly và cleanup cho test mới.
- [x] Hook crash test chỉ compile trong `debug_assertions`; không bật trong release (`src/lib.rs`, `src/storage/sqlite/telemetry.rs`, `src/core/telemetry/ingest.rs`, `src/runtime/supervisor.rs`).

Evidence 2026-11-07 khoảng 13:13 +07:00: fmt, Clippy và `cargo test --all` PASS (280 pass, 2 ignored: Docker integration và manual load); `sh -n tests/load/run.sh` PASS. `cargo test --release --locked --test crash_recovery` chạy 0 test như dự kiến vì test crash chỉ có ở debug. Cleanup tự động không chạy khi process bị SIGKILL hoặc máy mất điện; không xóa fixture của lần chạy trước. Đây là rà soát hiện tại, không chứng minh mọi task trước đã chạy gate riêng; các quy tắc vẫn áp dụng cho thay đổi sau.

## 15. Không làm trong Phase 4

- PostgreSQL (Phase 5).
- MQTT 5, topic alias, `$share`, MQTT over WebSocket.
- TLS MQTT listener tích hợp (nếu chưa có ADR; TCP proxy là đủ cho MVP với tài liệu).
- Rule engine, dynamic plugin, export data, read-only account.
- Cluster, HA, multi-tenancy, billing.
- Tính năng dashboard mới ngoài những gì Phase 3 đã ship.
- Thay đổi schema migration đã áp dụng.

Giữ nguyên phạm vi trên; các mục này không phải việc cần triển khai để hoàn tất Phase 4.

## 16. Commit và hồ sơ bàn giao

- [x] Các task Phase 4 đã được commit theo nhóm (`0c30af0` đến `4d6c8ae`); build/test của commit hiện tại có evidence ở §13–14. Chưa chạy lại build/test độc lập trên từng commit cũ.
- [x] Message trong lịch sử mô tả nhóm task; các message liệt kê ban đầu chỉ là gợi ý, không bắt buộc tạo commit release trước khi phát hành.
- [x] `git ls-files` không liệt kê file trong `target/`, `data/`, `web/dist/`, `web/node_modules/`, hoặc file `*.log`, `*.key`, `*.pem`, `.env`, `.env.*`. Đây là kiểm tra đường dẫn, không chứng minh toàn bộ nội dung hay lịch sử Git không có secret; tiếp tục review secret trước mỗi commit.
- [x] Hồ sơ bàn giao Phase 4 gồm dry-run cục bộ, load/restore và release thật `v0.1.1`; GHCR/OIDC/digest được xác minh tại [release report](../reports/release-v0.1.1-2026-11-09.md).

### Hồ sơ pre-release — snapshot 2026-11-08

- Commit code/test được kiểm tra gần nhất: `4d6c8ae10bde076df6fe2cd72ab08e5e0ef59c14`; hồ sơ này là thay đổi tài liệu sau commit đó. Các nhóm task trước đã được commit riêng; chưa xác minh lại từng commit độc lập. Chưa tạo tag hoặc publish trong lượt này.
- Test và gate: lần chạy 2026-11-07 khoảng 13:13 +07:00 ở §14 có 280 test pass, 2 ignored; gate A01–A17/A19/A20 và lần chạy release/frontend trước đó được ghi ở §13. Docker smoke và load test ignored có evidence riêng ở §8, §10 và §12.
- Dry-run cục bộ 2026-11-07 11:46:28–11:48:27 +07:00: kết quả chi tiết ở §12. Report scan `target/release-dry-run/trivy-image.json` ghi image ID `sha256:1188a53955cfd2b19ff130ae2ffd3ebb746a8ab1e08d7ff95f811d972352bada`; image cục bộ `iot-server:dry-run` hiện khớp ID này. Đây là ID image cục bộ, **không phải digest release GHCR đã ký**; chưa xác nhận commit nguồn chính xác của artifact dry-run.
- Artifact cục bộ, không được commit và có thể bị dọn: `target/release-dry-run/sbom-source.spdx.json`, `sbom-image.spdx.json`, `trivy-image.json`, `iot-server-v0.1.0-linux-x86_64.tar.gz`, `SHA256SUMS`, `SHA256SUMS.bundle` và `cosign.pub`. `sha256sum -c SHA256SUMS` PASS lúc kiểm tra khoảng 13:08; chữ ký dùng khóa tạm của dry-run, không phải OIDC release. Dry-run không được upload transparency-log entry.
- Load: [baseline 2026-11-07](../reports/load-baseline-2026-11-07.md), 59.999 publish request trong 60 giây, application ACK 51,201% và 29.279 ACK drop; không phải cam kết throughput, chưa đo retry. Restore: [drill 2026-11-07](../reports/restore-drill-2026-11-07.md) PASS ở mức artifact tự động với fake Docker adapter; smoke Docker thật được ghi riêng ở §8–9.
- Exception: image scan hiện không có exception; hai cảnh báo dependency unmaintained và hạn review ghi trong [release checklist](../release-checklist.md#exception-đang-áp-dụng).
- **Còn mở tại thời điểm snapshot:** chạy checklist trên đúng commit sẽ tag; ghi timestamp, commit/tag, digest GHCR, kết quả verify OIDC, SBOM/scan/checksum và đường dẫn artifact release; xác nhận workflow GHCR thực tế và kiểm tra lại dữ liệu nhạy cảm trước commit. Không thay các bằng chứng này bằng dry-run cục bộ.

### Release thật `v0.1.1` — đã xác minh

- Commit/tag: `59b44fdb22ff94b719bbc56d40aa5f8ca7e78932` / `v0.1.1`. Candidate tag `v0.1.0` fail trước GHCR push, không có GitHub Release; tag cũ được giữ nguyên.
- CI [run 42626360028](https://github.com/chanh-group/iot-platform/actions/runs/42626360028) và release [run 42628361802](https://github.com/chanh-group/iot-platform/actions/runs/42628361802) đều PASS.
- GitHub Release: [v0.1.1](https://github.com/chanh-group/iot-platform/releases/tag/v0.1.1). Release assets gồm binary Linux x86_64, source/image SBOM, Trivy report, `SHA256SUMS` và bundle; download + `sha256sum -c` PASS.
- GHCR digest đã ký: `sha256:e5c0b199fab5df3cf6535fcbea7bd08fdd18a376fe557eea5509b4fd260836eb`. Workflow xác minh image signature OIDC với identity `https://github.com/chanh-group/iot-platform/.github/workflows/release.yml@refs/tags/v0.1.1` và issuer `https://token.actions.githubusercontent.com`; `cosign verify-blob` checksums PASS.
- Source SBOM 859 packages, image SBOM 110; Trivy 0 HIGH/CRITICAL, 0 exception. `cargo audit` không còn vulnerability sau khi nâng SQLite bundled; hai advisory unmaintained có exception hiện hành.
- GHCR package private theo chủ đích cho nhóm; thành viên cần `read:packages`, anonymous 401 là expected. Dry-run cùng commit ở `target/release-dry-run-59b44fd-20261109/`, local image ID `sha256:fa36752e8b3d7a49962d930f9c842555192bca4986ac1b35898f32a9ad64c0b2` (không phải GHCR digest).
- **Phase 4 PASS. Phase 5 được mở** theo delivery plan §8; firmware client và đánh giá lại load/capacity vẫn là rủi ro vận hành.

---

> **Phase 4 PASS** khi tất cả gate ở §13 tick, không còn placeholder doc, dry-run và release thật GHCR/OIDC/artifact đã xác minh, và không còn item nào trong `src/` có tag TODO/P0/P1.
