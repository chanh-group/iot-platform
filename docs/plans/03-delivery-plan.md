# 03 — Kế hoạch giao hàng

- Trạng thái: **CHỐT cho MVP**; thứ tự phase là bắt buộc.
- Mỗi phase kết thúc bằng cổng pass/fail có bằng chứng chạy được; không chuyển phase khi cổng chưa đạt.
- Không bắt đầu code sản phẩm trước khi Phase 0 pass.

## 1. Nguyên tắc thực thi

1. **Vertical slice**: mỗi phase tạo một luồng chạy được từ MQTT/HTTP đến SQL, không chỉ scaffolding.
2. **Contract trước, code sau**: `02-contracts.md` là nguồn sự thật; sai contract thì sửa tài liệu + test trước.
3. **Test đi cùng task**: task không có test/evidence tương ứng chưa done.
4. **Không mở rộng scope**: mọi ý tưởng ngoài MVP ghi vào `docs/plans/backlog.md` (tạo khi cần), không code.
5. **Không tối ưu trước số đo**: chỉ thêm cache/projection/parallel writer khi benchmark chứng minh cần.
6. **Một writer telemetry**: không thêm đường ghi telemetry thứ hai trong MVP.

## 2. Tổng quan

| Phase | Mục tiêu | Cổng ra |
| --- | --- | --- |
| 0 | Spike RMQTT/SQLx/Axum | 6 nhóm điểm README + session expiry + protocol level |
| 1 | Nền tảng: CLI, config, SQLite, admin, health | A01 (một phần), A02 |
| 2 | MQTT auth/ACL, telemetry, ACK, history | A03, A05–A07, A19 (matrix MVP) |
| 3 | Device admin, command, runtime config, dashboard | A10–A15 |
| 4 | Tin cậy, bảo mật, shutdown, Docker, backup | A01, A04, A08, A09, A16, A17 |
| 5 | PostgreSQL/MySQL (sau MVP) | A18 cho từng backend |

Thứ tự phụ thuộc:

```text
P0 → P1 → P2 → P3 → P4 → (release MVP)
                         └→ P5
```

## 3. Phase 0 — Technical spike

**Mục tiêu:** chứng minh RMQTT 0.24 làm được đúng contract trước khi đầu tư code sản phẩm. Code spike là throwaway, không merge; chỉ giữ báo cáo.

| ID | Việc | File/artifact | Kiểm chứng |
| --- | --- | --- | --- |
| P0-01 | Tạo crate spike tối thiểu, pin `rmqtt = "=0.24.0"` feature `retain`; bật log | `spikes/rmqtt/Cargo.toml` (throwaway) | `cargo run` mở listener 1883 |
| P0-02 | Đăng ký hook auth/connect/connected/disconnected/subscribe ACL/publish ACL/publish | `spikes/rmqtt/src/main.rs` | Log đủ field: client_id, username, session, topic, QoS, retain, protocol |
| P0-03 | Từ chối protocol level 3/5; client_id≠username; sai token | spike | Mã RMQTT chốt: v3.1 `0x05`, v5 `0x87`, client ID mismatch `0x04`; không có session |
| P0-04 | ACL theo `02` §2.4; test wildcard/cross-device | spike + rumqttc | Không giao message; ghi lại deny đóng hay drop |
| P0-05 | Server publish `entry.publish(FromType::System, QoS1)` tới device online/offline | spike | Client nhận đúng topic/payload |
| P0-06 | `entry.kick` khi rotate/disable; duplicate client ID | spike | Connection cũ đóng; hook order ghi lại |
| P0-07 | Retained: status, thay thế, zero-length clear, cap topic/byte | spike | Subscriber mới nhận; cap ghi metric/log rồi đóng connection, không PUBACK; restart rỗng |
| P0-08 | Session expiry 1h cho `clean_session=0` | spike | Session biến mất sau expiry; ghi lại API/cơ chế |
| P0-09 | Lifecycle: spawn `run()`, abort, nhả port, không accept mới; kick session | spike | Abort không accept mới nhưng không đóng session; shutdown wrapper kick session trước abort |
| P0-10 | SQLx 0.9 migrate + SQLite WAL + Axum embed + WS echo trong cùng binary | `spikes/allinone` | Một process phục vụ cả 3 |
| P0-11 | Protocol smoke: QoS0/1/2, retain, LWT, session reconnect, MQTT5/3.1 reject | rumqttc | Kết quả từng case |
| P0-12 | Viết báo cáo pass/fail + API snippet + khác biệt contract | `docs/plans/spike-rmqtt.md` | Review xong |

**Cổng Phase 0 (tất cả phải pass):**

1. 6 nhóm điểm trong `README.md` §Cổng bắt buộc.
2. Session expiry áp dụng được cho MQTT 3.1.1 (hoặc có cơ chế wrapper rõ ràng).
3. Protocol level 3/5 bị từ chối ở hook/listener.
4. ACL deny ổn định: không ghi telemetry, không giao message (đóng hay drop đều chấp nhận, phải ghi rõ).
5. Retained cap đếm đúng khi thay thế/xóa/restart.
6. Không có panic khi abort listener/đóng process.

Nếu fail: dừng, cập nhật ADR-001 và `02` §9. Không tự viết broker/protocol MQTT trong MVP; chỉ được tiếp tục sau ADR và spike mới cho một broker nhúng hoàn chỉnh.

## 4. Phase 1 — Nền tảng

**Mục tiêu:** binary chạy được với config, migration, admin, health; chưa cần MQTT.

| ID | Việc | File chính | Phụ thuộc | Evidence |
| --- | --- | --- | --- | --- |
| P1-01 | Scaffold crate `iot-server`, edition 2024, MSRV 1.94; thêm deps/feature theo `01`; `lib.rs` + `main.rs` | `Cargo.toml`, `src/lib.rs`, `src/main.rs` | P0 | `cargo build` |
| P1-02 | Toolchain/lock/lint/deny | `rust-toolchain.toml`, `.gitignore`, `deny.toml` | P1-01 | `cargo fmt --check`, `clippy` |
| P1-03 | Error domain + HTTP error envelope + request ID | `src/core/error.rs`, `src/http/error.rs`, `src/observability/request_id.rs` | P1-01 | unit test envelope |
| P1-04 | Config schema/load/validate/paths/redaction | `src/config/{mod,schema,load,validate,paths}.rs` | P1-01 | `tests/config_validation.rs` |
| P1-05 | CLI + exit codes; `check` hoàn chỉnh | `src/cli.rs`, `src/main.rs` | P1-04 | `tests/cli_check.rs` |
| P1-06 | Migration `0001_init.sql`; pool/pragma/migrate; data dir lock | `migrations/sqlite/0001_init.sql`, `src/storage/sqlite/{mod,pool,migrate}.rs`, `src/runtime/lock.rs` | P1-04 | `tests/migration.rs` |
| P1-07 | `migrate` command + lock + exit code | `src/cli.rs`, `src/runtime/lock.rs` | P1-06 | CLI test idempotent |
| P1-08 | Admin bootstrap Argon2id; session create/verify/revoke; cookie | `src/core/auth.rs`, `src/storage/sqlite/users.rs`, `src/http/auth.rs` | P1-06 | `tests/auth.rs` |
| P1-09 | Login rate limit + CSRF middleware + request timeout/body limit | `src/http/middleware.rs`, `src/http/auth.rs` | P1-08 | unit/integration |
| P1-10 | Axum router shell, health/ready, static placeholder, `/auth/me` | `src/http/{mod,router,health,assets}.rs` | P1-03, P1-08 | `tests/http_shell.rs` |
| P1-11 | Logging JSON/text + reload; metrics registry skeleton | `src/observability/{logging,metrics}.rs` | P1-04 | log snapshot test |
| P1-12 | Supervisor + CancellationToken + SIGTERM/SIGINT; graceful stop HTTP/DB | `src/runtime/{mod,supervisor,shutdown}.rs` | P1-10 | `tests/shutdown_http.rs` |
| P1-13 | Test harness `TestServer` (temp dir, free port, config builder) | `tests/common/mod.rs` | P1-10 | compile + use |

**Cổng Phase 1:**

- `iot-server check`, `migrate`, `admin create`, `run` chạy đúng exit code.
- `GET /healthz` 200; `/readyz` 200 khi DB OK, 503 khi DB đóng.
- A02 pass: không có admin mặc định; bootstrap tạo được; login/logout hoạt động.
- Migration idempotent; lock chặn instance thứ hai (exit 5).
- `Cargo.lock` commit; `cargo clippy -D warnings` sạch.
- A01 mới đạt phần HTTP/DB; MQTT để Phase 2.

## 5. Phase 2 — MQTT và telemetry

**Mục tiêu:** luồng device → MQTT → validate → SQL commit → ACK → REST/WS hoạt động, ACL kín.

| ID | Việc | File chính | Phụ thuộc | Evidence |
| --- | --- | --- | --- | --- |
| P2-01 | RMQTT adapter: build context/listener từ config, spawn/abort, map type | `src/mqtt/{mod,server,config}.rs` | P1-06 | MQTT listener boot |
| P2-02 | Auth hook: protocol level, client_id/username, device+credential lookup, CONNACK | `src/mqtt/auth.rs`, `src/storage/sqlite/devices.rs` (lookup auth) | P2-01, P1-08 | T-A03 |
| P2-03 | ACL pure function + hook; deny mặc định; wildcard/cross-device | `src/core/acl.rs`, `src/mqtt/acl.rs` | P2-02 | table tests + T-A04 |
| P2-04 | Presence registry + generation + last_seen; connected/disconnected hooks | `src/core/presence.rs`, `src/mqtt/presence.rs` | P2-02 | unit + duplicate client test |
| P2-05 | Telemetry parser/validator: JSON, key/value, ts, event_id, raw hash | `src/core/telemetry/{mod,payload,validate}.rs` | P1-03 | `tests/telemetry_validation.rs` |
| P2-06 | Bounded ingest queue + batch worker `BEGIN IMMEDIATE` + dedup/conflict | `src/core/telemetry/{queue,worker}.rs`, `src/storage/sqlite/telemetry.rs` | P2-05 | T1–T10 |
| P2-07 | Application ACK publish qua `entry.publish` QoS1, non-retained | `src/mqtt/server_publish.rs` | P2-06 | T1/T9/T11 |
| P2-08 | Attributes ingest + upsert latest | `src/core/attributes.rs`, `src/storage/sqlite/attributes.rs` | P2-03 | attribute tests |
| P2-09 | Retained budget hook + metric | `src/mqtt/retained.rs` | P2-01 | T15/T16 |
| P2-10 | Telemetry history/latest + cursor REST | `src/http/telemetry.rs` | P2-06 | `tests/telemetry_api.rs` |
| P2-11 | Event bus broadcast + WS `/events` hello/subscribe/telemetry | `src/core/events.rs`, `src/http/ws.rs` | P2-06 | `tests/ws_telemetry.rs` |
| P2-12 | Protocol suite A19 (matrix MVP) | `tests/mqtt_protocol.rs` | P2-01..P2-09 | Q0/1/2, retain, LWT, session, dup client, v5/v3.1 |
| P2-13 | Metrics: ingest/queue/commit/connection | `src/observability/metrics.rs` | P2-06 | `/metrics` text |

**Cổng Phase 2:**

- A03, A05, A06, A07 pass.
- A04 pass ở mức unit + integration (kể cả wildcard).
- A19 pass cho matrix MVP: QoS 0/1/2, wildcard, retain, LWT, session reconnect, duplicate client ID, MQTT 5/3.1 reject.
- Không ACK `stored` trước commit (test kill/rollback).
- Retained cap 10k/16 MiB hoạt động; restart retained rỗng.
- Queue đầy không làm tăng RAM vô hạn; metric `ingest_rejected_total{reason=queue_full}`.

## 6. Phase 3 — Quản trị, command, runtime config, dashboard

**Mục tiêu:** admin vận hành được toàn bộ MVP từ dashboard; command có vòng đời đúng; runtime config an toàn.

| ID | Việc | File chính | Phụ thuộc | Evidence |
| --- | --- | --- | --- | --- |
| P3-01 | Device CRUD: list/create/get/patch, name unique, metadata, pagination | `src/core/device.rs`, `src/storage/sqlite/devices.rs`, `src/http/devices.rs` | P2-02 | `tests/device_api.rs` |
| P3-02 | Credential rotate: transaction, revoke, new token, kick, audit | `src/core/credential.rs`, `src/mqtt/server_publish.rs` | P3-01 | T-A14 |
| P3-03 | Command domain: create/idempotency/queue limit/state machine | `src/core/command/{mod,state,create}.rs`, `src/storage/sqlite/commands.rs` | P3-01 | `tests/command_domain.rs` |
| P3-04 | Dispatcher: online check, publish QoS1, retry/backoff, expiry, late ACK | `src/core/command/{dispatcher,ack,expiry}.rs` | P3-03, P2-04 | T-A10/A11 |
| P3-05 | Command REST: create/get/list by device | `src/http/commands.rs` | P3-03 | `tests/command_api.rs` |
| P3-06 | Runtime config store + PATCH atomic + apply hot fields | `src/config/runtime.rs`, `src/http/config.rs` | P1-04, P1-11 | T-A12/A13 |
| P3-07 | Audit events + redaction; gắn vào mutation | `src/core/audit.rs`, `src/storage/sqlite/audit.rs` | P3-01..P3-06 | `tests/audit.rs` |
| P3-08 | Overview endpoint + WS command/presence/config/system/reported_status + metrics | `src/http/overview.rs`, `src/core/events.rs`, `src/http/events.rs`, `src/observability/metrics.rs` | P3-04, P3-06 | `tests/overview.rs`, `tests/ws_events.rs`, `tests/metrics.rs` |
| P3-09 | Frontend shell: login, overview, devices, device detail, chart, commands, config | `web/src/*`, `web/package.json`, `web/vite.config.ts` | P3-01..P3-08 | `npm run check`, `npm run build` |
| P3-10 | uPlot telemetry chart: range picker, key selector, latest; REST refetch | `web/src/lib/TelemetryChart.svelte` | P3-09, P2-10 | manual checklist |
| P3-11 | WS client: subscribe device/overview, reconnect + REST backfill | `web/src/lib/ws.ts` | P3-08 | manual + unit |
| P3-12 | Asset embed/fallback; cache headers; dashboard=false | `src/http/assets.rs`, `src/http/router.rs` | P3-09 | `tests/assets.rs` |

**Cổng Phase 3:**

- A10–A14 pass.
- Dashboard làm được: login, xem online, tạo/disable/rotate device, xem chart theo khoảng, tạo command và xem trạng thái, sửa runtime config, thấy lỗi storage.
- runtime `PATCH`: revision conflict 409; field không tồn tại 400 `validation_error`; field tồn tại nhưng cần restart 422 `restart_required`; lỗi trước rename giữ config cũ, lỗi mở/fsync thư mục sau rename báo durability uncertain và config mới đã áp dụng trong process theo `02` §4.6.
- Credential rotate đóng connection cũ; token cũ không reconnect.
- Không có secret trong audit/log/response (test redaction).
- `svelte-check` sạch; build `web/dist` tái lập được.

## 7. Phase 4 — Tin cậy, bảo mật, đóng gói

**Mục tiêu:** đạt A01, A04, A08, A09, A15–A17; có thể phát hành MVP.

| ID | Việc | File chính | Phụ thuộc | Evidence |
| --- | --- | --- | --- | --- |
| P4-01 | Rà soát bounded resources: channel/queue/WS/MQTT/HTTP; thêm metric còn thiếu | toàn repo | P3 | audit checklist |
| P4-02 | DB failure/disk full: retry/backoff, readiness false, không ACK giả | `src/core/telemetry/worker.rs`, `src/runtime/readiness.rs` | P2-06 | T-A09 |
| P4-03 | Crash recovery: test hook pause trước/sau commit, kill -9, restart | `tests/crash_recovery.rs`, hook `cfg(debug_assertions)` | P4-02 | T-A08 |
| P4-04 | Shutdown: ngừng accept, kick MQTT, drain/commit deadline, exit code | `src/runtime/{shutdown,supervisor}.rs` | P1-12, P2-01 | T-A16 |
| P4-05 | Security suite: ACL wildcard, rotate/disable, CSRF, cookie, rate limit, redaction | `tests/security.rs` | P3 | pass |
| P4-06 | WebSocket slow client: queue đầy close 1013; REST backfill | `src/http/ws.rs`, `tests/ws_slow.rs` | P2-11 | T-A15 |
| P4-07 | Dockerfile multi-stage, non-root, read-only config, writable data | `deploy/Dockerfile`, `.dockerignore` | P3 build | image build |
| P4-08 | Compose SQLite + config mẫu + hướng dẫn volume/port | `deploy/compose.sqlite.yml`, `deploy/conf.docker.toml` | P4-07 | T-A17 |
| P4-09 | Backup/restore SQLite: dừng app hoặc backup API; test restore | `docs/operations.md`, `deploy/backup.sh` | P4-07 | restore test |
| P4-10 | Load test baseline 4 vCPU/8 GB/1000 conn; ghi hardware/QoS/size/p95/RAM/disk; định nghĩa alert cho DB/readiness lỗi, disk gần đầy, queue saturation, ingest reject và backup lỗi | `tests/load/`, `deploy/alerts/` hoặc script ngoài | P3 | báo cáo + alert rules |
| P4-11 | Tài liệu: README, deploy, API, MQTT, operations, troubleshooting | `README.md`, `docs/*` | P4-01..P4-09 | review |
| P4-12 | Release checklist + version/tag; `cargo deny`/`audit`, SBOM, dependency + OS-image scan, image signature/provenance; diễn tập restore | `docs/release-checklist.md`, CI release workflow | tất cả | artifact + pass |

**Cổng Phase 4 (release MVP):**

- A01–A17 và A19 pass trên SQLite.
- A18 **không** cần cho MVP; chỉ Phase 5.
- Docker recreate cùng volume giữ device/credential/telemetry/runtime config.
- SIGTERM trong deadline; không mất bản đã ACK; không ACK item chưa commit.
- Backup/restore SQLite đã test; tài liệu nêu rõ WAL.
- Alert rules đã được kiểm thử cho DB/readiness lỗi, disk gần đầy, queue saturation, ingest rejection và backup lỗi; diễn tập restore pass.
- Mỗi release có SBOM, kết quả scan dependency/OS image và image signature/provenance; không có critical/high vulnerability chưa có exception còn hạn.
- Không còn `unbounded_channel` cho dữ liệu nghiệp vụ.
- `cargo fmt --check`, `clippy -D warnings`, toàn bộ test, `svelte-check`, Docker smoke pass.

## 8. Phase 5 — Backend bổ sung (sau MVP)

Không bắt đầu trước khi Phase 4 release. Mỗi backend là một đợt riêng, không đổi kiến trúc.

| ID | Việc | Điều kiện |
| --- | --- | --- |
| P5-01 | Audit `Repository` trait; bỏ SQLite-specific leak | Phase 4 xong |
| P5-02 | PostgreSQL migrations + repository + pool; A18 | A18 pass |
| P5-03 | MySQL migrations + repository + pool; A18 | A18 pass |
| P5-04 | Compose PostgreSQL/MySQL + healthcheck + backup/restore docs | từng backend |
| P5-05 | Load test lại trên từng backend; công bố giới hạn | có số đo |
| P5-06 | Cân nhắc TLS MQTT tích hợp, MQTT 5, cross-device ACL | backlog §2.2 |

Quy tắc A18: cùng bộ integration test, migration, backup/restore, crash recovery như SQLite; không công bố hỗ trợ khi chưa pass.

## 9. Test strategy và mapping A01–A19

### 9.1. Tầng test

| Tầng | Nội dung | Chạy |
| --- | --- | --- |
| Unit | Config validate, ACL, payload validate, cursor, state machine, redaction | mỗi PR |
| Integration | MQTT → SQL → REST/WS với server thật, SQLite temp | mỗi PR |
| Protocol | QoS/retain/LWT/session/dup client/MQTT version | mỗi PR |
| Crash/shutdown | kill -9, SIGTERM, DB lỗi | nightly + release |
| Security | ACL wildcard, token rotate, CSRF, rate limit, secret scan | mỗi PR |
| Docker | build, run, recreate volume, health | release |
| Load | baseline, không gate | release + khi đổi hot path |

### 9.2. Acceptance and task evidence mapping

| ID | Phase | Test/evidence | Ghi chú |
| --- | --- | --- | --- |
| A01 | 1 + 2 + 4 | `tests/e2e_boot.rs`, Docker smoke | Migrate + HTTP + MQTT ready |
| A02 | 1 | `tests/cli_admin.rs`, `tests/auth.rs` | Không admin mặc định |
| A03 | 2 | `tests/mqtt_auth.rs` | Sai token/disable |
| A04 | 2 + 4 | `tests/mqtt_acl.rs`, `tests/security.rs` | Wildcard/cross-device |
| A05 | 2 | `tests/telemetry_flow.rs` | Commit + ACK + dashboard |
| A06 | 2 | `tests/telemetry_dedup.rs` | Duplicate |
| A07 | 2 | `tests/telemetry_conflict.rs` | Conflict |
| A08 | 4 | `tests/crash_recovery.rs` | Kill trước/sau commit/ACK |
| A09 | 4 | `tests/storage_failure.rs` | DB lỗi/disk/queue |
| A10 | 3 | `tests/command_offline.rs` | Pending → reconnect |
| A11 | 3 | `tests/command_ack.rs` | Resend/ACK trùng |
| A12 | 3 | `tests/runtime_config.rs` | Revision + restart |
| A13 | 3 | `tests/runtime_config.rs` | Sai/revision/file |
| A14 | 3 | `tests/credential_rotate.rs` | Kick + token cũ |
| A15 | 3 + 4 | `tests/ws_telemetry.rs` | Slow consumer, reconnect/backfill, session/logout; release gate remains Phase 4 |
| A16 | 4 | `tests/shutdown.rs` | SIGTERM drain |
| A17 | 4 | `tests/docker_smoke.sh` | Recreate volume |
| A18 | 5 | `tests/backends/*` | Chỉ backend công bố |
| A19 | 2 | `tests/mqtt_protocol.rs` | Matrix MVP |
| P3-08 | 3 | `tests/overview.rs`, `tests/ws_events.rs`, `tests/command_ack.rs`, `tests/metrics.rs`; WS regressions in `tests/ws_telemetry.rs` | 24-hour windows, storage health, channel routing/dedup, delayed command event version/refetch, ordered presence, LWT, queue coalescing, metric auth and transitions |

### 9.3. Test harness

- `tests/common/mod.rs`:
  - `TestServer::start()`: temp dir, config builder, free port cho HTTP/MQTT, spawn app, chờ `/readyz`.
  - `mqtt_client(device)`: rumqttc client với token; helper subscribe/publish/wait ACK.
  - `http_client()`: reqwest cookie jar.
  - `ws_client()`: tokio-tungstenite.
- Free port: bind `127.0.0.1:0`, lấy port, drop trước khi start; retry tối đa 3 lần nếu race.
- Mỗi test dùng data dir riêng; không dùng chung DB.
- Test crash dùng binary thật + `IOT_TEST_PAUSE_*` chỉ compile trong debug; không bật trong release.
- Không test phụ thuộc thời gian thực dài; inject clock/ttl ngắn qua config test.

## 10. CI

GitHub Actions (hoặc CI tương đương), các job song song khi có thể:

| Job | Bước |
| --- | --- |
| `rust` | `cargo fmt --check`; `cargo clippy --all-targets --all-features -- -D warnings`; `cargo test --all` |
| `frontend` | `npm ci`; `npm run check`; `npm run build`; upload `web/dist` |
| `integration` | Build web; `cargo test --test mqtt_protocol --test telemetry_flow ...` (SQLite) |
| `docker` | Build image; chạy container; `/healthz`; recreate volume; chạy A17 smoke |
| `audit` | `cargo deny check`; `cargo audit`; quét dependency + OS image; advisory chỉ được bỏ qua với exception có owner/expiry |
| `release` | Tag: build release; tạo SBOM; ký image + provenance/checksum; publish image và lưu artifact |

- Cache Cargo registry/target và npm; cache key theo lockfile.
- Không cần service Postgres/MySQL ở MVP.
- Build order: frontend trước `cargo build --release` vì `rust-embed`.
- Artifact: binary Linux x86_64 + image; không commit `web/dist`.
- CI phải chạy trên PR và main; tag chỉ sau khi main xanh.

## 11. Docker và vận hành

### 11.1. Image

- Multi-stage:
  1. `node:24-bookworm-slim`: `npm ci && npm run build`.
  2. `rust:1.98-bookworm`: copy web dist + source; `cargo build --release --locked`.
  3. `debian:bookworm-slim`: `ca-certificates`, `curl` (healthcheck), user `iot` UID 10001.
- Binary `/usr/local/bin/iot-server`; workdir `/app`; config `/app/conf.toml`; data `/app/data`.
- `ENTRYPOINT ["/usr/local/bin/iot-server"]`; `CMD ["run", "--config", "/app/conf.toml"]`.
- Không chạy root; không chứa source/target/node_modules.
- `STOPSIGNAL SIGTERM`; Compose `stop_grace_period: 20s`, `init: true`.

### 11.2. Compose SQLite

- `deploy/compose.sqlite.yml`: một service `iot-server`; publish 8080/1883 (hoặc chỉ localhost tùy môi trường); volume `./data:/app/data`; config mount `:ro`.
- Healthcheck: `curl -fsS http://127.0.0.1:8080/healthz`.
- Không publish MQTT plaintext ra Internet nếu không có TLS proxy; tài liệu cảnh báo.
- `conf.docker.toml`: bind `0.0.0.0`, `data_dir=/app/data`, `database.url=sqlite:///app/data/iot.db` (hoặc relative), `cookie_secure=false` chỉ khi local; production bật true sau proxy.

### 11.3. Backup/restore

- SQLite: dừng app (SIGTERM, chờ exit) rồi copy `iot.db` + `-wal`/`-shm`, hoặc dùng SQLite backup API/`VACUUM INTO` khi app dừng.
- Không copy riêng `.db` khi đang WAL và coi là đủ.
- Test restore trước release: restore vào data dir mới, chạy `run`, kiểm tra device/telemetry/runtime revision.
- PostgreSQL/MySQL: quy trình engine, Phase 5.

## 12. Definition of Done (MVP)

Một task chỉ done khi:

- Code build, fmt, clippy sạch; không `unwrap`/`expect` trên path runtime (trừ invariant đã chứng minh + comment).
- Có test/evidence tương ứng; test cũ vẫn pass.
- Contract `02` được cập nhật nếu hành vi thay đổi.
- Không log secret/payload; audit đúng hành động.
- Không thêm dependency ngoài `01` nếu chưa ghi lý do.
- Migration mới có ghi chú backup/rollback.

MVP done khi:

1. A01–A17 + A19 pass trên SQLite.
2. Docker image build/run/recreate volume pass; non-root; SIGTERM.
3. Backup/restore SQLite đã test.
4. Tài liệu: README, deploy, API, MQTT, operations, alert/runbook, release checklist.
5. Không còn TODO/P0/P1 trong code; backlog chỉ chứa mục sau MVP.
6. Load test baseline có báo cáo; alert rules + restore drill pass; không có cam kết throughput ngoài số đo.

## 13. Cắt scope khi trễ

Thứ tự cắt (từ trên xuống) — không cắt bảo mật/ACL/ACK:

1. `GET /metrics` + Prometheus (giữ `/overview`).
2. uPlot key selector nâng cao; chỉ giữ một chart cơ bản.
3. Search `q` trong danh sách device.
4. `GET /devices/{id}/attributes` trên dashboard (API vẫn giữ).
5. Load test đầy đủ; chỉ smoke 100 connection.

Không cắt: auth, ACL, token rotate, ACK sau commit, retention cap, shutdown, backup, audit, readiness.

## 14. Rủi ro tiến độ

| Rủi ro | Ảnh hưởng | Giảm thiểu |
| --- | --- | --- |
| RMQTT hook/API khác tài liệu | Chặn Phase 2 | Phase 0 bắt buộc; adapter cô lập |
| Session expiry v3.1.1 không có sẵn | Rò session | Wrapper cleanup + test; ghi ADR |
| SQLite write contention | Chậm/khóa | Một writer batch; transaction ngắn; WAL |
| Frontend Svelte 5/Vite 8/TS 7 mới | Build lỗi | Pin lockfile; CI sớm; fallback chart thuần |
| Crash test flaky | Không chứng minh A08 | Test hook pause xác định; chạy nightly |
| Docker healthcheck thiếu curl | Không probe | Cài curl hoặc bỏ healthcheck, dùng orchestrator |
| Không có số liệu phần cứng | Không cam kết | Baseline Phase 4; ghi rõ điều kiện |

## 15. Thứ tự commit đề xuất

1. `docs: lock contracts and delivery plan`
2. `chore: scaffold iot-server crate and CI`
3. `feat(config): typed config, validation, runtime snapshot`
4. `feat(storage): sqlite migration, pool, lock`
5. `feat(auth): admin bootstrap and session`
6. `feat(mqtt): rmqtt adapter, auth, acl, presence`
7. `feat(telemetry): ingest, batch, dedup, ack`
8. `feat(http): telemetry history/latest and ws`
9. `feat(device): crud and credential rotation`
10. `feat(command): lifecycle and dispatcher`
11. `feat(config): runtime patch and audit`
12. `feat(web): dashboard`
13. `test: acceptance A01–A17/A19 and crash`
14. `build: docker, compose, backup docs`
15. `release: MVP checklist`

Mỗi commit phải build/test được; không commit code chết hoặc feature flag nửa vời.
