# TODO — Ngày 2 / Phase 1: Nền tảng

Mục tiêu: có binary `iot-server` chạy được với `conf.toml`, migrate SQLite, bootstrap admin, session, health/readiness và shutdown. **Chưa làm MQTT, telemetry, command hay dashboard** — các phần đó thuộc Phase 2/3.

Nguồn sự thật:

- `docs/plans/01-stack-and-architecture.md` — stack/version/ADR.
- `docs/plans/02-contracts.md` — contract config, HTTP, SQLite, CLI.
- `docs/plans/03-delivery-plan.md` §4 — task P1-01..P1-13.
- `docs/plans/spike-rmqtt.md` — kết quả Phase 0.

## 0. Điều kiện vào

- [x] Phase 0 **PASS** ngày 2026-10-28; 13 test pass, 0 fail.
- [x] Contract Phase 0 đã chốt trong `02`:
  - MQTT 3.1 → CONNACK `0x05`; MQTT 5 → `0x87`; `client_id != username` → `0x04`.
  - Publish ACL deny đóng connection; subscribe ACL deny trả SUBACK `0x80`, giữ connection.
  - `MessagePublish` chạy trước `MessagePublishCheckAcl`; không ingest trong `MessagePublish`.
  - Retained cap dùng `BudgetedRetainStorage`; vượt cap đóng connection, không PUBACK, metric `retained_cap`.
  - Shutdown phải kick session trước khi abort listener.
- [x] `02` ghi chú SQLite: spike dùng `synchronous=FULL`; product MVP dùng `NORMAL` trừ khi có ADR mới.
- [ ] Đọc lại `02` §1, §3, §4, §5, §6 trước khi code.

## 1. Scaffold và tooling — P1-01, P1-02

- [ ] Đổi `Cargo.toml` thành package `iot-server`, version `0.1.0`, edition 2024, `rust-version = "1.94"`.
- [ ] Tạo `src/lib.rs` export module; `src/main.rs` chỉ gọi CLI.
- [ ] Tạo skeleton module: `config`, `runtime`, `storage`, `core`, `http`, `observability`, `cli`.
- [ ] Thêm dependency Phase 1 theo `01`; **chưa thêm `rmqtt`** (để P2-01).
- [ ] Tạo `rust-toolchain.toml` pin `1.98.0`, component `rustfmt`, `clippy`.
- [ ] Cập nhật `.gitignore`: `target/`, `data/`, `web/node_modules/`, `web/dist/`, log, `.env`.
- [ ] Tạo `deny.toml` tối thiểu; commit `Cargo.lock`.
- [ ] `cargo build` và `cargo run -- --version` chạy được.

**Evidence:** `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`.

## 2. Error và request ID — P1-03

- [ ] `src/core/error.rs`: domain error (`Validation`, `NotFound`, `Conflict`, `Unauthorized`, `Unavailable`, `Internal`).
- [ ] `src/http/error.rs`: map domain error → envelope `{code,message,request_id,details}`; không stack/SQL/secret.
- [ ] `src/observability/request_id.rs`: nhận `X-Request-ID` hợp lệ hoặc sinh UUID v7; luôn trả header.
- [ ] Test: envelope đúng; request ID lạ bị thay; không lộ chi tiết nội bộ.

## 3. Config — P1-04

- [ ] `src/config/schema.rs`: đủ section `server/http/mqtt/database/telemetry/commands/auth/runtime/logging`; `deny_unknown_fields`.
- [ ] `src/config/load.rs`: đọc `conf.toml`; resolve path theo thư mục config; đọc `url_env`; không interpolate chuỗi.
- [ ] `src/config/validate.rs`: range/cross-field theo `02` §5.2–§5.3; trả tất cả lỗi kèm path.
- [ ] `src/config/paths.rs`: `data_dir`; SQLite file và `runtime.settings_path` phải nằm trong `data_dir`; normalize lexical, không cần tồn tại khi `check`.
- [ ] `src/config/runtime.rs`: đọc/validate/merge `runtime.toml` nếu có; revision mặc định 0; parse lỗi → startup fail. **Chưa viết PATCH.**
- [ ] Redaction: `GET /config` sau này không lộ DB URL/token/password; hàm redaction có test.
- [ ] Điền `conf.toml` mẫu từ `02` §5.1 (không chứa secret thật).
- [ ] Test `tests/config_validation.rs`: valid; unknown field; `url` + `url_env`; env thiếu; range sai; path ngoài data_dir; runtime lỗi; redaction.

## 4. SQLite, migration, lock — P1-06, P1-07

- [ ] `migrations/sqlite/0001_init.sql` đúng DDL `02` §3.2.
- [ ] `src/storage/sqlite/pool.rs`: SQLx SQLite, pool `max_connections`, `busy_timeout`, `create_if_missing`.
- [ ] Pragma mỗi connection: WAL, `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000`, `wal_autocheckpoint=1000`.
- [ ] `src/storage/sqlite/migrate.rs`: `sqlx::migrate!`, chạy trước listener; lỗi → exit 4.
- [ ] `src/runtime/lock.rs`: exclusive lock `<data_dir>/.lock`; ghi PID; giữ đến khi process thoát; không lấy được → exit 5.
- [ ] `migrate` CLI: tạo data dir, lock, migrate, thoát; chạy lại idempotent.
- [ ] Test `tests/migration.rs`: bảng + `_sqlx_migrations`; pragma; migrate 2 lần; lock conflict exit 5.
- [ ] Ghi chú trong code: spike dùng `FULL`, product dùng `NORMAL` theo `02` §3.1.

## 5. Admin, session, CSRF — P1-08, P1-09

- [ ] `src/core/auth.rs`: Argon2id verify/hash; password min theo config; username regex.
- [ ] `src/storage/sqlite/users.rs`: tạo admin, tìm theo username; một admin duy nhất.
- [ ] `src/storage/sqlite/sessions.rs`: tạo/verify/revoke session; lưu SHA-256 token; cleanup hết hạn.
- [ ] `admin create --config`: prompt 2 lần hoặc `--password-file`; không nhận password qua argv; đã có admin → exit 6; audit `admin.create`.
- [ ] `http/auth.rs`: `POST /api/v1/auth/login`, `POST /logout`, `GET /me`.
- [ ] Cookie `iot_session`: HttpOnly, SameSite=Strict, Path=/, Max-Age theo TTL, Secure theo config.
- [ ] Login rate limit theo IP + username; 429 + `Retry-After`.
- [ ] `http/middleware.rs`: CSRF `X-IoT-CSRF`, `Content-Type`, Origin/Host; không CORS; request timeout/body limit.
- [ ] `sessions.last_seen_at` cập nhật tối đa 1 lần/5 phút.
- [ ] Test `tests/auth.rs`: không admin mặc định; create/login/logout; sai password; rate limit; cookie flags; CSRF fail; audit không chứa token.
- [ ] A02 pass.

## 6. HTTP shell, health, static — P1-10

- [ ] `src/http/router.rs`: `/api/v1` + fallback; body limit, timeout, request ID, trace.
- [ ] `GET /healthz` 200 `{"status":"ok"}`; không auth, không DB.
- [ ] `GET /readyz` kiểm tra `SELECT 1` timeout 1s; 200/503; không lộ lỗi.
- [ ] Placeholder dashboard HTML trong `src/http/assets.rs` (chưa cần `web/`); `dashboard=false` → 404.
- [ ] 404/405 JSON đúng `02`; static fallback không áp dụng cho `/api`.
- [ ] Test `tests/http_shell.rs`: health, ready, 404, 405, dashboard toggle.

## 7. Observability — P1-11

- [ ] `src/observability/logging.rs`: JSON/text, level từ config, `tracing-log`, request_id span.
- [ ] Không log token/password/payload/DB URL; test redaction.
- [ ] `src/observability/metrics.rs`: registry skeleton cho connection/ingest/commit/command; **chưa mở `/metrics`** (P2-13).
- [ ] Log startup/shutdown có config path, revision, bind addr; không log secret.

## 8. Supervisor và shutdown — P1-12

- [ ] `src/runtime/supervisor.rs`: spawn HTTP; theo dõi task; SIGTERM/SIGINT; CancellationToken.
- [ ] `src/runtime/shutdown.rs`: readiness false → ngừng accept HTTP → đóng DB pool → release lock; deadline `shutdown_timeout_ms`.
- [ ] Signal thứ hai exit ngay; panic task trọng yếu → exit có kiểm soát.
- [ ] Test `tests/shutdown_http.rs`: SIGTERM khi có request; exit 0; không panic; lock release.
- [ ] Chưa có MQTT; phần kick session để P2/P4.

## 9. Test harness — P1-13

- [ ] `tests/common/mod.rs`: temp data dir; config builder; free port; spawn app; chờ `/readyz`; HTTP client cookie jar.
- [ ] Retry free-port tối đa 3 lần; mỗi test data dir riêng.
- [ ] Không dùng chung `conf.toml`/DB; cleanup temp.
- [ ] MQTT/WS helper để Phase 2.

## 10. Kiểm tra cuối Phase 1

Chạy bắt buộc:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

CLI:

```sh
cargo run -- check --config conf.toml
cargo run -- migrate --config conf.toml
cargo run -- migrate --config conf.toml   # idempotent
cargo run -- admin create --config conf.toml
cargo run -- run --config conf.toml
```

HTTP:

```sh
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

Kiểm tra âm:

- [ ] Config sai → exit 3, không tạo DB.
- [ ] Hai process cùng data dir → process thứ hai exit 5.
- [ ] `admin create` lần hai → exit 6.
- [ ] DB đóng/read-only → `/readyz` 503; không panic.
- [ ] Mutation thiếu `X-IoT-CSRF` → 403.
- [ ] Không có admin mặc định; login sai → 401; login đúng → cookie.

## 11. Cổng Phase 1

- [ ] A02 pass.
- [ ] A01 đạt phần HTTP/DB; MQTT ready để Phase 2.
- [ ] `migrate` idempotent; lock chặn instance thứ hai.
- [ ] `/healthz` và `/readyz` đúng; readiness phản ánh DB.
- [ ] Admin/session/CSRF/rate limit hoạt động; không lộ secret.
- [ ] `cargo fmt`, `clippy -D warnings`, `cargo test --all` sạch.
- [ ] Không có code MQTT/telemetry/command/dashboard trong `src/` (ngoài skeleton rỗng).

Nếu gate fail: không sang Phase 2; ghi lỗi vào `docs/todos/TODO_1.md` và sửa.

## 12. Không làm trong Phase 1

- RMQTT adapter, hook auth/ACL, presence, retained cap (Phase 2).
- Telemetry ingest, dedup, ACK, history/latest (Phase 2).
- Device CRUD, credential rotate, command, runtime PATCH, audit đầy đủ (Phase 3).
- Svelte dashboard, uPlot, WebSocket (Phase 3).
- Docker, backup, load test, security suite (Phase 4).
- PostgreSQL/MySQL (Phase 5).

## 13. Commit

- [ ] Commit theo từng nhóm task; mỗi commit build/test được.
- [ ] Không commit `target/`, `data/`, `web/dist/`, log.
- [ ] Message gợi ý: `feat: phase 1 foundation`, `feat(config): typed config`, `feat(storage): sqlite migration`.
