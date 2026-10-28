# AGENTS.md

Hướng dẫn cho agent/code contributor trong repo này. Đọc trước khi sửa code.

## Nguồn sự thật

- `docs/SPEC.md` — phạm vi sản phẩm.
- `docs/plans/01-stack-and-architecture.md` — stack, version, ADR.
- `docs/plans/02-contracts.md` — contract MQTT/SQLite/HTTP. Sửa hành vi phải sửa file này trước.
- `docs/plans/03-delivery-plan.md` — thứ tự phase và test mapping.
- `docs/todos/TODO_1.md` — việc đang làm (Phase 1). `docs/todos/TODO_0.md` là hồ sơ Phase 0 đã xong.

Không đổi version, broker, frontend, TLS nếu không có ADR mới trong `01`.

## Trạng thái hiện tại

Phase 0 — **PASS** ngày 2026-10-28. Phase 1 chưa bắt đầu. Code trong `spikes/` là **throwaway**, không merge vào sản phẩm và không đặt trong `src/`. Chỉ báo cáo `docs/plans/spike-rmqtt.md` được giữ lại.

Phase 1 chỉ bắt đầu sau khi các khác biệt contract trong `docs/plans/spike-rmqtt.md` được chốt trong `docs/plans/02-contracts.md`. Đã chốt; Phase 1 đang ở bước TODO, chưa có code.

## Lệnh bắt buộc

Chạy trước khi báo xong việc:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

Với spike (crate riêng trong `spikes/*`):

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

Nếu crate spike không nằm trong workspace root, chạy các lệnh trên trong thư mục crate đó.

## Chuẩn code

- Rust edition 2024; MSRV 1.94, toolchain hiện tại 1.98.
- `cargo fmt` mặc định; không tắt lint để né lỗi.
- `clippy -D warnings` phải sạch, kể cả test.
- Không `unwrap`/`expect` trên đường chạy runtime; chỉ dùng khi là invariant đã chứng minh và có comment. Trong test/spike được phép.
- Không dùng `unbounded_channel` cho dữ liệu nghiệp vụ.
- Không log token, password, payload telemetry, database URL đầy đủ.
- Giữ hướng phụ thuộc: `core` không import `axum`/`rmqtt`/`sqlx`; adapter map sang domain type.
- Pin exact cho `rmqtt`, `sqlx`, `axum`, `svelte`, `vite`. Commit `Cargo.lock` (và `package-lock.json` khi có `web/`).
- Không thêm dependency ngoài danh sách `01` nếu chưa ghi lý do.
- Comment chỉ để giải thích "vì sao"; shortcut có giới hạn dùng `shortcut: <giới hạn>, <khi nào nâng cấp>`.

## Test

- Task không có test/evidence tương ứng chưa done.
- Logic mới không trivial phải có ít nhất một unit test hoặc assert-based self-check.
- Bug fix: grep mọi caller của hàm bị sửa, sửa ở nguồn chung, thêm test tái hiện.
- Integration test dùng temp dir riêng, port tự do; không phụ thuộc thời gian thực dài (inject clock/TTL ngắn).
- Test MQTT dùng `rumqttc`; không dùng thao tác tay làm evidence duy nhất.
- Phase 0 dùng đúng tên test trong `docs/todos/TODO_0.md` (`auth_hooks`, `protocol_reject`, `acl_cross_device`, `server_publish_kick`, `retained`, `session_expiry`, `shutdown`) để báo cáo chạy lại được.

## Git

- Commit nhỏ, mỗi commit build/test được.
- Không commit `web/dist`, `target/`, data dir, log.
- Spike nằm ngoài workspace sản phẩm; `Cargo.lock` của spike được commit cùng spike.
