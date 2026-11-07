# Diễn tập backup/restore SQLite — 2026-11-07

Lệnh: `cargo test --locked --test backup_restore -- --nocapture` — PASS.

Integration test seed admin, device, một telemetry event và runtime revision, dừng app nguồn, rồi chạy `deploy/backup.sh`. Fake Docker adapter của test chỉ thay thế điều khiển Compose/Engine; các bước shell copy, lock, file mode, checksum và publish thật của script chạy trên data directory fixture. Một lỗi copy dở được inject báo `iot_backup_last_attempt_success 0` mà không publish backup; lần thành công sau đổi thành 1, và lần thất bại tiếp theo đặt lại 0 trong khi giữ timestamp thành công cuối.

Với diễn tập restore, test verify checksum của output script thành công, làm hỏng `iot.db` đích, thay nó bằng output backup đó (gồm các sidecar hiện có và `runtime.toml`), rồi start server từ dữ liệu đã restore. Admin login, device đã seed, telemetry history event và runtime revision 1 đều được verify qua HTTP API. Backup nguồn được giữ nguyên.

Đây là diễn tập restore ở mức artifact tự động, không phải lần restore thứ hai trên một triển khai Compose đang chạy. Smoke test quyền/identity của backup Docker thật vẫn được bao phủ riêng bởi `tests/docker_smoke.sh`.
