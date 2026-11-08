# Preflight release MVP — 2026-11-08

**Kết luận: chưa đủ evidence để phát hành; Phase 5 vẫn bị chặn theo delivery plan §8.** Đây là báo cáo cục bộ, không phải release hay xác nhận production-ready.

- Thời gian kiểm tra: khoảng 09:46–09:48 +07:00. Commit code được kiểm tra: `6f55e9ee56348761bab913d2d1257655e63a0827`; working tree sạch trước khi tạo báo cáo này. Nhánh `master` theo `origin/master`; báo cáo là thay đổi tài liệu sau commit trên và chưa được commit.
- [CI run 42430259452](https://github.com/chanh-group/iot-platform/actions/runs/42430259452) trên commit đó: `rust`, `frontend`, `docker` và `audit` đều success. Đây không phải workflow release/OIDC.
- Cục bộ: `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all`, `cargo test --release --locked` và `git diff --check` PASS. Test Docker và manual load bị ignored; release còn một test race debug-only bị ignored và các crash hook không compile trong release. Chưa chạy lại riêng các test ignored trong lượt này.
- Frontend: `bun run --bun typecheck` và `bun run --bun test` PASS (29 test, 194 assertions). Chưa chạy lại frozen install hoặc build trong lượt này; CI frontend ở trên pass.
- `cargo deny check` PASS với cảnh báo dependency trùng; `cargo audit` PASS, không có vulnerability, còn hai cảnh báo unmaintained đã ghi exception trong release checklist.
- Không chạy `deploy/release-dry-run.sh`: script dùng tag cố định `iot-server:dry-run`, tag này đã tồn tại với local image ID `sha256:1188a53955cfd2b19ff130ae2ffd3ebb746a8ab1e08d7ff95f811d972352bada`; chạy build có thể đổi tag đó. Chưa tạo image, SBOM, Trivy report hoặc checksum mới gắn với commit ứng viên. `syft`, `trivy` và `cosign` không có trong `PATH` của môi trường kiểm tra. Không dùng artifact cũ để thay thế.
- MQTT: đã có báo cáo probe trước đó về CONNECT đồng thời cùng client ID bị RMQTT `try_lock` từ chối; chưa đối chiếu được nguyên nhân nội bộ của các lỗi reconnect tuần tự trên CI với probe đó. Probe nhỏ bổ sung được ghi bên dưới; hành vi firmware thật vẫn chưa được chứng minh.
- MQTT vẫn plaintext; không mở listener trực tiếp ra mạng không tin cậy, cần TLS proxy. Load baseline trước đây tạo 59.999 publish request trong 60 giây; sau drain có 59.999 PUBACK/row nhưng chỉ 30.720 application ACK và 29.279 ACK drop. Bài đo không có retry và không xác nhận commit trong cửa sổ 60 giây; không dùng để sizing hoặc cam kết throughput.
- Còn mở: kiểm chứng reconnect/retry và quyết định giới hạn vận hành; chạy release checklist đầy đủ trên đúng commit sẽ tag, gồm ignored gate, Docker smoke, alert, artifact mới và scan; xác minh nhánh release, GHCR/OIDC, digest/chữ ký và GitHub Release thật. Chưa tạo tag, push hay publish.

## Kiểm tra bổ sung khoảng 09:54–09:56 +07:00

- `cargo build --release --locked` và `sh tests/alerts.sh` PASS; promtool kiểm tra 10 rules và rule tests PASS. Không chạy lại Docker smoke cục bộ vì test tự build đè tag `iot-server:local` đang tồn tại; CI Docker job trên commit ứng viên đã pass.
- Build image bằng `docker build -f deploy/Dockerfile -t iot-server:preflight-6f55-20261108t0950 .` với tag mới: local image ID `sha256:0fdfe5205b92726356fa14d41899d16272f225a57053907024b2220624031dd9`; `--version` trả `iot-server 0.1.0`, UID `10001`. Build sử dụng cache; đây không phải image GHCR đã ký và không chứng minh rebuild không cache.
- Syft và Trivy chạy từ image tool có sẵn trên `docker save` archive của image mới, không yêu cầu sửa tag cũ. Source SBOM được tạo từ `git archive 6f55e9ee56348761bab913d2d1257655e63a0827`, không chứa working tree/report chưa commit: 859 packages; image SBOM: 110 packages. Report Trivy hợp lệ, gate PASS với 0 HIGH/CRITICAL và 0 exception. Report scan archive ghi config image ID `sha256:6c7d9056ff92c1f85472473ddfcd7dfe604f1b0fc8e7e3e3fe367a9a692a867d`, khác local manifest-list ID ở trên.
- Các file `sbom-source.spdx.json`, `sbom-image.spdx.json`, `trivy-image.json`, `image.tar` và `SHA256SUMS` nằm tại `/tmp/opencode/release-preflight-6f55-20261108t0950/`; `sha256sum -c SHA256SUMS` PASS. Chưa ký checksum, chưa tạo release artifact binary và chưa chạy toàn bộ dry-run; artifact tạm không phải release evidence và có thể bị dọn bởi hệ thống.
- Nguyên nhân reconnect tuần tự vẫn chưa được xác minh; mọi điều kiện release còn lại ở trên vẫn mở. Không tag, push, publish hay ghi đè image tag cũ.

## Probe MQTT Python nhỏ — 2026-11-08

- Chạy bốn lượt bằng `/tmp/opencode/mqtt-simulator-venv/bin/python` với `paho-mqtt==2.1.0`, binary release cục bộ của commit `6f55e9e`, server loopback và SQLite/credential mới riêng cho mỗi lượt. Probe sau đó được chuyển và mở rộng tại `tests/load/mqtt-probe.py`; các số đo này là từ phiên bản tạm trước đó, không phải test CI hay artifact release.
- Mỗi lượt: 16 CONNECT đồng thời cùng device ID, 1 CONNACK thành công và 15 `Server unavailable`; sau khi các kết nối thử đã đóng, probe reconnect thành công ngay ở lần đầu. Có cơ chế retry tối đa 8 lần với exponential backoff/jitter cho lỗi server unavailable, nhưng **nhánh retry đó không được thực thi** trong bốn lượt; kết quả không chứng minh client phục hồi sau 0x03 thực tế.
- Mỗi lượt: gửi một event QoS 1, nhận `stored`, giả lập client bỏ qua ACK đó rồi gửi lại chính payload byte-identical; nhận `duplicate`, tổng 2 PUBACK và SQLite có đúng một row. Đây là kiểm tra chức năng một event, không phải thí nghiệm tải/retry, không đo tỷ lệ ACK drop, throughput, resource hay firmware thực tế. Không đo ACL hoặc chứng minh số session đồng thời bằng observer độc lập.
- Probe xác nhận hành vi dưới CONNECT cạnh tranh nhưng không chứng minh các lỗi CI reconnect tuần tự cùng nguyên nhân. Không ghi token/payload vào báo cáo; rủi ro và release gate vẫn mở.

### Kiểm tra retry sau 0x03 — khoảng 10:07 +07:00

Probe được sửa để chỉ bắt đầu bước retry sau khi đã quan sát `Server unavailable` trong đợt CONNECT cùng ID; trước retry chờ jitter ngẫu nhiên, tối đa 8 lần với giới hạn exponential backoff. Năm lượt mới trên fixture độc lập đều có 13–15/16 CONNECT bị từ chối, sau đó **lần retry đầu tiên** thành công; mỗi lượt kiểm tra thêm `stored` → giả lập mất ACK → gửi lại cùng bytes → `duplicate` và đúng một row SQLite. Đây là retry mới sau khi toàn bộ CONNECT ban đầu đã kết thúc, không phải kiểm tra recovery trong khi nhiều client tiếp tục tranh chấp; chưa kiểm tra nhiều lần retry liên tiếp, ACL/session độc lập, retry dưới tải hoặc firmware thật. Không dùng kết quả làm bằng chứng giải thích lỗi CI tuần tự hay cam kết production.

### Smoke nhiều device — khoảng 10:10 +07:00

Sau đó probe được mở rộng: bốn lượt, mỗi lượt 16 device gửi song song một event, client cố ý bỏ qua `stored`, đợi 250 ms rồi gửi lại payload byte-identical. Mỗi lượt ghi nhận 32 PUBACK, 16 ACK `duplicate` và 16 row SQLite; đây vẫn chỉ là smoke, không đo throughput hay ACK drop ở tải cao. Source hiện nằm trong `tests/load/mqtt-probe.py` và được chạy lại sau khi chuyển vào repo; thời gian/số liệu trên là các lượt trước khi chuyển.

## Kiểm tra bổ sung khoảng 10:20 +07:00

- Chạy `cargo test --release --locked --test telemetry_flow byte_identical_retry_deduplicates_but_changed_bytes_conflict --quiet` 20 lượt: 20 pass. Đây không chứng minh lỗi `ServiceUnavailable` trên CI đã được sửa hoặc có cùng nguyên nhân với probe CONNECT cạnh tranh.
- Dùng image cosign `v3.1.4` có sẵn ký và verify `SHA256SUMS` của các artifact cục bộ trong `/tmp/opencode/release-preflight-6f55-20261108t0950/`: `Verified OK` với `cosign2.pub` và `SHA256SUMS.bundle`. Khóa private tạm do lần chạy này tạo đã được xóa; đây không phải chữ ký image, không phải OIDC và không phải release artifact. Lần thử đầu thất bại do container không ghi được thư mục HOME; `cosign.pub` của lần đó còn lại nhưng không dùng để verify bundle.
- Lưu ý minh bạch: `cosign sign-blob` đã được gọi với cấu hình mặc định transparency log, không chỉ định `--tlog-upload=false`; bundle chứa một transparency-log entry. Không có token hay payload telemetry trong `SHA256SUMS`. Chưa push image, tag Git hay tạo GitHub Release.
