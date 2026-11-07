# ThingsBoardRust

IoT server all-in-one viết bằng Rust: MQTT broker, HTTP API và SQLite chạy trong một process. Dự án hướng tới quản lý thiết bị, nhận telemetry và gửi lệnh mà không cần broker MQTT bên ngoài.

> **Đang phát triển, chưa phải bản phát hành production.** Phase 0–2 đã đạt gate; Phase 3 và Phase 4 đang hoàn tất, còn gate cuối, release checklist, SBOM và scan. Xem [kế hoạch giao hàng](docs/plans/03-delivery-plan.md) và [TODO Phase 4](docs/todos/TODO_4.md) để biết phạm vi và tiến độ.

## Hiện có

- MQTT 3.1.1 với xác thực token thiết bị, ACL theo thiết bị, presence, session/retained có giới hạn và LWT.
- Nhận telemetry, ghi SQLite theo batch, deduplicate theo `event_id` và trả application ACK sau commit; nhận attributes và lưu giá trị mới nhất.
- Admin session, API quản trị thiết bị/credential/command/runtime config, WebSocket telemetry, health checks, metrics tùy chọn và dashboard SolidJS.
- SQLite nhúng, migration, cấu hình TOML, CLI tạo admin, Docker image non-root, Compose SQLite và quy trình backup/restore.

Chi tiết payload, API, giới hạn và hành vi lỗi nằm trong [contract](docs/plans/02-contracts.md). Contract có thể mô tả cả chức năng **chưa triển khai**, không phải toàn bộ là tính năng hiện có.

## Yêu cầu

- Runtime: Linux x86_64 cho image Docker. Mức chạy thử tối thiểu là 1 vCPU và 512 MiB RAM; khuyến nghị 2 vCPU/2 GiB. Chưa có benchmark cho cấu hình nhỏ nhất. Baseline đã đo dùng 4 vCPU/8 GiB cho 1.000 kết nối, 1.000 yêu cầu publish/s và vẫn drop 48,8% application ACK, nên không dùng baseline làm cam kết dung lượng.
- Đĩa: dung lượng phụ thuộc retention và tải. Baseline tăng khoảng 22,7 MB/phút ở 1.000 yêu cầu/s trong 60 giây đo; không ngoại suy tuyến tính cho 30 ngày. Xem [hướng dẫn vận hành](docs/operations.md).
- Build từ source: Rust edition 2024, MSRV **1.94**; toolchain dùng trong kế hoạch là **1.98**, Cargo và môi trường build phù hợp với `Cargo.lock`.
- Docker Engine và Compose v2 nếu dùng image; các cổng HTTP/MQTT đã cấu hình phải còn trống.

## Quick start với Docker (SQLite)

Image được build tại chỗ, chưa phải image phát hành. Chạy từ thư mục gốc repository:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
cd deploy
./prepare-data.sh
docker compose -f compose.sqlite.yml run --rm iot-server migrate --config /app/conf.toml
docker compose -f compose.sqlite.yml run --rm iot-server admin create --config /app/conf.toml
docker compose -f compose.sqlite.yml up -d
curl http://127.0.0.1:8080/healthz
```

`admin create` yêu cầu nhập và xác nhận mật khẩu, tối thiểu 12 ký tự. Lệnh này cần schema đã migrate và data directory không bị process khác giữ lock, nên chạy trước khi `up` hoặc dừng server trước. Không có admin mặc định.

Compose chỉ publish cổng ra `127.0.0.1`; MQTT là plaintext và `cookie_secure = false` chỉ dành cho localhost. Chi tiết deploy, cấu hình, log, shutdown, backup/restore, retention, monitoring và troubleshooting: [docs/operations.md](docs/operations.md).

## Chạy cục bộ

Từ thư mục gốc repository:

```sh
cargo build --locked
cargo run --locked -- check
cargo run --locked -- migrate
cargo run --locked -- admin create --username admin
cargo run --locked -- run
```

Lệnh tạo admin yêu cầu nhập và xác nhận mật khẩu qua terminal; mặc định tối thiểu 12 ký tự. Không có admin mặc định. Chạy migration và tạo admin trước khi khởi động server; chỉ một process được giữ lock của data directory tại một thời điểm.

Mặc định CLI dùng `./conf.toml`. Chọn file khác bằng `--config`:

```sh
cargo run --locked -- --config /path/to/conf.toml check
```

Mặc định HTTP lắng nghe tại `127.0.0.1:8080`, MQTT tại `127.0.0.1:1883`, dữ liệu nằm trong `./data`. Xem [`conf.toml`](conf.toml) để điều chỉnh. Các đường dẫn tương đối trong file cấu hình resolve theo thư mục chứa file đó, không theo thư mục làm việc khi chạy lệnh.

Kiểm tra server sau khi khởi động:

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

`/healthz` kiểm tra process còn sống; `/readyz` kiểm tra readiness của database, MQTT, ingest và command dispatcher. Dừng server bằng Ctrl+C hoặc SIGTERM; xem [shutdown](docs/operations.md#shutdown-and-restart).

## Bảo mật và dữ liệu

- Cấu hình mặc định chỉ lắng nghe trên loopback. Chưa có TLS tích hợp; không đưa HTTP hoặc MQTT trực tiếp ra mạng không tin cậy. Nếu cần truy cập từ xa, phải có lớp bảo vệ và TLS phù hợp ở bên ngoài.
- Không ghi password, device token, metrics token hoặc database URL nhạy cảm vào repository hay log. Metrics mặc định tắt; khi bật, token được lấy từ biến môi trường được chỉ định trong cấu hình.
- `data/` chứa database và dữ liệu vận hành; giữ riêng với source. Không xóa hoặc ghi đè database khi triển khai lại. Quy trình backup/restore, WAL và retention nằm trong [docs/operations.md](docs/operations.md).
- WebSocket là kênh realtime best-effort, không phải lưu trữ bền vững; dữ liệu đã commit được đọc lại từ SQLite/API.

## Phát triển và kiểm thử

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

Integration test dùng SQLite và MQTT cục bộ; cần quyền tạo thư mục tạm và bind cổng loopback. Quy tắc đóng góp và các lệnh bắt buộc nằm trong [AGENTS.md](AGENTS.md).

## Tài liệu

- [Hướng dẫn vận hành](docs/operations.md)
- [HTTP API](docs/api.md)
- [MQTT guide](docs/mqtt.md)
- [Đặc tả sản phẩm](docs/SPEC.md)
- [Stack và kiến trúc](docs/plans/01-stack-and-architecture.md)
- [Contract MQTT, SQLite, HTTP và cấu hình](docs/plans/02-contracts.md)
- [Kế hoạch giao hàng](docs/plans/03-delivery-plan.md)
- [Baseline tải 2026-11-07](docs/reports/load-baseline-2026-11-07.md)
- [Checklist Phase 4](docs/todos/TODO_4.md)
- [Rust style guide](docs/RUST_STYLE_GUIDE.md)
