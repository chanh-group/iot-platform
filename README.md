# ThingsBoardRust

IoT server all-in-one viết bằng Rust: MQTT broker, HTTP API và SQLite chạy trong một process. Dự án hướng tới quản lý thiết bị, nhận telemetry và gửi lệnh mà không cần broker MQTT bên ngoài.

> **Đang phát triển, chưa phải bản phát hành production.** Phase 0–2 đã đạt gate; Phase 3 đang triển khai. Dashboard, quản trị thiết bị đầy đủ, command và runtime config qua API chưa hoàn thiện. Xem [kế hoạch giao hàng](docs/plans/03-delivery-plan.md) và [TODO Phase 3](docs/todos/TODO_3.md) để biết phạm vi và tiến độ.

## Hiện có

- MQTT 3.1.1 với xác thực token thiết bị, ACL theo thiết bị, presence và giới hạn tài nguyên broker.
- Nhận telemetry, ghi SQLite theo batch, deduplicate theo `event_id` và trả application ACK sau commit; nhận attributes và lưu giá trị mới nhất.
- Admin session, API đọc telemetry/attributes, WebSocket telemetry, health checks và metrics tùy chọn.
- SQLite nhúng, migration, cấu hình TOML và CLI tạo admin. Chưa có API hoàn chỉnh để tạo hoặc xoay credential thiết bị; không dùng dữ liệu test làm quy trình provisioning production.

Chi tiết payload, API, giới hạn và hành vi lỗi nằm trong [contract](docs/plans/02-contracts.md). Contract có thể mô tả cả chức năng **chưa triển khai**, không phải toàn bộ là tính năng hiện có.

## Yêu cầu

- Rust edition 2024; MSRV **1.94**. Toolchain dùng trong kế hoạch là **1.98**.
- Cargo và môi trường build phù hợp với các dependency trong `Cargo.lock`.
- Các cổng HTTP/MQTT đã cấu hình phải còn trống.

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

Mặc định HTTP lắng nghe tại `127.0.0.1:8080`, MQTT tại `127.0.0.1:1883`, dữ liệu nằm trong `./data`. Xem [`conf.toml`](conf.toml) để điều chỉnh. Các đường dẫn tương đối phụ thuộc thư mục làm việc khi chạy lệnh.

Kiểm tra server sau khi khởi động:

```sh
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

`/healthz` kiểm tra process còn sống; `/readyz` kiểm tra readiness của database, MQTT và ingest. Dừng server bằng Ctrl+C.

## Bảo mật và dữ liệu

- Cấu hình mặc định chỉ lắng nghe trên loopback. Chưa có TLS tích hợp; không đưa HTTP hoặc MQTT trực tiếp ra mạng không tin cậy. Nếu cần truy cập từ xa, phải có lớp bảo vệ và TLS phù hợp ở bên ngoài.
- Không ghi password, device token, metrics token hoặc database URL nhạy cảm vào repository hay log. Metrics mặc định tắt; khi bật, token được lấy từ biến môi trường được chỉ định trong cấu hình.
- `data/` chứa database và dữ liệu vận hành; giữ riêng với source. Không xóa hoặc ghi đè database khi triển khai lại. Quy trình backup/restore và release đầy đủ thuộc Phase 4.
- WebSocket là kênh realtime best-effort, không phải lưu trữ bền vững; dữ liệu đã commit được đọc lại từ SQLite/API.

## Phát triển và kiểm thử

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

Integration test dùng SQLite và MQTT cục bộ; cần quyền tạo thư mục tạm và bind cổng loopback. Quy tắc đóng góp và các lệnh bắt buộc nằm trong [AGENTS.md](AGENTS.md).

## Tài liệu

- [Đặc tả sản phẩm](docs/SPEC.md)
- [Stack và kiến trúc](docs/plans/01-stack-and-architecture.md)
- [Contract MQTT, SQLite, HTTP và cấu hình](docs/plans/02-contracts.md)
- [Kế hoạch giao hàng](docs/plans/03-delivery-plan.md)
- [Hồ sơ Phase 2](docs/todos/TODO_2.md) · [Checklist Phase 3](docs/todos/TODO_3.md)
- [Rust style guide](docs/RUST_STYLE_GUIDE.md)
