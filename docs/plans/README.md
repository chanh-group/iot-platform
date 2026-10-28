# Kế hoạch triển khai — Rust IoT Server All-in-One

- Trạng thái: **kế hoạch đã chốt**, chưa viết code sản phẩm.
- Phạm vi: MVP theo `docs/SPEC.md` §2.1; các mục §2.2 và §2.3 không làm trong đợt này.
- Một binary, một process: MQTT broker + HTTP API + dashboard + core + SQLite nhúng.

## Tài liệu

| Tài liệu | Nội dung |
| --- | --- |
| [`01-stack-and-architecture.md`](01-stack-and-architecture.md) | Stack đã khóa version, ADR broker/frontend/storage/TLS/auth, kiến trúc module, luồng dữ liệu, backpressure, shutdown, rủi ro |
| [`02-contracts.md`](02-contracts.md) | Contract MQTT, schema SQLite, HTTP API, WebSocket, `conf.toml`/`runtime.toml`, mã lỗi, test vector |
| [`03-delivery-plan.md`](03-delivery-plan.md) | Phase 0–5, task theo file, thứ tự phụ thuộc, test mapping A01–A19, CI/Docker, definition of done |

## Quyết định đã khóa

| Thành phần | Quyết định |
| --- | --- |
| Runtime | Rust 1.98 (MSRV 1.94), Tokio 1.54 |
| HTTP/WebSocket | Axum 0.8.9, Tower-HTTP 0.7, Axum-Extra 0.12 |
| MQTT broker | **RMQTT 0.24.0** ở chế độ library, feature `retain`; không broker ngoài |
| MQTT protocol | Chỉ MQTT 3.1.1 trong MVP; từ chối MQTT 5 và 3.1 ở hook |
| Database | SQLx 0.9 + SQLite WAL; PostgreSQL/MySQL sau MVP |
| CLI | Clap 4.6 |
| Config | Serde + TOML 1.1; precedence default → `conf.toml` → `runtime.toml` |
| Frontend | Svelte 5.57 + Vite 8.3 + TypeScript 7.0 + uPlot 1.6; nhúng bằng `rust-embed` |
| Auth admin | Argon2id; session cookie HttpOnly/SameSite=Strict |
| Auth thiết bị | Token 32 byte ngẫu nhiên, lưu SHA-256; một credential active/thiết bị |
| TLS | MVP dùng reverse proxy cho HTTPS và MQTT TLS; TLS tích hợp sau MVP |
| Đóng gói | Một binary release + Docker multi-stage, chạy non-root |

## Cổng bắt buộc trước khi code

Phase 0 là **technical spike ngắn** để xác nhận RMQTT 0.24 đáp ứng đúng các điểm sau. Chỉ khi cả 6 điểm pass mới bắt đầu Phase 1:

1. Đăng ký được hook auth, connect, connected/disconnected, subscribe ACL, publish ACL, publish.
2. Hook thấy được `client_id`, `username`, session id, protocol level, topic, QoS, retain; từ chối được auth/ACL và MQTT 5/3.1.
3. Server publish trực tiếp tới session (`entry.publish`, QoS 1) và kick session (`entry.kick`) hoạt động.
4. Retained message hoạt động; giới hạn retained bằng hook đếm được topic/byte; session expiry 1h áp dụng cho MQTT 3.1.1 `clean_session=0`.
5. `MqttServer::run()` spawn/abort được; không còn accept connection sau khi dừng.
6. SQLx 0.9 migrate + SQLite WAL + Axum static embed chạy cùng một binary.

Nếu một điểm không đạt: dừng, mở lại ADR-001 trong `01-stack-and-architecture.md`. Không tự viết broker/protocol MQTT như một fallback MVP; chỉ tiếp tục sau một ADR và spike mới chứng minh một broker nhúng khác đạt toàn bộ contract.

## Không làm trong MVP

- Cluster, HA, multi-tenancy, billing.
- MQTT 5, TLS tích hợp trong process (chỉ proxy), WebSocket transport cho MQTT.
- Rule engine, OTA, Modbus/OPC-UA/BLE/LoRaWAN.
- PostgreSQL/MySQL (Phase 5, sau khi A01–A17 + A19 đạt trên SQLite).
- Cross-device pub/sub; MVP chỉ cho phép thiết bị pub/sub trong topic của chính nó.
- Aggregation/downsampling telemetry, dashboard builder kéo thả.
- Export/import dữ liệu giữa các engine.

## Cách đọc

- Cần biết **vì sao chọn** và **ai phụ thuộc ai**: đọc `01`.
- Cần viết code đúng contract: đọc `02` trước, `03` sau.
- Mọi thay đổi version, broker, frontend, TLS phải sửa `01` và ghi ADR mới; không đổi ngầm trong code.
