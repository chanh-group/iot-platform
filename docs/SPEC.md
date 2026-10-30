# SPEC — Rust IoT Server All-in-One

- Phiên bản tài liệu: 0.1
- Ngày: 2026-10-28
- Trạng thái: đặc tả đề xuất để triển khai; chưa phải sản phẩm đã hoàn thành.
- Tên binary tạm thời: `iot-server`.

## 1. Mục tiêu

Xây dựng một IoT platform thu gọn bằng Rust, lấy cảm hứng từ mô hình ThingsBoard, chạy bằng một binary hoặc Docker và cấu hình khởi động qua `conf.toml`.

Ứng dụng tích hợp MQTT broker để node kết nối trực tiếp và pub/sub, HTTP server phục vụ dashboard/API, quản lý thiết bị, lưu telemetry và gửi lệnh xuống thiết bị. Dashboard cho phép thay đổi các thiết lập runtime được hỗ trợ.

“All-in-one” nghĩa là broker, API, dashboard và core chạy trong cùng một process. SQLite được nhúng; PostgreSQL/MySQL là database server bên ngoài, kết nối bằng URL hoặc triển khai kèm qua Docker Compose. Không tự viết PostgreSQL/MySQL server và không yêu cầu broker MQTT bên ngoài.

## 2. Phạm vi và nguyên tắc

### 2.1. MVP

- Một instance, một không gian quản lý, tài khoản quản trị.
- MQTT broker tích hợp; xác thực node và ACL topic.
- Tạo, xem, cập nhật, vô hiệu hóa thiết bị và xoay vòng credential.
- Nhận telemetry JSON, lưu SQLite, truy vấn lịch sử và xem realtime.
- Gửi lệnh có `command_id`, theo dõi xác nhận từ thiết bị.
- Dashboard nhúng trong binary, không cần Node.js khi chạy.
- Cấu hình TOML, kiểm tra trước khởi động và cập nhật runtime có revision.
- Log có cấu trúc, health/readiness, graceful shutdown, Docker image.

### 2.2. Sau MVP

- PostgreSQL và MySQL với migration, kiểm thử tương đương SQLite.
- TLS MQTT tích hợp, nếu MVP triển khai TLS qua TCP proxy.
- Thuộc tính thiết bị nâng cao, rule đơn giản và export dữ liệu.
- Tài khoản chỉ đọc, giới hạn chi tiết theo nhóm thiết bị.

### 2.3. Ngoài phạm vi ban đầu

- Cluster broker, HA, multi-tenancy, billing.
- Rule engine kéo thả, plugin động, marketplace.
- OTA firmware, Modbus, OPC-UA, BLE, LoRaWAN.
- Tương thích toàn bộ API/protocol của ThingsBoard.
- Cam kết exactly-once end-to-end hoặc công suất chưa benchmark.

## 3. Kiến trúc

Thiết kế modular monolith: module nội bộ tách trách nhiệm, không tách microservice.

```mermaid
flowchart TD
    N["Node IoT"] <-->|MQTT| M["Broker tích hợp"]
    W["Trình duyệt"] <-->|HTTP và WebSocket| H["API và dashboard"]
    M <--> C["Core: device, telemetry, command"]
    H <--> C
    H --> R["Runtime configuration"]
    R --> M
    R --> C
    C --> D["Storage abstraction"]
    D --> S["SQLite"]
    D --> P["PostgreSQL hoặc MySQL"]
```

| Module | Trách nhiệm |
| --- | --- |
| config | Parse, validate, hợp nhất cấu hình, revision, snapshot runtime |
| mqtt | Broker adapter, authentication, ACL, sự kiện kết nối và publish |
| core/device | Thiết bị, credential, trạng thái kết nối |
| core/telemetry | Validate, chuẩn hóa, deduplicate và lưu dữ liệu |
| core/command | Vòng đời lệnh và application acknowledgement |
| storage | Repository theo nghiệp vụ, transaction, migration |
| http | REST API, phiên đăng nhập, WebSocket, static assets |
| runtime | Khởi động, giám sát task, shutdown |
| observability | Log, health, counters và latency |

Core không phụ thuộc loại HTTP framework hoặc kiểu dữ liệu nội bộ của broker. Dùng bounded channel ở các điểm chuyển giao bất đồng bộ; không dùng queue RAM không giới hạn. Lỗi task quan trọng phải ảnh hưởng readiness hoặc làm process thoát có kiểm soát.

## 4. Stack dự kiến và quyết định còn mở

| Thành phần | Đề xuất |
| --- | --- |
| Runtime | Tokio |
| HTTP / WebSocket | Axum |
| CLI | Clap |
| TOML / JSON | Serde, toml, serde_json |
| SQL | SQLx; driver và migration riêng từng backend |
| Log | tracing, tracing-subscriber |
| Shutdown | CancellationToken |
| Frontend | TypeScript và một framework được chốt khi triển khai |
| Static assets | Nhúng bundle frontend bằng cơ chế compile-time |
| Broker | Thư viện broker nhúng, chọn sau thử nghiệm kỹ thuật |

Các lựa chọn trên là định hướng, chưa khóa version hoặc xác nhận mọi khả năng của thư viện. `rumqttc` là MQTT client, không được dùng thay cho MQTT broker.

Trước khi triển khai broker cần kiểm tra license, vòng đời bảo trì, MQTT conformance, hooks auth/ACL, giới hạn tài nguyên, session, shutdown và khả năng tích hợp pipeline. Nếu không có thư viện phù hợp, phải viết quyết định kiến trúc riêng trước khi tự triển khai giao thức.

## 5. MQTT và mô hình thiết bị

### 5.1. Giao thức

Mục tiêu MVP là MQTT 3.1.1, hỗ trợ QoS 0/1/2, wildcard subscription, retained message, Keep Alive, Last Will và session theo chuẩn. MQTT 5 là hạng mục sau. Không quảng bá broker tương thích đầy đủ nếu chưa qua kiểm thử conformance; nếu broker lựa chọn thiếu tính năng, phải cập nhật ma trận hỗ trợ trước release.

MVP không cam kết session/retained state sống qua process restart. Session giữa các lần reconnect trong cùng vòng đời process phải theo chuẩn. Dữ liệu telemetry đã commit và command đã tạo phải bền vững độc lập với broker session.

### 5.2. Danh tính và quyền

- Mỗi thiết bị có ID ổn định và một credential có thể thu hồi.
- MQTT username là `device_id`, password là token; token không ghi vào log.
- Credential được lưu dưới dạng hash phù hợp, token gốc chỉ hiển thị lúc cấp.
- `client_id` phải khớp quy tắc của thiết bị; MVP dùng một kết nối hoạt động/thiết bị.
- Kết nối mới cùng client ID thay thế kết nối cũ theo MQTT; cập nhật trạng thái bằng connection generation để tránh sự kiện cũ ghi đè trạng thái mới.
- Không tin `device_id` do payload tự khai báo; kiểm tra với danh tính kết nối.
- Thiết bị bị disable hoặc thu hồi credential phải bị ngắt kết nối đang mở và bị từ chối reconnect.

| Topic | Node publish | Node subscribe | Ý nghĩa |
| --- | --- | --- | --- |
| `devices/{id}/telemetry` | Có | Không | Telemetry cần lưu |
| `devices/{id}/telemetry/ack` | Không | Có | Xác nhận ứng dụng sau commit |
| `devices/{id}/attributes` | Có | Không | Thuộc tính do node báo cáo |
| `devices/{id}/commands` | Không | Có | Lệnh từ server |
| `devices/{id}/commands/ack` | Có | Không | Kết quả thực thi |
| `devices/{id}/status` | Có | Có | Trạng thái do node/LWT báo cáo |
| `devices/{id}/data/#` | Có | Có | Pub/sub thông thường, không tự lưu SQL |

Mặc định deny mọi quyền khác. Server nội bộ có principal riêng để gửi command và telemetry ACK. ACL phải kiểm tra cả SUBSCRIBE filter và giao nhận message; wildcard không được mở rộng quyền ra thiết bị khác. Pub/sub giữa nhiều node chỉ được bật bằng ACL được admin cấp rõ ràng, có kiểm thử thu hồi quyền.

Trạng thái online do kết nối xác thực quyết định; status/LWT chỉ là tín hiệu bổ sung. Sau server restart, thiết bị được coi offline cho đến khi reconnect.

### 5.3. Payload telemetry

```json
{
  "event_id": "019c-example-stable-id",
  "ts": 1793149200000,
  "values": {
    "temperature": 27.5,
    "humidity": 65,
    "relay": true
  }
}
```

- `event_id`: node sinh và giữ nguyên khi retry; bắt buộc cho luồng xác nhận bền vững.
- `ts`: Unix milliseconds UTC; thiếu thì dùng thời gian server nhận.
- `received_at`: server tự ghi, không cho client ghi đè.
- `values`: object phẳng, tối đa 128 key; giá trị number hữu hạn, boolean hoặc string tối đa 4 KiB; không nhận object/array/null trong MVP.
- ID thiết bị lấy từ principal và topic. Tổng payload giới hạn bởi cấu hình.
- Timestamp sai kiểu hoặc vượt chính sách cho phép phải bị từ chối; không âm thầm sửa.
- Retained publish vào topic telemetry/command không được xử lý thành telemetry/lệnh; trả lỗi ứng dụng cho telemetry có event ID hợp lệ.

### 5.4. Tính bền vững và backpressure

MQTT ACK chỉ xác nhận ở tầng giao thức, không đồng nghĩa SQL đã commit. Luồng đáng tin cậy dùng application ACK:

1. Node publish telemetry và giữ bản local cho đến khi nhận ACK ứng dụng.
2. Server xác thực, kiểm tra payload và đưa vào queue có giới hạn.
3. Worker ghi transaction và deduplicate theo `(device_id, event_id)`.
4. Chỉ sau commit, server publish ACK trạng thái `stored`; bản gửi lại giống hệt nhận `duplicate`.
5. Node xóa bản local sau `stored` hoặc `duplicate`; timeout thì retry cùng ID.

Nếu cùng ID nhưng nội dung khác, trả `event_id_conflict`. DB lỗi hoặc queue đầy thì không trả ACK thành công; có thể trả lỗi retryable khi nhận diện được event. Node retry với exponential backoff và jitter. QoS 0 không có application retry được coi best-effort.

Bản ghi dedup nằm cùng transaction với telemetry. Bảo đảm dedup chỉ trong thời gian còn giữ dữ liệu; ID cũ hơn retention không được coi là được bảo vệ vĩnh viễn. Không cam kết exactly-once.

WebSocket chỉ phát telemetry đã commit. Mất kết nối WebSocket không làm mất dữ liệu trong SQL; client tải lại lịch sử khi reconnect.

## 6. HTTP API và dashboard

Prefix API: `/api/v1`. JSON request/response, thời gian UTC milliseconds, lỗi có `code`, `message`, `request_id`; không trả secret hoặc stack trace.

| Method | Endpoint | Hành vi |
| --- | --- | --- |
| POST | `/auth/login` | Tạo session quản trị |
| POST | `/auth/logout` | Thu hồi session hiện tại |
| GET/POST | `/devices` | Danh sách có phân trang / tạo thiết bị |
| GET/PATCH | `/devices/{id}` | Chi tiết / đổi tên, metadata, enabled |
| POST | `/devices/{id}/credentials/rotate` | Cấp token mới, thu hồi token cũ |
| GET | `/devices/{id}/telemetry` | Lịch sử theo thời gian và key |
| GET | `/devices/{id}/telemetry/latest` | Giá trị mới nhất theo key |
| POST | `/devices/{id}/commands` | Tạo command, trả 202 và command ID |
| GET | `/commands/{id}` | Trạng thái/kết quả lệnh |
| GET | `/config` | Cấu hình hiệu lực đã che secret và revision |
| PATCH | `/config/runtime` | Cập nhật trường hot-reload với revision |
| GET | `/events` | WebSocket đã xác thực |
| GET | `/healthz` | Process sống |
| GET | `/readyz` | DB và dịch vụ thiết yếu sẵn sàng |

Telemetry history yêu cầu `from`, `to`, `limit`; mặc định 100, tối đa 1000. Cursor ổn định dựa trên thứ tự `(ts, internal_id)`, khoảng thời gian dùng `[from, to)`. Latest chọn theo `(ts, received_at, internal_id)` để kết quả xác định khi trùng timestamp. Chưa có aggregation/downsampling trong MVP.

Dashboard gồm: đăng nhập, tổng quan kết nối, danh sách/chi tiết thiết bị, tạo/disable/rotate token, biểu đồ telemetry theo khoảng thời gian, trạng thái command, chỉnh runtime config và hiển thị lỗi lưu dữ liệu. Không cần dashboard builder kéo thả.

WebSocket có event type, event ID và payload; giới hạn số subscription và queue mỗi client. Client chậm bị disconnect có lý do; reconnect phải truy vấn bù qua REST, không giả định WebSocket có replay bền vững.

HTTP ingestion cho node là phần mở rộng, không bắt buộc MVP: HTTP server MVP phục vụ quản trị và dashboard; thiết bị gửi telemetry qua MQTT.

## 7. Command

- Tạo command phải lưu SQL trước khi trả HTTP 202.
- Command chứa `command_id`, `name`, `params`, `created_at`, `expires_at`.
- Trạng thái: `pending`, `dispatched`, `succeeded`, `failed`, `expired`.
- Thiết bị offline: giữ `pending` đến reconnect hoặc hết hạn; queue có giới hạn.
- `dispatched` chỉ có nghĩa đã bàn giao cho broker, không có nghĩa thiết bị đã thực thi.
- ACK thiết bị phải khớp device ID và command ID; `succeeded`/`failed` là terminal.
- Worker có thể gửi lại sau mất kết nối/restart; node phải deduplicate bằng command ID.
- Lệnh mặc định không retained; không gửi lại lệnh đã hết hạn.
- HTTP tạo command hỗ trợ idempotency key để retry không tạo lệnh mới.
- ACK lặp không làm đổi kết quả terminal; ACK đến sau expiry được ghi nhận để chẩn đoán nhưng không tự chuyển thành thành công.

## 8. Storage

### 8.1. Mô hình logic

| Bảng | Nội dung và ràng buộc chính |
| --- | --- |
| users | Admin, password hash, enabled |
| sessions | Session hash, user, expiry, revoke |
| devices | ID, tên, metadata, enabled, created/updated |
| device_credentials | Device ID, token hash, trạng thái |
| telemetry_events | Internal ID, device ID, event ID, ts, received_at, values JSON, payload hash |
| device_attributes | Device ID, key, value, updated_at |
| commands | Command ID, device, payload, trạng thái, expiry, retry metadata, result |
| audit_events | Ai thực hiện thao tác quản trị nào và khi nào |
| schema_migrations | Phiên bản schema theo backend |

Unique `(device_id, event_id)`; index telemetry `(device_id, ts, internal_id)`, command `(status, expires_at)`. Không dùng timestamp đơn lẻ làm khóa. Latest có thể tính qua truy vấn trước; chỉ thêm cache/bảng projection khi có benchmark.

### 8.2. Backend

- SQLite là mặc định, dùng WAL và thiết kế một đường ghi batch để hạn chế cạnh tranh writer.
- PostgreSQL/MySQL dùng pool hữu hạn, transaction và prepared/bound parameters.
- Migration và kiểu lưu JSON do từng backend quyết định; repository giữ semantics chung.
- Đổi URL không tự migrate dữ liệu giữa các engine. Công cụ export/import là hạng mục riêng.
- Migrate trước khi mở listener; lỗi migration làm khởi động thất bại. Không tự downgrade schema.
- Retention dựa trên `received_at`, xóa theo batch, không giữ lock dài.
- Khi DB mất kết nối hoặc disk đầy: readiness false, ngừng xác nhận lưu thành công, giữ RAM hữu hạn và phục hồi với backoff.
- DB connection string không được trả nguyên văn qua API/log.

## 9. Cấu hình

### 9.1. Ví dụ `conf.toml`

Đây là schema mục tiêu để triển khai. Giá trị giới hạn là mặc định đề xuất, không phải số liệu throughput đã đo.

```toml
config_version = 1

[server]
name = "my-iot-server"
data_dir = "./data"
shutdown_timeout_ms = 10000

[http]
listen = "127.0.0.1:8080"
dashboard = true
max_body_bytes = 262144

[mqtt]
listen = "127.0.0.1:1883"
allow_anonymous = false
max_connections = 1000
max_packet_bytes = 262144
max_inflight_per_client = 32
max_queued_messages_per_client = 256

[database]
url = "sqlite://./data/iot.db"
max_connections = 5
connect_timeout_ms = 5000
# Với database ngoài: xóa url và dùng url_env.
# url_env = "IOT_DATABASE_URL"
# Giá trị env ví dụ: postgres://iot:password@postgres:5432/iot
# Hoặc: mysql://iot:password@mysql:3306/iot

[telemetry]
queue_capacity = 4096
batch_size = 500
flush_interval_ms = 1000
retention_days = 30
max_future_skew_ms = 300000

[commands]
default_ttl_ms = 60000
max_pending_per_device = 100
retry_initial_ms = 1000
retry_max_ms = 30000

[runtime]
settings_path = "./data/runtime.toml"

[logging]
level = "info"
format = "json"
```

Path tương đối được resolve theo thư mục chứa `conf.toml`, không theo working directory. SQLite file path trong URL cũng theo quy tắc này. Không tự tạo/thay thế database ngoài khi URL sai. Kiểm tra kiểu, range, trường không biết và quan hệ như `batch_size <= queue_capacity`.

`url` và `url_env` loại trừ nhau. `*_env` là hành vi của ứng dụng, không phải interpolation tự có của TOML. Env thiếu phải báo lỗi; không fallback ngầm. Không hỗ trợ thay thế biến tùy ý trong chuỗi ở MVP.

### 9.2. Precedence và runtime

Precedence: default → `conf.toml` → `runtime.toml` đối với trường được phép override. Environment chỉ giải quyết tham chiếu secret ở trường được định nghĩa; CLI chọn file và subcommand, không có lớp override tùy ý.

| Thiết lập | Áp dụng nóng |
| --- | --- |
| Tên thiết bị, credential, enabled, ACL | Có; đây là dữ liệu nghiệp vụ lưu SQL |
| Log level | Có |
| Retention days, batch size, flush interval | Có, từ batch/job tiếp theo |
| Database URL/engine/pool | Không, restart |
| HTTP/MQTT bind address, TLS settings | Không, restart |
| Data directory, queue capacity | Không, restart |

PATCH config runtime yêu cầu `expected_revision`. Xung đột trả 409. Trường không hỗ trợ đổi nóng trả 422 với danh sách `restart_required`; không ghi cấu hình nửa chừng.

Quy trình update: khóa cập nhật → kiểm tra revision → tạo/validate toàn bộ snapshot → chuẩn bị tài nguyên cần thiết → ghi file tạm cùng filesystem, sync và atomic rename → thay snapshot trong RAM bằng thao tác không thất bại → trả revision mới. Worker đọc snapshot nhất quán; lỗi ghi file giữ cấu hình cũ. File runtime tồn tại nhưng lỗi parse phải làm startup fail rõ ràng, không bỏ qua âm thầm. Không hỗ trợ tự watch file ở MVP.

## 10. Vận hành và bảo mật

- Không có admin/password mặc định. Lệnh bootstrap tạo admin qua nhập tương tác hoặc secret file; password hash bằng thuật toán phù hợp như Argon2id.
- Session dùng cookie HttpOnly, SameSite và Secure khi HTTPS; chống CSRF cho thao tác thay đổi trạng thái và kiểm tra Origin WebSocket.
- Mặc định bind localhost. Khi công khai qua mạng, phải triển khai TLS: HTTPS reverse proxy và MQTT TLS proxy, hoặc listener TLS tích hợp được kiểm thử.
- Không coi port MQTT plaintext là an toàn chỉ vì chạy trong Docker.
- CORS mặc định same-origin; không dùng wildcard với credential.
- Giới hạn login, connection, packet, topic, retained bytes, session và WebSocket queue để tránh tài nguyên không giới hạn.
- Tách xác thực node và người dùng; không dùng device token gọi API admin.
- Không ghi token/password/payload nhạy cảm vào log mặc định. Audit các thay đổi credential, thiết bị và config nhưng redact secret.
- Readiness false khi DB hoặc task trọng yếu lỗi; health không tiết lộ cấu hình.
- Metric tối thiểu: active connections, ingest accepted/rejected, retained-cap rejection (`iot_retained_cap_rejected_total`), queue depth, commit latency/errors, command pending/expired. Không gắn device ID làm label metric toàn cục gây cardinality lớn.
- Endpoint metrics mặc định tắt; khi bật phải dùng credential máy-đến-máy riêng, không dùng session dashboard, và chỉ mở cho hệ thống giám sát qua network policy hoặc mTLS/reverse proxy. Release phải có alert cho DB/readiness lỗi, disk gần đầy, queue saturation, ingest rejection và backup lỗi.

Shutdown: ngừng nhận kết nối/request mới → dừng producer → drain queue và commit trong deadline → đóng broker/HTTP/DB. Không xác nhận thành công cho item chưa commit. Command chưa kết thúc phục hồi từ SQL; telemetry chưa nhận application ACK do node retry. Hết deadline phải log số lượng còn chờ rồi thoát có kiểm soát.

## 11. CLI và đóng gói

```bash
iot-server check --config conf.toml
iot-server migrate --config conf.toml
iot-server admin create --config conf.toml
iot-server run --config conf.toml
iot-server --version
```

`check` validate cấu hình và tham chiếu env, không sửa DB. `migrate` áp dụng migration một cách tường minh; `run` cũng thực hiện migration an toàn trước mở listener. Không cho hai instance cùng quản lý một file SQLite/runtime config trong MVP.

Docker image dùng binary release và static dashboard nhúng; chạy non-root, log stdout/stderr, nhận SIGTERM. Container bind `0.0.0.0` theo cấu hình triển khai; chỉ publish các port cần dùng. File config mount read-only, data/runtime path mount writable.

Ví dụ CLI mục tiêu, dùng image build tại chỗ:

```bash
docker run --rm --name iot-server \
  -p 8080:8080 -p 1883:1883 \
  -v "$PWD/conf.toml:/app/conf.toml:ro" \
  -v "$PWD/data:/app/data" \
  iot-server:local run --config /app/conf.toml
```

File config cho ví dụ Docker phải bind `0.0.0.0:8080` và `0.0.0.0:1883`; path data tương đối resolve thành `/app/data`. Image name là tên đề xuất, chưa phải image đã phát hành.

Compose có ba biến thể tài liệu: SQLite, PostgreSQL, MySQL. Database server ngoài có volume và healthcheck; ứng dụng vẫn phải retry kết nối, không chỉ dựa vào startup ordering. Không nhúng password mẫu cố định vào image.

Backup SQLite dùng SQLite backup API hoặc dừng ứng dụng trước khi copy; không copy riêng file `.db` đang dùng WAL rồi coi là backup đầy đủ. PostgreSQL/MySQL dùng quy trình backup/restore của engine. Test restore trước release hỗ trợ backend đó.

## 12. Tổ chức source

Bắt đầu bằng một Rust crate; chỉ tách workspace khi ranh giới module hoặc thời gian build cần thiết.

| Path | Nội dung |
| --- | --- |
| `src/main.rs` | CLI và entry point |
| `src/config/` | Schema, validation, runtime snapshots |
| `src/runtime/` | Lifecycle, supervisor, shutdown |
| `src/mqtt/` | Embedded broker adapter, auth, ACL |
| `src/core/` | Device, telemetry, command services |
| `src/storage/` | Repository và backend implementations |
| `src/http/` | API, middleware, WebSocket, assets |
| `src/observability/` | Log và health |
| `migrations/sqlite/` | SQLite migrations |
| `migrations/postgres/` | PostgreSQL migrations, giai đoạn sau |
| `migrations/mysql/` | MySQL migrations, giai đoạn sau |
| `web/` | Frontend source và build |
| `tests/` | Integration, protocol và end-to-end |
| `deploy/` | Dockerfile, Compose và config mẫu |

Ưu tiên typed config và repository theo nghiệp vụ. Không tự tạo ORM, dynamic plugin ABI hoặc generic event framework trước khi có nhu cầu thực tế. Chỉ dùng in-process broadcast cho tín hiệu realtime best-effort; không coi broadcast channel là queue bền vững.

## 13. Tiêu chí nghiệm thu

| ID | Kịch bản | Kết quả bắt buộc |
| --- | --- | --- |
| A01 | Chạy binary mới với SQLite | Migrate thành công; dashboard/API và MQTT sẵn sàng |
| A02 | Không có admin | Không có tài khoản mặc định; bootstrap tạo được admin |
| A03 | Token sai/thiết bị disable | MQTT bị từ chối; không ghi telemetry |
| A04 | Node A truy cập topic B, kể cả wildcard | Không đọc/ghi được dữ liệu B |
| A05 | Gửi telemetry hợp lệ | Commit SQL, nhận application ACK, dashboard thấy dữ liệu |
| A06 | Gửi lại cùng event ID/payload | Một bản ghi, ACK duplicate |
| A07 | Cùng event ID, payload khác | Báo conflict, giữ nguyên bản ghi cũ |
| A08 | Kill process trước/sau commit, trước ACK | Retry không mất dữ liệu đã ACK và không tạo bản trùng trong retention |
| A09 | DB lỗi, disk đầy, queue đầy | RAM hữu hạn; không có ACK lưu thành công giả; readiness phản ánh lỗi |
| A10 | Node offline khi tạo command | Pending bền vững; reconnect nhận lệnh chưa hết hạn |
| A11 | Command resend/ACK trùng | Command ID giữ nguyên; terminal state nhất quán |
| A12 | Runtime update hợp lệ | Lưu revision; restart giữ thay đổi |
| A13 | Config sai, revision cũ, lỗi ghi file | Từ chối toàn bộ update; cấu hình hiệu lực không bị cập nhật một phần |
| A14 | Rotate token khi node đang online | Kết nối cũ bị đóng; token cũ không reconnect được |
| A15 | WebSocket client chậm/reconnect | Queue hữu hạn; tải bù lịch sử qua REST được |
| A16 | SIGTERM | Drain có deadline; restart không mất bản đã ACK |
| A17 | Docker recreate với cùng volume | Giữ thiết bị, credential, telemetry và runtime config |
| A18 | Mỗi backend SQL được công bố hỗ trợ | Cùng bộ integration test và backup/restore pass |
| A19 | Broker protocol suite | Kiểm thử QoS, wildcard, retain, LWT, session, duplicate client ID phù hợp ma trận hỗ trợ |

Kiểm thử gồm unit test validation/ACL/state machine, integration test MQTT→SQL→REST/WebSocket, crash recovery và backend compatibility. Load test phải ghi rõ hardware, engine, kích thước message, QoS, số connection, tốc độ ingest, p95 latency, RAM và disk growth. Chưa đặt throughput thành cam kết release trước khi có kết quả đo.

## 14. Lộ trình

1. **Technical spike:** chọn broker nhúng, kiểm tra auth/ACL và khả năng kiểm thử giao thức; chốt frontend và ghi quyết định kiến trúc.
2. **Nền tảng:** CLI, TOML, runtime, SQLite migration, bootstrap admin, HTTP health và dashboard shell.
3. **Luồng telemetry:** device credentials, MQTT, ACL, validate, SQL commit, application ACK và lịch sử.
4. **Quản trị:** dashboard realtime, command, runtime config, audit và token rotation.
5. **Độ tin cậy:** bounded resources, crash/retry, shutdown, test bảo mật và Docker/backup.
6. **Backend bổ sung:** PostgreSQL rồi MySQL, mỗi backend qua cùng tiêu chí nghiệm thu.

MVP hoàn tất khi A01–A17 và A19 đạt trên SQLite, tài liệu triển khai và backup/restore có thể thực hiện được. Release MVP phải có SBOM, quét dependency/OS image, image signature/provenance, alert/runbook và restore drill pass. A18 là điều kiện riêng để công bố hỗ trợ từng backend SQL tiếp theo.

## 15. Những quyết định cần xác nhận khi triển khai

- Broker library và khả năng session persistence qua restart.
- TLS tích hợp ngay trong MVP hay triển khai qua proxy có tài liệu rõ ràng.
- Frontend framework và thư viện chart.
- Dải phần cứng đích và các mục tiêu hiệu năng sau benchmark.
- Thời gian lưu command/audit và giới hạn retained/session phù hợp phần cứng.

Không mục nào ở đây ngầm yêu cầu cluster, external MQTT broker hoặc database server khi người dùng chọn chế độ SQLite.
