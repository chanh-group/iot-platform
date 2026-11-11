# Vận hành

SQLite vẫn là backend mặc định. Tài liệu này giữ nguyên quy trình SQLite bên dưới và bổ sung mục triển khai PostgreSQL riêng; nguồn sự thật hành vi là [02 — Contract kỹ thuật](plans/02-contracts.md).

## Triển khai với Docker Compose (SQLite)

Điều kiện:

- Docker Engine với Compose v2 trên Linux x86_64. Image chạy với UID/GID `10001`.
- Filesystem cục bộ cho `IOT_DATA_DIR`. SQLite WAL cần POSIX lock hoạt động đúng; không đặt thư mục dữ liệu trên NFS, SMB hoặc filesystem không cung cấp lock tin cậy.
- Cổng host còn trống (mặc định `8080`/`1883`) hoặc đổi qua `IOT_HTTP_PORT`/`IOT_MQTT_PORT`.
- Filesystem dữ liệu đủ chỗ cho database, WAL, backup và bản copy tạm khi restore.

Build image cục bộ và chuẩn bị bind mount trước lần khởi động đầu tiên:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
cd deploy
./prepare-data.sh
```

`prepare-data.sh` tạo `deploy/data` thuộc sở hữu `10001:10001`. Script từ chối thư mục không rỗng thuộc user khác thay vì đổi chủ sở hữu. Không để Compose tự tạo bind mount bằng root, và không chạy `prepare-data.sh` lên dữ liệu đang có mà chưa backup.

Khởi tạo schema và admin duy nhất khi server đang dừng:

```sh
docker compose -f compose.sqlite.yml run --rm iot-server migrate --config /app/conf.toml
docker compose -f compose.sqlite.yml run --rm iot-server admin create --config /app/conf.toml
```

`run` dùng cùng image, config và data volume nhưng không publish port. `admin create` yêu cầu schema đã migrate, hỏi mật khẩu hai lần (tối thiểu 12 ký tự) và giữ lock độc quyền của data directory, nên không được có server đang chạy. Không có admin mặc định.

Khởi động và kiểm tra:

```sh
docker compose -f compose.sqlite.yml up -d
docker compose -f compose.sqlite.yml ps
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

Hai cổng publish trong file Compose mẫu chỉ bind vào `127.0.0.1`. Config mẫu dùng MQTT plaintext và `cookie_secure = false`; chỉ dành cho localhost. Với triển khai từ xa, phải terminate HTTPS và MQTT TLS ở proxy, đặt `cookie_secure = true`, và không publish MQTT plaintext ra mạng không tin cậy.

Log, restart và stop:

```sh
docker compose -f compose.sqlite.yml logs -f --tail=200 iot-server
docker compose -f compose.sqlite.yml restart iot-server
docker compose -f compose.sqlite.yml stop iot-server
docker compose -f compose.sqlite.yml down
```

`down` giữ nguyên thư mục dữ liệu bind mount. Không xóa `deploy/data` để "reset" triển khai; hãy backup và restore có chủ đích.

## Nâng cấp

1. Tạo backup và kiểm tra checksum (xem bên dưới).
2. Build image mới với cùng tag local.
3. Chạy `docker compose -f compose.sqlite.yml up -d`; Compose tạo lại container và server áp dụng migration còn thiếu khi khởi động.
4. Kiểm tra `/readyz`, device/history mong đợi qua API và revision runtime config.

Nếu khởi động lỗi, giữ image cũ và backup. Không xóa database hoặc file WAL để ép server khởi động.

## Cấu hình

Triển khai Compose mount `deploy/conf.docker.toml` read-only tại `/app/conf.toml`. File này bind listener vào `0.0.0.0` bên trong container và lưu dữ liệu dưới `/app/data`. [`conf.toml`](../conf.toml) ở repository là mẫu đầy đủ cho local/mặc định; range và quy tắc validate nằm trong [02 §5](plans/02-contracts.md).

Biến Compose:

| Biến | Mặc định | Mục đích |
| --- | --- | --- |
| `IOT_HTTP_PORT` | `8080` | Cổng HTTP trên host |
| `IOT_MQTT_PORT` | `1883` | Cổng MQTT trên host |
| `IOT_DATA_DIR` | `./data` | Đường dẫn bind mount trên host, tương đối so với `deploy/` |
| `IOT_METRICS_TOKEN` | rỗng | Bearer token truyền vào container; chỉ cần khi bật metrics |

Sửa `conf.docker.toml` và restart với các field không hot-reload. Chỉ các setting sau hot-reload được: `logging.level`, `telemetry.retention_days`, `telemetry.batch_size` và `telemetry.flush_interval_ms`. Cập nhật chúng qua `PATCH /api/v1/config/runtime` (xem [hướng dẫn API](api.md)); sửa trực tiếp `runtime.toml` chỉ được đọc lúc khởi động. Mọi field config khác cần restart, và `conf.toml` không được watch.

Đường dẫn tương đối trong file config resolve theo thư mục chứa file đó, không theo thư mục làm việc của process. `server.data_dir` phải chứa file SQLite và `runtime.settings_path`. `check` validate mà không tạo file; `run`, `migrate` và `admin create` tạo data directory và giữ lock của nó.

## Log

Log mặc định ra stdout dạng JSON, nên Docker thu lại. Dùng:

```sh
docker compose -f compose.sqlite.yml logs -f --tail=200 iot-server
```

Đặt `[logging] format = "text"` để log dễ đọc khi chạy local; `format` cần restart. `logging.level` (`trace`, `debug`, `info`, `warn`, `error`) đổi được qua runtime config API mà không cần restart. Lỗi CLI và cấu hình ghi ra stderr. Token, password, payload và database URL không được ghi log.

## Shutdown và restart

Container nhận `SIGTERM` khi `docker compose stop`, `restart` hoặc `down`. Server ngừng nhận HTTP request và MQTT session mới, drain telemetry và application ACK trong `server.shutdown_timeout_ms` (mặc định 10 000 ms), rồi dừng MQTT, HTTP và database. Compose cho phép grace period 20 giây.

- Signal thứ hai buộc thoát ngay với exit code `1`.
- Nếu hết deadline drain, process log phần việc còn lại và thoát nonzero. Không bỏ qua exit code đó; kiểm tra log và xác minh dữ liệu sau khi restart.
- Không dùng `docker kill` hoặc `kill -9` trừ khi process đã không phản hồi; shutdown không sạch có thể để lại WAL cần giữ nguyên để phục hồi.

Sau shutdown không sạch, giữ nguyên toàn bộ data directory trước khi sửa, rồi chạy `PRAGMA integrity_check` trên bản copy đã dừng.

## Backup và restore SQLite

Từ thư mục `deploy/`, tạo backup khi Compose đang chạy:

```sh
./backup.sh /path/to/backups/iot-2026-11-06
```

Chạy với đúng Compose project identity đã dùng để khởi động service. Ví dụ, nếu service được start bằng `docker compose -p field-gateway ...`, đặt `COMPOSE_PROJECT_NAME=field-gateway` cho lệnh backup; nếu tên thiếu hoặc sai, script từ chối copy.

Script dùng Compose project hiện tại (`COMPOSE_PROJECT_NAME` nếu có), từ chối chạy nếu project không có container `iot-server` hoặc mount `/app/data` của container không khớp `IOT_DATA_DIR`, gửi SIGTERM và đợi grace period 20 giây đã cấu hình, rồi xác nhận container đã dừng. Sau đó script dùng runtime image với quyền root để đọc data mount UID/GID 10001. Một lock độc quyền nonblocking trên `/data/.lock` được giữ trong suốt thời gian copy `iot.db`, các file `-wal`/`-shm` còn lại và `runtime.toml`; nếu process app khác đang giữ lock, backup thất bại. Điều này bảo vệ cả trường hợp sai Compose project và trường hợp service tự restart trong lúc copy file.

Thư mục backup mới có mode `0700`; các artifact gồm `SHA256SUMS` có mode `0600`, thuộc user host đang chạy. Mọi lỗi copy/checksum đều abort và xóa staging directory thay vì publish backup dở. Script từ chối ghi đè backup đã tồn tại. Service vẫn dừng sau backup; từ `deploy/`, khởi động lại bằng:

```sh
docker compose -f compose.sqlite.yml up -d
```

**Không bao giờ chỉ copy `iot.db` khi ứng dụng đang chạy ở WAL mode.** Transaction đã commit gần đây có thể còn trong `iot.db-wal`; bản copy database đơn lẻ có thể bỏ mất chúng mà không báo lỗi. Hoặc dừng app như trên, hoặc dùng SQLite online backup API từ công cụ hiểu SQLite.

Verify và restore vào data directory mới:

```sh
cd /path/to/backups/iot-2026-11-06
sha256sum -c SHA256SUMS
cd /path/to/ThingsBoardRust/deploy
mkdir restore-data
cp /path/to/backups/iot-2026-11-06/iot.db restore-data/
for sidecar in iot.db-wal iot.db-shm; do
  if [ -f "/path/to/backups/iot-2026-11-06/$sidecar" ]; then
    cp "/path/to/backups/iot-2026-11-06/$sidecar" restore-data/
  fi
done
if [ -f /path/to/backups/iot-2026-11-06/runtime.toml ]; then cp /path/to/backups/iot-2026-11-06/runtime.toml restore-data/; fi
```

Gán file đã restore cho UID/GID non-root của container, rồi start với thư mục đó:

```sh
sudo chown -R 10001:10001 restore-data
IOT_DATA_DIR="$PWD/restore-data" docker compose -f compose.sqlite.yml up -d
```

Không chạy `prepare-data.sh` trên thư mục không rỗng thuộc user khác. Kiểm tra `/readyz`, device/history mong đợi qua API đã xác thực và revision runtime config. Giữ nguyên backup gốc cho đến khi verify xong.

## Retention và tính dung lượng đĩa

Retention telemetry do `telemetry.retention_days` điều khiển (1–3 650, mặc định 30). Một lượt sweep chạy lúc khởi động và sau đó mỗi giờ, xóa các row có `received_at < now - retention_days` theo batch 5 000. Setting này hot-reload được. Command (kể cả expired), audit event, attribute mới nhất và credential đã revoke được giữ vô thời hạn.

Xóa telemetry không tự động thu nhỏ file SQLite. WAL checkpoint tái sử dụng page theo thời gian; chỉ chạy `VACUUM` trên database đã dừng, sau khi có backup đã verify, và phải còn đủ chỗ cho bản copy tạm. Không xóa file `-wal` hoặc `-shm` khi process đang chạy.

Baseline đã đo tăng khoảng 22,7 MB mỗi phút ở 1 000 publish request/s. Con số này xấp xỉ 32,7 GB mỗi ngày với cùng tốc độ offered, nhưng không ngoại suy tuyến tính cho 30 ngày: retention và tái sử dụng page làm thay đổi kích thước dài hạn. Dùng nó làm cận trên ban đầu, monitor `du -h deploy/data/iot.db*` và đo lại trên phần cứng/tốc độ thật. Tính dung lượng data filesystem cho retention, backup và bản copy restore; alert khi dung lượng trống còn từ 20% trở xuống (đã dùng 80%).

## WAL và monitoring dung lượng

SQLite dùng WAL với `synchronous=FULL` và checkpoint tự động. File `-wal` và `-shm` là bình thường khi server chạy; chỉ có chúng không phải là lỗi. Không xóa hoặc đổi tên chúng khi process đang chạy. Monitor `df -h` cho data filesystem (alert khi đã dùng 80%), `du -h deploy/data/iot.db*` cho tăng trưởng database/WAL, `/readyz` cho readiness, và exit code/checksum của backup job. Monitor dung lượng đích backup riêng. Cạn đĩa làm giảm readiness và khiến telemetry được retry/reject thay vì ACK là đã lưu.

## Monitoring

Dùng các check sau cùng nhau:

- `/healthz` trả lời khi process còn sống; không kiểm tra database.
- `/readyz` trả `503` kèm trạng thái từng thành phần khi database, MQTT listener, ingest worker hoặc command dispatcher không khỏe.
- `/api/v1/overview` cung cấp counter device, telemetry, command, ingest và storage cho dashboard.
- `/metrics` export Prometheus metric khi được bật rõ ràng.

Bật metrics trong `deploy/conf.docker.toml`:

```toml
[http]
metrics = true
metrics_bearer_token_env = "IOT_METRICS_TOKEN"
```

Cung cấp `IOT_METRICS_TOKEN` (tối thiểu 32 byte) qua Compose hoặc secret manager, rồi scrape bằng:

```sh
curl -fsS -H "Authorization: Bearer $IOT_METRICS_TOKEN" \
  http://127.0.0.1:8080/metrics
```

Không bao giờ expose `/metrics` công khai. Endpoint từ chối cookie admin và yêu cầu Bearer token. Tên metric và ngữ nghĩa nằm trong [02 §4.7](plans/02-contracts.md); các nhóm hữu ích nhất là MQTT connection/kick failure, ingest accepted/rejected theo reason, queue depth, commit latency/error, application ACK drop, command pending/expired/error, WebSocket client/drop, audit error, retention error và backup freshness.

## Alert rules

`deploy/alerts/prometheus.rules.yml` chứa Prometheus rule cho metrics scrape, kết quả `/readyz` và probe availability (scrape down hoặc thiếu `probe_success`), data filesystem, telemetry queue saturation, ingest rejection, application ACK bị drop, SQLite commit error, backup thất bại/quá cũ. Ví dụ giả định Prometheus scrape `/metrics` với `job="iot-server"`, Blackbox Exporter probe `/readyz` với `job="iot-server-readyz"`, và Node Exporter báo data filesystem tại `mountpoint="/var/lib/iot-server"`. Chỉnh matcher mountpoint cho filesystem host chứa `IOT_DATA_DIR`; giữ label scrape/probe khớp với rule.

Để alert backup freshness, bật textfile collector của Node Exporter và chạy backup với thư mục collector hiện có, ví dụ:

```sh
cd deploy
IOT_BACKUP_METRICS_DIR=/var/lib/node_exporter/textfile_collector \
  ./backup.sh /path/to/backups/iot-2026-11-06
```

Mỗi lần backup hợp lệ cập nhật atomic `iot_backup_last_attempt_timestamp_seconds` và `iot_backup_last_attempt_success`; `IotBackupFailed` alert khi kết quả gần nhất thất bại hoặc thiếu metric trạng thái, trong khi `iot_backup_last_success_timestamp_seconds` không đổi cho đến khi có backup thành công. `IotBackupMissing` cũng alert nếu 24 giờ không ghi nhận thành công. Đảm bảo thư mục textfile collector ghi được bởi user chạy backup. Lên lịch backup thường xuyên hơn ngưỡng freshness và monitor dung lượng đích backup riêng. Kiểm tra cú pháp rule và case firing/clearing bằng `sh tests/alerts.sh` (cần Docker; dùng Prometheus `promtool`).

Nếu cập nhật textfile lỗi, script cảnh báo nhưng giữ exit status của backup; metric trước đó có thể bị stale. Monitor exit status của scheduler và stderr của script riêng, kể cả lỗi ghi collector.

### Runbook

| Alert | Kiểm tra và hành động đầu tiên |
| --- | --- |
| `IotServerUnavailable` | Xác nhận container/process đang chạy và metrics port/token đúng. Kiểm tra `docker compose ps` và log. Nếu process đã chết, giữ dữ liệu và restart; điều tra lỗi cuối cùng trước khi lặp lại. |
| `IotServerNotReady` | Gọi `/readyz` và xác định check nào fail. Kiểm tra dung lượng đĩa, commit error gần đây, trạng thái MQTT listener và lỗi command dispatcher. Không restart lặp lại mà không giữ log/dữ liệu. |
| `IotReadinessProbeUnavailable` | Kiểm tra target Blackbox Exporter, đường mạng và scrape `probe_success`. Alert này có thể fire dù ứng dụng khỏe. |
| `IotDataDiskLow` | Giải phóng dung lượng hoặc mở rộng data filesystem. Không xóa WAL/SHM. Chỉ giảm retention sau khi hiểu yêu cầu dữ liệu; verify backup trước. |
| `IotTelemetryQueueSaturated` | Giảm offered load hoặc sửa nghẽn storage. Kiểm tra commit error, độ trễ đĩa và `iot_telemetry_queue_depth`. Tăng `queue_capacity` cần restart và chỉ thêm buffer có giới hạn, không tăng throughput. |
| `IotTelemetryAckDropped` | Application ACK queue tràn; device phải retry các ACK bị mất. Kiểm tra tải, ingest latency và queue depth. Baseline 1 000 request/s drop khoảng 48,8% ACK, nên đây là tín hiệu capacity thật ở tốc độ đó. |
| `IotIngestRejected` | Xem `iot_ingest_rejected_total{reason}`. Lỗi validation là bug client; `queue_full` là server quá tải; `storage_unavailable` là lỗi database/đĩa. Sửa nguyên nhân, không chỉ sửa alert. |
| `IotDatabaseCommitErrors` | Coi là critical: kiểm tra dung lượng đĩa, sức khỏe filesystem, SQLite lock và log. Readiness phải đang fail. Dừng ghi, giữ data directory, verify integrity trên bản copy đã dừng và restore backup nếu cần. |
| `IotBackupMissing` / `IotBackupFailed` | Kiểm tra scheduler, dung lượng đích backup, tên Compose project, khớp `IOT_DATA_DIR` và lock conflict. Xem stderr và textfile collector; chạy lại backup sau khi sửa nguyên nhân. |

## Troubleshooting

Exit code CLI:

| Exit | Ý nghĩa |
| ---: | --- |
| `0` | Lệnh thành công |
| `1` | Lỗi runtime, gồm lỗi database trong `admin create`, hoặc vượt shutdown deadline |
| `2` | Dùng CLI sai |
| `3` | Validate cấu hình hoặc environment thất bại |
| `4` | Mở/tạo database hoặc migration thất bại (`run`/`migrate`) |
| `5` | Data-directory lock đang bị giữ |
| `6` | Admin đã tồn tại |

**Lock conflict (exit 5).** Một process `run`, `migrate` hoặc `admin create` khác đang giữ `<data_dir>/.lock`. Dừng process/container kia; lock được nhả khi process đó thoát. Xóa `.lock` không nhả lock đang bị process khác giữ và có thể che mất conflict thật. Nếu container đã biến mất, xác minh không có process host nào dùng cùng data directory.

**Migration hoặc database failure (exit 4).** Chạy `iot-server check` trước, rồi kiểm tra quyền và dung lượng trống trên data filesystem. Đường dẫn database phải nằm trong `data_dir`. Không xóa database để thử lại. Giữ nguyên toàn bộ thư mục, chạy `PRAGMA integrity_check` trên bản copy đã dừng, và restore backup đã verify nếu database hỏng. `admin create` không migrate; chạy `migrate` trước. Lỗi schema/database trong `admin create` trả exit `1`, không phải `4`.

**Admin đã tồn tại (exit 6).** MVP cho phép đúng một admin. Dùng tài khoản hiện có; không có CLI reset password. Nếu mất credential, phục hồi từ backup hoặc can thiệp database có kiểm soát sau khi giữ bản database hiện tại.

**Configuration error (exit 3).** Lỗi nêu path bị sai. Kiểm tra field lạ, range không hợp lệ, `allow_anonymous = true`, thiếu `url_env`, metrics token ngắn hơn 32 byte, hoặc path nằm ngoài `data_dir`. `check` không tạo data directory hoặc database.

**Disk full.** Readiness giảm, commit fail, telemetry được retry hoặc reject; server không được ACK là đã lưu. Giải phóng hoặc mở rộng filesystem, kiểm tra dung lượng đích backup, rồi mới để retry tiếp tục. Không xóa WAL/SHM khi process đang chạy. Sau khi phục hồi, verify integrity và `/readyz` xanh trở lại.

**Database lỗi hoặc nghi hỏng.** Dừng service, giữ nguyên toàn bộ data directory, chạy `PRAGMA integrity_check` trên bản copy. Restore backup vào thư mục mới nếu không thể sửa an toàn. Giữ nguyên bản gốc cho đến khi verify thành công.

**MQTT connection bị từ chối.** Xác nhận client dùng MQTT 3.1.1, `username = device_id`, `client_id = device_id` và token hiện tại. `0x04` bao gồm credential sai, device không tồn tại/đã disable và ID không khớp; `0x05`/`0x87` nghĩa là sai protocol version. Rotate hoặc disable device sẽ kick session đang mở. Kiểm tra connection cap và địa chỉ listener.

**MQTT publish bị disconnect hoặc timeout.** Publish bị ACL từ chối sẽ đóng connection mà không có PUBACK. Kiểm tra prefix topic, wildcard, retained flag, số level topic và packet size. SUBACK `0x80` nghĩa là filter subscription không được phép. Vượt retained cap cũng đóng connection.

**Login fail hoặc lặp.** `401 invalid_credentials` nghĩa là admin không tồn tại hoặc sai mật khẩu; `429 rate_limited` nghĩa là phải đợi `Retry-After`. Nếu `cookie_secure = true` nhưng browser dùng HTTP, cookie không được gửi; dùng HTTPS hoặc chỉ đặt `false` cho HTTP local.

**Dashboard trả 404.** Kiểm tra `http.dashboard`; khi false, `/` và asset chủ động không khả dụng trong khi API vẫn chạy. Với debug build, thiếu `web/dist` trả trang hướng dẫn build; release build yêu cầu asset nhúng.

Sau shutdown không sạch hoặc nghi lỗi storage, giữ một bản copy của toàn bộ data directory trước khi sửa, rồi dùng SQLite integrity check trên bản copy đã dừng (ví dụ `sqlite3 iot.db 'PRAGMA integrity_check;'`). Database khỏe trả `ok`.

## PostgreSQL 18.6 với Docker Compose (local/single host)

Biến thể `deploy/compose.postgres.yml` chạy app và `postgres:18.6-bookworm` với hai named volume độc lập. Chỉ HTTP/MQTT publish lên loopback host; port 5432 không publish. PostgreSQL healthcheck gating là một tiện ích khởi động, không thay thế retry bounded trong app. Image PostgreSQL 18 dùng volume mount `/var/lib/postgresql` (không dùng mount cũ `/var/lib/postgresql/data`).

Tạo secret hex ngẫu nhiên và đặt tên DB. File password phải được giữ ngoài Git và chỉ operator đọc được:

```sh
cd deploy
mkdir -p secrets
chmod 0700 secrets
umask 077
openssl rand -hex 32 > secrets/postgres_password
chmod 0600 secrets/postgres_password
export IOT_POSTGRES_PASSWORD_FILE="$PWD/secrets/postgres_password"
export IOT_DATABASE_NAME=iot
```

Bootstrap và start:

```sh
./compose-postgres.sh run --rm iot-server migrate --config /app/conf.toml
openssl rand -base64 32 > secrets/admin_password
chmod 0644 secrets/admin_password
./compose-postgres.sh run --rm \
  --volume "$PWD/secrets/admin_password:/run/secrets/admin_password:ro,z" \
  iot-server admin create --username admin --config /app/conf.toml \
  --password-file /run/secrets/admin_password
./compose-postgres.sh up -d
curl -fsS http://127.0.0.1:8080/readyz
```

Admin file phải đọc được bởi UID container `10001`: chỉ dùng `0644` trong thư mục `0700` trên host tin cậy và xóa file sau bootstrap. Bind mount `:z` thay SELinux label của file; không dùng file chia sẻ nếu việc đổi label ảnh hưởng ứng dụng khác.

`compose-postgres.sh` chỉ dành cho local/isolated Compose, đọc password từ file và tạo URL với `sslmode=disable` tường minh theo contract. Nó không ghi URL vào file, nhưng Docker lưu environment của app trong metadata; người có quyền Docker daemon/root vẫn có thể đọc biến môi trường. Dùng quyền daemon hạn chế và không dùng helper này cho production. DB remote phải cấu hình `sslmode=verify-full` với CA phù hợp; inject URL từ secret manager qua `IOT_DATABASE_URL`, không dùng URL mẫu local. Khi tạo volume mới, `IOT_POSTGRES_DATABASE` mặc định theo `IOT_DATABASE_NAME`; với volume đã tồn tại, đổi tên không tạo database hay chuyển dữ liệu tự động.

### PostgreSQL backup

Tạo backup nhất quán khi app đang chạy:

```sh
./backup-postgres.sh /path/to/backups/iot-postgres-2026-11-10
```

Script kiểm tra container PostgreSQL thuộc đúng Compose project hiện tại, dùng secret file trong container, tạo custom-format dump bằng `pg_dump`, ghi version/database và SHA-256 manifest vào staging directory mode `0700`, rồi publish atomic. Destination đã tồn tại thì bị từ chối; lỗi dump không publish backup dở. Không truyền URL/password qua argv hoặc log. Giữ dump, manifest và secret theo chính sách lưu trữ/khôi phục của operator; dump chứa toàn bộ dữ liệu nhạy cảm.

### Restore vào database mới

Restore không ghi đè database đang dùng:

```sh
cd /path/to/backups/iot-postgres-2026-11-10
sha256sum -c SHA256SUMS
cd /path/to/ThingsBoardRust/deploy
export IOT_POSTGRES_PASSWORD_FILE="$PWD/secrets/postgres_password"
./restore-postgres.sh /path/to/backups/iot-postgres-2026-11-10 iot_restore_20261110
IOT_DATABASE_NAME=iot_restore_20261110 ./compose-postgres.sh up -d --force-recreate iot-server
curl -fsS http://127.0.0.1:8080/readyz
```

Restore script chỉ nhận database mới chưa tồn tại, yêu cầu target PostgreSQL 18 và xác minh checksum trước `pg_restore`; database gốc không bị sửa. Sau khi app reconnect, verify admin login, devices/credentials qua API, telemetry history, command, audit và runtime revision. Giữ nguyên database/backup gốc tới khi drill được xác nhận. Nếu restore dở, không đổi `IOT_DATABASE_NAME` sang DB đó; điều tra/xóa chỉ database mới do drill sở hữu, không `down -v` volume.

`docker compose down` giữ cả named volumes; `docker compose down -v` xóa DB và app runtime data, không dùng trong vận hành thường ngày. Healthcheck DB không tự restart container unhealthy. Runbook outage: kiểm tra `/readyz`, `docker compose ps`/logs, volume còn gắn, secrets và network; sau DB recovery app tự thử lại có backoff. Không giả định application ACK thành công cho transaction chưa commit. `sslmode=disable` chỉ phù hợp mạng Compose local cô lập, không phải cấu hình production.

Local integration/restore drill có lệnh `sh tests/postgres_compose.sh`. Script tạo project, port, credential, secret và image tag riêng; seed qua CLI/API/MQTT, pg_dump vào staging, restore sang DB mới, down/up rồi kiểm tra API/MQTT/runtime. Nó xóa đúng project volumes và image tag do chính lượt chạy tạo. Không chạy trên Compose project/database khác với fixture test.
