# Chạy SQLite với Compose trên máy cục bộ

Build image và chuẩn bị thư mục dữ liệu được bind mount trước lần khởi động đầu tiên:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
deploy/prepare-data.sh
docker compose -f deploy/compose.sqlite.yml up -d
```

Server chạy với UID/GID `10001`; `prepare-data.sh` tạo `deploy/data` với chủ sở hữu đó. Script kiểm tra thư mục đã tồn tại trên host trước khi gọi Docker, nên sẽ từ chối thư mục không rỗng có chủ sở hữu không đúng mà không mount `:Z` hoặc đổi nhãn SELinux. Hãy sao lưu dữ liệu hiện có và chủ động sửa quyền sở hữu. Không để Compose tạo thư mục bind mount ban đầu bằng root.

Cả hai cổng được publish chỉ bind vào `127.0.0.1`. MQTT không mã hóa: không đưa cổng này ra Internet nếu không có TLS proxy. `cookie_secure = false` chỉ dành cho HTTP cục bộ; hãy bật tùy chọn này khi phục vụ qua HTTPS.

Để dừng dịch vụ an toàn, chạy `docker compose -f deploy/compose.sqlite.yml down`.

Quy trình backup/restore SQLite, kiểm tra checksum, WAL và dung lượng đĩa: [`docs/operations.md`](../docs/operations.md).

## PostgreSQL 18.6 trên máy cục bộ

Biến thể riêng dùng image `postgres:18.6-bookworm`, volume riêng cho PostgreSQL và volume app cho `runtime.toml`. Compose chờ healthcheck DB; app vẫn có retry bounded khi khởi động. Chỉ HTTP/MQTT publish lên loopback host, không publish port PostgreSQL.

Tạo password hex ngẫu nhiên trong file riêng, không commit:

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

File admin phải đọc được bởi UID container `10001`; quyền `0644` chỉ an toàn khi thư mục `secrets` là `0700` và host/Docker được tin cậy. Xóa file admin sau bootstrap. Bind mount `:z` gán lại SELinux label cho file được mount; không dùng file chia sẻ với ứng dụng khác nếu thay đổi label gây ảnh hưởng.

`compose-postgres.sh` đọc password từ file và tạo `IOT_DATABASE_URL` với `sslmode=disable` tường minh cho network local/isolated Compose. URL không lưu trong Compose hoặc image, nhưng Docker metadata của app vẫn chứa environment; chỉ operator được tin cậy mới được quyền Docker. Đây **không** phải production helper: DB remote phải dùng `sslmode=verify-full`, CA phù hợp và secret manager. `IOT_DATABASE_NAME` chọn DB app sẽ dùng; `IOT_POSTGRES_DATABASE` chọn DB ban đầu khi volume mới được tạo và mặc định theo `IOT_DATABASE_NAME`. Đổi tên không tự tạo DB trong volume đã tồn tại.

`./compose-postgres.sh down` giữ named volumes. Không thêm `-v` trong vận hành thường ngày. PostgreSQL backup/restore: [`docs/operations.md`](../docs/operations.md#postgresql-186-với-docker-compose-local-single-host).
