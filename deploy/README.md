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
