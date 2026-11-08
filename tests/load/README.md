# Baseline tải MQTT

Chạy từ thư mục gốc repository trên source đã checkout. Baseline P4-10 dùng Docker cgroup (4 vCPU / 8 GiB RAM). Runner build một image harness Debian nhỏ và đặt dữ liệu SQLite tạm dưới `target/` để đo tăng trưởng trên đĩa dự án, không phải `/tmp` (thường là tmpfs):

```sh
IOT_LOAD_CGROUP=1 IOT_LOAD_DURATION_SECONDS=60 sh tests/load/run.sh
```

Runner compile test trên host, rồi chạy binary của test trong container Debian tối giản bị Docker giới hạn ở bốn CPU và 8 GiB bộ nhớ; nó không benchmark release image của server.

Mỗi lần chạy dùng thư mục riêng dưới `target/load-tmp` (hoặc `IOT_LOAD_TMPDIR`), tránh xung đột relabel SELinux với lần chạy khác. Lần chạy thành công xóa thư mục của nó. Lần chạy thất bại hoặc bị ngắt để lại thư mục để kiểm tra; chỉ xóa sau khi xác nhận container của nó đã dừng.

Integration test ignored khởi động các thành phần thật của ứng dụng trong cùng process với storage SQLite tạm, tạo 1 000 credential thiết bị, mở 1 000 MQTT 3.1.1 session đồng thời và publish một event duy nhất mỗi client mỗi giây ở QoS 1. Một publish request chỉ được đếm nếu được tạo trước deadline thật của cửa sổ; nhịp đến trễ không được gửi bù. Con số là tốc độ tạo request, không chứng minh thời điểm broker nhận hoặc commit trong cửa sổ. ACK vẫn được drain tối đa 10 giây sau cửa sổ. Nhịp bị bỏ qua hoặc đến trễ không được đếm riêng. Test báo CONNECT-to-CONNACK p95, số callback presence hook chạy đồng thời tối đa (gồm công việc đang chờ thao tác SQL inline), đỉnh CONNECT đang chờ, số MQTT PUBACK, tỷ lệ/p95 application ACK, số row đã commit, số ACK bị drop, đỉnh RSS của process, đỉnh bộ nhớ cgroup khi có, và tăng trưởng file dữ liệu SQLite. Gauge callback đo công việc hook inline chạy đồng thời, không phải một queue tường minh của broker. Đây là baseline loopback cục bộ, không phải cam kết capacity production.

Dùng `IOT_LOAD_DURATION_SECONDS` để rút ngắn lần chạy smoke cho developer. Mục tiêu là 1 000 message/giây; dùng 60 giây cho bản ghi baseline. Bài đo ghi nhận hành vi quan sát được và cố ý không đặt ngưỡng release cho application ACK rate.

## Probe Python: CONNECT cạnh tranh và telemetry retry

Probe `tests/load/mqtt-probe.py` chạy **thủ công**, tách biệt với baseline tải và CI. Cần Python 3.14 (dùng UUID v7) và `paho-mqtt==2.1.0`; dependency chỉ dành cho test, lý do ghi trong `docs/plans/01-stack-and-architecture.md`. Tại repo root:

```sh
cargo build --release --locked
uv run --python 3.14 --with paho-mqtt==2.1.0 python tests/load/mqtt-probe.py
uv run --python 3.14 --with paho-mqtt==2.1.0 python -m unittest discover -s tests/load -p 'test_mqtt_probe.py'
```

Script tự tạo thư mục/SQLite và credential fixture, chạy binary release trên loopback với port riêng, rồi dọn fixture của chính nó. Nó không dùng broker đang chạy, không in token hoặc payload. Mỗi lượt tạo 16 CONNECT cùng device ID, quan sát `Server unavailable`, retry với jitter/backoff tối đa 8 lần; sau đó 16 device gửi telemetry QoS 1, cố ý bỏ qua ACK `stored`, đợi timeout và gửi lại đúng bytes, yêu cầu `duplicate` và đúng số row SQLite. Mỗi điều kiện có assert; nếu không xuất hiện `Server unavailable`, probe fail thay vì coi nhánh retry đã được kiểm chứng.

Đây là **smoke nhỏ**, không phải benchmark tải hay bằng chứng hành vi firmware thật; không chứng minh nguyên nhân mọi lỗi CI. Chạy concurrent probe nhiều lần có thể cho số CONNACK khác nhau; không dùng kết quả làm throughput/SLO. Lệnh không sửa `iot-server:local` hoặc các container đã có.
