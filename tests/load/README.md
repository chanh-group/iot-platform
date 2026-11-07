# Baseline tải MQTT

Chạy từ thư mục gốc repository trên source đã checkout. Baseline P4-10 dùng Docker cgroup (4 vCPU / 8 GiB RAM). Runner build một image harness Debian nhỏ và đặt dữ liệu SQLite tạm dưới `target/` để đo tăng trưởng trên đĩa dự án, không phải `/tmp` (thường là tmpfs):

```sh
IOT_LOAD_CGROUP=1 IOT_LOAD_DURATION_SECONDS=60 sh tests/load/run.sh
```

Runner compile test trên host, rồi chạy binary của test trong container Debian tối giản bị Docker giới hạn ở bốn CPU và 8 GiB bộ nhớ; nó không benchmark release image của server.

Mỗi lần chạy dùng thư mục riêng dưới `target/load-tmp` (hoặc `IOT_LOAD_TMPDIR`), tránh xung đột relabel SELinux với lần chạy khác. Lần chạy thành công xóa thư mục của nó. Lần chạy thất bại hoặc bị ngắt để lại thư mục để kiểm tra; chỉ xóa sau khi xác nhận container của nó đã dừng.

Integration test ignored khởi động các thành phần thật của ứng dụng trong cùng process với storage SQLite tạm, tạo 1 000 credential thiết bị, mở 1 000 MQTT 3.1.1 session đồng thời và publish một event duy nhất mỗi client mỗi giây ở QoS 1. Một publish request chỉ được đếm nếu được tạo trước deadline thật của cửa sổ; nhịp đến trễ không được gửi bù. Con số là tốc độ tạo request, không chứng minh thời điểm broker nhận hoặc commit trong cửa sổ. ACK vẫn được drain tối đa 10 giây sau cửa sổ. Nhịp bị bỏ qua hoặc đến trễ không được đếm riêng. Test báo CONNECT-to-CONNACK p95, số callback presence hook chạy đồng thời tối đa (gồm công việc đang chờ thao tác SQL inline), đỉnh CONNECT đang chờ, số MQTT PUBACK, tỷ lệ/p95 application ACK, số row đã commit, số ACK bị drop, đỉnh RSS của process, đỉnh bộ nhớ cgroup khi có, và tăng trưởng file dữ liệu SQLite. Gauge callback đo công việc hook inline chạy đồng thời, không phải một queue tường minh của broker. Đây là baseline loopback cục bộ, không phải cam kết capacity production.

Dùng `IOT_LOAD_DURATION_SECONDS` để rút ngắn lần chạy smoke cho developer. Mục tiêu là 1 000 message/giây; dùng 60 giây cho bản ghi baseline. Bài đo ghi nhận hành vi quan sát được và cố ý không đặt ngưỡng release cho application ACK rate.
