# Baseline tải MQTT — 2026-11-07

## Phương pháp

- Lệnh: `IOT_LOAD_CGROUP=1 IOT_LOAD_DURATION_SECONDS=60 sh tests/load/run.sh`
- Harness: `tests/load_baseline.rs` ignored, server và client chạy trong cùng process trên loopback, profile test của `cargo test`; Docker giới hạn process kết hợp ở 4 vCPU và 8 GiB, với 8 192 file descriptor. Không phải release image.
- Storage: database SQLite tạm trên `/home` (Btrfs, `compress=zstd:1`) trên ổ WD PC SN5000S 476,9 GB NVMe; SQLite WAL mode, `synchronous=FULL`.
- Host: AMD Ryzen 7 H 255, 8 core / 16 thread; 29 GiB RAM. Rust/Cargo 1.98.0.
- Khối lượng đo: 1 000 client MQTT 3.1.1 kết nối đồng thời; QoS 1, mỗi client gửi một payload telemetry JSON 43–47 byte mỗi giây, `event_id` duy nhất, rải đều trong từng giây; đo 60 giây, mục tiêu 1 000 publish request/s (60 000 trong cửa sổ).
- Cách đếm: một publish request được đếm khi runner bắt đầu tạo nó trong lúc `Instant::now() < end`. `client.publish(...).await` sau đó đưa request qua hàng đợi client. Vì vậy con số là **số publish request được tạo trong cửa sổ**; nó không chứng minh mọi request đã tới broker, đã commit hoặc đã được ACK trong đúng 60 giây đó. Tổng PUBACK, application ACK và số row đã commit được drain tối đa 10 giây sau cửa sổ và không phải phép đo latency giới hạn trong cửa sổ.
- Nhịp: runner không ghi riêng số nhịp bị bỏ qua hoặc đến trễ. Lần chạy này tạo 59 999 request, thiếu một so với mục tiêu 60 000; chỉ riêng việc thiếu đó không chứng minh nhịp nào đến trễ hay vì sao.

Các số dưới đây đến từ lần chạy lại đã sửa. Runner trước đó gate publish theo số đã gửi, nên client bị trễ vẫn có thể tạo publish sau deadline 60 giây trong khi tổng vẫn chia cho 60 giây. Hiện runner kiểm tra đồng hồ thật trước khi tạo từng request, và `tests/load_baseline.rs::delayed_ticks_after_the_measurement_window_are_not_published` bao phủ trường hợp nhịp trễ.

## Kết quả

| Phép đo | Kết quả |
| --- | ---: |
| Tốc độ publish request mục tiêu | 1 000 request/s (tổng 60 000) |
| Publish request được tạo trong cửa sổ | 59 999 (thiếu một so với mục tiêu; nhịp bị bỏ qua không được đếm riêng) |
| Tốc độ tạo publish request | 999,983 request/s (59 999 / 60 giây) |
| MQTT PUBACK (tổng sau khi drain hoàn tất) | 59 999 / 59 999 (100%) |
| Row SQLite đã commit (tổng sau khi drain hoàn tất) | 59 999 / 59 999 |
| Application ACK (tổng sau khi drain hoàn tất) | 30 720 / 59 999 (51,201%) |
| Application ACK bị drop | 29 279 (48,799%) |
| Application ACK p95 (chỉ các ACK nhận được) | 515,627 ms |
| CONNECT-to-CONNACK p95 | 1 510,652 ms |
| Đỉnh số lần CONNECT đang chờ | 1 000 |
| Đỉnh số presence hook đang chạy | 711 |
| Đỉnh RSS của process load test | 136 008 KiB |
| Đỉnh bộ nhớ cgroup | 149 332 KiB |
| Tăng trưởng SQLite DB/WAL/SHM | 22 731 624 byte / 60 giây (khoảng 22,7 MB/phút) |

Con số 999,983 request/s là tốc độ tạo publish request trong cửa sổ, không chứng minh broker nhận hoặc SQLite commit ở tốc độ đó hay trong đúng cửa sổ. Đến cuối giai đoạn drain sau cửa sổ, cả 59 999 request đã tạo đều có PUBACK và row đã commit; một số có thể đã hoàn tất sớm hơn, nhưng harness không ghi timestamp broker nhận hoặc commit so với cửa sổ. Muốn xác định các thời điểm đó, hoặc nguyên nhân thiếu một request, cần instrumentation riêng.

Tỷ lệ application ACK 51,201% **không phải pass hiệu năng**: 29 279 application ACK (48,799%) bị drop ở tốc độ offered này, nên device sẽ phải retry các kết quả bị thiếu. ACK p95 chỉ tính trên các ACK nhận được và loại trừ phần bị drop, nên nó không mô tả latency end-to-end cho toàn bộ event. Bài đo cũng không tạo tải retry; hành vi dưới tải retry chưa được đo. Baseline này là tín hiệu lỗi đối với vận hành an toàn ở 1 000 request/s mỗi instance, không phải cam kết throughput hay SLO phát hành.

Phép đo là chạy loopback trong cùng process trên một host, không phải triển khai release image hay benchmark với client generator độc lập. Đỉnh hook đếm callback connect/disconnect đang chạy, không phải một queue tường minh. Cần lặp lại trên phần cứng triển khai và giới hạn tài nguyên thật trước khi sizing production.
