# PostgreSQL MQTT load baseline — 2026-11-11

Chạy `sh tests/load/postgres-fixture.sh` trên commit nền `0052915` cùng thay đổi P5 chưa commit. Lượt chạy cô lập 2026-11-11 16:35:16–16:36:35 UTC; test 1 passed, 0 failed. Log đầy đủ của lượt này: `/tmp/opencode/p5-postgres-load-final.log` trên máy chạy, không phải artifact bền vững. Fixture tạo PostgreSQL container riêng rồi cleanup.

- PostgreSQL 18.6, image `postgres:18.6-bookworm`, image ID `sha256:f1ea2a1a4fb7b68d4836be1fdcb8a6466a89b072158bf9d97663600538c294c5`; Docker Engine 29.9.2; Linux x86_64, 16 logical CPU. Runner không đọc được cgroup CPU/memory limit; không có cgroup memory peak.
- Pool tối đa 5 connections, connect timeout 5.000 ms, tối đa 5 lần kết nối; telemetry retry 100–5.000 ms. 1.000 client MQTT QoS 1; payload trung bình 46,590 byte; cửa sổ tạo publish 60 giây, drain 10,011 giây.
- 60.000 publish request được tạo, 60.000 MQTT PUBACK, 60.000 telemetry rows được commit cho devices của lượt đo; 25.809 application ACK `stored`, không có `rejected`, 34.191 ACK thiếu (56,985%). Metric server `telemetry_ack_dropped` bằng 34.191. PUBACK không chứng minh commit; số rows được query riêng sau drain.
- Application ACK p95 810,774 ms; CONNACK p95 1.703,170 ms; server process RSS peak quan sát được 85.524 KiB. PostgreSQL `pg_database_size` tăng từ 9.803.455 lên 40.477.199 byte (30.673.744 byte); không bao gồm toàn bộ WAL/container disk.

Đây là một phép đo trên một host, không có lặp lại hoặc so sánh SQLite cùng điều kiện. ACK drop cao; không dùng làm SLO, sizing production hay bằng chứng hỗ trợ PostgreSQL. Cần thiết bị retry cùng event ID và bytes cho ACK bị thiếu. Kết quả không mô phỏng mạng thực, power loss, HA/failover hoặc tải đa instance.
