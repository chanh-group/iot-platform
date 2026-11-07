# MQTT load baseline — 2026-11-07

## Method

- Command: `IOT_LOAD_CGROUP=1 IOT_LOAD_DURATION_SECONDS=60 sh tests/load/run.sh`
- Harness: ignored `tests/load_baseline.rs`, in-process server and clients on local loopback, `cargo test` test profile; Docker limits the combined process to 4 vCPU and 8 GiB, with 8,192 file descriptors. Not the release image.
- Storage: temporary SQLite database on `/home` (Btrfs, `compress=zstd:1`) backed by WD PC SN5000S 476.9 GB NVMe; SQLite WAL mode, `synchronous=FULL`.
- Host: AMD Ryzen 7 H 255, 8 cores / 16 threads; 29 GiB RAM. Rust/Cargo 1.98.0.
- Workload: 1,000 MQTT 3.1.1 clients connected concurrently; QoS 1, one 43–47 byte JSON telemetry payload per client per second, unique `event_id`, staggered across each second; 60-second measurement, target 1,000 publish requests/s (60,000 over the window).
- What is counted: a publish request is counted when the runner starts creating it while `Instant::now() < end`. `client.publish(...).await` then passes the request through the client queue. The count is therefore **publish requests created in-window**; it does not prove that every request reached the broker, was committed, or was acknowledged inside those 60 seconds. PUBACK, application ACK, and committed-row totals are drained for up to 10 seconds after the window and are not window-bounded latency measurements.
- Ticks: the runner does not record skipped or late ticks separately. This run created 59,999 requests, one fewer than the 60,000 target; that shortfall alone does not prove which tick was late or why.

The numbers below are from the corrected rerun. An earlier runner gated publishes on the number already sent, so a delayed client could create a publish after the 60-second deadline while the total was still divided by 60 seconds. It now checks the real clock before creating each request, and `tests/load_baseline.rs::delayed_ticks_after_the_measurement_window_are_not_published` covers the late-tick case.

## Result

| Measurement | Result |
| --- | ---: |
| Target publish request rate | 1,000 requests/s (60,000 total) |
| Publish requests created in-window | 59,999 (one fewer than target; skipped ticks are not counted separately) |
| Publish request creation rate | 999.983 requests/s (59,999 / 60 s) |
| MQTT PUBACK (drain-complete total) | 59,999 / 59,999 (100%) |
| Committed SQLite rows (drain-complete total) | 59,999 / 59,999 |
| Application ACK (drain-complete total) | 30,720 / 59,999 (51.201%) |
| Application ACK dropped | 29,279 (48.799%) |
| Application ACK p95 (received ACKs only) | 515.627 ms |
| CONNECT-to-CONNACK p95 | 1,510.652 ms |
| Peak pending CONNECT attempts | 1,000 |
| Peak in-flight presence hooks | 711 |
| Peak load-test process RSS | 136,008 KiB |
| Peak cgroup memory | 149,332 KiB |
| SQLite DB/WAL/SHM growth | 22,731,624 bytes / 60 s (about 22.7 MB/min) |

The 999.983 requests/s figure is the rate at which publish requests were created in-window, not proof that the broker received or SQLite committed them at that rate or inside the window. By the end of the post-window drain, all 59,999 created requests had a PUBACK and a committed row; some may have completed earlier, but the harness does not timestamp broker receipt or commits relative to the window. Establishing those timings, or the cause of the one-request shortfall, would require separate instrumentation.

The 51.201% application ACK rate is **not a performance pass**: 29,279 application ACKs (48.799%) were dropped at this offered rate, so devices would need to retry the missing results. The ACK p95 covers only the ACKs that were received and excludes the dropped share, so it does not describe end-to-end latency for all events. The test also generates no retry traffic; behavior under retry load is unmeasured. This baseline is a failure signal for safe operation at 1,000 requests/s per instance, not a throughput promise or a release SLO.

The measurement is a same-process loopback run on one host, not a release-image deployment or independent client-generator benchmark. The hook peak counts in-flight connect/disconnect callbacks, not an explicit queue. Repeat on deployment hardware and with its actual resource limits before sizing production.
