# MQTT load baseline

Run from the repository root against the checked-out source. The P4-10 baseline uses Docker cgroups (4 vCPU / 8 GiB RAM). The runner builds a small Debian harness image and puts SQLite temp data under `target/` so growth is measured on the project disk, not `/tmp` (often tmpfs):

```sh
IOT_LOAD_CGROUP=1 IOT_LOAD_DURATION_SECONDS=60 sh tests/load/run.sh
```

The runner compiles the test on the host, then runs its binary in a minimal Debian container constrained by Docker to four CPUs and 8 GiB memory; it does not benchmark the server's release image.

Each run uses its own directory under `target/load-tmp` (or `IOT_LOAD_TMPDIR`), avoiding SELinux relabel conflicts with another run. Successful runs remove their directory. Failed or interrupted runs leave it for inspection; remove it only after verifying its container has stopped.

The ignored integration test starts the real application components in-process with temporary SQLite storage, creates 1,000 device credentials, opens 1,000 simultaneous MQTT 3.1.1 sessions, and publishes one unique event per client per second at QoS 1. A publish request is counted only if it is created before the real window deadline; a tick delivered late is not backfilled. The count is a request-creation rate, not proof of broker receipt or commit timing within the window. ACKs are still drained for up to 10 seconds after the window. Skipped or late ticks are not counted separately. It reports CONNECT-to-CONNACK p95, maximum concurrent presence hook callbacks (including work waiting on inline SQL operations), pending-CONNECT peak, MQTT PUBACK count, application ACK rate/p95, committed rows, ACK drops, peak process RSS, cgroup memory peak when available, and SQLite data-file growth. The callback gauge measures concurrent inline hook work, not an explicit broker queue. This is a local loopback baseline, not a production capacity guarantee.

Use `IOT_LOAD_DURATION_SECONDS` to shorten a developer smoke run. The target is 1,000 messages/second; use 60 seconds for the baseline record. This measures observed behavior and deliberately sets no application ACK-rate release threshold.
