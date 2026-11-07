# Operations

This is the operator guide for the SQLite MVP. The technical source of truth for behaviour is [02 — Contract kỹ thuật](plans/02-contracts.md); this document covers deployment, configuration, logging, shutdown, backup/restore, retention, monitoring, alerts and troubleshooting.

## Deploy with Docker Compose (SQLite)

Prerequisites:

- Docker Engine with Compose v2 on Linux x86_64. The image runs as UID/GID `10001`.
- A local filesystem for `IOT_DATA_DIR`. SQLite WAL needs working POSIX locks; do not place the data directory on NFS, SMB, or another filesystem that does not provide reliable locks.
- Free host ports (default `8080`/`1883`) or remapped `IOT_HTTP_PORT`/`IOT_MQTT_PORT`.
- The data filesystem has room for the database, WAL, backups, and temporary restore copies.

Build the local image and prepare the bind mount before the first start:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
cd deploy
./prepare-data.sh
```

`prepare-data.sh` creates `deploy/data` owned by `10001:10001`. It refuses a non-empty directory owned by another user rather than changing ownership. Do not let Compose create the bind mount as root, and do not run `prepare-data.sh` over existing data without a backup.

Initialize the schema and the single admin while the server is stopped:

```sh
docker compose -f compose.sqlite.yml run --rm iot-server migrate --config /app/conf.toml
docker compose -f compose.sqlite.yml run --rm iot-server admin create --config /app/conf.toml
```

`run` uses the same image, config and data volume but does not publish ports. `admin create` requires the migrated schema, prompts twice for the password (minimum 12 characters), and takes the exclusive data-directory lock, so no server may be running. There is no default admin.

Start and verify:

```sh
docker compose -f compose.sqlite.yml up -d
docker compose -f compose.sqlite.yml ps
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/readyz
```

Both published ports bind to `127.0.0.1` in the sample Compose file. The sample config uses plaintext MQTT and `cookie_secure = false`; it is for localhost only. For a remote deployment, terminate HTTPS and MQTT TLS at a proxy, set `cookie_secure = true`, and do not publish plaintext MQTT to an untrusted network.

Logs, restart and stop:

```sh
docker compose -f compose.sqlite.yml logs -f --tail=200 iot-server
docker compose -f compose.sqlite.yml restart iot-server
docker compose -f compose.sqlite.yml stop iot-server
docker compose -f compose.sqlite.yml down
```

`down` keeps the bind-mounted data directory. Do not delete `deploy/data` to "reset" a deployment; use a backup and a deliberate restore instead.

## Upgrade

1. Create a backup and verify its checksums (see below).
2. Build the new image with the same local tag.
3. Run `docker compose -f compose.sqlite.yml up -d`; Compose recreates the container and the server applies pending migrations at startup.
4. Check `/readyz`, the expected device/history through the API, and the runtime config revision.

If startup fails, keep the old image and backup. Do not delete the database or WAL files to force a start.

## Configuration

The Compose deployment mounts `deploy/conf.docker.toml` read-only at `/app/conf.toml`. It binds the listeners to `0.0.0.0` inside the container and stores data under `/app/data`. The repository's [`conf.toml`](../conf.toml) is the complete local/default sample; field ranges and validation rules are in [02 §5](plans/02-contracts.md).

Compose variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `IOT_HTTP_PORT` | `8080` | Host port for HTTP |
| `IOT_MQTT_PORT` | `1883` | Host port for MQTT |
| `IOT_DATA_DIR` | `./data` | Host bind-mount path, relative to `deploy/` |
| `IOT_METRICS_TOKEN` | empty | Bearer token passed to the container; required only when metrics are enabled |

Edit `conf.docker.toml` and restart for fields that are not hot-reloadable. The only hot-reloadable settings are `logging.level`, `telemetry.retention_days`, `telemetry.batch_size`, and `telemetry.flush_interval_ms`. Update them through `PATCH /api/v1/config/runtime` (see the [API guide](api.md)); directly editing `runtime.toml` is only read at startup. All other config fields need a restart, and `conf.toml` is never watched.

Relative paths in a config file resolve against the directory containing that file, not the process working directory. `server.data_dir` must contain the SQLite file and `runtime.settings_path`. `check` validates without creating files; `run`, `migrate`, and `admin create` create the data directory and take its lock.

## Logging

Logs go to stdout as JSON by default, so Docker captures them. Use:

```sh
docker compose -f compose.sqlite.yml logs -f --tail=200 iot-server
```

Set `[logging] format = "text"` for human-readable local logs; `format` requires a restart. `logging.level` (`trace`, `debug`, `info`, `warn`, `error`) can be changed through the runtime config API without a restart. CLI and configuration errors are written to stderr. Tokens, passwords, payloads, and database URLs are not logged.

## Shutdown and restart

The container receives `SIGTERM` on `docker compose stop`, `restart`, or `down`. The server stops accepting new HTTP requests and MQTT sessions, drains telemetry and application ACKs within `server.shutdown_timeout_ms` (default 10 000 ms), then stops MQTT, HTTP and the database. Compose allows a 20-second grace period.

- A second signal forces an immediate exit with status `1`.
- If the drain deadline expires, the process logs the remaining work and exits nonzero. Do not ignore that exit; inspect the logs and verify data after restart.
- Do not use `docker kill` or `kill -9` unless the process is already unresponsive; an unclean stop can leave a WAL that must be preserved for recovery.

After an unclean stop, preserve the whole data directory before attempting repair, then run `PRAGMA integrity_check` on a stopped copy.

## SQLite backup and restore

From the `deploy/` directory, create a backup while Compose is running:

```sh
./backup.sh /path/to/backups/iot-2026-11-06
```

Run it with the same Compose project identity used to start the service. For example, if it was started with `docker compose -p field-gateway ...`, set `COMPOSE_PROJECT_NAME=field-gateway` for the backup command; if the name is missing or wrong, the script refuses to copy.

The script uses the current Compose project (`COMPOSE_PROJECT_NAME` if set), refuses to proceed if that project has no `iot-server` container or the container's `/app/data` mount does not match `IOT_DATA_DIR`, sends SIGTERM and waits for the configured 20-second grace period, and verifies the container is stopped. It then uses the runtime image as root to read the UID/GID 10001 data mount. A nonblocking exclusive lock on `/data/.lock` is held through copying `iot.db`, any remaining `-wal`/`-shm`, and `runtime.toml`; if another app process has the lock, backup fails. This protects against both a mismatched Compose project and the service restarting while files are copied.

The new backup directory is mode `0700`, and artifacts including `SHA256SUMS` are mode `0600`, owned by the invoking host user. Any copy/checksum failure aborts and removes the staging directory rather than publishing a partial backup. The script refuses to overwrite an existing backup. The service remains stopped; from `deploy/`, restart it with:

```sh
docker compose -f compose.sqlite.yml up -d
```

**Never copy only `iot.db` while the application is running in WAL mode.** Recent committed transactions may still be in `iot.db-wal`; a standalone database copy can silently omit them. Either stop the app as above or use SQLite's online backup API from a SQLite-aware tool.

Verify and restore to a fresh data directory:

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

Give the restored files to the container's non-root UID/GID, then start against that directory:

```sh
sudo chown -R 10001:10001 restore-data
IOT_DATA_DIR="$PWD/restore-data" docker compose -f compose.sqlite.yml up -d
```

Do not run `prepare-data.sh` on a non-empty directory owned by another user. Verify `/readyz`, the expected device/history through the authenticated API, and the runtime config revision. Keep the original backup unchanged until verification is complete.

## Retention and disk sizing

Telemetry retention is controlled by `telemetry.retention_days` (1–3 650, default 30). A sweep runs at startup and then hourly, deleting rows with `received_at < now - retention_days` in batches of 5 000. The setting is hot-reloadable. Commands (including expired ones), audit events, the latest attributes, and revoked credentials are kept indefinitely.

Deleting telemetry does not automatically shrink the SQLite file. WAL checkpoints reuse pages over time; run `VACUUM` only on a stopped database, after a verified backup, and allow free space for a temporary copy. Do not delete `-wal` or `-shm` files while the process is running.

The measured baseline grew about 22.7 MB per minute at 1 000 publish requests/s. That is roughly 32.7 GB per day at the same offered rate, but do not extrapolate it linearly for 30 days: retention and page reuse change the long-term size. Use it as an initial upper bound, monitor `du -h deploy/data/iot.db*`, and re-measure on the deployment hardware and actual rate. Size the data filesystem for retention plus backups and a restore copy, and alert when free space is at or below 20% (80% used).

## WAL and disk monitoring

SQLite uses WAL with `synchronous=FULL` and automatic checkpoints. The `-wal` and `-shm` files are normal while the server is running; their presence alone is not a failure. Do not delete or rename them under a running process. Monitor `df -h` for the data filesystem (alert at 80%), `du -h deploy/data/iot.db*` for database/WAL growth, `/readyz` for readiness, and backup job exit status plus checksum verification. Monitor backup destination space separately. Disk exhaustion degrades readiness and causes telemetry writes to be retried/rejected rather than acknowledged as stored.

## Monitoring

Use these checks together:

- `/healthz` answers when the process is alive; it does not check the database.
- `/readyz` returns `503` with per-component status when the database, MQTT listener, ingest worker, or command dispatcher is unhealthy.
- `/api/v1/overview` provides device, telemetry, command, ingest, and storage counters for the dashboard.
- `/metrics` exports Prometheus metrics when explicitly enabled.

Enable metrics in `deploy/conf.docker.toml`:

```toml
[http]
metrics = true
metrics_bearer_token_env = "IOT_METRICS_TOKEN"
```

Provide `IOT_METRICS_TOKEN` (at least 32 bytes) through Compose or a secret manager, then scrape with:

```sh
curl -fsS -H "Authorization: Bearer $IOT_METRICS_TOKEN" \
  http://127.0.0.1:8080/metrics
```

Never expose `/metrics` publicly. The endpoint rejects the admin cookie and requires the Bearer token. Metric names and semantics are in [02 §4.7](plans/02-contracts.md); the most useful groups are MQTT connections/kick failures, ingest accepted/rejected by reason, queue depth, commit latency/errors, application ACK drops, command pending/expired/errors, WebSocket clients/drops, audit errors, retention errors, and backup freshness.

## Alert rules

`deploy/alerts/prometheus.rules.yml` contains Prometheus rules for the metrics scrape, `/readyz` result and probe availability (scrape down or `probe_success` missing), data filesystem, telemetry queue saturation, ingest rejections, dropped application ACKs, SQLite commit errors, and failed/stale backups. The example assumes Prometheus scrapes `/metrics` as `job="iot-server"`, a Blackbox Exporter probes `/readyz` as `job="iot-server-readyz"`, and Node Exporter reports the data filesystem at `mountpoint="/var/lib/iot-server"`. Adjust that mountpoint matcher to the host filesystem containing `IOT_DATA_DIR`; keep the scrape/probe job labels consistent with the rules.

To alert on backup freshness, enable Node Exporter's textfile collector and run backups with its existing collector directory, for example:

```sh
cd deploy
IOT_BACKUP_METRICS_DIR=/var/lib/node_exporter/textfile_collector \
  ./backup.sh /path/to/backups/iot-2026-11-06
```

Every valid backup run atomically updates `iot_backup_last_attempt_timestamp_seconds` and `iot_backup_last_attempt_success`; `IotBackupFailed` alerts when the latest result failed or its status metric is missing, while `iot_backup_last_success_timestamp_seconds` remains unchanged until a backup succeeds. `IotBackupMissing` also alerts if no success has been recorded in 24 hours. Ensure the textfile collector directory is writable by the backup operator. Schedule backups more frequently than the freshness threshold and monitor backup destination capacity separately. Check rule syntax and firing/clearing cases with `sh tests/alerts.sh` (requires Docker; uses Prometheus `promtool`).

If updating the textfile fails, the script warns but retains the backup's exit status; the previous metric can remain stale. Monitor scheduler exit status and script stderr independently, including collector write failures.

### Runbook

| Alert | First checks and action |
| --- | --- |
| `IotServerUnavailable` | Confirm the container/process is running and the metrics port/token are correct. Check `docker compose ps` and `logs`. If the process is down, preserve data and restart; investigate the last error before repeating. |
| `IotServerNotReady` | Fetch `/readyz` and note which check fails. Check disk space, recent commit errors, MQTT listener state, and command dispatcher errors. Do not restart repeatedly without preserving logs and data. |
| `IotReadinessProbeUnavailable` | Check the Blackbox Exporter target, network path, and `probe_success` scrape. This can fire even when the application is healthy. |
| `IotDataDiskLow` | Free space or expand the data filesystem. Do not delete WAL/SHM. Reduce retention only after understanding the data requirement; verify backups first. |
| `IotTelemetryQueueSaturated` | Reduce offered load or fix the storage bottleneck. Check commit errors, disk latency, and `iot_telemetry_queue_depth`. Increasing `queue_capacity` requires a restart and only adds bounded buffer, not throughput. |
| `IotTelemetryAckDropped` | The application ACK queue overflowed; devices must retry missing ACKs. Check load, ingest latency, and queue depth. The 1 000 requests/s baseline dropped about 48.8% of ACKs, so this is a real capacity signal at that rate. |
| `IotIngestRejected` | Inspect `iot_ingest_rejected_total{reason}`. Validation errors are client bugs; `queue_full` is server saturation; `storage_unavailable` is a database/disk problem. Fix the source, not only the alert. |
| `IotDatabaseCommitErrors` | Treat as critical: check disk space, filesystem health, SQLite locks, and logs. Readiness should be failing. Stop writes, preserve the data directory, verify integrity on a stopped copy, and restore a backup if needed. |
| `IotBackupMissing` / `IotBackupFailed` | Check the scheduler, backup destination space, Compose project name, `IOT_DATA_DIR` match, and lock conflicts. Inspect stderr and the textfile collector; rerun a backup after fixing the cause. |

## Troubleshooting

CLI exit codes:

| Exit | Meaning |
| ---: | --- |
| `0` | Command completed successfully |
| `1` | Runtime failure, including database errors in `admin create`, or shutdown deadline exceeded |
| `2` | Invalid CLI usage |
| `3` | Configuration or environment validation failed |
| `4` | Database open/create or migration failed (`run`/`migrate`) |
| `5` | Data-directory lock is held |
| `6` | An admin already exists |

**Lock conflict (exit 5).** Another `run`, `migrate`, or `admin create` holds `<data_dir>/.lock`. Stop the other process or container; the lock is released when that process exits. Deleting `.lock` does not release a lock held by another process and can hide the real conflict. If the container is gone, verify no host process is using the same data directory.

**Migration or database failure (exit 4).** Run `iot-server check` first, then inspect permissions and free space on the data filesystem. The database path must be inside `data_dir`. Do not delete the database to retry. Preserve the full directory, run `PRAGMA integrity_check` on a stopped copy, and restore a verified backup if the database is damaged. `admin create` does not migrate; run `migrate` first. A schema/database error in `admin create` is reported as exit `1`, not `4`.

**Admin already exists (exit 6).** The MVP allows exactly one admin. Use the existing account; there is no reset-password CLI. If the credential is lost, recover from a backup or perform a controlled database intervention after preserving the current database.

**Configuration error (exit 3).** The error names the failing path. Check unknown fields, invalid ranges, `allow_anonymous = true`, a missing `url_env`, a metrics token shorter than 32 bytes, or a path outside `data_dir`. `check` does not create the data directory or database.

**Disk full.** Readiness drops, commits fail, and telemetry is retried or rejected; the server must not acknowledge data as stored. Free space or expand the filesystem, check backup destination usage, and only then let retries continue. Do not delete WAL/SHM files under a running process. After recovery, verify integrity and that `/readyz` is green.

**Database errors or suspected corruption.** Stop the service, preserve the complete data directory, and run `PRAGMA integrity_check` on a copy. Restore a backup into a fresh directory if repair is not safe. Keep the original untouched until verification succeeds.

**MQTT connection refused.** Confirm the client uses MQTT 3.1.1, `username = device_id`, `client_id = device_id`, and the current token. `0x04` covers wrong credentials, unknown/disabled devices, and ID mismatch; `0x05`/`0x87` mean the protocol version is wrong. Rotating or disabling a device kicks its existing session. Check the connection cap and listener address.

**MQTT publish disconnects or times out.** A denied publish closes the connection without a PUBACK. Check the topic prefix, wildcard use, retained flag, topic levels, and packet size. A SUBACK `0x80` means the subscription filter is not allowed. Exceeding retained caps also closes the connection.

**Login fails or loops.** `401 invalid_credentials` means the admin does not exist or the password is wrong; `429 rate_limited` means wait for `Retry-After`. If `cookie_secure = true` but the browser uses HTTP, the cookie is not sent; use HTTPS or set it to `false` only for local HTTP.

**Dashboard returns 404.** Check `http.dashboard`; when false, `/` and assets are intentionally unavailable while the API still works. In a debug build, a missing `web/dist` returns a build-instruction page; release builds require the embedded assets.

After an unclean stop or suspected storage issue, preserve a copy of the complete data directory before repair, then use SQLite integrity checks on a stopped copy (for example `sqlite3 iot.db 'PRAGMA integrity_check;'`). A healthy database returns `ok`.
