# Operations

## Local Docker Compose (SQLite)

Build `iot-server:local`, prepare the bind mount, then start the service:

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
cd deploy
./prepare-data.sh
docker compose -f compose.sqlite.yml up -d
```

The Compose ports bind to localhost. The sample config disables secure cookies and uses plaintext MQTT; do not expose it to an untrusted network. Use TLS termination and set `cookie_secure = true` for an HTTPS deployment.

## SQLite backup and restore

From the `deploy/` directory entered above, create a backup while Compose is running:

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

## WAL and disk monitoring

SQLite uses WAL with `synchronous=FULL` and automatic checkpoints. The `-wal` and `-shm` files are normal while the server is running; their presence alone is not a failure. Do not delete or rename them under a running process. Monitor `df -h` for the data filesystem (alert at 80%), `du -h deploy/data/iot.db*` for database/WAL growth, `/readyz` for readiness, and backup job exit status plus checksum verification. Monitor backup destination space separately. Disk exhaustion degrades readiness and causes telemetry writes to be retried/rejected rather than acknowledged as stored.

## Alert rules

`deploy/alerts/prometheus.rules.yml` contains Prometheus rules for the metrics scrape, `/readyz` result and probe availability (scrape down or `probe_success` missing), data filesystem, telemetry queue saturation, ingest rejections, dropped application ACKs, SQLite commit errors, and failed/stale backups. The example assumes Prometheus scrapes `/metrics` as `job="iot-server"`, a Blackbox Exporter probes `/readyz` as `job="iot-server-readyz"`, and Node Exporter reports the data filesystem at `mountpoint="/var/lib/iot-server"`. Adjust that mountpoint matcher to the host filesystem containing `IOT_DATA_DIR`; keep the scrape/probe job labels consistent with the rules. To enable the authenticated metrics endpoint, set `metrics = true` and `metrics_bearer_token_env = "IOT_METRICS_TOKEN"` in `[http]` in `conf.docker.toml`, then provide `IOT_METRICS_TOKEN` to Compose from a secret manager. Do not expose the endpoint publicly.

To alert on backup freshness, enable Node Exporter's textfile collector and run backups with its existing collector directory, for example:

```sh
cd deploy
IOT_BACKUP_METRICS_DIR=/var/lib/node_exporter/textfile_collector \
  ./backup.sh /path/to/backups/iot-2026-11-06
```

Every valid backup run atomically updates `iot_backup_last_attempt_timestamp_seconds` and `iot_backup_last_attempt_success`; `IotBackupFailed` alerts when the latest result failed or its status metric is missing, while `iot_backup_last_success_timestamp_seconds` remains unchanged until a backup succeeds. `IotBackupMissing` also alerts if no success has been recorded in 24 hours. Ensure the textfile collector directory is writable by the backup operator. Schedule backups more frequently than the freshness threshold and monitor backup destination capacity separately. Check rule syntax and firing/clearing cases with `sh tests/alerts.sh` (requires Docker; uses Prometheus `promtool`).

If updating the textfile fails, the script warns but retains the backup's exit status; the previous metric can remain stale. Monitor scheduler exit status and script stderr independently, including collector write failures.

After an unclean stop or suspected storage issue, preserve a copy of the complete data directory before repair, then use SQLite integrity checks on a stopped copy (for example `sqlite3 iot.db 'PRAGMA integrity_check;'`). A healthy database returns `ok`.
