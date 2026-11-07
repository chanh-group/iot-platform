# SQLite backup/restore drill — 2026-11-07

Command: `cargo test --locked --test backup_restore -- --nocapture` — PASS.

The integration test seeds an admin, device, telemetry event, and runtime revision, stops the source app, and runs `deploy/backup.sh`. Its fake Docker adapter substitutes only Compose/Engine control; the script's actual shell copy, lock, file mode, checksum, and publication steps run against the fixture data directory. An injected partial copy failure reports `iot_backup_last_attempt_success 0` without publishing a backup; a later success changes it to 1, and a subsequent failure resets it to 0 while preserving the last-success timestamp.

For the restore drill, the test verifies the checksum of the successful script output, corrupts the destination `iot.db`, replaces it from that backup output (including present sidecars and `runtime.toml`), then starts the server from the restored data. Admin login, the seeded device, telemetry history event, and runtime revision 1 all verify through HTTP APIs. The source backup is left intact.

This is an automated artifact-level restore drill, not a second live Compose deployment restore. The real Docker backup permission/identity smoke remains covered separately by `tests/docker_smoke.sh`.
