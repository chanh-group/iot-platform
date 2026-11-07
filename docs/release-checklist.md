# Release checklist MVP

Checklist này áp dụng cho release MVP trên SQLite. Chỉ tag sau khi toàn bộ mục
bên dưới pass trên đúng commit sẽ được tag. Nguồn sự thật: [SPEC §13](SPEC.md),
[contract](plans/02-contracts.md), [delivery plan §7/§10](plans/03-delivery-plan.md)
và [operations](operations.md).

## 0. Chuẩn bị

- Toolchain: Rust **1.98** (`rust-toolchain.toml`), Bun **1.4.2**, Docker Engine +
  Compose v2, `cargo-deny` **0.20.2**, `cargo-audit` **0.22.2**, `syft` **1.57.0**,
  `trivy` **0.76.2**, `cosign` **3.1.4**, Python 3 (chỉ stdlib, cho
  `deploy/check-trivy-report.py`).
- Cập nhật version ở `Cargo.toml` và `web/package.json`; tag phải là
  `v<major>.<minor>.<patch>` khớp cả hai file. Workflow release kiểm tra điều này.
- Làm việc trên nhánh mặc định đã xanh (hiện là `master`), `git status` sạch.
  Không tag từ nhánh chưa merge.
- Ký image bằng keyless OIDC trong GitHub Actions. Dry-run cục bộ dùng khóa
  cosign tạm thời và **không push** image hay release.

## 1. Baseline build và test

Tại repo root, sau khi build frontend (bước này tạo `web/dist` cho `rust-embed`):

```sh
cd web
bun install --frozen-lockfile
bun run --bun typecheck
bun run --bun test
bun run --bun build
cd ..

cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
cargo build --release --locked
git diff --check
```

Kỳ vọng: mọi lệnh exit 0; không có warning bị bỏ qua. Ghi lại số test pass và
timestamp. Nếu frontend hoặc `web/dist` lỗi, không tiếp tục.

## 2. Dependency, license và advisory

```sh
cargo deny check
cargo audit
```

- `cargo deny`: advisories, bans, licenses, sources đều `ok`.
- `cargo audit`: không có vulnerability. Cảnh báo unmaintained chỉ được chấp nhận
  khi đã ghi trong bảng exception bên dưới.
- Không chấp nhận advisory critical/high khi chưa có exception còn hạn với owner,
  lý do, mitigation và ngày hết hạn. Ghi exception mới vào bảng dưới đây trước khi
  release.

### Exception đang áp dụng

| ID | Package | Nguồn | Owner | Lý do / mitigation | Hết hạn |
| --- | --- | --- | --- | --- | --- |
| RUSTSEC-2023-0089 | `atomic-polyfill 1.0.3` | transitive qua `rmqtt` | maintainer | Unmaintained, không phải vulnerability; chưa có bản thay thế trong `rmqtt` 0.24.0. Theo dõi upstream khi nâng `rmqtt`. | 2027-02-05 |
| RUSTSEC-2024-0384 | `instant 0.1.13` | transitive qua `rmqtt` | maintainer | Unmaintained, không phải vulnerability; chưa có bản thay thế trong `rmqtt` 0.24.0. Theo dõi upstream khi nâng `rmqtt`. | 2027-02-05 |

### Backlog trong source

```sh
test -z "$(grep -rnE 'TODO|FIXME|\bP[01]\b' src/ || true)"
```

Không còn item TODO/P0/P1 trong `src/`. Backlog còn lại chỉ là Phase 5 trong
[delivery plan §8](plans/03-delivery-plan.md).

## 3. Docker image và smoke

```sh
docker build -f deploy/Dockerfile -t iot-server:local .
docker run --rm iot-server:local --version
docker run --rm --entrypoint id iot-server:local -u   # phải in 10001
sh tests/docker_smoke.sh
```

`tests/docker_smoke.sh` chạy A17: dựng Compose, seed dữ liệu, recreate container
cùng volume, kiểm tra device/credential/telemetry/runtime revision qua API và
MQTT, rồi dọn fixture. Chỉ tiếp tục khi script pass.

## 4. Acceptance gate

Chạy `cargo test --all` (đã gồm unit/integration) và ghi tên test + timestamp cho
từng mục. Các test chính:

| Gate | Test |
| --- | --- |
| A01 | `tests/migration.rs::migration_creates_schema_and_is_idempotent`, `tests/http_shell.rs::healthz_is_public_and_readyz_tracks_the_database`, `tests/mqtt_boot.rs::mqtt_listener_accepts_connections_and_readiness_tracks_it`; Docker smoke §3 |
| A02 | `tests/cli_admin.rs::admin_create_is_single_and_audited`, `tests/auth.rs::no_default_admin_and_me_requires_session` |
| A03 | `tests/mqtt_auth.rs::valid_token_connects_and_marks_presence`, `::bad_credentials_are_all_refused_with_the_same_code` |
| A04 | `tests/mqtt_acl.rs::subscribe_outside_own_prefix_is_refused_and_keeps_the_connection`, `::cross_device_publish_is_denied_not_routed_and_disconnects` |
| A05 | `tests/telemetry_flow.rs::valid_telemetry_is_stored_with_server_timestamp_and_raw_hash` |
| A06 | `tests/telemetry_flow.rs::byte_identical_retry_deduplicates_but_changed_bytes_conflict` |
| A07 | `tests/telemetry_flow.rs::byte_identical_retry_deduplicates_but_changed_bytes_conflict` |
| A08 | `tests/crash_recovery.rs::kill_before_commit_recovers_without_losing_or_duplicating_event`, `::kill_after_commit_before_ack_retries_as_duplicate`, `::kill_after_ack_preserves_telemetry_and_recovers_pending_command` |
| A09 | `tests/storage_failure.rs::database_failure_never_publishes_stored_and_recovers`, `::sqlite_full_never_panics_or_publishes_stored_and_recovers`, `::saturated_ingest_queue_stays_bounded_and_exports_rejection_metric` |
| A10 | `tests/command_offline.rs::offline_command_stays_in_sql_then_restart_recovery_publishes_same_id` |
| A11 | `tests/command_ack.rs::duplicate_unknown_cross_device_and_late_acks_are_atomic`, `::ack_races_with_dispatch_and_expiry_never_reopens_a_terminal_command` |
| A12 | `tests/runtime_config.rs::runtime_patch_persists_preserves_overrides_and_broadcasts_revisions`, `::runtime_patch_survives_a_real_server_restart` |
| A13 | `tests/runtime_config.rs::runtime_patch_distinguishes_unknown_restart_noop_and_invalid_fields`, `::runtime_rename_failure_returns_config_write_failed_without_applying` |
| A14 | `tests/credential_rotate.rs::rotates_an_online_device_and_rejects_the_previous_token` |
| A15 | `tests/ws_telemetry.rs::slow_websocket_is_closed_and_releases_its_connection_slot`, `::subscribed_clients_receive_only_committed_telemetry_and_refetch_after_reconnect` |
| A16 | `tests/shutdown_http.rs::sigterm_drains_in_flight_request_and_releases_lock`, `::sigterm_commits_queued_telemetry_then_kicks_mqtt_and_releases_lock`, `::shutdown_deadline_logs_uncommitted_telemetry_and_exits_nonzero`, `::second_signal_exits_immediately_without_graceful_cleanup` |
| A17 | `tests/docker_smoke.sh` / `tests/docker_smoke.rs::compose_recreate_preserves_data_and_live_interfaces` |
| A19 | `tests/mqtt_protocol.rs` (toàn bộ ma trận QoS/wildcard/retain/LWT/session/duplicate client/version) |
| A20 | `src/runtime/supervisor.rs::tests::a20_fail_closed_shutdown_is_verified_in_a_child_process`, `src/mqtt/server.rs::tests::both_failed_kicks_fence_a_live_mqtt_session_in_both_directions` |

Ngoài ra chạy lại bộ bảo mật Phase 2/3 đã liệt kê trong
[TODO_4 §5](todos/TODO_4.md) và backup/restore drill mục 5.

## 5. Backup/restore drill

```sh
cargo test --test backup_restore
sh tests/docker_smoke.sh
```

Kiểm tra thêm bằng tay trên môi trường staging: chạy `deploy/backup.sh`, verify
SHA-256, restore vào data dir mới, chạy `iot-server run`, đăng nhập và xác nhận
device/telemetry/runtime revision. Ghi timestamp và artifact backup vào hồ sơ
release. Cảnh báo WAL: không copy riêng `.db` khi app đang chạy.

## 6. Alert và runbook

- `deploy/alerts/prometheus.rules.yml` đã được review.
- `sh tests/alerts.sh` pass (Prometheus `promtool check rules` và test firing).
- Runbook trong [operations §Alert rules](operations.md#alert-rules) khớp tên alert
  và metric hiện tại.
- Xác nhận ngưỡng disk, ingest reject, ACK drop và backup freshness vẫn đúng với
  môi trường release.

## 7. SBOM

Workflow tạo hai SBOM SPDX JSON:

- `sbom-source.spdx.json`: source + `Cargo.lock`/`web/bun.lock`.
- `sbom-image.spdx.json`: image đã push (theo digest).

Dry-run cục bộ:

```sh
syft scan dir:. --exclude './target/**' --exclude './web/node_modules/**' \
  --exclude './web/dist/**' --exclude './data/**' --exclude './.git/**' \
  -q -o spdx-json=target/release-dry-run/sbom-source.spdx.json
syft scan iot-server:local -q -o spdx-json=target/release-dry-run/sbom-image.spdx.json
```

SBOM phải được lưu cùng release; không chỉnh sửa sau khi ký.

## 8. Quét dependency và OS image

```sh
trivy image --scanners vuln --severity HIGH,CRITICAL --format json \
  --output target/release-dry-run/trivy-image.json --exit-code 0 iot-server:local
python3 deploy/check-trivy-report.py \
  target/release-dry-run/trivy-image.json deploy/trivy-exceptions.json
```

- Gate chặn **mọi** HIGH/CRITICAL; không dùng `--ignore-unfixed`.
- Exception nằm trong `deploy/trivy-exceptions.json`. Mỗi entry phải có đủ
  `id`, `package`, `owner`, `reason`, `mitigation`, `expires` (YYYY-MM-DD).
  `deploy/check-trivy-report.py` từ chối entry thiếu field, trùng cặp
  id/package, đã hết hạn, hoặc không khớp finding HIGH/CRITICAL hiện tại.
- `trivy-image.json` là báo cáo đầy đủ, không áp exception, lưu cùng release.
- Gate chỉ chấp nhận report image hợp lệ: `ArtifactType` là
  `container_image`, có `ArtifactName`, `Metadata.OS.Family`, `Results` là
  array không rỗng. Mỗi result phải có `Target`, `Class`, `Type`; phải có ít
  nhất một result `Class: os-pkgs` với `Type` khớp `Metadata.OS.Family`. Mọi
  result/finding phải đúng kiểu và finding HIGH/CRITICAL phải có
  `VulnerabilityID` + `PkgName`. Report thiếu/sai cấu trúc bị từ chối, không
  được coi là sạch.
- Baseline dry-run 2026-11-07 trên Ubuntu 24.04 (sau `apt-get upgrade`):
  0 finding HIGH/CRITICAL, `deploy/trivy-exceptions.json` rỗng.

## 9. Ký image và provenance

Workflow release dùng GitHub OIDC keyless. Image được push tạm ở tag
`staging-<commit-sha>`, quét, ký, **verify theo digest và verify checksum bundle
trước khi promote**:

```sh
cosign sign --yes "$IMAGE@$DIGEST"
cosign sign-blob --yes --bundle dist/SHA256SUMS.bundle dist/SHA256SUMS
cosign verify \
  --certificate-identity "https://github.com/chanh-group/iot-platform/.github/workflows/release.yml@refs/tags/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$IMAGE@$DIGEST"
cosign verify-blob --bundle dist/SHA256SUMS.bundle \
  --certificate-identity "https://github.com/chanh-group/iot-platform/.github/workflows/release.yml@refs/tags/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  dist/SHA256SUMS

# Chỉ chạy sau khi verify pass.
docker buildx imagetools create \
  -t "$IMAGE:$VERSION" \
  -t "$IMAGE:$MAJOR_MINOR" \
  -t "$IMAGE:latest" \
  "$IMAGE@$DIGEST"
tag_digest=$(docker buildx imagetools inspect "$IMAGE:$VERSION" --format '{{.Manifest.Digest}}')
test "$tag_digest" = "$DIGEST"
cosign verify \
  --certificate-identity "https://github.com/chanh-group/iot-platform/.github/workflows/release.yml@refs/tags/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$IMAGE:$VERSION"
```

`imagetools create` giữ nguyên digest, nên chữ ký theo digest áp dụng cho các tag
đã promote. Docker build dùng `provenance: mode=max` và `sbom: true` để gắn
provenance/SBOM attestation vào image. Nếu scan, ký hoặc verify trước promote
thất bại, chỉ tag `staging-*` tồn tại; không có tag version, major/minor hay
`latest` để người dùng pull nhầm. Dry-run cục bộ dùng khóa tạm và
`cosign verify-blob --key`; khóa private bị xóa sau khi verify. Không dùng khóa
dry-run cho release thật.

## 10. Tag, publish và artifact

```sh
git tag -a "v$VERSION" -m "iot-server v$VERSION"
git push origin "v$VERSION"
```

Workflow `release` chạy khi tag `v*.*.*`:

1. Kiểm tra tag khớp `Cargo.toml` và `web/package.json`.
2. Build/test/audit như trên.
3. Build và push image ở tag staging `staging-<commit-sha>`; chưa tạo tag
   version/major/minor/`latest`.
4. Tạo SBOM, quét HIGH/CRITICAL, ký digest, verify chữ ký image + checksum
   bundle, promote tag version/major-minor/`latest`, rồi verify tag trỏ đúng
   digest đã ký.
5. Tạo GitHub Release kèm:
   - `iot-server-<tag>-linux-x86_64.tar.gz`
   - `sbom-source.spdx.json`, `sbom-image.spdx.json`
   - `trivy-image.json`
   - `SHA256SUMS`, `SHA256SUMS.bundle`

Nếu bước scan hoặc ký fail, workflow dừng trước khi promote và chỉ còn tag
`staging-*`; không có tag phát hành để người dùng pull nhầm. Tag staging không
phải bản phát hành và có thể được dọn định kỳ.

Kiểm tra thủ công sau workflow:

```sh
docker pull ghcr.io/chanh-group/iot-platform:v$VERSION
docker run --rm ghcr.io/chanh-group/iot-platform:v$VERSION --version
sha256sum -c SHA256SUMS
```

Image chỉ được coi là phát hành khi digest, chữ ký, SBOM và báo cáo scan có trong
release. Không rebuild rồi tag lại cùng version; nếu cần sửa, tăng patch version.

## 11. Dry-run cục bộ

Chạy toàn bộ pipeline không push:

```sh
PATH="$HOME/.cargo/bin:$PATH" deploy/release-dry-run.sh
```

Script chạy baseline, audit, build release, build Docker image, tạo SBOM, quét
Trivy, đóng gói artifact, tạo checksum và ký/verify bằng khóa cosign tạm trong
`target/release-dry-run/`. Script không gọi registry, không tạo tag và không tạo
GitHub Release. Script từ chối chạy nếu `OUT_DIR` đã tồn tại; xóa thư mục cũ bằng
tay nếu muốn chạy lại. Ghi timestamp + danh sách artifact vào hồ sơ release.

## 12. Hồ sơ release

Điền vào mục §16 của [TODO_4](todos/TODO_4.md) sau khi xong:

- Timestamp bắt đầu/kết thúc, commit SHA, tag.
- Số test pass và tên các gate A01–A17/A19/A20.
- Image digest, kết quả `cosign verify`.
- Đường dẫn SBOM, báo cáo Trivy, checksum/signature.
- Kết quả load baseline và restore drill.
- Exception còn hiệu lực (nếu có) và ngày review tiếp theo; exception scan image
  nằm trong `deploy/trivy-exceptions.json` và phải còn `expires`.
