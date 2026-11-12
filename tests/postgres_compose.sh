#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp_root=$(mktemp -d "${TMPDIR:-/tmp/opencode}/iot-pg-compose.XXXXXX")
project="iot-p5-compose-$(cat /proc/sys/kernel/random/uuid)"
image="iot-server:$project"
image_built=0
restore_volume=
restore_volume_owned=0
export COMPOSE_PROJECT_NAME="$project"
export IOT_SERVER_IMAGE="$image"
export IOT_POSTGRES_PASSWORD_FILE="$tmp_root/postgres_password"
export IOT_DATABASE_NAME=iot
export IOT_POSTGRES_DATABASE=iot
export IOT_COMPOSE_STATE_FILE="$tmp_root/a18-state.json"
export IOT_TEST_ADMIN_PASSWORD
export IOT_TEST_ADMIN_USER=compose-admin
export IOT_HTTP_PORT
export IOT_MQTT_PORT
export IOT_TEST_POSTGRES_PORT

IOT_TEST_ADMIN_PASSWORD=$(openssl rand -hex 16)
openssl rand -hex 32 > "$IOT_POSTGRES_PASSWORD_FILE"
chmod 0600 "$IOT_POSTGRES_PASSWORD_FILE"
IOT_TEST_ADMIN_PASSWORD_FILE="$tmp_root/admin_password"
printf '%s\n' "$IOT_TEST_ADMIN_PASSWORD" > "$IOT_TEST_ADMIN_PASSWORD_FILE"
chmod 0644 "$IOT_TEST_ADMIN_PASSWORD_FILE"

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}
IOT_HTTP_PORT=$(free_port)
IOT_MQTT_PORT=$(free_port)
IOT_TEST_POSTGRES_PORT=$(free_port)
export IOT_TEST_POSTGRES_URL="postgresql://iot:$(cat "$IOT_POSTGRES_PASSWORD_FILE")@127.0.0.1:${IOT_TEST_POSTGRES_PORT}/iot?sslmode=disable"
export IOT_EXPECTED_DATABASE=iot

if [ -n "$(docker ps -aq --filter "label=com.docker.compose.project=$project")" ] || \
   [ -n "$(docker volume ls -q --filter "label=com.docker.compose.project=$project")" ] || \
   [ -n "$(docker network ls -q --filter "label=com.docker.compose.project=$project")" ]; then
    echo "refusing to use an existing Compose project: $project" >&2
    rm -f "$IOT_POSTGRES_PASSWORD_FILE" "$IOT_TEST_ADMIN_PASSWORD_FILE"
    rmdir "$tmp_root"
    exit 1
fi

compose() {
    if [ -n "${RESTORE_OVERRIDE:-}" ]; then
        "$repo_root/deploy/compose-postgres.sh" \
            --file "$repo_root/tests/compose.postgres.test.yml" \
            --file "$RESTORE_OVERRIDE" "$@"
    else
        "$repo_root/deploy/compose-postgres.sh" \
            --file "$repo_root/tests/compose.postgres.test.yml" "$@"
    fi
}
cleanup() {
    unset RESTORE_OVERRIDE
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [ "$restore_volume_owned" -eq 1 ]; then docker volume rm "$restore_volume" >/dev/null 2>&1 || true; fi
    if [ "$image_built" -eq 1 ]; then docker image rm "$image" >/dev/null 2>&1 || true; fi
    rm -rf -- "$tmp_root"
}
trap cleanup EXIT HUP INT TERM

(
    unset IOT_POSTGRES_DATABASE
    export IOT_DATABASE_NAME=iot_custom_test
    compose config --format json | python3 -c '
import json, sys
services = json.load(sys.stdin)["services"]
assert services["postgres"]["environment"]["POSTGRES_DB"] == "iot_custom_test"
assert "/iot_custom_test?" in services["iot-server"]["environment"]["IOT_DATABASE_URL"]
'
)

docker build -f deploy/Dockerfile -t "$image" "$repo_root"
image_built=1
compose config --quiet
compose up -d postgres

for _ in $(seq 1 60); do
    container=$(compose ps --all --quiet postgres)
    [ -n "$container" ] || { sleep 1; continue; }
    health=$(docker inspect --format '{{.State.Health.Status}}' "$container")
    [ "$health" = healthy ] && break
    sleep 1
done
[ "$(docker inspect --format '{{.State.Health.Status}}' "$container")" = healthy ]

compose run --rm --no-deps iot-server migrate --config /app/conf.toml
compose run --rm --no-deps \
    --volume "$IOT_TEST_ADMIN_PASSWORD_FILE:/run/secrets/admin_password:ro,z" \
    iot-server admin create --username "$IOT_TEST_ADMIN_USER" \
    --config /app/conf.toml --password-file /run/secrets/admin_password
compose up -d

IOT_COMPOSE_TEST_STAGE=seed cargo test --manifest-path "$repo_root/Cargo.toml" \
    --test postgres_compose -- --ignored --nocapture

# Stop writes before pairing the database dump with its separate runtime.toml snapshot.
app_container=$(compose ps --all --quiet iot-server)
[ -n "$app_container" ]
source_volume="${project}_iot_data"
compose stop iot-server
runtime_meta=$(docker run --rm --user 0:0 \
    --mount "type=volume,source=$source_volume,target=/app/data,readonly" \
    postgres:18.6-bookworm stat -c '%a:%u:%g' /app/data/runtime.toml)
data_dir_meta=$(docker run --rm --user 0:0 \
    --mount "type=volume,source=$source_volume,target=/app/data,readonly" \
    postgres:18.6-bookworm stat -c '%a:%u:%g' /app/data)
docker run --rm --user 0:0 \
    --mount "type=volume,source=$source_volume,target=/app/data,readonly" \
    postgres:18.6-bookworm tar -C /app/data -cpf - runtime.toml > "$tmp_root/runtime.tar"
chmod 0600 "$tmp_root/runtime.tar"
sha256sum "$tmp_root/runtime.tar" > "$tmp_root/runtime.sha256"

backup="$tmp_root/backup"
if IOT_DATABASE_NAME=iot_missing "$repo_root/deploy/backup-postgres.sh" "$tmp_root/failed-backup"; then
    echo "backup unexpectedly succeeded for a missing database" >&2
    exit 1
fi
[ ! -e "$tmp_root/failed-backup" ]
if find "$tmp_root" -maxdepth 1 -name 'failed-backup.tmp.*' -print -quit | grep -q .; then
    echo "failed backup left a staging directory" >&2
    exit 1
fi
"$repo_root/deploy/backup-postgres.sh" "$backup"
(cd "$backup" && sha256sum -c SHA256SUMS)
[ "$(stat -c %a "$backup")" = 700 ]
for file in postgres.dump POSTGRES_VERSION DATABASE SHA256SUMS; do
    [ "$(stat -c %a "$backup/$file")" = 600 ]
done
if "$repo_root/deploy/backup-postgres.sh" "$backup"; then
    echo "backup unexpectedly overwrote an existing directory" >&2
    exit 1
fi
bad_backup="$tmp_root/bad-backup"
cp -a "$backup" "$bad_backup"
printf 'corruption\n' >> "$bad_backup/postgres.dump"
if "$repo_root/deploy/restore-postgres.sh" "$bad_backup" iot_bad_checksum; then
    echo "restore unexpectedly accepted a corrupt backup" >&2
    exit 1
fi
cp "$backup/postgres.dump" "$bad_backup/postgres.dump"
printf '17.0\n' > "$bad_backup/POSTGRES_VERSION"
(cd "$bad_backup" && sha256sum postgres.dump POSTGRES_VERSION DATABASE > SHA256SUMS)
if "$repo_root/deploy/restore-postgres.sh" "$bad_backup" iot_bad_version; then
    echo "restore unexpectedly accepted an unsupported PostgreSQL version" >&2
    exit 1
fi
if "$repo_root/deploy/restore-postgres.sh" "$backup" iot; then
    echo "restore unexpectedly overwrote an existing database" >&2
    exit 1
fi
"$repo_root/deploy/restore-postgres.sh" "$backup" iot_restore_test

# A valid checksum does not make a non-custom-format dump restorable. The restore
# script creates its target first, so this asserts the partial-failure boundary.
bad_format="$tmp_root/bad-format"
mkdir -m 0700 "$bad_format"
cp "$backup/POSTGRES_VERSION" "$backup/DATABASE" "$bad_format/"
printf 'not a PostgreSQL custom-format archive\n' > "$bad_format/postgres.dump"
(cd "$bad_format" && sha256sum postgres.dump POSTGRES_VERSION DATABASE > SHA256SUMS)
source_events_before=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=iot \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM telemetry_events"
    ')
if "$repo_root/deploy/restore-postgres.sh" "$bad_format" iot_bad_format; then
    echo "restore unexpectedly accepted a checksum-valid invalid-format dump" >&2
    exit 1
fi
[ "$IOT_DATABASE_NAME" = iot ]
failed_restore=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=postgres \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM pg_database WHERE datname = '\''iot_bad_format'\''"
    ')
[ "$failed_restore" -eq 1 ]
failed_tables=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=iot_bad_format \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM pg_tables WHERE schemaname = '\''public'\''"
    ')
[ "$failed_tables" -eq 0 ]
source_events=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=iot \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM telemetry_events"
    ')
[ "$source_events" -eq "$source_events_before" ]
docker compose --project-name "$project" --file "$repo_root/deploy/compose.postgres.yml" \
    exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        dropdb --host=127.0.0.1 --username="$POSTGRES_USER" iot_bad_format
    '

compose down --remove-orphans
restore_volume="${project}_iot_data_restore"
if docker volume inspect "$restore_volume" >/dev/null 2>&1; then
    echo "refusing to use existing restore volume: $restore_volume" >&2
    exit 1
fi
docker volume create "$restore_volume" >/dev/null
restore_volume_owned=1
sha256sum -c "$tmp_root/runtime.sha256"
docker run --rm -i --user 0:0 \
    --mount "type=volume,source=$restore_volume,target=/restore" \
    postgres:18.6-bookworm tar -C /restore -xpf - < "$tmp_root/runtime.tar"
IFS=: read -r data_dir_mode data_dir_uid data_dir_gid <<EOF
$data_dir_meta
EOF
docker run --rm --user 0:0 --mount "type=volume,source=$restore_volume,target=/restore" \
    postgres:18.6-bookworm sh -ec "chown '$data_dir_uid:$data_dir_gid' /restore; chmod '$data_dir_mode' /restore"
source_meta=$(docker run --rm --user 0:0 --mount "type=volume,source=$restore_volume,target=/restore" \
    postgres:18.6-bookworm stat -c '%a:%u:%g' /restore/runtime.toml)
[ "$source_meta" = "$runtime_meta" ]
cat > "$tmp_root/restore-compose.yml" <<EOF
services: {}
volumes:
  iot_data:
    external: true
    name: $restore_volume
  postgres_data:
    external: true
    name: ${project}_postgres_data
EOF
RESTORE_OVERRIDE="$tmp_root/restore-compose.yml"
export RESTORE_OVERRIDE
export IOT_DATABASE_NAME=iot_restore_test
export IOT_EXPECTED_DATABASE=iot_restore_test
export IOT_TEST_POSTGRES_URL="postgresql://iot:$(cat "$IOT_POSTGRES_PASSWORD_FILE")@127.0.0.1:${IOT_TEST_POSTGRES_PORT}/iot_restore_test?sslmode=disable"
compose up -d

for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:${IOT_HTTP_PORT}/readyz" >/dev/null 2>&1; then break; fi
    sleep 1
done
curl -fsS "http://127.0.0.1:${IOT_HTTP_PORT}/readyz" >/dev/null
IOT_COMPOSE_TEST_STAGE=verify cargo test --manifest-path "$repo_root/Cargo.toml" \
    --test postgres_compose -- --ignored --nocapture

original_events=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=iot \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM telemetry_events"
    ')
[ "$original_events" -gt 0 ]
failed_targets=$(docker compose --project-name "$project" \
    --file "$repo_root/deploy/compose.postgres.yml" exec -T postgres sh -ec '
        export PGPASSWORD="$(cat /run/secrets/postgres_password)"
        psql --host=127.0.0.1 --username="$POSTGRES_USER" --dbname=postgres \
            --tuples-only --no-align --command="SELECT COUNT(*) FROM pg_database WHERE datname IN ('\''iot_bad_checksum'\'', '\''iot_bad_version'\'')"
    ')
[ "$failed_targets" -eq 0 ]
