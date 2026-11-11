#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp_root=$(mktemp -d "${TMPDIR:-/tmp/opencode}/iot-pg-compose.XXXXXX")
project="iot-p5-compose-$$"
image="iot-server:pg-compose-smoke-$$"
image_built=0
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
chmod 0600 "$IOT_TEST_ADMIN_PASSWORD_FILE"

free_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}
IOT_HTTP_PORT=$(free_port)
IOT_MQTT_PORT=$(free_port)
IOT_TEST_POSTGRES_PORT=$(free_port)
export IOT_TEST_POSTGRES_URL="postgresql://iot:$(cat "$IOT_POSTGRES_PASSWORD_FILE")@127.0.0.1:${IOT_TEST_POSTGRES_PORT}/iot?sslmode=disable"
export IOT_EXPECTED_DATABASE=iot

compose() {
    "$repo_root/deploy/compose-postgres.sh" \
        --file "$repo_root/tests/compose.postgres.test.yml" "$@"
}
cleanup() {
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    if [ "$image_built" -eq 1 ]; then docker image rm "$image" >/dev/null 2>&1 || true; fi
    rm -rf -- "$tmp_root"
}
trap cleanup EXIT HUP INT TERM

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
    --volume "$IOT_TEST_ADMIN_PASSWORD_FILE:/run/secrets/admin_password:ro" \
    iot-server admin create --username "$IOT_TEST_ADMIN_USER" \
    --config /app/conf.toml --password-file /run/secrets/admin_password
compose up -d

IOT_COMPOSE_TEST_STAGE=seed cargo test --manifest-path "$repo_root/Cargo.toml" \
    --test postgres_compose -- --ignored --nocapture

backup="$tmp_root/backup"
"$repo_root/deploy/backup-postgres.sh" "$backup"
(cd "$backup" && sha256sum -c SHA256SUMS)
"$repo_root/deploy/restore-postgres.sh" "$backup" iot_restore_test

compose down --remove-orphans
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
