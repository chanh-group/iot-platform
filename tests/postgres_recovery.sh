#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
name="iot-pg-outage-$$"
password=$(cat /proc/sys/kernel/random/uuid)
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
cleanup() {
    docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

docker run -d \
    --name "$name" \
    --label io.iot.test-fixture=postgres-outage \
    --health-cmd='pg_isready -U postgres -d iot_test' \
    --health-interval=1s \
    --health-timeout=1s \
    --health-retries=30 \
    -e POSTGRES_PASSWORD="$password" \
    -e POSTGRES_DB=iot_test \
    -p "127.0.0.1:${port}:5432" \
    postgres:18.6-bookworm >/dev/null

for _ in $(seq 1 60); do
    health=$(docker inspect --format '{{.State.Health.Status}}' "$name")
    [ "$health" = healthy ] && break
    sleep 1
done
[ "$(docker inspect --format '{{.State.Health.Status}}' "$name")" = healthy ]

url="postgresql://postgres:${password}@127.0.0.1:${port}/iot_test?sslmode=disable"

cargo test --manifest-path "$repo_root/Cargo.toml" --test postgres_outage
IOT_TEST_POSTGRES_URL="$url" \
IOT_TEST_POSTGRES_CONTAINER="$name" \
cargo test --manifest-path "$repo_root/Cargo.toml" --test postgres_outage -- --ignored --nocapture

docker exec "$name" createdb -U postgres iot_a18_test >/dev/null
crash_url="postgresql://postgres:${password}@127.0.0.1:${port}/iot_a18_test?sslmode=disable"
IOT_TEST_POSTGRES_URL="$crash_url" \
    cargo test --manifest-path "$repo_root/Cargo.toml" --test crash_recovery -- --test-threads=1
IOT_TEST_POSTGRES_URL="$crash_url" \
    cargo test --manifest-path "$repo_root/Cargo.toml" --test backend_a18 -- --ignored --nocapture
