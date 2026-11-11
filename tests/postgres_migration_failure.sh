#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
name="iot-pg-migration-failure-$$"
password=$(cat /proc/sys/kernel/random/uuid)
port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
created=0
cleanup() {
    if [ "$created" -eq 1 ]; then
        docker rm -f "$name" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT HUP INT TERM

docker run -d \
    --name "$name" \
    --label io.iot.test-fixture=postgres-migration-failure \
    --health-cmd='pg_isready -U postgres -d iot_test' \
    --health-interval=1s \
    --health-timeout=1s \
    --health-retries=30 \
    -e POSTGRES_PASSWORD="$password" \
    -e POSTGRES_DB=iot_test \
    -p "127.0.0.1:${port}:5432" \
    postgres:18.6-bookworm >/dev/null
created=1

for _ in $(seq 1 60); do
    health=$(docker inspect --format '{{.State.Health.Status}}' "$name")
    [ "$health" = healthy ] && break
    sleep 1
done
[ "$(docker inspect --format '{{.State.Health.Status}}' "$name")" = healthy ]

cargo test --manifest-path "$repo_root/Cargo.toml" --test postgres_migration_failure
IOT_TEST_POSTGRES_URL="postgresql://postgres:${password}@127.0.0.1:${port}/iot_test?sslmode=disable" \
IOT_TEST_POSTGRES_CONTAINER="$name" \
    cargo test --manifest-path "$repo_root/Cargo.toml" --test postgres_migration_failure -- --ignored --nocapture
