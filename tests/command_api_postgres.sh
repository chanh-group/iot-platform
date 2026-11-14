#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
name=
created=0
cleanup() {
    if [ "$created" -eq 1 ] && [ "$(docker inspect --format '{{ index .Config.Labels "io.iot.test-fixture" }}' "$name" 2>/dev/null || true)" = mqtt-auth ]; then
        docker rm -f "$name" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT HUP INT TERM
run_case() {
    test_name=$1
    name="iot-command-api-$(cat /proc/sys/kernel/random/uuid)"
    password=$(cat /proc/sys/kernel/random/uuid)
    port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    docker run -d --name "$name" --label io.iot.test-fixture=mqtt-auth \
        --health-cmd='pg_isready -U postgres -d iot_test' --health-interval=1s --health-timeout=1s --health-retries=30 \
        -e POSTGRES_PASSWORD="$password" -e POSTGRES_DB=iot_test \
        -p "127.0.0.1:${port}:5432" postgres:18.6-bookworm >/dev/null
    created=1
    for _ in $(seq 1 60); do
        [ "$(docker inspect --format '{{.State.Health.Status}}' "$name")" = healthy ] && break
        sleep 1
    done
    [ "$(docker inspect --format '{{.State.Health.Status}}' "$name")" = healthy ]
    IOT_TEST_POSTGRES_URL="postgresql://postgres:${password}@127.0.0.1:${port}/iot_test?sslmode=disable" \
    IOT_TEST_POSTGRES_CONTAINER="$name" IOT_TEST_POSTGRES_PORT="$port" \
        cargo test --manifest-path "$repo_root/Cargo.toml" --test command_api "$test_name" -- --ignored --exact --nocapture
    cleanup
    created=0
}
run_case idempotency_uses_raw_request_bytes_and_does_not_audit_replays_postgres
run_case concurrent_creates_do_not_exceed_pending_queue_limit_postgres
run_case audit_failure_rolls_back_command_and_returns_storage_error_postgres
run_case expired_command_is_visible_in_detail_and_filtered_list_postgres
run_case command_endpoints_require_admin_and_post_requires_csrf_postgres
run_case create_validates_fields_and_device_state_postgres
