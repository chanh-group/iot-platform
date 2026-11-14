#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run_case() (
    test_name=$1
    name="iot-mqtt-protocol-$(cat /proc/sys/kernel/random/uuid)"
    password=$(cat /proc/sys/kernel/random/uuid)
    port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    created=0
    cleanup() {
        if [ "$created" -eq 1 ] && [ "$(docker inspect --format '{{ index .Config.Labels "io.iot.test-fixture" }}' "$name" 2>/dev/null || true)" = mqtt-protocol ]; then
            docker rm -f "$name" >/dev/null 2>&1 || true
        fi
    }
    trap cleanup EXIT HUP INT TERM

    docker run -d --name "$name" --label io.iot.test-fixture=mqtt-protocol \
        --health-cmd='pg_isready -U postgres -d iot_test' \
        --health-interval=1s --health-timeout=1s --health-retries=30 \
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
        cargo test --manifest-path "$repo_root/Cargo.toml" --test mqtt_protocol "$test_name" -- --ignored --exact --nocapture
)

if [ "$#" -eq 1 ]; then
    run_case "$1"
else
    run_case qos_levels_and_own_device_wildcard_round_trip_postgres
    run_case max_inflight_per_client_limits_unacknowledged_qos1_deliveries_postgres
    run_case subscription_limit_is_enforced_by_the_listener_postgres
    run_case persistent_session_reconnects_then_expires_postgres
    run_case broker_restart_discards_persistent_sessions_postgres
    run_case retained_last_will_is_replayed_after_ungraceful_disconnect_postgres
    run_case topic_level_limit_is_enforced_by_the_listener_postgres
    run_case packet_limit_is_enforced_by_the_listener_postgres
    run_case connection_limit_rejects_an_additional_device_postgres
fi
