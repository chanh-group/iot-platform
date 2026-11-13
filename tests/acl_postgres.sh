#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

run_case() (
    test_name=$1
    name="iot-mqtt-acl-$(cat /proc/sys/kernel/random/uuid)"
    password=$(cat /proc/sys/kernel/random/uuid)
    port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
    created=0
    cleanup() {
        if [ "$created" -eq 1 ] && [ "$(docker inspect --format '{{ index .Config.Labels "io.iot.test-fixture" }}' "$name" 2>/dev/null || true)" = mqtt-acl ]; then
            docker rm -f "$name" >/dev/null 2>&1 || true
        fi
    }
    trap cleanup EXIT HUP INT TERM

    docker run -d \
        --name "$name" \
        --label io.iot.test-fixture=mqtt-acl \
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
        cargo test --manifest-path "$repo_root/Cargo.toml" --test mqtt_acl "$test_name" -- --ignored --exact --nocapture
)

run_case own_topics_are_allowed_for_pub_and_sub_postgres
run_case subscribe_outside_own_prefix_is_refused_and_keeps_the_connection_postgres
run_case cross_device_publish_is_denied_not_routed_and_disconnects_postgres
run_case server_only_and_system_topics_are_denied_for_publish_postgres
run_case retained_publish_to_server_topics_is_denied_postgres
