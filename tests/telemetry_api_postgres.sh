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
    name="iot-telemetry-api-$(cat /proc/sys/kernel/random/uuid)"
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
        cargo test --manifest-path "$repo_root/Cargo.toml" --test telemetry_api "$test_name" -- --ignored --exact --nocapture
    cleanup
    created=0
}
run_case history_pages_are_stable_and_key_filter_projects_matching_values_postgres
run_case latest_uses_received_time_then_internal_id_as_tie_breakers_postgres
run_case commit_order_backfill_finds_late_events_and_paginates_by_checkpoint_postgres
run_case missing_device_and_invalid_range_or_cursor_return_safe_errors_postgres
run_case starting_checkpoint_avoids_scanning_old_history_before_a_missed_event_postgres
run_case commit_checkpoint_does_not_advance_past_an_uncommitted_batch_postgres
run_case ignored_duplicate_and_conflict_advance_checkpoint_without_backfill_rows_postgres
