#!/bin/sh
set -eu

IMAGE=postgres:18.6-bookworm
RANDOM_TAG=$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')
FIXTURE_LABEL="iot-p5-load-$RANDOM_TAG"
NAME=$FIXTURE_LABEL
PASSWORD=$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
CONTAINER_ID=
TEST_WRAPPER_PID=
TEST_PGID=
PGID_FILE=$(mktemp "${TMPDIR:-/tmp}/iot-load-pgid.XXXXXX")

group_is_alive() {
    [ -n "$TEST_PGID" ] || return 1
    ps -eo pgid=,sid= | awk -v group="$TEST_PGID" \
        '$1 == group && $2 == group { found = 1 } END { exit !found }'
}

terminate_test_group() {
    if [ -n "$TEST_WRAPPER_PID" ] && [ -z "$TEST_PGID" ]; then
        for _ in $(seq 1 100); do
            if [ -s "$PGID_FILE" ]; then
                TEST_PGID=$(cat "$PGID_FILE")
                break
            fi
            kill -0 "$TEST_WRAPPER_PID" 2>/dev/null || break
            sleep 0.05
        done
    fi
    if group_is_alive; then
        kill -TERM -- "-$TEST_PGID" 2>/dev/null || true
        for _ in $(seq 1 50); do
            group_is_alive || break
            sleep 0.1
        done
        if group_is_alive; then
            kill -KILL -- "-$TEST_PGID" 2>/dev/null || true
        fi
    fi
    if [ -n "$TEST_WRAPPER_PID" ]; then
        wait "$TEST_WRAPPER_PID" 2>/dev/null || true
        TEST_WRAPPER_PID=
    fi
    TEST_PGID=
}

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    terminate_test_group
    if [ -n "$CONTAINER_ID" ]; then
        docker rm -fv "$CONTAINER_ID" >/dev/null 2>&1 || true
    fi
    rm -f "$PGID_FILE"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

if CREATED_ID=$(docker run -d --name "$NAME" \
    --label "org.thingsboardrust.fixture=$FIXTURE_LABEL" \
    -e POSTGRES_USER=iot_load -e POSTGRES_PASSWORD="$PASSWORD" \
    -e POSTGRES_DB=iot_load_fixture -p "127.0.0.1:$PORT:5432" "$IMAGE"); then
    CONTAINER_ID=$CREATED_ID
else
    echo 'could not create PostgreSQL load fixture' >&2
    exit 1
fi

ready=0
for _ in $(seq 1 90); do
    if docker exec "$CONTAINER_ID" pg_isready -U iot_load -d iot_load_fixture >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
[ "$ready" -eq 1 ] || { echo 'fixture PostgreSQL did not become ready' >&2; exit 1; }

IMAGE_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}')
test "$(docker inspect --format '{{.Id}}' "$CONTAINER_ID")" = "$CONTAINER_ID"
test "$(docker inspect --format '{{.Image}}' "$CONTAINER_ID")" = "$IMAGE_ID"
test "$(docker inspect --format '{{.Config.Image}}' "$CONTAINER_ID")" = "$IMAGE"
test "$(docker inspect --format '{{index .Config.Labels "org.thingsboardrust.fixture"}}' "$CONTAINER_ID")" = "$FIXTURE_LABEL"
test "$(docker inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$CONTAINER_ID")" = "$PORT"

printf 'fixture_image=%s\nfixture_image_id=%s\nstarted_utc=%s\ndocker_engine=%s\nhost=%s\nlogical_cpus=%s\n' \
    "$IMAGE" "$IMAGE_ID" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "$(docker version --format '{{.Server.Version}}')" "$(uname -a)" "$(nproc)"
python3 tests/load/test_cgroup.py
python3 tests/load/test_cgroup.py --report

if ! command -v setsid >/dev/null 2>&1; then
    echo 'setsid is required to isolate load-test process cleanup' >&2
    exit 1
fi

IOT_LOAD_POSTGRES_URL="postgres://iot_load:$PASSWORD@127.0.0.1:$PORT/iot_load_fixture?sslmode=disable" \
    IOT_LOAD_POSTGRES_PASSWORD="$PASSWORD" \
    IOT_LOAD_POSTGRES_PORT="$PORT" \
    IOT_LOAD_POSTGRES_CONTAINER_ID="$CONTAINER_ID" \
    IOT_LOAD_POSTGRES_FIXTURE_LABEL="$FIXTURE_LABEL" \
    IOT_LOAD_POSTGRES_IMAGE_ID="$IMAGE_ID" \
    IOT_LOAD_METRICS_TOKEN=load-test-metrics-token-not-for-production \
        setsid --fork --wait sh -c '
            pgid_file=$1
            shift
            pgid=$(ps -o pgid= -p "$$" | tr -d " ")
            sid=$(ps -o sid= -p "$$" | tr -d " ")
            [ "$pgid" = "$$" ] && [ "$sid" = "$$" ] || exit 125
            printf "%s\n" "$pgid" > "$pgid_file"
            exec "$@"
        ' iot-load-test-group "$PGID_FILE" cargo test --test load_baseline one_thousand_qos1_clients_measure_postgres_load -- --ignored --nocapture &
TEST_WRAPPER_PID=$!
for _ in $(seq 1 100); do
    [ -s "$PGID_FILE" ] && break
    kill -0 "$TEST_WRAPPER_PID" 2>/dev/null || break
    sleep 0.05
done
TEST_PGID=$(cat "$PGID_FILE" 2>/dev/null || true)
if [ -z "$TEST_PGID" ] || ! group_is_alive; then
    echo 'could not establish isolated load-test process group' >&2
    exit 1
fi
while kill -0 "$TEST_WRAPPER_PID" 2>/dev/null; do
    state=$(ps -o stat= -p "$TEST_WRAPPER_PID" 2>/dev/null | tr -d ' ' || true)
    case "$state" in
        ''|Z*) break ;;
    esac
    sleep 0.05
done
if wait "$TEST_WRAPPER_PID"; then
    status=0
else
    status=$?
fi
terminate_test_group
if [ "$status" -eq 0 ]; then
    printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
else
    printf 'failed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2
    exit "$status"
fi
