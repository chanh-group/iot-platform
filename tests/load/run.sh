#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
load_root=${IOT_LOAD_TMPDIR:-"$repo/target/load-tmp"}
duration=${IOT_LOAD_DURATION_SECONDS:-60}
test_name=one_thousand_qos1_clients_measure_ack_latency_and_resource_growth
mkdir -p "$load_root"
load_tmp=$(mktemp -d "$load_root/run.XXXXXX")
trap 'rm -rf -- "$load_tmp"' EXIT
load_tmp=$(CDPATH= cd -- "$load_tmp" && pwd)
echo "Load test files: $load_tmp"
cd "$repo"
if [ "${IOT_LOAD_CGROUP:-0}" = 1 ]; then
    cargo test --locked --test load_baseline --no-run
    binary=$(find "$repo/target/debug/deps" -maxdepth 1 -type f -name 'load_baseline-*' -executable \
        -printf '%T@ %p\n' | sort -nr | sed -n '1s/^[^ ]* //p')
    test -n "$binary"
    cp -- "$binary" "$load_tmp/load_baseline"
    chmod 0755 "$load_tmp/load_baseline"
    cp -- "$repo/tests/load/.dockerignore" "$load_tmp/.dockerignore"
    cp -- "$repo/tests/load/Dockerfile" "$load_tmp/Dockerfile"
    mkdir -p "$load_tmp/tmp"
    image=$(docker build --quiet "$load_tmp")
    echo 'Running with Docker cgroup limits: 4 vCPU, 8 GiB RAM.'
    docker run --rm --cpus=4 --memory=8g --memory-swap=8g --ulimit nofile=8192:8192 \
        --user "$(id -u):$(id -g)" \
        --volume "$load_tmp:/bench:Z" \
        --env "IOT_LOAD_DURATION_SECONDS=$duration" --env IOT_LOAD_CGROUP=1 --env TMPDIR=/bench/tmp \
        "$image" "$test_name" --ignored --exact --nocapture
else
    TMPDIR="$load_tmp" IOT_LOAD_DURATION_SECONDS="$duration" \
        cargo test --locked --test load_baseline "$test_name" -- --ignored --exact --nocapture
fi
