#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
container=$(docker create prom/prometheus:v3.8.0)
trap 'docker rm -f "$container" >/dev/null 2>&1 || true' EXIT HUP INT TERM
docker cp "$repo/deploy/alerts/prometheus.rules.yml" "$container:/tmp/prometheus.rules.yml"
docker cp "$repo/deploy/alerts/prometheus.rules.test.yml" "$container:/tmp/prometheus.rules.test.yml"
docker start "$container" >/dev/null
docker exec "$container" /bin/promtool check rules /tmp/prometheus.rules.yml
docker exec "$container" /bin/promtool test rules /tmp/prometheus.rules.test.yml
