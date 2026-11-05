#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"
cargo test --locked --test docker_smoke -- --ignored --nocapture
