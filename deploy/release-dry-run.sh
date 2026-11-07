#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -n1)
tag="v$version"
out=${OUT_DIR:-target/release-dry-run}
image="iot-server:dry-run"

step() {
    printf '\n== %s ==\n' "$*"
}

need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "missing $1; see docs/release-checklist.md" >&2
        exit 1
    }
}

for tool in cargo bun docker syft trivy cosign; do
    need "$tool"
done
cargo deny --version >/dev/null
cargo audit --version >/dev/null

rm -rf "$out"
mkdir -p "$out"

step "Frontend"
(
    cd web
    bun install --frozen-lockfile
    bun run --bun typecheck
    bun run --bun test
    bun run --bun build
)

step "Baseline Rust ($tag)"
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
git diff --check

step "Release build"
cargo build --release --locked

step "Dependency, license and advisory checks"
cargo deny check
cargo audit

step "Docker image"
docker build -f deploy/Dockerfile -t "$image" .
docker run --rm "$image" --version
uid=$(docker run --rm --entrypoint id "$image" -u)
test "$uid" = 10001

step "Source SBOM"
syft scan dir:. \
    --exclude './target/**' \
    --exclude './web/node_modules/**' \
    --exclude './web/dist/**' \
    --exclude './data/**' \
    --exclude './.git/**' \
    -q -o "spdx-json=$out/sbom-source.spdx.json"

step "Image SBOM"
syft scan "$image" -q -o "spdx-json=$out/sbom-image.spdx.json"

step "Image vulnerability gate (fixable HIGH/CRITICAL)"
trivy image --quiet --scanners vuln --severity HIGH,CRITICAL \
    --ignore-unfixed --exit-code 1 "$image"

step "Full image vulnerability report"
trivy image --quiet --scanners vuln --severity HIGH,CRITICAL \
    --format json --output "$out/trivy-image.json" --exit-code 0 "$image"

step "Artifact and checksums"
tar -C target/release -czf "$out/iot-server-$tag-linux-x86_64.tar.gz" iot-server
(
    cd "$out"
    sha256sum \
        iot-server-*.tar.gz \
        sbom-source.spdx.json \
        sbom-image.spdx.json \
        trivy-image.json > SHA256SUMS
)

step "Sign and verify checksums with an ephemeral key"
export COSIGN_PASSWORD
COSIGN_PASSWORD=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
cosign generate-key-pair --output-key-prefix "$out/cosign" >/dev/null
cosign sign-blob --yes --key "$out/cosign.key" \
    --bundle "$out/SHA256SUMS.bundle" "$out/SHA256SUMS"
cosign verify-blob --key "$out/cosign.pub" \
    --bundle "$out/SHA256SUMS.bundle" "$out/SHA256SUMS"
rm -f "$out/cosign.key"

step "Verify artifact checksums"
(cd "$out" && sha256sum -c SHA256SUMS)

printf '\nrelease dry-run OK\n'
printf 'tag:     %s\n' "$tag"
printf 'image:   %s\n' "$image"
printf 'output:  %s\n' "$out"
