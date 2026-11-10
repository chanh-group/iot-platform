# Release `v0.1.1` — 2026-11-09

## Verification

- Release commit: `59b44fdb22ff94b719bbc56d40aa5f8ca7e78932`, tag `v0.1.1`.
- CI [run 42626360028](https://github.com/chanh-group/iot-platform/actions/runs/42626360028): Rust, frontend, Docker and audit jobs all passed on the release commit.
- Release workflow [run 42628361802](https://github.com/chanh-group/iot-platform/actions/runs/42628361802): passed and published [GitHub Release `v0.1.1`](https://github.com/chanh-group/iot-platform/releases/tag/v0.1.1).
- GHCR digest: `sha256:e5c0b199fab5df3cf6535fcbea7bd08fdd18a376fe557eea5509b4fd260836eb`. The release workflow verified the image signature and promoted tag against this digest using OIDC identity `https://github.com/chanh-group/iot-platform/.github/workflows/release.yml@refs/tags/v0.1.1` and issuer `https://token.actions.githubusercontent.com`.
- Release assets include the Linux x86_64 binary, source/image SBOMs, `trivy-image.json`, `SHA256SUMS`, and `SHA256SUMS.bundle`. Downloaded assets passed `sha256sum -c`; local `cosign verify-blob` on the bundle passed with the release OIDC identity and issuer.
- Source SBOM: 859 packages; image SBOM: 110 packages. Trivy: 0 HIGH/CRITICAL findings and 0 exceptions.
- `cargo audit`: no vulnerabilities after using bundled SQLite 3.54.1 via `libsqlite3-sys 0.39.1`; two existing unmaintained transitive advisories remain covered by the recorded exceptions.
- Dry-run on the exact release commit: `target/release-dry-run-59b44fd-20261109/`, local image ID `sha256:fa36752e8b3d7a49962d930f9c842555192bca4986ac1b35898f32a9ad64c0b2`. This local ID is not the GHCR digest.

## Distribution and prior candidate

- GHCR package visibility is intentionally **private** for the group's project. Group members must authenticate with `read:packages`; anonymous pull returning 401 is expected.
- The earlier `v0.1.0` tag's release workflow failed before GHCR login/push and created no GitHub Release. The tag was left unchanged; the first successful release is `v0.1.1`.

## Phase 5 gate

The Phase 4 release gate is satisfied. P5-01 storage/contract audit may begin; backend implementation remains gated on the audit and its contract/test prerequisites.
