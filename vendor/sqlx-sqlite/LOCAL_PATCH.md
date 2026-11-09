# Local SQLx SQLite manifest backport

This is the crates.io `sqlx-sqlite` 0.9.0 source (`MIT OR Apache-2.0`; crate checksum `488e99c397a62007e4229aec669a179816339afc6d2620ca6fa420dbee2e982c`). Only the `libsqlite3-sys` upper bound in `Cargo.toml` is changed from `<0.38.0` to `<0.40.0`, backporting upstream commits `94aafe3a68884d923b0798a767c8d7f6cfda89d2` and `10462fa4414c6c7f1ed1d39bb31678e3b726878c`. This permits the patched `libsqlite3-sys 0.39.1` / SQLite 3.54.1 while SQLx remains pinned to 0.9.0. Remove this local patch when a pinned SQLx release includes the widened bound.
