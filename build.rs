#[path = "build_support.rs"]
mod build_support;

use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=web/dist");
    println!("cargo:rerun-if-changed=web/dist/index.html");

    if std::env::var("PROFILE").is_ok_and(|profile| profile == "release")
        && let Err(error) = build_support::validate_bundle(Path::new("web/dist"))
    {
        panic!(
            "invalid frontend bundle: {error}. Run `cd web && bun install --frozen-lockfile && bun run --bun build` before `cargo build --release --locked`"
        );
    }
}
