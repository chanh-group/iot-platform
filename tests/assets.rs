#[path = "../build_support.rs"]
mod build_support;

mod common;

use std::path::Path;

use common::TestApp;
use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, CONTENT_TYPE};
use serde_json::json;

fn bundled_index_path() -> String {
    concat!(env!("CARGO_MANIFEST_DIR"), "/web/dist/index.html").to_owned()
}

fn asset_path<'a>(index: &'a str, marker: &str) -> Option<&'a str> {
    let path = index.split_once(marker)?.1.split_once('"')?.0;
    path.starts_with("/assets/").then_some(path)
}

fn is_hashed_asset(path: &str) -> bool {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let Some(stem) = file_name.rsplit_once('.').map(|(stem, _)| stem) else {
        return false;
    };
    let Some(separator) = stem.len().checked_sub(9) else {
        return false;
    };
    let Some(hash) = stem.get(separator + 1..) else {
        return false;
    };
    separator > 0
        && stem.as_bytes().get(separator) == Some(&b'-')
        && hash.len() == 8
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

#[test]
fn hashed_asset_check_matches_the_server_hash_length_and_alphabet() {
    assert!(is_hashed_asset("/assets/index-abc-_xyz.js"));
    assert!(!is_hashed_asset("/assets/index-abcdefghx.js"));
}

#[test]
fn release_guard_rejects_missing_index_references_in_an_isolated_fixture() {
    let directory = tempfile::tempdir().unwrap();
    let dist = directory.path().join("dist");
    std::fs::create_dir_all(dist.join("assets")).unwrap();
    std::fs::write(
        dist.join("index.html"),
        r#"<script src="/assets/app-BYTembvD.js"></script><link rel="icon" href="/favicon.svg">"#,
    )
    .unwrap();
    let error = build_support::validate_bundle(&dist).unwrap_err();
    assert!(
        error.contains("/assets/app-BYTembvD.js"),
        "unexpected validation error: {error}"
    );

    std::fs::write(dist.join("assets/app-BYTembvD.js"), "bundle").unwrap();
    let error = build_support::validate_bundle(&dist).unwrap_err();
    assert!(
        error.contains("/favicon.svg"),
        "unexpected validation error: {error}"
    );

    std::fs::write(dist.join("favicon.svg"), "icon").unwrap();
    build_support::validate_bundle(&dist).expect("all index references exist in the fixture");
}

#[test]
fn release_guard_requires_a_local_script_in_a_nonempty_index() {
    let directory = tempfile::tempdir().unwrap();
    let dist = directory.path().join("dist");
    std::fs::create_dir_all(&dist).unwrap();

    for index in [
        "",
        "<main>dashboard</main>",
        "<script src=\"https://cdn.example/app.js\"></script>",
    ] {
        std::fs::write(dist.join("index.html"), index).unwrap();
        let error = build_support::validate_bundle(&dist).unwrap_err();
        assert!(error.contains("no local <script src> entry"), "{error}");
    }

    std::fs::write(dist.join("index.html"), "<script src=\"app.js\"></script>").unwrap();
    let error = build_support::validate_bundle(&dist).unwrap_err();
    assert!(
        error.contains("references missing asset: app.js"),
        "{error}"
    );

    std::fs::write(dist.join("app.js"), "bundle").unwrap();
    build_support::validate_bundle(&dist).expect("a local JS bundle is present");
}

#[tokio::test]
async fn serves_spa_and_hashed_assets_without_masking_api_paths() {
    let server = TestApp::start().await;
    let bundle_exists = Path::new(&bundled_index_path()).is_file();

    let response = reqwest::get(format!("{}/", server.base)).await.unwrap();
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    assert!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    if bundle_exists {
        assert_eq!(response.status(), StatusCode::OK);
    } else {
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    let index = response.text().await.unwrap();
    if bundle_exists {
        for (marker, name) in [("src=\"", "JS"), ("href=\"", "CSS")] {
            let asset = asset_path(&index, marker)
                .unwrap_or_else(|| panic!("built index references its {name} bundle"));
            assert!(
                is_hashed_asset(asset),
                "Vite {name} asset should be content-hashed"
            );
            let response = reqwest::get(format!("{}{}", server.base, asset))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_TYPE).unwrap(),
                mime_guess::from_path(asset)
                    .first_or_octet_stream()
                    .essence_str()
            );
            assert_eq!(
                response.headers().get(CACHE_CONTROL).unwrap(),
                "public, max-age=31536000, immutable"
            );
            assert!(!response.bytes().await.unwrap().is_empty());
        }

        let favicon_path = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/web/dist/favicon.svg"));
        if favicon_path.is_file() {
            let response = reqwest::get(format!("{}/favicon.svg", server.base))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers().get(CONTENT_TYPE).unwrap(),
                "image/svg+xml"
            );
            assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
            let expected = std::fs::read(favicon_path).unwrap();
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                expected.as_slice()
            );
        }
    } else {
        assert!(index.contains("Dashboard bundle missing"));
        assert!(index.contains("bun run --bun build"));
    }

    let fallback = reqwest::get(format!("{}/devices/example", server.base))
        .await
        .unwrap();
    assert_eq!(
        fallback.status(),
        if bundle_exists {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    );
    assert_eq!(fallback.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(fallback.text().await.unwrap(), index);

    for path in [
        "/assets/missing.js",
        "/missing.js",
        "/api",
        "/api/v1/unknown",
    ] {
        let response = reqwest::get(format!("{}{path}", server.base))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert!(
            response
                .headers()
                .get(CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["code"],
            "not_found"
        );
    }

    let response = reqwest::get(format!("{}/api/v1/auth/login", server.base))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["code"],
        "method_not_allowed"
    );

    let health = reqwest::get(format!("{}/healthz", server.base))
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(
        health.json::<serde_json::Value>().await.unwrap(),
        json!({ "status": "ok" })
    );
}

#[tokio::test]
async fn metrics_route_is_not_swallowed_by_spa_fallback() {
    let token = "test-metrics-token-with-more-than-32-bytes";
    let server = TestApp::builder().metrics_token(token).start().await;
    let response = reqwest::Client::new()
        .get(format!("{}/metrics", server.base))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response
            .headers()
            .get(CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    assert!(response.text().await.unwrap().contains("iot_"));
}

#[tokio::test]
async fn dashboard_disabled_keeps_api_health_and_mqtt_available() {
    let server = TestApp::builder().dashboard(false).start().await;
    let client = reqwest::Client::new();

    for path in [
        "/",
        "/devices/example",
        "/assets/missing.js",
        "/favicon.svg",
    ] {
        let response = client
            .get(format!("{}{path}", server.base))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }

    assert_eq!(
        client
            .get(format!("{}/api/v1/auth/me", server.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{}/readyz", server.base))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    tokio::net::TcpStream::connect(server.mqtt_addr)
        .await
        .expect("dashboard=false must leave the MQTT listener available");
}

#[cfg(not(debug_assertions))]
#[tokio::test]
async fn copied_release_binary_serves_bundle_without_source_or_javascript_runtime() {
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    struct RunningServer(Child);

    impl Drop for RunningServer {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("iot-server");
    std::fs::copy(env!("CARGO_BIN_EXE_iot-server"), &executable).unwrap();
    assert!(!directory.path().join("web").exists());

    let http_port = free_port();
    let mqtt_port = free_port();
    let config_path = directory.path().join("conf.toml");
    std::fs::write(
        &config_path,
        format!(
            "config_version = 1\n\
             [server]\n\
             data_dir = \"./data\"\n\
             [http]\n\
             listen = \"127.0.0.1:{http_port}\"\n\
             [mqtt]\n\
             listen = \"127.0.0.1:{mqtt_port}\"\n\
             [database]\n\
             url = \"sqlite://./data/iot.db\"\n"
        ),
    )
    .unwrap();

    let process = Command::new(&executable)
        .arg("--config")
        .arg(&config_path)
        .arg("run")
        .current_dir(directory.path())
        .env("PATH", "")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut process = RunningServer(process);
    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();
    let mut index_response = None;
    for _ in 0..100 {
        if let Some(status) = process.0.try_wait().unwrap() {
            panic!("release binary exited before serving dashboard: {status}");
        }
        if let Ok(response) = client.get(format!("{base}/")).send().await
            && response.status() == StatusCode::OK
        {
            index_response = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let response = index_response.expect("copied release binary should serve its embedded index");
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    let index = response.text().await.unwrap();
    let javascript = asset_path(&index, "src=\"").expect("release index references bundled JS");
    assert!(is_hashed_asset(javascript));
    let javascript_response = client
        .get(format!("{base}{javascript}"))
        .send()
        .await
        .unwrap();
    assert_eq!(javascript_response.status(), StatusCode::OK);
    assert_eq!(
        javascript_response.headers().get(CACHE_CONTROL).unwrap(),
        "public, max-age=31536000, immutable"
    );
    assert!(!javascript_response.bytes().await.unwrap().is_empty());

    let favicon_response = client
        .get(format!("{base}/favicon.svg"))
        .send()
        .await
        .unwrap();
    assert_eq!(favicon_response.status(), StatusCode::OK);
    assert_eq!(
        favicon_response.headers().get(CONTENT_TYPE).unwrap(),
        "image/svg+xml"
    );
    assert!(!favicon_response.bytes().await.unwrap().is_empty());
}
