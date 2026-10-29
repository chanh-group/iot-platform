mod common;

use common::TestApp;
use reqwest::StatusCode;
use reqwest::header::CACHE_CONTROL;
use serde_json::{Value, json};

async fn get(server: &TestApp, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{}{path}", server.base))
        .send()
        .await
        .unwrap()
}

async fn json_body(response: reqwest::Response) -> Value {
    response.json().await.unwrap()
}

#[tokio::test]
async fn healthz_is_public_and_readyz_tracks_the_database() {
    let server = TestApp::start().await;

    let health = get(&server, "/healthz").await;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(json_body(health).await, json!({ "status": "ok" }));

    let ready = get(&server, "/readyz").await;
    assert_eq!(ready.status(), StatusCode::OK);
    assert_eq!(
        json_body(ready).await,
        json!({
            "status": "ready",
            "checks": { "database": "ok", "mqtt": "ok", "ingest": "ok" }
        })
    );

    server.pool.close().await;

    let health = get(&server, "/healthz").await;
    assert_eq!(health.status(), StatusCode::OK);

    let ready = get(&server, "/readyz").await;
    assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = ready.text().await.unwrap();
    assert!(body.contains(r#""status":"not_ready""#));
    assert!(!body.contains("sqlite"));
    assert!(!body.contains("SELECT"));
}

#[tokio::test]
async fn api_unknown_paths_and_methods_are_json_errors() {
    let server = TestApp::start().await;

    let response = get(&server, "/api/v1/nope").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response.headers().contains_key("x-request-id"));
    assert_eq!(json_body(response).await["code"], "not_found");

    let response = get(&server, "/api/v1/auth/login").await;
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(json_body(response).await["code"], "method_not_allowed");
}

#[tokio::test]
async fn dashboard_serves_index_and_spa_fallback_but_not_assets() {
    let server = TestApp::start().await;

    let response = get(&server, "/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-store");
    assert!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html")
    );
    assert!(response.text().await.unwrap().contains("IoT Server"));

    let response = get(&server, "/devices/abc").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("IoT Server"));

    let response = get(&server, "/assets/app.js").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(json_body(response).await["code"], "not_found");
}

#[tokio::test]
async fn dashboard_disabled_returns_404_but_health_still_works() {
    let server = TestApp::builder().dashboard(false).start().await;

    for path in ["/", "/devices/abc", "/assets/app.js"] {
        let response = get(&server, path).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        assert_eq!(json_body(response).await["code"], "not_found", "{path}");
    }

    let response = get(&server, "/healthz").await;
    assert_eq!(response.status(), StatusCode::OK);
}
