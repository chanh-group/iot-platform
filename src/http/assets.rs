//! Embedded dashboard assets (`02` §4.9).

use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "web/dist/"]
#[allow_missing = true]
struct DashboardAssets;

const BUILD_GUIDANCE: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Dashboard bundle missing</title></head>
<body>
  <h1>Dashboard bundle missing</h1>
  <p>Build the frontend, then restart the server:</p>
  <pre>cd web &amp;&amp; bun install --frozen-lockfile &amp;&amp; bun run --bun build</pre>
</body>
</html>
"#;

pub(crate) fn index() -> Response {
    match DashboardAssets::get("index.html") {
        Some(file) => file_response("index.html", file.data.into_owned()),
        None => missing_frontend(),
    }
}

pub(crate) fn asset(path: &str) -> Option<Response> {
    if !valid_asset_path(path) {
        return None;
    }

    DashboardAssets::get(path).map(|file| file_response(path, file.data.into_owned()))
}

fn valid_asset_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains('\\')
        && path
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

fn file_response(path: &str, data: Vec<u8>) -> Response {
    let content_type = mime_guess::from_path(path)
        .first_or_octet_stream()
        .to_string();
    let mut response = Response::new(Body::from(data));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_str(&content_type)
            .unwrap_or_else(|_| header::HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        if path == "index.html" {
            header::HeaderValue::from_static("no-store")
        } else if has_content_hash(path) {
            header::HeaderValue::from_static("public, max-age=31536000, immutable")
        } else {
            header::HeaderValue::from_static("no-cache")
        },
    );
    response
}

fn has_content_hash(path: &str) -> bool {
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

fn missing_frontend() -> Response {
    let status = if cfg!(debug_assertions) {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (
        status,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        BUILD_GUIDANCE,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::has_content_hash;

    #[test]
    fn only_hashed_asset_names_get_immutable_cache() {
        assert!(has_content_hash("assets/index-BYTembvD.js"));
        assert!(has_content_hash("assets/index-JKAN3xRv.css"));
        assert!(has_content_hash("assets/index-abc-_xyz.js"));
        assert!(!has_content_hash("assets/vendor.js"));
        assert!(!has_content_hash("assets/logo-123.png"));
    }
}
