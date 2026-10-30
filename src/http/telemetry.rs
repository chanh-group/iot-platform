//! Admin telemetry history and latest-value endpoints.

use axum::Json;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::domain_error::DomainError;
use crate::storage::sqlite::telemetry_query;

use super::AppState;
use super::api_error::ApiError;
use super::auth::AuthSession;
use crate::observability::request_id::RequestId;

const MAX_RANGE_MS: i64 = 31 * 24 * 60 * 60 * 1_000;
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1_000;

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<i64>,
    cursor: Option<String>,
    keys: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LatestQuery {
    keys: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Cursor {
    version: u8,
    device_id: String,
    from: i64,
    to: i64,
    after_ts: i64,
    after_id: i64,
    keys: Vec<String>,
}

pub async fn history(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| validation_error(&request_id, "invalid query"))?;
    let (Some(from), Some(to)) = (query.from, query.to) else {
        return Err(validation_error(&request_id, "from and to are required"));
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if to <= from || to.saturating_sub(from) > MAX_RANGE_MS {
        return Err(validation_error(&request_id, "invalid time range"));
    }
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(validation_error(&request_id, "limit must be 1..=1000"));
    }
    let keys = parse_keys(query.keys.as_deref(), &request_id)?;
    if !telemetry_query::device_exists(&state.pool, &device_id)
        .await
        .map_err(|error| internal_error(error, &request_id))?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "device_not_found",
            "device not found".to_owned(),
            request_id,
        ));
    }

    let after = query
        .cursor
        .as_deref()
        .map(|cursor| decode_cursor(cursor, &device_id, from, to, &keys, &request_id))
        .transpose()?;
    let rows = telemetry_query::history(&state.pool, &device_id, from, to, after, &keys, limit + 1)
        .await
        .map_err(|error| internal_error(error, &request_id))?;

    let has_more = rows.len() as i64 > limit;
    let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                encode_cursor(
                    &Cursor {
                        version: 1,
                        device_id: device_id.clone(),
                        from,
                        to,
                        after_ts: row.ts,
                        after_id: row.internal_id,
                        keys: keys.clone(),
                    },
                    &request_id,
                )
            })
            .transpose()?
    } else {
        None
    };
    let items: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let values = telemetry_query::filtered_values(&row.values_json, &keys)
                .map_err(|error| internal_error(error, &request_id))?;
            Ok(json!({
                "internal_id": row.internal_id,
                "event_id": row.event_id,
                "ts": row.ts,
                "received_at": row.received_at,
                "values": values,
            }))
        })
        .collect::<Result<_, ApiError>>()?;
    Ok(Json(json!({ "items": items, "next_cursor": next_cursor })))
}

pub async fn latest(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    query: Result<Query<LatestQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| validation_error(&request_id, "invalid query"))?;
    let keys = parse_keys(query.keys.as_deref(), &request_id)?;
    if !telemetry_query::device_exists(&state.pool, &device_id)
        .await
        .map_err(|error| internal_error(error, &request_id))?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "device_not_found",
            "device not found".to_owned(),
            request_id,
        ));
    }
    let rows = telemetry_query::latest(&state.pool, &device_id, &keys)
        .await
        .map_err(|error| internal_error(error, &request_id))?;
    let items: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let values = telemetry_query::filtered_values(&row.values_json, &[])
                .map_err(|error| internal_error(error, &request_id))?;
            let value = values.get(&row.key).cloned().unwrap_or(Value::Null);
            Ok(json!({
                "key": row.key,
                "value": value,
                "ts": row.ts,
                "received_at": row.received_at,
                "event_id": row.event_id,
            }))
        })
        .collect::<Result<_, ApiError>>()?;
    Ok(Json(json!({ "items": items })))
}

fn parse_keys(keys: Option<&str>, request_id: &str) -> Result<Vec<String>, ApiError> {
    let Some(keys) = keys else {
        return Ok(Vec::new());
    };
    let mut keys: Vec<String> = keys.split(',').map(str::to_owned).collect();
    keys.sort();
    keys.dedup();
    if keys.len() > 16
        || keys.iter().any(|key| {
            key.is_empty()
                || key.len() > 64
                || key.starts_with('$')
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_.:-".contains(&byte))
        })
    {
        return Err(validation_error(request_id, "invalid keys filter"));
    }
    Ok(keys)
}

fn encode_cursor(cursor: &Cursor, request_id: &str) -> Result<String, ApiError> {
    serde_json::to_vec(cursor)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|error| internal_error(error, request_id))
}

fn decode_cursor(
    encoded: &str,
    device_id: &str,
    from: i64,
    to: i64,
    keys: &[String],
    request_id: &str,
) -> Result<(i64, i64), ApiError> {
    let invalid = || {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            "invalid_cursor",
            "invalid telemetry cursor".to_owned(),
            request_id.to_owned(),
        )
    };
    if encoded.len() > 4_096 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    if cursor.version != 1
        || cursor.device_id != device_id
        || cursor.from != from
        || cursor.to != to
        || cursor.keys != keys
        || cursor.after_id <= 0
        || cursor.after_ts < from
        || cursor.after_ts >= to
    {
        return Err(invalid());
    }
    Ok((cursor.after_ts, cursor.after_id))
}

fn validation_error(request_id: &str, message: &str) -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "validation_error",
        message.to_owned(),
        request_id.to_owned(),
    )
}

fn internal_error(error: impl Into<anyhow::Error>, request_id: &str) -> ApiError {
    ApiError::from_domain(DomainError::Internal(error.into()), request_id.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_bound_to_range_and_key_filter() {
        let cursor = Cursor {
            version: 1,
            device_id: "device-a".to_owned(),
            from: 10,
            to: 20,
            after_ts: 12,
            after_id: 3,
            keys: vec!["x".to_owned()],
        };
        let encoded = encode_cursor(&cursor, "request").unwrap();
        assert_eq!(
            decode_cursor(&encoded, "device-a", 10, 20, &["x".to_owned()], "request").unwrap(),
            (12, 3)
        );
        assert!(decode_cursor(&encoded, "device-a", 10, 21, &["x".to_owned()], "request").is_err());
        assert!(decode_cursor(&encoded, "device-a", 10, 20, &["y".to_owned()], "request").is_err());
        assert!(decode_cursor(&encoded, "device-b", 10, 20, &["x".to_owned()], "request").is_err());
    }
}
