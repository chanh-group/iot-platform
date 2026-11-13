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
use crate::core::telemetry::{TelemetryHistoryRange, filtered_values};

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
    after_id: Option<i64>,
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
    #[serde(default)]
    time_basis: HistoryTimeBasis,
    from: i64,
    to: i64,
    #[serde(alias = "after_ts")]
    after_time: i64,
    after_id: i64,
    keys: Vec<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HistoryTimeBasis {
    #[default]
    EventTimestamp,
    CommitId,
}

#[derive(Deserialize)]
pub struct AttributeQuery {
    limit: Option<i64>,
    cursor: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttributeCursor {
    v: u8,
    device_id: String,
    key: String,
}

pub async fn history(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| validation_error(&request_id, "invalid query"))?;
    let keys = parse_keys(query.keys.as_deref(), &request_id)?;
    let decoded_cursor = query
        .cursor
        .as_deref()
        .map(|cursor| decode_cursor(cursor, &request_id))
        .transpose()?;
    let (from, requested_to, time_basis) = match (query.after_id, query.from, query.to) {
        (Some(after_id), None, None) => (after_id, None, HistoryTimeBasis::CommitId),
        (None, Some(from), Some(to)) => (from, Some(to), HistoryTimeBasis::EventTimestamp),
        _ => {
            return Err(validation_error(&request_id, "provide from/to or after_id"));
        }
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if from < 0
        || (time_basis == HistoryTimeBasis::EventTimestamp
            && requested_to.is_none_or(|to| to <= from || to.saturating_sub(from) > MAX_RANGE_MS))
    {
        return Err(validation_error(&request_id, "invalid time range"));
    }
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(validation_error(&request_id, "limit must be 1..=1000"));
    }
    if !state
        .telemetry_repository
        .device_exists(&device_id)
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

    let to = if time_basis == HistoryTimeBasis::CommitId {
        let committed_through = state
            .telemetry_repository
            .committed_through()
            .await
            .map_err(|error| internal_error(error, &request_id))?;
        if from > committed_through
            || decoded_cursor
                .as_ref()
                .is_some_and(|cursor| cursor.to > committed_through)
        {
            return Err(if decoded_cursor.is_some() {
                invalid_cursor(&request_id)
            } else {
                validation_error(&request_id, "after_id is beyond the committed checkpoint")
            });
        }
        decoded_cursor
            .as_ref()
            .map_or(committed_through, |cursor| cursor.to)
    } else {
        requested_to.ok_or_else(|| validation_error(&request_id, "from and to are required"))?
    };
    if to < from || (time_basis == HistoryTimeBasis::EventTimestamp && to == from) {
        return Err(validation_error(&request_id, "invalid time range"));
    }

    let after = decoded_cursor
        .as_ref()
        .map(|cursor| {
            if cursor.device_id != device_id
                || cursor.time_basis != time_basis
                || cursor.from != from
                || cursor.to != to
                || cursor.keys != keys
                || match time_basis {
                    HistoryTimeBasis::EventTimestamp => {
                        cursor.after_time < from || cursor.after_time >= to
                    }
                    HistoryTimeBasis::CommitId => cursor.after_id <= from || cursor.after_id > to,
                }
            {
                return Err(invalid_cursor(&request_id));
            }
            Ok((cursor.after_time, cursor.after_id))
        })
        .transpose()?;
    let rows = state
        .telemetry_repository
        .history(
            &device_id,
            TelemetryHistoryRange {
                from,
                to,
                commit_order: time_basis == HistoryTimeBasis::CommitId,
            },
            after,
            &keys,
            limit + 1,
        )
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
                        time_basis,
                        from,
                        to,
                        after_time: if time_basis == HistoryTimeBasis::EventTimestamp {
                            row.ts
                        } else {
                            0
                        },
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
            let values = filtered_values(&row.values_json, &keys)
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
    let mut response = json!({ "items": items, "next_cursor": next_cursor });
    if time_basis == HistoryTimeBasis::CommitId {
        response["through_id"] = json!(to);
    }
    Ok(Json(response))
}

/// Returns the latest attributes received from a device.
pub async fn attributes(
    State(state): State<AppState>,
    AuthSession(_session): AuthSession,
    RequestId(request_id): RequestId,
    Path(device_id): Path<String>,
    query: Result<Query<AttributeQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(query) = query.map_err(|_| validation_error(&request_id, "invalid query"))?;
    let limit = query.limit.unwrap_or(50);
    if !(1..=200).contains(&limit) {
        return Err(validation_error(&request_id, "limit must be 1..=200"));
    }
    let after = query
        .cursor
        .as_deref()
        .map(|encoded| {
            let invalid = || {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "invalid attributes cursor".to_owned(),
                    request_id.clone(),
                )
            };
            if encoded.len() > 4096 {
                return Err(invalid());
            }
            let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
            let cursor: AttributeCursor = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if cursor.v != 1
                || cursor.device_id != device_id
                || cursor.key.is_empty()
                || cursor.key.len() > 64
            {
                return Err(invalid());
            }
            Ok(cursor.key)
        })
        .transpose()?;
    if !state
        .telemetry_repository
        .device_exists(&device_id)
        .await
        .map_err(|_| storage_unavailable(&request_id))?
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "device_not_found",
            "device not found".to_owned(),
            request_id,
        ));
    }
    let mut rows = state
        .attributes_repository
        .latest(&device_id, after.as_deref(), limit + 1)
        .await
        .map_err(|_| storage_unavailable(&request_id))?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_cursor = if has_more {
        rows.last()
            .map(|row| {
                serde_json::to_vec(&AttributeCursor {
                    v: 1,
                    device_id: device_id.clone(),
                    key: row.key.clone(),
                })
                .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
                .map_err(|error| internal_error(error, &request_id))
            })
            .transpose()?
    } else {
        None
    };
    let items = rows
        .into_iter()
        .map(|row| {
            let value: Value = serde_json::from_str(&row.value_json)
                .map_err(|error| internal_error(error, &request_id))?;
            Ok(json!({ "key": row.key, "value": value, "device_ts": row.device_ts, "updated_at": row.updated_at }))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
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
    if !state
        .telemetry_repository
        .device_exists(&device_id)
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
    let rows = state
        .telemetry_repository
        .latest(&device_id, &keys)
        .await
        .map_err(|error| internal_error(error, &request_id))?;
    let items: Vec<Value> = rows
        .into_iter()
        .map(|row| {
            let values = filtered_values(&row.values_json, &[])
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

fn decode_cursor(encoded: &str, request_id: &str) -> Result<Cursor, ApiError> {
    if encoded.len() > 4_096 {
        return Err(invalid_cursor(request_id));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| invalid_cursor(request_id))?;
    let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| invalid_cursor(request_id))?;
    if cursor.version != 1 || cursor.after_id <= 0 {
        return Err(invalid_cursor(request_id));
    }
    Ok(cursor)
}

fn invalid_cursor(request_id: &str) -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "invalid_cursor",
        "invalid telemetry cursor".to_owned(),
        request_id.to_owned(),
    )
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

fn storage_unavailable(request_id: &str) -> ApiError {
    ApiError::from_domain(
        DomainError::Unavailable("storage unavailable".to_owned()),
        request_id.to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_is_bound_to_range_and_key_filter() {
        let cursor = Cursor {
            version: 1,
            device_id: "device-a".to_owned(),
            time_basis: HistoryTimeBasis::EventTimestamp,
            from: 10,
            to: 20,
            after_time: 12,
            after_id: 3,
            keys: vec!["x".to_owned()],
        };
        let encoded = encode_cursor(&cursor, "request").unwrap();
        assert_eq!(decode_cursor(&encoded, "request").unwrap().after_time, 12);
        assert!(decode_cursor("not-a-cursor", "request").is_err());

        let legacy = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "version": 1,
                "device_id": "device-a",
                "from": 10,
                "to": 20,
                "after_ts": 12,
                "after_id": 3,
                "keys": ["x"]
            }))
            .unwrap(),
        );
        let legacy = decode_cursor(&legacy, "request").unwrap();
        assert_eq!(legacy.time_basis, HistoryTimeBasis::EventTimestamp);
        assert_eq!(legacy.after_time, 12);
    }
}
