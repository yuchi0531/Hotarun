use std::sync::Arc;

use axum::{body::Body, extract::{Query, State}, http::{header, StatusCode}, response::{IntoResponse, Response}, Json, Router, routing::put};
use serde::Deserialize;

use crate::{config::{AppState, ChannelType}, error::ApiError, scan::{ChannelScanStatus, ScanMode}};

#[derive(Debug, Deserialize)]
pub struct ScanQuery {
    #[serde(rename = "type")]
    pub channel_type: Option<String>,
    #[serde(default, alias = "dryRun")]
    pub dry_run: bool,
    #[serde(default)]
    pub refresh: bool,
    #[serde(default, alias = "async")]
    pub async_: bool,
    #[serde(default, rename = "serviceType", alias = "service_type")]
    pub service_type: Option<i64>,
    #[serde(default, rename = "scanMode", alias = "scan_mode")]
    pub scan_mode: Option<String>,
}

fn parse_type(value: Option<&str>) -> Result<Option<ChannelType>, ApiError> {
    value.map(|value| match value.to_ascii_uppercase().as_str() {
        "GR" => Ok(ChannelType::GR), "BS" => Ok(ChannelType::BS), "CS" => Ok(ChannelType::CS), "SKY" => Ok(ChannelType::SKY), "BS4K" => Ok(ChannelType::BS4K),
        _ => Err(ApiError::bad_request(format!("invalid scan type: {value}"))),
    }).transpose()
}

fn parse_scan_mode(value: Option<&str>) -> Result<Option<ScanMode>, ApiError> {
    value.map(|value| match value.to_ascii_lowercase().as_str() {
        "channel" => Ok(ScanMode::Channel),
        "service" => Ok(ScanMode::Service),
        _ => Err(ApiError::bad_request(format!("invalid scanMode: {value}"))),
    }).transpose()
}

async fn start_scan(State(state): State<Arc<AppState>>, Query(query): Query<ScanQuery>) -> Result<Response, ApiError> {
    let kind = parse_type(query.channel_type.as_deref())?;
    let scan_mode = parse_scan_mode(query.scan_mode.as_deref())?;
    let accepted = state.scan.begin(Arc::clone(&state), kind, query.dry_run, query.refresh, query.async_, query.service_type, scan_mode)
        .await.map_err(|error| {
            if error.contains("already") {
                ApiError::new(409, error)
            } else if error.contains("invalid") || error.contains("unavailable") {
                ApiError::new(400, error)
            } else if error.contains("no compatible")
                || error.contains("no available")
                || error.contains("all tuner scan attempts failed")
                || error.contains("no valid services")
                || error.contains("no valid TS")
                || error.contains("no data")
                || error.contains("channel scan timeout")
                || error.contains("scan timed out")
            {
                ApiError::new(503, error)
            } else {
                ApiError::new(500, error)
            }
        })?;
    if accepted.is_some() {
        return Response::builder().status(StatusCode::ACCEPTED).header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"code":202,"reason":"accepted","errors":[]}"#))
            .map_err(|error| ApiError::new(500, error.to_string()));
    }
    let result = state.scan.status().await;
    if result.status == "error" {
        let error = result.error.clone().unwrap_or_else(|| "scan failed".to_owned());
        return Err(ApiError::with_errors(
            if error.contains("timed out") || error.contains("timeout") { 503 } else { 500 },
            error.clone(),
            vec![error],
        ));
    }
    let body = serde_yaml::to_string(&result.channels)
        .map_err(|error| ApiError::new(500, error.to_string()))?;
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from(body)).map_err(|error| ApiError::new(500, error.to_string()))
}

async fn scan_status(State(state): State<Arc<AppState>>) -> Json<ChannelScanStatus> { Json(state.scan.status().await) }

async fn cancel_scan(State(state): State<Arc<AppState>>) -> Result<impl IntoResponse, ApiError> {
    match state.scan.cancel().await.map_err(|error| ApiError::new(500, error))? {
        true => Ok((StatusCode::PARTIAL_CONTENT, Json(serde_json::json!({"code": 206, "reason": "scan cancelled", "errors": []})))),
        false => Err(ApiError::not_found("no scan is running")),
    }
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/api/config/channels/scan", put(start_scan).get(scan_status).delete(cancel_scan))
}
