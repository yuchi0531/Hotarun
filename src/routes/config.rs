use std::sync::Arc;

use axum::{Json, Router, extract::State, http::StatusCode, routing::{get, post}};
use serde::Deserialize;

use crate::{config::{validate_channel_pairs, validate_service_item_ids, write_yaml_atomic, AppState, Channel, ServerConfig, Tuner}, error::ApiError};

/// GET /api/config/channels — メモリ保持した設定の素読み返却。
async fn list_channels(State(state): State<Arc<AppState>>) -> Json<Vec<Channel>> {
    Json(state.channels.clone())
}

/// GET /api/config/tuners — メモリ保持した設定の素読み返却。
async fn list_tuners(State(state): State<Arc<AppState>>) -> Json<Vec<Tuner>> {
    Json(state.tuners.clone())
}

async fn get_server(State(state): State<Arc<AppState>>) -> Json<ServerConfig> { Json(state.server.clone()) }

async fn save_server(State(state): State<Arc<AppState>>, Json(config): Json<ServerConfig>) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    if let Err(errors) = config.validate() { return Err(ApiError::with_errors(400, "invalid server configuration", errors)); }
    let directory = state.config_dir.as_deref().ok_or_else(|| ApiError::new(400, "configuration directory is unavailable"))?;
    write_yaml_atomic(&directory.join("server.yml"), &config).map_err(|error| ApiError::new(500, error))?;
    state.record_log(1, "server configuration saved; restart required").await;
    Ok((StatusCode::OK, Json(serde_json::json!({"code":200,"reason":"saved; restart required","errors":[],"restartRequired":true}))))
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ConfigPayload<T> { List(Vec<T>), Wrapped { items: Vec<T> } }

impl<T> ConfigPayload<T> { fn into_items(self) -> Vec<T> { match self { Self::List(items) | Self::Wrapped { items } => items } } }

async fn save_channels(State(state): State<Arc<AppState>>, Json(payload): Json<ConfigPayload<Channel>>) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let items = payload.into_items();
    if let Err(errors) = validate_channel_pairs(&items) {
        return Err(ApiError::with_errors(400, "duplicate channel identity", errors));
    }
    if let Err(errors) = validate_service_item_ids(&items) {
        let reason = if errors.iter().any(|error| error.contains("duplicate ServiceItemId")) {
            "duplicate ServiceItemId"
        } else {
            "invalid ServiceItemId"
        };
        return Err(ApiError::with_errors(400, reason, errors));
    }
    let _lock = state.channel_config_lock.lock().await;
    save_config(&state, "channels.yml", &items).await
}

async fn save_tuners(State(state): State<Arc<AppState>>, Json(payload): Json<ConfigPayload<Tuner>>) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    save_config(&state, "tuners.yml", &payload.into_items()).await
}

async fn save_config<T: serde::Serialize>(state: &AppState, name: &str, value: &[T]) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let directory = state.config_dir.as_deref().ok_or_else(|| ApiError::new(400, "configuration directory is unavailable"))?;
    write_yaml_atomic(&directory.join(name), &value).map_err(|error| ApiError::new(500, error))?;
    state.record_log(1, format!("{name} saved; restart required")).await;
    Ok((StatusCode::OK, Json(serde_json::json!({"code":200,"reason":"saved; restart required","errors":[],"restartRequired":true}))))
}

async fn restart(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    state.restart_requested.store(true, std::sync::atomic::Ordering::Release);
    state.restart.notify_waiters();
    state.record_log(1, "restart requested").await;
    Json(serde_json::json!({"code":202,"reason":"restart requested","errors":[]}))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/config/channels", get(list_channels).put(save_channels).post(save_channels))
        .route("/api/config/tuners", get(list_tuners).put(save_tuners))
        .route("/api/config/server", get(get_server).put(save_server))
        .route("/api/config/restart", post(restart))
}
