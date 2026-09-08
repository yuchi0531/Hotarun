use std::sync::Arc;

use axum::{Json, Router, extract::State, routing::get};

use crate::config::{AppState, Channel, Tuner};

/// GET /api/config/channels — メモリ保持した設定の素読み返却。
async fn list_channels(State(state): State<Arc<AppState>>) -> Json<Vec<Channel>> {
    Json(state.channels.clone())
}

/// GET /api/config/tuners — メモリ保持した設定の素読み返却。
async fn list_tuners(State(state): State<Arc<AppState>>) -> Json<Vec<Tuner>> {
    Json(state.tuners.clone())
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/config/channels", get(list_channels))
        .route("/api/config/tuners", get(list_tuners))
}
