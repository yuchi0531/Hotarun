pub mod config;
pub mod error;
pub mod routes;
pub mod scan;
pub mod tuner;

use std::sync::Arc;

use axum::Router;

use crate::config::AppState;
use crate::error::{fallback_404, method_not_allowed_405};

/// Build the complete HTTP application, including access checks and the
/// documented 404/405 fallbacks.  Keeping this in the library lets tests send
/// real requests through the same router as the daemon.
pub fn app(state: Arc<AppState>) -> Router {
    routes::config::router()
        .merge(routes::api::router())
        .merge(routes::stream::router())
        .merge(routes::scan::router())
        .merge(routes::ui::router())
        .fallback(fallback_404)
        .method_not_allowed_fallback(method_not_allowed_405)
        .layer(axum::middleware::from_fn(routes::normalize_rejections))
        .layer(axum::middleware::from_fn_with_state(Arc::clone(&state), routes::access_control))
        .with_state(state)
}
