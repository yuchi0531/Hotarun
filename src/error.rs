use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;

/// 共通エラー形式 (§11): `{"code","reason","errors":[]}`。
#[derive(Debug, Clone, Serialize)]
pub struct ApiError {
    pub code: u16,
    pub reason: String,
    #[serde(default)]
    pub errors: Vec<String>,
}

impl ApiError {
    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            errors: Vec::new(),
        }
    }

    pub fn with_errors(code: u16, reason: impl Into<String>, errors: Vec<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            errors,
        }
    }

    pub fn not_found(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND.as_u16(), reason)
    }

    pub fn bad_request(reason: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST.as_u16(), reason)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let code = StatusCode::from_u16(self.code)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (code, Json(self)).into_response()
    }
}

/// 未実装の EPG 等は 404 JSON (§11)。501 にしない。
pub async fn fallback_404() -> impl IntoResponse {
    ApiError::not_found("not found")
}

/// 既存パスへの未対応メソッドは JSON 405 (§11)。
pub async fn method_not_allowed_405() -> impl IntoResponse {
    ApiError::new(
        StatusCode::METHOD_NOT_ALLOWED.as_u16(),
        "method not allowed",
    )
}
