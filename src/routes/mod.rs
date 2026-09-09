pub mod config;
pub mod api;
pub mod stream;
pub mod scan;
pub mod ui;

#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub struct UnixPeerCredentials {
    pub uid: u32,
    pub gid: u32,
}

#[cfg(unix)]
impl UnixPeerCredentials {
    pub fn is_allowed(self) -> bool {
        // The socket is chmod 0660 at bind time.  Repeat the ownership check
        // at request time so a custom listener cannot accidentally turn a
        // missing/invalid peer credential into anonymous access.
        self.uid == unsafe { libc::geteuid() } || self.gid == unsafe { libc::getegid() }
    }
}

#[cfg(unix)]
impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, tokio::net::UnixListener>>
    for UnixPeerCredentials
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, tokio::net::UnixListener>) -> Self {
        let credentials = stream
            .io()
            .peer_cred()
            .map(|peer| UnixPeerCredentials {
                uid: peer.uid() as u32,
                gid: peer.gid() as u32,
            })
            .unwrap_or(UnixPeerCredentials {
                uid: u32::MAX,
                gid: u32::MAX,
            });
        credentials
    }
}

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};

use crate::error::ApiError;
use crate::config::AppState;

/// TCP peers are intentionally not filtered by client IP. Unix requests carry
/// peer credentials instead of a socket address; they are checked against the
/// process owner/group. A missing extension remains allowed only for in-process
/// Router tests. The real Unix listener always installs a credential extension,
/// using an invalid sentinel when the kernel lookup fails.
pub async fn access_control(
    State(state): State<std::sync::Arc<AppState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    #[cfg(unix)]
    let peer_allowed = if let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<UnixPeerCredentials>>() {
        peer.is_allowed()
    } else if state.server.socket.is_some() {
        // A TCP-shaped extension is not proof of a Unix peer. This also keeps
        // tests/custom listeners from bypassing credential validation.
        false
    } else {
        // Client IP/CIDR is not an access-control boundary. Keep accepting
        // arbitrary TCP peers, including requests with ConnectInfo attached.
        true
    };

    #[cfg(not(unix))]
    let peer_allowed = true;

    if !peer_allowed {
        let reason = "unix socket peer is not authorized";
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new(403, reason)),
        )
            .into_response();
    }

    if let Some(reason) = invalid_referrer(&request) {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new(403, reason)),
        )
            .into_response();
    }
    let origin = request.headers().get(header::ORIGIN).cloned();
    if let Some(reason) = invalid_origin(&request) {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new(403, reason)),
        )
            .into_response();
    }
    if request.method() == axum::http::Method::OPTIONS {
        let mut response = StatusCode::NO_CONTENT.into_response();
        add_cors_headers(&mut response, origin.as_ref());
        return response;
    }
    let mut response = next.run(request).await;
    add_cors_headers(&mut response, origin.as_ref());
    response
}

/// Axum's built-in extractor rejections are text responses.  The public API
/// promises the same JSON error envelope for malformed JSON and query
/// strings, so normalize only those otherwise-unhandled 400/422 responses.
pub async fn normalize_rejections(request: Request<Body>, next: Next) -> Response {
    if request.uri().query().is_some_and(|query| !valid_query_encoding(query)) {
        return ApiError::bad_request("invalid query string").into_response();
    }
    let response = next.run(request).await;
    if !matches!(response.status(), StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY)
        || response.headers().get(header::CONTENT_TYPE).is_some_and(|value| value.to_str().ok().is_some_and(|value| value.starts_with("application/json")))
    {
        return response;
    }
    ApiError::with_errors(400, "invalid request", vec!["request JSON or query is invalid".to_owned()]).into_response()
}

fn valid_query_encoding(query: &str) -> bool {
    let bytes = query.as_bytes();
    bytes.iter().enumerate().all(|(index, byte)| {
        *byte != b'%' || bytes.get(index + 1).is_some_and(|next| next.is_ascii_hexdigit())
            && bytes.get(index + 2).is_some_and(|next| next.is_ascii_hexdigit())
    })
}

fn add_cors_headers(response: &mut Response, origin: Option<&axum::http::HeaderValue>) {
    let Some(origin) = origin else { return };
    response.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
    response.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_CREDENTIALS, axum::http::HeaderValue::from_static("true"));
    response.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_METHODS, axum::http::HeaderValue::from_static("GET, HEAD, PUT, POST, DELETE, OPTIONS"));
    response.headers_mut().insert(header::ACCESS_CONTROL_ALLOW_HEADERS, axum::http::HeaderValue::from_static("Content-Type, X-Mirakurun-Priority"));
}

#[derive(Debug, PartialEq, Eq)]
struct OriginTuple {
    scheme: String,
    host: String,
    port: u16,
}

fn origin_tuple(value: &str) -> Option<OriginTuple> {
    let uri: axum::http::Uri = value.parse().ok()?;
    let scheme = uri.scheme_str()?.to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") || (!uri.path().is_empty() && uri.path() != "/") || uri.query().is_some() {
        return None;
    }
    let authority = uri.authority()?;
    let host = authority.host().to_ascii_lowercase();
    let port = authority.port_u16().unwrap_or_else(|| if scheme == "https" { 443 } else { 80 });
    Some(OriginTuple { scheme, host, port })
}

fn host_tuple(value: &str) -> Option<(String, Option<u16>)> {
    let authority: axum::http::uri::Authority = value.parse().ok()?;
    let host = authority.host().to_ascii_lowercase();
    Some((host, authority.port_u16()))
}

/// CORS and CSRF checks use the complete origin tuple.  This daemon serves
/// plain HTTP, so an HTTPS origin is not equivalent to the HTTP listener even
/// when the host text is identical.  No Origin means a curl/non-browser
/// request and remains allowed.
fn invalid_origin(request: &Request<Body>) -> Option<String> {
    let Some(value) = request.headers().get(header::ORIGIN) else { return None };
    let value = match value.to_str() {
        Ok(value) => value,
        Err(_) => return Some("invalid Origin header".to_owned()),
    };
    let Some(origin) = origin_tuple(value) else {
        return Some("invalid Origin header".to_owned());
    };
    if origin.scheme != "http" {
        return Some("Origin scheme is not allowed".to_owned());
    }
    let Some(host) = request.headers().get(header::HOST).and_then(|value| value.to_str().ok()).and_then(host_tuple) else {
        return Some("Origin requires a matching Host header".to_owned());
    };
    if origin.host != host.0 || origin.port != host.1.unwrap_or(80) {
        return Some("Origin does not match request host and port".to_owned());
    }
    None
}

/// If a browser sends Referer, require its host to match Host. Empty
/// or absent headers remain compatible with curl and Mirakurun clients.
fn invalid_referrer(request: &Request<Body>) -> Option<String> {
    let values = [header::REFERER];
    if !values.iter().any(|name| request.headers().contains_key(name)) {
        return None;
    }
    let Some(host_header) = request.headers().get(header::HOST) else {
        return Some("Referer requires a matching Host header".to_owned());
    };
    let Ok(host_header) = host_header.to_str() else {
        return Some("invalid Host header".to_owned());
    };
    let host = authority_host(host_header);
    let host = authority_host(host);
    for name in values {
        let Some(value) = request.headers().get(&name) else { continue };
        let Ok(value) = value.to_str() else {
            return Some(format!("invalid {name} header"));
        };
        let authority = value.split_once("://").map(|(_, rest)| rest).unwrap_or(value).split(['/', '?', '#']).next().unwrap_or("");
        let authority = authority.rsplit('@').next().unwrap_or(authority);
        let origin_host = authority_host(authority);
        if origin_host.is_empty() || !origin_host.eq_ignore_ascii_case(host) {
            return Some(format!("{name} host does not match request host"));
        }
    }
    None
}

fn authority_host(authority: &str) -> &str {
    let authority = authority.trim();
    if let Some(host) = authority.strip_prefix('[') {
        return host.split(']').next().unwrap_or("");
    }
    if authority.matches(':').count() > 1 {
        return authority;
    }
    authority
        .rsplit_once(':')
        .filter(|(_, port)| port.chars().all(|c| c.is_ascii_digit()))
        .map(|(host, _)| host)
        .unwrap_or(authority)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn checks_origin_against_request_host() {
        let ok = Request::builder()
            .header(header::HOST, "127.0.0.1:40772")
            .header(header::ORIGIN, "http://127.0.0.1:40772")
            .body(Body::empty())
            .unwrap();
        assert!(invalid_referrer(&ok).is_none());
        let bad = Request::builder()
            .header(header::HOST, "127.0.0.1:40772")
            .header(header::ORIGIN, "https://evil.example")
            .body(Body::empty())
            .unwrap();
        assert!(invalid_origin(&bad).is_some());
        let bad_scheme = Request::builder()
            .header(header::HOST, "127.0.0.1:40772")
            .header(header::ORIGIN, "https://127.0.0.1:40772")
            .body(Body::empty())
            .unwrap();
        assert!(invalid_origin(&bad_scheme).is_some());
        let bad_port = Request::builder()
            .header(header::HOST, "127.0.0.1:40772")
            .header(header::ORIGIN, "http://127.0.0.1:40773")
            .body(Body::empty())
            .unwrap();
        assert!(invalid_origin(&bad_port).is_some());
    }

    #[cfg(unix)]
    #[test]
    fn unix_peer_credentials_are_required_and_match_owner_or_group() {
        let owner = UnixPeerCredentials {
            uid: unsafe { libc::geteuid() } as u32,
            gid: u32::MAX,
        };
        assert!(owner.is_allowed());
        let group = UnixPeerCredentials {
            uid: u32::MAX,
            gid: unsafe { libc::getegid() } as u32,
        };
        assert!(group.is_allowed());
        assert!(!UnixPeerCredentials { uid: u32::MAX, gid: u32::MAX }.is_allowed());
    }

    #[tokio::test]
    async fn configured_unix_listener_rejects_requests_without_peer_credentials() {
        let mut state = AppState::default();
        state.server.socket = Some("/run/hotarun/hotarun.sock".to_owned());
        let response = crate::app(std::sync::Arc::new(state))
            .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
