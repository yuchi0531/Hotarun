pub mod config;
pub mod api;
pub mod stream;

use std::net::{IpAddr, Ipv6Addr, SocketAddr};

use axum::{
    body::Body,
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};

use crate::error::ApiError;

/// The listener is LAN-facing, so reject non-local/private peers before any
/// route handler can allocate a tuner. A missing ConnectInfo is allowed for
/// in-process tests; real TCP requests are served with ConnectInfo installed.
pub async fn access_control(request: Request<Body>, next: Next) -> Response {
    if let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<SocketAddr>>() {
        if !is_allowed_peer(peer.ip()) {
            return (
                StatusCode::FORBIDDEN,
                Json(ApiError::new(403, "client address is not allowed")),
            )
                .into_response();
        }
    }

    if let Some(reason) = invalid_referrer(&request) {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new(403, reason)),
        )
            .into_response();
    }
    next.run(request).await
}

fn is_allowed_peer(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || is_unique_local(ip) || is_link_local_v6(ip),
    }
}

fn is_unique_local(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xfe00) == 0xfc00
}

fn is_link_local_v6(ip: Ipv6Addr) -> bool {
    (ip.segments()[0] & 0xffc0) == 0xfe80
}

/// If a browser sends Origin/Referer, require its host to match Host. Empty
/// or absent headers remain compatible with curl and Mirakurun clients.
fn invalid_referrer(request: &Request<Body>) -> Option<String> {
    let values = [header::ORIGIN, header::REFERER];
    if !values.iter().any(|name| request.headers().contains_key(name)) {
        return None;
    }
    let Some(host_header) = request.headers().get(header::HOST) else {
        return Some("Origin/Referer requires a matching Host header".to_owned());
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
        let authority = value
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(value)
            .split(['/', '?', '#'])
            .next()
            .unwrap_or("");
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

    #[test]
    fn allows_loopback_and_private_lan_addresses() {
        assert!(is_allowed_peer("127.0.0.1".parse().unwrap()));
        assert!(is_allowed_peer("192.168.1.20".parse().unwrap()));
        assert!(is_allowed_peer("10.2.3.4".parse().unwrap()));
        assert!(!is_allowed_peer("8.8.8.8".parse().unwrap()));
    }

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
        assert!(invalid_referrer(&bad).is_some());
    }
}
