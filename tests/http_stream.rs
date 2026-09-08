use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{to_bytes, Body, HttpBody};
use axum::http::{header, Method, Request, HeaderMap, StatusCode};
use hotarun::app;
use hotarun::config::{AppState, Channel, ChannelType, Tuner};
use tower::ServiceExt;

fn fixture() -> String {
    std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").expect("Rust fixture binary")
}

fn state() -> Arc<AppState> {
    Arc::new(AppState::from_lists(
        vec![
            channel("GR", "27", ChannelType::GR, None),
            channel("BS", "BS01_0", ChannelType::BS, None),
            channel("CS", "CS2", ChannelType::CS, None),
            channel("BS4K", "logical", ChannelType::BS4K, Some("tlv")),
        ],
        vec![Tuner {
            name: "fixture".to_owned(),
            types: vec![ChannelType::GR, ChannelType::BS, ChannelType::CS, ChannelType::BS4K],
            command: Some(format!("{} dispatch <channel>", fixture())),
            tlv_decoder: Some(format!("{} decoder-upper", fixture())),
            decoder: None,
            extra: HashMap::new(),
        }],
    ))
}

fn one_tuner_state(command: &str) -> Arc<AppState> {
    Arc::new(AppState::from_lists(
        vec![channel("BS4K", "logical", ChannelType::BS4K, Some("tlv"))],
        vec![Tuner {
            name: "fixture".to_owned(),
            types: vec![ChannelType::BS4K],
            command: Some(command.to_owned()),
            tlv_decoder: None,
            decoder: None,
            extra: HashMap::new(),
        }],
    ))
}

fn channel(name: &str, value: &str, channel_type: ChannelType, physical: Option<&str>) -> Channel {
    Channel {
        name: name.to_owned(),
        channel_type,
        channel: value.to_owned(),
        serviceId: Some(101),
        tunerChannels: physical.map(|physical| HashMap::from([("fixture".to_owned(), physical.to_owned())])),
        extra: HashMap::from([(
            String::from("networkId"),
            serde_json::json!(match channel_type {
                ChannelType::GR => 1,
                ChannelType::BS => 2,
                ChannelType::CS => 3,
                ChannelType::BS4K => 5,
                ChannelType::SKY => 6,
            }),
        )]),
    }
}

async fn open(state: Arc<AppState>, method: Method, uri: &str) -> axum::response::Response {
    app(state)
        .oneshot(Request::builder().method(method).uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn read_stream_response(state: Arc<AppState>, uri: &str) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = open(Arc::clone(&state), Method::GET, uri).await;
    let status = response.status();
    let headers = response.headers().clone();
    let mut body = response.into_body();
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)),
    )
    .await
    .expect("fixture stream did not produce a frame")
    .expect("fixture stream ended before first frame")
    .expect("fixture stream body error")
    .into_data()
    .expect("fixture stream frame was not data");
    state.manager.stop_all().await;
    let mut bytes = first.to_vec();
    bytes.extend_from_slice(&to_bytes(body, 16 * 1024 * 1024).await.unwrap());
    (status, headers, bytes)
}

#[tokio::test]
async fn real_http_router_covers_gr_bs_cs_and_bs4k_tlv_headers_decode_and_eof() {
    for (uri, is_tlv) in [
        ("/api/channels/GR/27/stream", false),
        ("/api/channels/BS/BS01_0/stream", false),
        ("/api/channels/CS/CS2/stream", false),
        ("/api/channels/BS4K/logical/stream?decode=0", true),
    ] {
        let state = state();
        let (status, headers, body) = read_stream_response(Arc::clone(&state), uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body:?}");
        assert_eq!(headers[header::CONTENT_TYPE], "video/MP2T");
        assert_eq!(headers["X-Mirakurun-Tuner-User-ID"], "0");
        if is_tlv {
            assert_eq!(body, b"TLV-raw-45328");
        } else {
            assert!(body.len() >= 188 && body.chunks_exact(188).all(|packet| packet[0] == 0x47));
        }
        state.manager.stop_all().await;
    }
}

#[tokio::test]
async fn real_http_router_filters_pat_pmt_for_gr_bs_cs_and_bypasses_bs4k_filtering() {
    for uri in [
        "/api/services/100101/stream",
        "/api/services/200101/stream",
        "/api/services/300101/stream",
    ] {
        let state = state();
        let (status, _, body) = read_stream_response(Arc::clone(&state), uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert!(body.chunks_exact(188).all(|packet| {
            let pid = (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2]);
            matches!(pid, 0 | 0x100 | 0x101)
        }), "{uri}: service filter leaked a PID");
    }

    let state = state();
    let (status, _, body) = read_stream_response(state, "/api/services/500101/stream").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"TLV-RAW-45328");
}

#[tokio::test]
async fn real_http_router_covers_bs4k_decoder_service_sharing_and_error_routes() {
    let state = state();
    let (status, _, body) = read_stream_response(Arc::clone(&state), "/api/channels/BS4K/logical/stream").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"TLV-RAW-45328");
    let service_state = self::state();
    let (status, _, body) = read_stream_response(service_state, "/api/services/500101/stream").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"TLV-RAW-45328");

    for (method, uri, expected) in [
        (Method::GET, "/api/config/channels", StatusCode::OK),
        (Method::GET, "/api/config/tuners", StatusCode::OK),
        (Method::GET, "/api/channels/BS4K/missing/stream", StatusCode::NOT_FOUND),
        (Method::GET, "/api/channels/BS4K/logical/stream?decode=2", StatusCode::BAD_REQUEST),
        (Method::GET, "/api/does-not-exist", StatusCode::NOT_FOUND),
        (Method::POST, "/api/config/channels", StatusCode::METHOD_NOT_ALLOWED),
    ] {
        let response = open(Arc::clone(&state), method, uri).await;
        assert_eq!(response.status(), expected, "{uri}");
        let _ = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
    }
    state.manager.stop_all().await;
}

#[tokio::test]
async fn real_http_router_covers_pat_filter_fanout_priority_takeover_release_and_shutdown() {
    let fixture = fixture();
    let filter_state = Arc::new(AppState::from_lists(
        vec![channel("GR", "27", ChannelType::GR, None)],
        vec![Tuner {
            name: "fixture".to_owned(),
            types: vec![ChannelType::GR],
            command: Some(format!("{fixture} dispatch <channel>")),
            tlv_decoder: None,
            decoder: None,
            extra: HashMap::new(),
        }],
    ));
    let response = open(Arc::clone(&filter_state), Method::GET, "/api/channels/GR/27/stream").await;
    let mut body_stream = response.into_body();
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body_stream).poll_frame(cx)),
    )
    .await
    .expect("PAT fixture did not produce a frame")
    .expect("PAT fixture ended before a frame")
    .expect("PAT fixture body error")
    .into_data()
    .unwrap();
    filter_state.manager.stop_all().await;
    let mut body = first.to_vec();
    body.extend_from_slice(&to_bytes(body_stream, 16 * 1024 * 1024).await.unwrap());
    assert!(body.chunks_exact(188).any(|packet| {
        let pid = (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2]);
        pid == 0
    }));
    filter_state.manager.stop_all().await;

    let fanout_state = one_tuner_state(&format!("{fixture} raw-hold"));
    let first = open(Arc::clone(&fanout_state), Method::GET, "/api/channels/BS4K/logical/stream").await;
    let second = open(Arc::clone(&fanout_state), Method::GET, "/api/channels/BS4K/logical/stream").await;
    assert_eq!(first.headers()["X-Mirakurun-Tuner-User-ID"], second.headers()["X-Mirakurun-Tuner-User-ID"]);
    fanout_state.manager.stop_all().await;
    let _ = to_bytes(first.into_body(), 16 * 1024).await.unwrap();
    let _ = to_bytes(second.into_body(), 16 * 1024).await.unwrap();
    assert_eq!(fanout_state.manager.use_count(0).await.unwrap(), 0);

    let takeover_state = Arc::new(AppState::from_lists(
        vec![
            channel("first", "first", ChannelType::BS4K, Some("phys-a")),
            channel("second", "second", ChannelType::BS4K, Some("phys-b")),
        ],
        vec![Tuner {
            name: "fixture".to_owned(),
            types: vec![ChannelType::BS4K],
            command: Some(format!("{fixture} raw-hold")),
            tlv_decoder: None,
            decoder: None,
            extra: HashMap::new(),
        }],
    ));
    let low = app(Arc::clone(&takeover_state))
        .oneshot(Request::builder()
            .method(Method::GET)
            .uri("/api/channels/BS4K/first/stream")
            .header("X-Mirakurun-Priority", "10")
            .body(Body::empty())
            .unwrap())
        .await
        .unwrap();
    assert_eq!(low.status(), StatusCode::OK);
    let high = Request::builder()
        .method(Method::GET)
        .uri("/api/channels/BS4K/second/stream")
        .header("X-Mirakurun-Priority", "20")
        .body(Body::empty())
        .unwrap();
    let high = app(Arc::clone(&takeover_state)).oneshot(high).await.unwrap();
    assert_eq!(high.status(), StatusCode::OK);
    assert_eq!(high.headers()["X-Mirakurun-Tuner-User-ID"], "0");
    takeover_state.manager.stop_all().await;
    let _ = to_bytes(low.into_body(), 16 * 1024).await.unwrap();
    let _ = to_bytes(high.into_body(), 16 * 1024).await.unwrap();

    let missing = Arc::new(AppState::from_lists(Vec::new(), Vec::new()));
    let response = open(missing, Method::GET, "/api/channels/BS4K/nope/stream").await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let unavailable = Arc::new(AppState::from_lists(
        vec![channel("BS4K", "logical", ChannelType::BS4K, None)],
        Vec::new(),
    ));
    let response = open(unavailable, Method::GET, "/api/channels/BS4K/logical/stream").await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let failed = one_tuner_state("/definitely/missing-hotarun-fixture");
    let response = open(failed, Method::GET, "/api/channels/BS4K/logical/stream").await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn real_http_router_respawns_a_stream_and_keeps_the_body_lease_valid() {
    let state = one_tuner_state(&format!("{} respawn", fixture()));
    let response = open(Arc::clone(&state), Method::GET, "/api/channels/BS4K/logical/stream").await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)),
    )
    .await
    .expect("initial HTTP stream frame timeout")
    .expect("initial HTTP stream ended")
    .expect("initial HTTP stream error")
    .into_data()
    .unwrap();
    let second = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)),
    )
    .await
    .expect("respawned HTTP stream frame timeout")
    .expect("respawned HTTP stream ended")
    .expect("respawned HTTP stream error")
    .into_data()
    .unwrap();
    assert_eq!(first.as_ref(), b"respawn-data");
    assert_eq!(second.as_ref(), b"respawn-data");
    state.manager.stop_all().await;
    let _ = to_bytes(body, 16 * 1024).await.unwrap();
    assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
}
