use std::sync::Arc;
use std::{fs, time::{SystemTime, UNIX_EPOCH}};

use axum::{body::{to_bytes, Body, HttpBody}, extract::ConnectInfo, http::{Method, Request, StatusCode}};
use hotarun::{app, config::AppState};
use tower::ServiceExt;

#[cfg(unix)]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn admin_routes_expose_health_ui_and_scan_lifecycle() {
    let state = Arc::new(AppState::default());
    let response = app(Arc::clone(&state)).oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app(Arc::clone(&state)).oneshot(Request::builder().uri("/ui/").body(Body::empty()).unwrap()).await.unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
        panic!("scan failed: {}", String::from_utf8_lossy(&body));
    }
    assert!(to_bytes(response.into_body(), 64 * 1024).await.unwrap().starts_with(b"<!doctype html>"));

    let response = app(Arc::clone(&state)).oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=GR&dryRun=true").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "text/plain; charset=utf-8");

    let response = app(Arc::clone(&state)).oneshot(Request::builder().uri("/api/config/channels/scan").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(status.windows(b"status".len()).any(|window| window == b"status"));
}

#[tokio::test]
async fn admin_config_routes_persist_atomically_and_bound_log_history() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-admin-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("server.yml"), "maxLogHistory: 1\n").unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));

    let request = |uri: &'static str, body: &'static str| {
        Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    };
    let response = app(Arc::clone(&state))
        .oneshot(request("/api/config/server", r#"{"port":40772,"CIDR":["127.0.0.0/8"],"logLevel":1,"maxLogHistory":1}"#))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(fs::read_to_string(directory.join("server.yml"))
        .unwrap()
        .contains("CIDR:"));

    let response = app(Arc::clone(&state))
        .oneshot(request("/api/config/channels", "[]"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(fs::read_to_string(directory.join("channels.yml")).unwrap(), "[]\n");

    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().uri("/api/log").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let log = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let log: serde_json::Value = serde_json::from_slice(&log).unwrap();
    assert_eq!(log["maxLogHistory"], 1);
    assert_eq!(log["entries"].as_array().unwrap().len(), 1);

    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn server_config_rejects_blank_socket_without_writing_it() {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-server-socket-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    for socket in ["", "   ", "\t\n"] {
        let response = app(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/api/config/server")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(r#"{{"port":40772,"socket":{socket:?}}}"#)))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
        assert_eq!(body["code"], 400);
    }
    assert!(!directory.join("server.yml").exists());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn config_routes_allow_any_tcp_peer_and_reject_duplicate_channels() {
    let mut configured = AppState::default();
    configured.server.cidr = vec!["127.0.0.0/8".to_owned()];
    configured.server.admin_cidr = vec!["127.0.0.0/8".to_owned()];
    let state = Arc::new(configured);
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/api/config/channels")
        .header("content-type", "application/json")
        .body(Body::from(r#"[{"name":"a","type":"GR","channel":"27"},{"name":"b","type":"GR","channel":"27"}]"#))
        .unwrap();
    let response = app(Arc::clone(&state)).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let mut request = Request::builder().uri("/api/config/channels").body(Body::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo("192.168.1.2:1234".parse::<std::net::SocketAddr>().unwrap()));
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn malformed_json_and_query_use_the_common_error_envelope() {
    let state = Arc::new(AppState::default());
    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/server")
                .header("content-type", "application/json")
                .body(Body::from("{"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert_eq!(body["code"], 400);
    assert!(body["errors"].is_array());

    let response = app(state)
        .oneshot(Request::builder().uri("/api/channels?bad=%") .body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert_eq!(body["code"], 400);
    assert!(body["errors"].is_array());
}

#[cfg(unix)]
#[tokio::test]
async fn unix_socket_accepts_a_peer_with_the_server_uid() {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-unix-valid-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    let socket = directory.join("hotarun.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let mut state = AppState::default();
    state.server.socket = Some(socket.display().to_string());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app(Arc::new(state)).into_make_service_with_connect_info::<hotarun::routes::UnixPeerCredentials>(),
        )
        .await
        .unwrap();
    });

    let mut client = tokio::net::UnixStream::connect(&socket).await.unwrap();
    client
        .write_all(b"GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut buffer = [0_u8; 4096];
        loop {
            let count = client.read(&mut buffer).await.unwrap();
            if count == 0 {
                break;
            }
            response.extend_from_slice(&buffer[..count]);
            if response.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200"));
    server.abort();
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cors_requires_the_full_http_origin_tuple_and_keeps_valid_origin() {
    let state = Arc::new(AppState::default());
    let valid = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .uri("/api/health")
                .header("host", "127.0.0.1:40772")
                .header("origin", "http://127.0.0.1:40772")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(valid.status(), StatusCode::OK);
    assert_eq!(valid.headers()["access-control-allow-origin"], "http://127.0.0.1:40772");
    assert_eq!(valid.headers()["access-control-allow-credentials"], "true");

    for origin in ["https://127.0.0.1:40772", "http://127.0.0.1:40773", "http://evil.example:40772"] {
        let response = app(Arc::clone(&state))
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .header("host", "127.0.0.1:40772")
                    .header("origin", origin)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(body["code"], 403);
    }
}

#[tokio::test]
async fn scan_without_a_compatible_tuner_is_not_saved_as_success() {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-no-tuner-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let response = app(state)
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=BS4K")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(!directory.join("channels.yml").exists());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn channel_config_rejects_duplicate_service_item_ids_across_channel_pairs() {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-service-duplicate-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let request = Request::builder()
        .method(Method::PUT)
        .uri("/api/config/channels")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"[
                {"name":"first","type":"GR","channel":"27","serviceId":101,"networkId":7},
                {"name":"second","type":"GR","channel":"28","serviceId":101,"networkId":7}
            ]"#,
        ))
        .unwrap();
    let response = app(state).oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert_eq!(body["code"], 400);
    assert_eq!(body["reason"], "duplicate ServiceItemId");
    assert!(!directory.join("channels.yml").exists());
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn synchronous_scan_returns_worker_error_as_common_json() {
    let state = Arc::new(AppState::default());
    let response = app(state)
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=GR")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert_eq!(body["code"], 400);
    assert!(body["reason"].as_str().unwrap().contains("configuration directory"));
    assert!(body["errors"].is_array());
}

#[tokio::test]
async fn asynchronous_scan_exposes_error_and_log_in_status() {
    let state = Arc::new(AppState::default());
    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=GR&async=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let status = loop {
        let response = app(Arc::clone(&state))
            .oneshot(Request::builder().uri("/api/config/channels/scan").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
        if value["status"] == "error" {
            break value;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    };
    assert!(status["error"].as_str().unwrap().contains("configuration directory"));
    let response = app(state)
        .oneshot(Request::builder().uri("/api/log").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let log: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
    assert!(log["entries"].as_array().unwrap().iter().any(|entry| {
        entry["message"].as_str().unwrap_or_default().contains("scan failed")
    }));
}

#[tokio::test]
async fn scan_persists_one_channel_and_exposes_all_detected_services() {
    let fixture = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-services-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("tuners.yml"),
        format!(
            "- name: fixture\n  types: [GR]\n  command: '{} scan-dispatch <channel>'\n",
            fixture.replace('\'', "''")
        ),
    )
    .unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=GR")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        let body = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
        panic!("scan failed: {}", String::from_utf8_lossy(&body));
    }
    let channels: serde_yaml::Value =
        serde_yaml::from_slice(&to_bytes(response.into_body(), 256 * 1024).await.unwrap()).unwrap();
    assert_eq!(channels.as_sequence().unwrap().len(), 50);
    let first = channels.as_sequence().unwrap().iter().find(|channel| {
        channel["channel"].as_str() == Some("13")
    }).unwrap();
    assert_eq!(first["serviceId"], 101);
    assert_eq!(first["services"][0]["serviceId"], 202);

    // The fixture has no NIT, so the detector's fallback networkId is 0 and
    // the corresponding ServiceItemIds are the service IDs themselves.  Use
    // the single aggregated channel as a restarted in-memory snapshot; the
    // scan intentionally does not hot-reload the running AppState.
    let aggregated: hotarun::config::Channel = serde_yaml::from_value(first.clone()).unwrap();
    let reloaded = Arc::new(AppState::from_lists(vec![aggregated], Vec::new()));
    for service_id in [101, 202] {
        let response = app(Arc::clone(&reloaded))
            .oneshot(
                Request::builder()
                    .uri(format!("/api/services/{service_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cancelling_scan_reaps_the_scanner_process_and_lease() {
    let fixture = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-cancel-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(
        directory.join("tuners.yml"),
        format!(
            "- name: fixture\n  types: [GR]\n  command: '{} hold'\n",
            fixture.replace('\'', "''")
        ),
    )
    .unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=GR&async=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    for _ in 0..50 {
        if state.manager.pid(0).await.unwrap().is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(state.manager.pid(0).await.unwrap().is_some());

    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/api/config/channels/scan")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    state.manager.wait_for_idle(0).await.unwrap();
    assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    assert_eq!(state.manager.pid(0).await.unwrap(), None);
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn scan_shares_an_existing_stream_without_stopping_it() {
    let fixture = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap();
    let channel = hotarun::config::Channel {
        name: "GR".to_owned(),
        channel_type: hotarun::config::ChannelType::GR,
        channel: "27".to_owned(),
        serviceId: Some(101),
        tunerChannels: None,
        extra: std::collections::HashMap::new(),
    };
    let tuner = hotarun::config::Tuner {
        name: "fixture".to_owned(),
        types: vec![hotarun::config::ChannelType::GR],
        command: Some(format!("{fixture} repeat-hold")),
        tlv_decoder: None,
        decoder: None,
        extra: std::collections::HashMap::new(),
    };
    let state = Arc::new(AppState::from_lists(vec![channel], vec![tuner]));
    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().uri("/api/channels/GR/27/stream").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let first = tokio::time::timeout(std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)))
        .await.unwrap().unwrap().unwrap();
    assert!(first.into_data().is_ok());
    let pid = state.manager.pid(0).await.unwrap();
    assert_eq!(state.manager.use_count(0).await.unwrap(), 1);

    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=GR&async=true").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    for _ in 0..100 {
        if state.manager.use_count(0).await.unwrap() == 2 { break; }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(state.manager.pid(0).await.unwrap(), pid);
    assert_eq!(state.manager.use_count(0).await.unwrap(), 2);

    let second = tokio::time::timeout(std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut body).poll_frame(cx)))
        .await.unwrap().unwrap().unwrap();
    assert!(second.into_data().is_ok());

    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().method(Method::DELETE).uri("/api/config/channels/scan").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    for _ in 0..100 {
        if !state.scan.is_running().await { break; }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(state.manager.pid(0).await.unwrap(), pid);
    assert_eq!(state.manager.use_count(0).await.unwrap(), 1);
    state.manager.stop_all().await;
}

#[tokio::test]
async fn shared_scan_reads_multiple_fanout_chunks_before_detecting_services() {
    let fixture = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap();
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-shared-multipart-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("channels.yml"), "- name: existing\n  type: GR\n  channel: 27\n  serviceId: 101\n").unwrap();
    fs::write(
        directory.join("tuners.yml"),
        format!(
            "- name: fixture\n  types: [GR]\n  command: '{} multipart-hold <channel>'\n",
            fixture.replace('\'', "''")
        ),
    )
    .unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().uri("/api/channels/GR/27/stream").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let pid = state.manager.pid(0).await.unwrap();

    let response = app(Arc::clone(&state))
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri("/api/config/channels/scan?type=GR&refresh=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(state.manager.pid(0).await.unwrap(), pid);
    assert_eq!(state.manager.use_count(0).await.unwrap(), 1);
    let body = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
    let channels: serde_yaml::Value = serde_yaml::from_slice(&body).unwrap();
    assert_eq!(channels.as_sequence().unwrap().len(), 1);
    assert!(channels[0]["services"].as_sequence().is_some_and(|services| !services.is_empty()));

    state.manager.stop_all().await;
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn failed_scan_keeps_existing_channels_and_reports_sync_and_async_errors() {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-failed-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("channels.yml"), "- name: existing\n  type: BS4K\n  channel: BS4K45328\n  serviceId: 101\n").unwrap();
    fs::write(directory.join("tuners.yml"), "- name: broken\n  types: [BS4K]\n  command: /definitely/missing/hotarun-tuner <channel>\n").unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));

    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=BS4K&refresh=true").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(fs::read_to_string(directory.join("channels.yml")).unwrap().contains("existing"));

    let response = app(Arc::clone(&state))
        .oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=BS4K&refresh=true&async=true").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let status = loop {
        let response = app(Arc::clone(&state))
            .oneshot(Request::builder().uri("/api/config/channels/scan").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
        if status["status"] == "error" { break status; }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(status["error"].as_str().unwrap().contains("all tuner scan attempts failed"));
    assert!(fs::read_to_string(directory.join("channels.yml")).unwrap().contains("existing"));
    fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn valid_ts_without_services_is_a_scan_failure_and_preserves_channels() {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("hotarun-scan-empty-{suffix}"));
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("channels.yml"), "- name: existing\n  type: GR\n  channel: 27\n  serviceId: 101\n").unwrap();
    let fixture = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap();
    fs::write(directory.join("tuners.yml"), format!("- name: empty\n  types: [GR]\n  command: '{} valid-ts-empty <channel>'\n", fixture.replace('\'', "''"))).unwrap();
    let state = Arc::new(AppState::load_from_dir(&directory));
    let response = app(Arc::clone(&state)).oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=GR&refresh=true").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(fs::read_to_string(directory.join("channels.yml")).unwrap().contains("existing"));
    let response = app(Arc::clone(&state)).oneshot(Request::builder().method(Method::PUT).uri("/api/config/channels/scan?type=GR&refresh=true&async=true").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let status = loop {
        let response = app(Arc::clone(&state)).oneshot(Request::builder().uri("/api/config/channels/scan").body(Body::empty()).unwrap()).await.unwrap();
        let status: serde_json::Value = serde_json::from_slice(&to_bytes(response.into_body(), 16 * 1024).await.unwrap()).unwrap();
        if status["status"] == "error" { break status; }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(status["error"].as_str().unwrap().contains("no valid services"));
    assert!(fs::read_to_string(directory.join("channels.yml")).unwrap().contains("existing"));
    fs::remove_dir_all(directory).unwrap();
}
