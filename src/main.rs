mod config;
mod error;
mod routes;
mod tuner;

use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use tracing_subscriber::{EnvFilter, fmt};

use config::AppState;
use error::{fallback_404, method_not_allowed_405};

const DEFAULT_PORT: u16 = 40772;
const DEFAULT_CONFIG_DIR: &str = "/etc/hotarun";

fn arg_error(msg: &str) -> ! {
    eprintln!("error: {msg}");
    eprintln!("usage: hotarun [--config-dir DIR] [--port PORT]");
    std::process::exit(2);
}

fn parse_args() -> (String, u16) {
    let mut config_dir = std::env::var("HOTARUN_CONFIG_DIR")
        .unwrap_or_else(|_| DEFAULT_CONFIG_DIR.to_owned());
    let mut port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_PORT);

    let mut it = std::env::args().skip(1).peekable();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config-dir" => match it.next() {
                Some(v) => config_dir = v,
                None => arg_error("missing value for --config-dir"),
            },
            s if s.starts_with("--config-dir=") => {
                let v = &s["--config-dir=".len()..];
                if v.is_empty() {
                    arg_error("missing value for --config-dir");
                }
                config_dir = v.to_owned();
            }
            "--port" | "-p" => match it.next() {
                Some(v) => match v.parse::<u16>() {
                    Ok(p) => port = p,
                    Err(_) => arg_error(&format!("invalid --port value: {v}")),
                },
                None => arg_error("missing value for --port"),
            },
            s if s.starts_with("--port=") => {
                let v = &s["--port=".len()..];
                match v.parse::<u16>() {
                    Ok(p) => port = p,
                    Err(_) => arg_error(&format!("invalid --port value: {v}")),
                }
            }
            "--help" | "-h" => {
                println!("hotarun [--config-dir DIR] [--port PORT]");
                std::process::exit(0);
            }
            "--" => {
                if let Some(v) = it.next() {
                    arg_error(&format!("unexpected argument: {v}"));
                }
                break;
            }
            s if s.starts_with('-') => {
                arg_error(&format!("unknown option: {s}"));
            }
            s => {
                arg_error(&format!("unexpected argument: {s}"));
            }
        }
    }
    (config_dir, port)
}

#[tokio::main]
async fn main() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let (config_dir, port) = parse_args();
    tracing::info!(config_dir = %config_dir, port, "starting hotarun");

    // 起動時に読み込み・メモリ保持。ホットリロードなし (§6)。
    let state = Arc::new(AppState::load_from_dir(std::path::Path::new(
        &config_dir,
    )));
    tracing::info!(
        channels = state.channels.len(),
        tuners = state.tuners.len(),
        "config loaded"
    );

    let app: Router = routes::config::router()
        .merge(routes::api::router())
        .merge(routes::stream::router())
        .fallback(fallback_404)
        .method_not_allowed_fallback(method_not_allowed_405)
        .layer(axum::middleware::from_fn(routes::access_control))
        .with_state(Arc::clone(&state));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind failed");
    tracing::info!(%addr, "listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
        .with_graceful_shutdown(shutdown_signal(Arc::clone(&state)))
        .await
        .expect("serve failed");
}

async fn shutdown_signal(state: Arc<AppState>) {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(error = %e, "ctrl_c handler failed");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "SIGTERM handler failed");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
    // Close fan-out senders and stop child processes before graceful serve
    // starts waiting for long-lived stream response bodies.
    state.manager.stop_all().await;
    state.decoders.stop_all().await;
}
