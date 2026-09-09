use std::{fs::{self, OpenOptions}, io::{self, Write}, net::SocketAddr, path::PathBuf, sync::Arc};

use tracing_subscriber::{fmt, fmt::writer::MakeWriter, EnvFilter};

use hotarun::config::AppState;

#[cfg(unix)]
fn remove_existing_socket(path: &std::path::Path) -> Result<(), String> {
    use std::os::unix::fs::FileTypeExt;
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {
            fs::remove_file(path).map_err(|error| format!("remove old socket failed: {error}"))
        }
        Ok(_) => Err(format!("refusing to replace non-socket path: {}", path.display())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("inspect socket path failed: {error}")),
    }
}

#[cfg(unix)]
fn restrict_socket_mode(path: &std::path::Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path)
        .map_err(|error| format!("inspect bound socket failed: {error}"))?
        .permissions();
    permissions.set_mode(0o660);
    fs::set_permissions(path, permissions).map_err(|error| format!("set unix socket mode failed: {error}"))
}

const DEFAULT_CONFIG_DIR: &str = "/etc/hotarun";

fn arg_error(msg: &str) -> ! {
    eprintln!("error: {msg}");
    eprintln!("usage: hotarun [--config-dir DIR] [--port PORT]");
    std::process::exit(2);
}

fn tracing_filter_for(level: i8) -> &'static str {
    match level {
        -1 => "off",
        0 => "error",
        1 => "info",
        2 => "debug",
        _ => "trace",
    }
}

#[derive(Clone)]
struct LogFileWriter {
    path: PathBuf,
}

impl<'a> MakeWriter<'a> for LogFileWriter {
    type Writer = Box<dyn Write + Send>;

    fn make_writer(&'a self) -> Self::Writer {
        match OpenOptions::new().create(true).append(true).open(&self.path) {
            Ok(file) => Box::new(file),
            Err(error) => {
                eprintln!("failed to open {}: {error}", self.path.display());
                Box::new(io::stderr())
            }
        }
    }
}

fn init_tracing(level: i8) {
    let log_path = PathBuf::from("/var/log/hotarun/hotarun.log");
    let writer = match log_path.parent().and_then(|parent| fs::create_dir_all(parent).ok().map(|_| ())) {
        Some(()) => LogFileWriter { path: log_path },
        None => {
            eprintln!("failed to create /var/log/hotarun; logging to stderr");
            LogFileWriter { path: PathBuf::from("/dev/stderr") }
        }
    };
    fmt()
        .with_writer(writer)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(tracing_filter_for(level))),
        )
        .init();
}

fn parse_args() -> (String, Option<u16>) {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from<I>(args: I) -> (String, Option<u16>)
where
    I: IntoIterator<Item = String>,
{
    let mut config_dir = std::env::var("HOTARUN_CONFIG_DIR")
        .unwrap_or_else(|_| DEFAULT_CONFIG_DIR.to_owned());
    let mut port: Option<u16> = std::env::var("PORT").ok().and_then(|v| v.parse().ok());

    let mut it = args.into_iter().peekable();
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
                    Ok(p) => port = Some(p),
                    Err(_) => arg_error(&format!("invalid --port value: {v}")),
                },
                None => arg_error("missing value for --port"),
            },
            s if s.starts_with("--port=") => {
                let v = &s["--port=".len()..];
                match v.parse::<u16>() {
                    Ok(p) => port = Some(p),
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

fn effective_port(cli_port: Option<u16>, server_port: u16) -> u16 {
    cli_port.unwrap_or(server_port)
}

#[tokio::main]
async fn main() {
    let (config_dir, cli_port) = parse_args();
    let configured_level = hotarun::config::load_server_config(
        std::path::Path::new(&config_dir).join("server.yml").as_path(),
    )
    .log_level;
    init_tracing(configured_level);

    while match run_server(&config_dir, cli_port).await {
        Ok(restart) => restart,
        Err(error) => {
            tracing::error!(%error, "server stopped safely");
            false
        }
    } {}
}

async fn run_server(config_dir: &str, cli_port: Option<u16>) -> Result<bool, String> {
    let config_path = std::path::Path::new(config_dir);
    let loaded_server = hotarun::config::load_server_config_checked(&config_path.join("server.yml"))?;
    let port = effective_port(cli_port, loaded_server.port);
    tracing::info!(config_dir = %config_dir, port, socket = ?loaded_server.socket, "starting hotarun");

    // 起動時に読み込み・メモリ保持。ホットリロードなし (§6)。
    let state = Arc::new(AppState::load_from_dir(config_path));
    tracing::info!(channels = state.channels.len(), tuners = state.tuners.len(), "config loaded");
    let app = hotarun::app(Arc::clone(&state));

    if let Some(socket) = loaded_server.socket.as_deref() {
        #[cfg(unix)]
        {
            let socket_path = std::path::Path::new(socket);
            remove_existing_socket(socket_path)?;
            let listener = tokio::net::UnixListener::bind(socket_path)
                .map_err(|error| format!("bind unix socket failed: {error}"))?;
            restrict_socket_mode(socket_path)?;
            tracing::info!(%socket, mode = "0660", "listening");
            axum::serve(listener, app.into_make_service_with_connect_info::<hotarun::routes::UnixPeerCredentials>())
                .with_graceful_shutdown(shutdown_signal(Arc::clone(&state)))
                .await
                .map_err(|error| format!("serve failed: {error}"))?;
        }
        #[cfg(not(unix))]
        return Err("unix sockets are not supported on this platform".to_owned());
    } else {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|error| format!("bind failed: {error}"))?;
        tracing::info!(%addr, "listening");
        axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(shutdown_signal(Arc::clone(&state)))
            .await
            .map_err(|error| format!("serve failed: {error}"))?;
    }
    Ok(state.restart_requested.load(std::sync::atomic::Ordering::Acquire))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_cli_port_wins_over_server_port() {
        assert_eq!(effective_port(Some(1234), 5678), 1234);
        assert_eq!(effective_port(None, 5678), 5678);
    }

    #[test]
    fn cli_port_is_kept_as_an_option_until_server_config_is_loaded() {
        let (_, port) = parse_args_from(["--port".to_owned(), "1234".to_owned()]);
        assert_eq!(port, Some(1234));
    }

    #[cfg(unix)]
    #[test]
    fn socket_cleanup_refuses_regular_files_and_symlinks() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!("hotarun-socket-cleanup-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        let regular = root.join("regular");
        fs::write(&regular, b"do not delete").unwrap();
        assert!(remove_existing_socket(&regular).is_err());
        assert!(regular.exists());

        let target = root.join("target");
        fs::write(&target, b"keep").unwrap();
        let link = root.join("link");
        symlink(&target, &link).unwrap();
        assert!(remove_existing_socket(&link).is_err());
        assert!(link.exists());
        assert!(target.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn socket_cleanup_removes_only_an_existing_unix_socket() {
        use std::os::unix::fs::MetadataExt;

        let root = std::env::temp_dir().join(format!("hotarun-socket-only-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root).unwrap();
        let socket = root.join("hotarun.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        drop(listener);
        restrict_socket_mode(&socket).unwrap();
        assert_eq!(fs::symlink_metadata(&socket).unwrap().mode() & 0o777, 0o660);
        assert!(remove_existing_socket(&socket).is_ok());
        assert!(!socket.exists());
        let restarted = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        drop(restarted);
        fs::remove_dir_all(root).unwrap();
    }
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
        _ = state.restart.notified() => {
            tracing::info!("restart requested");
        },
    }
    tracing::info!("shutdown signal received");
    // Close fan-out senders and stop child processes before graceful serve
    // starts waiting for long-lived stream response bodies.
    state.manager.stop_all().await;
    state.decoders.stop_all().await;
}
