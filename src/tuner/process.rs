//! 外部チューナープロセス管理 (SPEC §8)。
//!
//! - `spawn(program, args)` のみ。shell 禁止。
//! - stdout / stderr を pipe 取得し PID を保持。stderr はログのみに使う。
//! - 停止は SIGTERM → 6秒 → SIGKILL。dvbv5系のみ即KILL。

use std::{
    collections::HashMap,
    process::Stdio,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::{sync::Notify, task::JoinHandle};

/// SIGTERM 後の猶予。超過で SIGKILL (§8)。
pub const STOP_GRACE: Duration = Duration::from_secs(6);

/// dvbv5-zap系か。真の場合のみ停止時即KILL (§8)。
pub fn is_immediate_kill_program(program: &str) -> bool {
    let base = program.rsplit('/').next().unwrap_or(program);
    base.contains("dvbv5")
}

/// 起動中のチューナープロセス。stdout は `Child` 内に保持し、
/// Phase3 の Stream Manager が `take_stdout()` で引き取る。
pub struct SpawnedTuner {
    child: Child,
    /// 起動直後の PID。`Child::id()` が取れなければ 0。
    pub pid: u32,
    /// 起動 program 名 (即KILL判定用に保持)。
    pub program: String,
    stderr_task: Option<JoinHandle<()>>,
}

/// 起動中の per-client decoder。チューナーとは別プロセスで、stdin/stdout
/// をストリーム配信のパイプとして使う。
pub struct SpawnedDecoder {
    child: Child,
    pub pid: u32,
    pub program: String,
    stderr_task: Option<JoinHandle<()>>,
}

/// Application-owned registry for per-request decoders.  The HTTP body owns
/// the pipes, while this registry retains ownership of the child itself so a
/// server shutdown never relies on stdin reaching EOF.
#[derive(Debug, Default)]
pub struct DecoderRegistry {
    next_id: AtomicU64,
    state: Mutex<DecoderRegistryState>,
    shutdown: Arc<Notify>,
}

#[derive(Debug, Default)]
struct DecoderRegistryState {
    closing: bool,
    entries: HashMap<u64, Arc<Mutex<Option<SpawnedDecoder>>>>,
}

#[derive(Debug, Clone)]
pub struct RegisteredDecoder {
    id: u64,
    registry: Arc<DecoderRegistry>,
    process: Arc<Mutex<Option<SpawnedDecoder>>>,
}

impl DecoderRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a decoder unless shutdown has started.  The closing check and
    /// insertion share one mutex with `stop_all`'s drain, so no decoder can
    /// appear after the shutdown drain.
    pub async fn register(
        self: &Arc<Self>,
        decoder: SpawnedDecoder,
    ) -> Result<RegisteredDecoder, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let process = Arc::new(Mutex::new(Some(decoder)));
        let accepted = {
            let mut state = self.state.lock().expect("decoder registry poisoned");
            if state.closing {
                false
            } else {
                state.entries.insert(id, Arc::clone(&process));
                true
            }
        };
        if !accepted {
            let decoder = {
                let mut process = process.lock().expect("decoder process poisoned");
                process.take()
            };
            if let Some(mut decoder) = decoder {
                stop_decoder(&mut decoder)
                    .await
                    .map_err(|error| format!("decoder shutdown failed: {error}"))?;
            }
            return Err("decoder registry is shutting down".to_owned());
        }
        Ok(RegisteredDecoder { id, registry: Arc::clone(self), process })
    }

    pub fn active_count(&self) -> usize {
        self.state
            .lock()
            .expect("decoder registry poisoned")
            .entries
            .len()
    }

    pub fn shutdown_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Kill and reap every decoder, including ones whose HTTP body is still
    /// blocked reading input or output.
    pub async fn stop_all(&self) {
        // Wake the pipe tasks first. Killing the children below then unblocks
        // any write/read that was already in progress.
        self.shutdown.notify_waiters();
        let processes = {
            let mut state = self.state.lock().expect("decoder registry poisoned");
            state.closing = true;
            state.entries.drain().map(|(_, process)| process).collect::<Vec<_>>()
        };
        for process in processes {
            let decoder = process.lock().expect("decoder process poisoned").take();
            if let Some(mut decoder) = decoder {
                if let Err(error) = stop_decoder(&mut decoder).await {
                    tracing::warn!(error = %error, "decoder shutdown failed");
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn test_fixture_command(mode: &str) -> String {
    let path = std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap_or_else(|_| {
        std::env::current_exe()
            .expect("test executable path")
            .parent()
            .and_then(|path| path.parent())
            .expect("target directory")
            .join("hotarun-test-fixture")
            .display()
            .to_string()
    });
    format!("{path} {mode}")
}

impl RegisteredDecoder {
    pub fn take_stdin(&self) -> Option<ChildStdin> {
        self.process.lock().expect("decoder process poisoned").as_mut()?.take_stdin()
    }

    pub fn take_stdout(&self) -> Option<ChildStdout> {
        self.process.lock().expect("decoder process poisoned").as_mut()?.take_stdout()
    }

    pub async fn stop(&self) {
        let decoder = self.process.lock().expect("decoder process poisoned").take();
        if let Some(mut decoder) = decoder {
            if let Err(error) = stop_decoder(&mut decoder).await {
                tracing::warn!(error = %error, "decoder cleanup failed");
            }
        }
        self.registry
            .state
            .lock()
            .expect("decoder registry poisoned")
            .entries
            .remove(&self.id);
    }
}

impl std::fmt::Debug for SpawnedDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnedDecoder")
            .field("pid", &self.pid)
            .field("program", &self.program)
            .finish()
    }
}

impl SpawnedDecoder {
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    fn abort_stderr(&mut self) {
        if let Some(h) = self.stderr_task.take() {
            h.abort();
        }
    }
}

impl std::fmt::Debug for SpawnedTuner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpawnedTuner")
            .field("pid", &self.pid)
            .field("program", &self.program)
            .finish()
    }
}

impl SpawnedTuner {
    /// stdout を Stream Manager へ引き渡す。
    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    /// 終了待ち (借用)。
    pub async fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }

    /// ノンブロッキング終了確認。
    pub fn try_wait(
        &mut self,
    ) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    fn abort_stderr(&mut self) {
        if let Some(h) = self.stderr_task.take() {
            h.abort();
        }
    }
}

/// `spawn(program, args)`。stdin=null / stdout+stderr=pipe / shell不使用。
/// 成功した時点で起動成功とみなす (初回バイト待ちなし・§8)。
/// `kill_on_drop(true)` で管理外への孤児化を防ぐ。明示停止は `stop_process` 経由。
pub async fn spawn_program(
    program: &str,
    args: &[String],
) -> std::io::Result<SpawnedTuner> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let pid = child.id().unwrap_or(0);
    let stderr_task = child
        .stderr
        .take()
        .map(|stderr| spawn_stderr_logger(stderr, program.to_owned()));
    tracing::info!(pid, program = %program, "tuner process spawned");
    Ok(SpawnedTuner {
        child,
        pid,
        program: program.to_owned(),
        stderr_task,
    })
}

/// `spawn(program, args)` で decoder を起動する。shell は使わず、stdin/stdout
/// はパイプ、stderr はチューナーと同じくログ専用とする。
pub async fn spawn_decoder_program(
    program: &str,
    args: &[String],
) -> std::io::Result<SpawnedDecoder> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let pid = child.id().unwrap_or(0);
    let stderr_task = child
        .stderr
        .take()
        .map(|stderr| spawn_stderr_logger(stderr, program.to_owned()));
    tracing::info!(pid, program = %program, "decoder process spawned");
    Ok(SpawnedDecoder {
        child,
        pid,
        program: program.to_owned(),
        stderr_task,
    })
}

/// decoder の stdin/stdout が閉じた後もプロセスを孤児にしないための後始末。
pub async fn stop_decoder(proc: &mut SpawnedDecoder) -> std::io::Result<()> {
    if proc.child.try_wait()?.is_none() {
        let _ = proc.child.kill().await;
    }
    let _ = proc.child.wait().await;
    proc.abort_stderr();
    Ok(())
}

/// stderr はストリームに混ぜずログのみに使う (§8)。
fn spawn_stderr_logger(stderr: ChildStderr, program: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            if line.is_empty() {
                continue;
            }
            tracing::warn!(program = %program, line = %line, "tuner stderr");
        }
    })
}

/// 停止: dvbv5系は即KILL、それ以外は SIGTERM → 6秒 → SIGKILL (§8)。
pub async fn stop_process(proc: &mut SpawnedTuner) -> std::io::Result<()> {
    // 既に終了済みなら何もしない。
    if let Some(_status) = proc.try_wait()? {
        proc.abort_stderr();
        return Ok(());
    }

    if is_immediate_kill_program(&proc.program) {
        tracing::info!(pid = proc.pid, program = %proc.program, "immediate SIGKILL (dvbv5)");
        proc.child.kill().await?;
        let _ = proc.child.wait().await;
        proc.abort_stderr();
        return Ok(());
    }

    // SIGTERM を直接送る (tokio の kill は SIGKILL のため libc を使用)。
    match proc.child.id() {
        Some(pid) => {
            let ret = unsafe { libc::kill(pid as i32, libc::SIGTERM) };
            if ret != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    let _ = proc.child.wait().await;
                    proc.abort_stderr();
                    return Ok(());
                }
                tracing::warn!(pid, program = %proc.program, error = %err, "SIGTERM failed");
            } else {
                tracing::info!(pid, program = %proc.program, "SIGTERM sent");
            }
        }
        None => {
            let _ = proc.child.wait().await;
            proc.abort_stderr();
            return Ok(());
        }
    }

    match tokio::time::timeout(STOP_GRACE, proc.child.wait()).await {
        Ok(_) => {
            proc.abort_stderr();
            Ok(())
        }
        Err(_) => {
            tracing::warn!(pid = proc.pid, program = %proc.program, "stop grace expired, SIGKILL");
            proc.child.kill().await?;
            let _ = proc.child.wait().await;
            proc.abort_stderr();
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_program() -> String {
        std::env::var("CARGO_BIN_EXE_hotarun-test-fixture").unwrap_or_else(|_| {
            std::env::current_exe()
                .expect("test executable path")
                .parent()
                .and_then(|path| path.parent())
                .expect("target directory")
                .join("hotarun-test-fixture")
                .display()
                .to_string()
        })
    }

    #[test]
    fn dvbv5_only_is_immediate_kill() {
        assert!(is_immediate_kill_program("dvbv5-zap"));
        assert!(is_immediate_kill_program("/usr/bin/dvbv5-zap"));
        assert!(!is_immediate_kill_program("recpt1"));
        assert!(!is_immediate_kill_program("/usr/bin/recdvb"));
    }

    #[tokio::test]
    async fn decoder_shutdown_serializes_drain_and_late_registration() {
        let registry = DecoderRegistry::new();
        let decoder = spawn_decoder_program(&fixture_program(), &["hold".to_owned()])
            .await
            .expect("fixture decoder");
        let ((), result) = tokio::join!(registry.stop_all(), registry.register(decoder));
        if let Ok(registered) = result {
            // Registration won the mutex before shutdown; stop_all must still
            // have drained and reaped it rather than leaving an entry behind.
            assert!(registered.take_stdin().is_none());
        }
        assert_eq!(registry.active_count(), 0);

        let decoder = spawn_decoder_program(&fixture_program(), &["hold".to_owned()])
            .await
            .expect("fixture decoder");
        let result = registry.register(decoder).await;
        assert!(result.is_err(), "registration after shutdown must be rejected");
        assert_eq!(registry.active_count(), 0);
    }
}
