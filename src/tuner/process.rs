//! 外部チューナープロセス管理 (SPEC §8)。
//!
//! - `spawn(program, args)` のみ。shell 禁止。
//! - stdout / stderr を pipe 取得し PID を保持。stderr はログのみに使う。
//! - 停止は SIGTERM → 6秒 → SIGKILL。dvbv5系のみ即KILL。

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

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

    #[test]
    fn dvbv5_only_is_immediate_kill() {
        assert!(is_immediate_kill_program("dvbv5-zap"));
        assert!(is_immediate_kill_program("/usr/bin/dvbv5-zap"));
        assert!(!is_immediate_kill_program("recpt1"));
        assert!(!is_immediate_kill_program("/usr/bin/recdvb"));
    }
}
