//! チューナースロット群の状態管理 (SPEC §8§9§10)。
//!
//! - 起動成功は spawn 成功で即時確定 (初回バイト待ちなし)。
//! - 停止は [`crate::tuner::process::stop_process`] (SIGTERM→6秒→SIGKILL)。
//! - release は正常 100ms / 異常 1000ms。
//! - 異常終了時に残存要求 (`use_count > 0`) があれば同一chで respawn。
//! - 3連続失敗で FAULT (要再起動・端末状態)。
//! - slot単位ロック: `TunerManager` 自体は `Arc` 共有し、各slotのみ
//!   短時間ロックする。状態遷移のみロック内で行い、`release_wait` の
//!   sleep や `stop_process` の wait (最大6秒) はロック解放後に行い、
//!   他slotを阻塞しない (Critical直列化の解消)。
//! - 確実なreaper/guard: 切断時の解放は `release_hint_sync` (try_lockのみの
//!   同期デクリメント) + `stop_if_idle` の非同期停止 + `spawn_watcher` の
//!   orphan回収で三重化し、fire-and-forget単独にしない。
//! - 共有/fan-out (§9§10): 同一物理chは `acquire` で共有し、per-clientへの
//!   配信はノンブロッキングfan-out (HWM 16MB相当、overflow時はchunkをdropし接続維持)。
//! - 常駐監視: `spawn_watcher` が spawn直後の wait→poll_exit/respawn を
//!   担当する。終了は `stop_all` (shutdown連携) で全slot停止する。

use std::fmt;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, OnceLock, Weak,
};
use std::time::Duration;

use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use super::command::build_tuner_command;
use super::process::{SpawnedTuner, spawn_program, stop_process};
use super::state::TunerState;
use crate::config::{Channel, ChannelType, Tuner};

/// 正常 release 後の再利用待ち (§8: 100ms)。
pub const RELEASE_FAST: Duration = Duration::from_millis(100);
/// 異常 release 後の再利用待ち (§8: 1000ms)。
pub const RELEASE_SLOW: Duration = Duration::from_millis(1000);
/// 最終クライアント切断後の物理ch維持時間 (§10)。
pub const IDLE_GRACE: Duration = Duration::from_secs(3);
/// FAULT になる連続失敗回数 (§8: 3連続)。
pub const MAX_CONSECUTIVE_ERRORS: u32 = 3;
/// 終了監視タスクの poll 間隔。
const WATCH_INTERVAL: Duration = Duration::from_millis(500);
/// ストリーム1チャンク (§10: 32KBノンブロッキングfan-out)。
pub const STREAM_CHUNK_SIZE: usize = 32 * 1024;
/// per-clientキュー長。32KB x 512 = 16MBでHWM 16MB相当 (§10)。
pub const STREAM_QUEUE_LEN: usize = 512;
/// Number of chunks rejected because a subscriber queue was full.
pub static STREAM_FANOUT_OVERFLOWS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static NEXT_PUMP_TOKEN: AtomicU64 = AtomicU64::new(1);

/// 起動・確保の失敗理由。Busy (再試行可) と Spawn失敗 (即500) を分離する。
/// 従来の `contains("spawn")` 文字列判定を廃止するためのenum化。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TunerError {
    /// 稼働中・競合。再試行後に尽きたら 503 (§9: 50 x 250ms)。
    Busy(String),
    /// spawn失敗 (command不正・実行失敗等)。即500。
    SpawnFailed(String),
    Disabled(String),
    Fault(String),
    NoCommand(String),
    NotFound(String),
    Other(String),
}

impl TunerError {
    /// 再試行可能なBusyか。
    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Busy(_))
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Busy(m)
            | Self::SpawnFailed(m)
            | Self::Disabled(m)
            | Self::Fault(m)
            | Self::NoCommand(m)
            | Self::NotFound(m)
            | Self::Other(m) => m,
        }
    }
}

impl fmt::Display for TunerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for TunerError {}

/// release 待ち。`after_error=true` で 1000ms、通常は 100ms。
/// 呼び出し側は slot ロックを保持せずに呼ぶこと (全slot阻塞防止)。
pub async fn release_wait(after_error: bool) {
    if after_error {
        tokio::time::sleep(RELEASE_SLOW).await;
    } else {
        tokio::time::sleep(RELEASE_FAST).await;
    }
}

fn command_usable(tuner: &Tuner) -> bool {
    tuner
        .command
        .as_ref()
        .is_some_and(|c| !c.trim().is_empty())
}

/// 1チューナー分の状態 + プロセス。
#[derive(Debug)]
pub struct TunerSlot {
    name: String,
    command_template: Option<String>,
    /// 対応放送種別 (§9 条件1)。
    types: Vec<ChannelType>,
    state: TunerState,
    pid: Option<u32>,
    current_channel: Option<String>,
    wanted_channel: Option<String>,
    use_count: usize,
    consecutive_errors: u32,
    /// 世代。release/respawnの間違ったプロセスを停止しないために使う。
    process_generation: u64,
    /// spawn 中の処理を stop と区別するための世代。
    start_generation: u64,
    process: Option<SpawnedTuner>,
    /// fan-out先 (§10)。各要素が1クライアントへのboundedキュー。
    /// pumpが `try_send` でノンブロッキング配信し、遅延者がいても切断しない。
    senders: Vec<mpsc::Sender<Vec<u8>>>,
    /// Scan subscriptions are closed when the current tuner process reaches
    /// EOF.  HTTP subscriptions remain open so the watcher can respawn the
    /// process and continue an existing stream.
    scan_senders: Vec<mpsc::Sender<Vec<u8>>>,
    /// A process is shareable only after stdout has been handed to its pump.
    /// This keeps HTTP acquisition out of the start/setup window.
    pump_ready: bool,
    /// stdout pump token.  Unlike `process_generation`, this changes for a
    /// respawn too: a logical lease can survive a respawn, but bytes buffered
    /// by the old OS process must not survive it.
    pump_token: u64,
    /// Priority of each active stream request.  The maximum is the priority
    /// of the physical stream and is used for takeover decisions.
    request_priorities: Vec<i32>,
    /// A respawn has reserved the current demand and is about to start.
    /// Releases clear this reservation so a last-moment disconnect cannot
    /// turn the respawn into an orphan process.
    respawn_pending: bool,
    /// 最終lease解放後の遅延停止をslot単位で世代管理する。
    idle_generation: u64,
    idle_scheduled: bool,
    idle_notify: Arc<Notify>,
}

impl TunerSlot {
    fn new(_index: usize, tuner: &Tuner) -> Self {
        let usable = command_usable(tuner);
        Self {
            name: tuner.name.clone(),
            command_template: tuner.command.clone(),
            types: tuner.types.clone(),
            state: if usable {
                TunerState::Idle
            } else {
                TunerState::Disabled
            },
            pid: None,
            current_channel: None,
            wanted_channel: None,
            use_count: 0,
            consecutive_errors: 0,
            process_generation: 0,
            start_generation: 0,
            process: None,
            senders: Vec::new(),
            scan_senders: Vec::new(),
            pump_ready: false,
            pump_token: 0,
            request_priorities: Vec::new(),
            respawn_pending: false,
            idle_generation: 0,
            idle_scheduled: false,
            idle_notify: Arc::new(Notify::new()),
        }
    }

    fn stream_priority(&self) -> i32 {
        self.request_priorities.iter().copied().max().unwrap_or(0)
    }

    fn cancel_idle_stop(&mut self) {
        if self.idle_scheduled {
            self.idle_scheduled = false;
            self.idle_generation = self.idle_generation.wrapping_add(1);
            self.idle_notify.notify_one();
        }
    }

    fn remove_request_priority(&mut self, priority: Option<i32>) {
        if let Some(priority) = priority {
            if let Some(pos) = self
                .request_priorities
                .iter()
                .position(|value| *value == priority)
            {
                self.request_priorities.remove(pos);
            }
        } else {
            self.request_priorities.pop();
        }
    }

    /// 失敗を1回記録し ERROR / FAULT を返す (sleepなし・テスト用にも使う)。
    fn record_failure(&mut self) -> TunerState {
        self.consecutive_errors = self.consecutive_errors.saturating_add(1);
        if self.consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
            self.state = TunerState::Fault;
            self.current_channel = None;
            self.wanted_channel = None;
            self.pid = None;
            TunerState::Fault
        } else {
            self.state = TunerState::Error;
            TunerState::Error
        }
    }

    fn record_success(&mut self) {
        self.consecutive_errors = 0;
    }
}

/// チューナー群。`Arc` で共有し、slot単位でロックする。
/// 長時間の sleep / wait 中はロックを保持しない。
#[derive(Debug)]
pub struct TunerManager {
    slots: Vec<tokio::sync::Mutex<TunerSlot>>,
    shutdown: AtomicBool,
    shutdown_epoch: AtomicU64,
    /// `new()` is retained for unit-level use; `shared()` installs this weak
    /// reference so idle timers can own the manager without a reference cycle.
    self_ref: OnceLock<Weak<TunerManager>>,
}

/// 将来の Scheduler / Stream Manager 用の共有型 (slot単位ロック)。
pub type SharedTunerManager = Arc<TunerManager>;

impl TunerManager {
    pub fn new(tuners: Vec<Tuner>) -> Self {
        let slots = tuners
            .iter()
            .enumerate()
            .map(|(i, t)| tokio::sync::Mutex::new(TunerSlot::new(i, t)))
            .collect();
        Self {
            slots,
            shutdown: AtomicBool::new(false),
            shutdown_epoch: AtomicU64::new(0),
            self_ref: OnceLock::new(),
        }
    }

    pub fn shared(tuners: Vec<Tuner>) -> SharedTunerManager {
        let manager = Arc::new(Self::new(tuners));
        let _ = manager.self_ref.set(Arc::downgrade(&manager));
        manager
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    fn slot_mutex(
        &self,
        index: usize,
    ) -> Result<&tokio::sync::Mutex<TunerSlot>, String> {
        self.slots
            .get(index)
            .ok_or_else(|| format!("no such tuner: {index}"))
    }

    pub async fn state(&self, index: usize) -> Result<TunerState, String> {
        Ok(self.slot_mutex(index)?.lock().await.state)
    }

    pub async fn pid(&self, index: usize) -> Result<Option<u32>, String> {
        Ok(self.slot_mutex(index)?.lock().await.pid)
    }

    pub async fn current_channel(
        &self,
        index: usize,
    ) -> Result<Option<String>, String> {
        Ok(self.slot_mutex(index)?.lock().await.current_channel.clone())
    }

    pub async fn consecutive_errors(
        &self,
        index: usize,
    ) -> Result<u32, String> {
        Ok(self.slot_mutex(index)?.lock().await.consecutive_errors)
    }

    pub async fn use_count(&self, index: usize) -> Result<usize, String> {
        Ok(self.slot_mutex(index)?.lock().await.use_count)
    }

    pub async fn name(&self, index: usize) -> Result<String, String> {
        Ok(self.slot_mutex(index)?.lock().await.name.clone())
    }

    /// 要再起動 (FAULT) か。
    pub async fn needs_restart(&self, index: usize) -> Result<bool, String> {
        Ok(self.slot_mutex(index)?.lock().await.state.needs_restart())
    }

    /// 新規受け付け可能 (IDLE) か。
    pub async fn is_usable(&self, index: usize) -> Result<bool, String> {
        Ok(self.slot_mutex(index)?.lock().await.state.can_accept())
    }

    /// 指定の放送種別に対応しているか (§9 条件1)。
    pub async fn supports_type(
        &self,
        index: usize,
        channel_type: ChannelType,
    ) -> Result<bool, String> {
        Ok(self.slot_mutex(index)?.lock().await.types.contains(&channel_type))
    }

    /// 空き (IDLE) かつ指定 type に対応し、確保可能なチューナーか
    /// (§9 条件1-3の簡易判定。条件4は物理ch解決が常に可能なため真)。
    pub async fn is_available_for(
        &self,
        index: usize,
        channel_type: ChannelType,
    ) -> bool {
        match self.slots.get(index) {
            Some(m) => {
                let s = m.lock().await;
                s.state.can_accept() && s.types.contains(&channel_type)
            }
            None => false,
        }
    }

    /// 稼働中 (確保済みで解放待ち) か。リトライ継続判定用。
    pub async fn is_busy(&self, index: usize) -> bool {
        match self.slots.get(index) {
            Some(m) => {
                let s = m.lock().await;
                s.state.is_active() || s.state == TunerState::Error
            }
            None => false,
        }
    }

    /// `Channel` から物理chを解決する (§9 `resolve_channel`)。
    pub async fn physical_channel_for(
        &self,
        index: usize,
        channel: &Channel,
    ) -> Result<String, String> {
        let s = self.slot_mutex(index)?.lock().await;
        if let Some(map) = channel.tunerChannels.as_ref() {
            if let Some(v) = map.get(&s.name) {
                return Ok(v.clone());
            }
        }
        // Legacy compatibility only: an explicit tuner mapping always wins.
        if let Some(physical) = channel.extra.get("physicalChannel").and_then(|value| value.as_str()) {
            return Ok(physical.to_owned());
        }
        // Mirakurun service-mode entries identify a service as
        // `<logical-channel>:<serviceId>`.  The service suffix is not a tuner
        // command channel; use the logical prefix when no explicit mapping is
        // configured.
        if let Some(service_id) = channel.serviceId {
            if let Some((logical, suffix)) = channel.channel.rsplit_once(':') {
                if suffix.parse::<i64>().ok() == Some(service_id) {
                    return Ok(logical.to_owned());
                }
            }
        }
        Ok(channel.channel.clone())
    }

    /// 手動有効/無効。稼働中の無効化は EBUSY扱いで拒否する。
    pub async fn set_disabled(
        &self,
        index: usize,
        disabled: bool,
    ) -> Result<TunerState, String> {
        let mut slot = self.slot_mutex(index)?.lock().await;
        if disabled {
            if slot.process.is_some() || slot.state.is_active() {
                return Err(format!("tuner {index} is busy"));
            }
            slot.state = TunerState::Disabled;
            slot.wanted_channel = None;
            slot.current_channel = None;
            slot.pid = None;
        } else {
            if slot.state == TunerState::Fault {
                return Err(format!("tuner {index} is FAULT, restart required"));
            }
            if slot.process.is_some() || slot.state.is_active() {
                return Err(format!("tuner {index} is busy"));
            }
            if !slot
                .command_template
                .as_ref()
                .is_some_and(|c| !c.trim().is_empty())
            {
                return Err(format!("tuner {index} has no command"));
            }
            slot.state = TunerState::Idle;
            slot.consecutive_errors = 0;
        }
        Ok(slot.state)
    }

    /// 起動。物理ch解決 **済み** の文字列を受け取り `<channel>` へ代入する。
    /// spawn 成功で即時成功 (初回バイト待ちなし)。
    /// 状態遷移のみ短時間ロック内で行い、長時間の sleep / wait は
    /// ロック解放後に行う (Critical直列化の解消)。
    /// 失敗は [`TunerError`] で返す。Busyのみ再試行可、Spawn失敗は即500。
    pub async fn start(
        &self,
        index: usize,
        physical_channel: &str,
    ) -> Result<u32, TunerError> {
        self.start_inner(index, physical_channel, false, 0)
            .await
            .map(|(pid, _)| pid)
    }

    /// Start a tuner for a stream request with its Mirakurun priority.
    pub async fn start_with_priority(
        &self,
        index: usize,
        physical_channel: &str,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        self.start_inner(index, physical_channel, false, priority)
            .await
    }

    /// 新規起動とrespawnでsendersの扱いを分ける。
    /// respawnでは既存clientの購読を維持し、新規起動だけ残骸を破棄する。
    async fn start_inner(
        &self,
        index: usize,
        physical_channel: &str,
        preserve_senders: bool,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        let (template, start_generation, start_epoch) = {
            let mut s = self
                .slot_mutex(index)
                .map_err(TunerError::NotFound)?
                .lock()
                .await;
            s.cancel_idle_stop();
            if self.shutdown.load(Ordering::Acquire) {
                return Err(TunerError::Other(
                    "tuner manager is shutting down".to_owned(),
                ));
            }
            match s.state {
                TunerState::Disabled => {
                    return Err(TunerError::Disabled(format!(
                        "tuner {index} is disabled"
                    )));
                }
                TunerState::Fault => {
                    return Err(TunerError::Fault(format!(
                        "tuner {index} is FAULT, restart required"
                    )));
                }
                TunerState::Starting
                | TunerState::Tuning
                | TunerState::Streaming
                | TunerState::Stopping => {
                    return Err(TunerError::Busy(format!(
                        "tuner {index} is busy ({})",
                        s.state
                    )));
                }
                TunerState::Idle | TunerState::Error => {}
            }
            if preserve_senders {
                if !s.respawn_pending
                    || s.use_count == 0
                    || s.wanted_channel.as_deref() != Some(physical_channel)
                {
                    s.respawn_pending = false;
                    if s.use_count == 0 && !s.state.is_terminal() {
                        s.current_channel = None;
                        s.wanted_channel = None;
                        s.senders.clear();
                        s.scan_senders.clear();
                        s.pump_ready = false;
                        s.state = TunerState::Idle;
                    }
                    return Err(TunerError::Busy(format!(
                        "tuner {index} has no respawn demand"
                    )));
                }
                s.respawn_pending = false;
            }
            let template = s.command_template.clone().ok_or_else(|| {
                TunerError::NoCommand(format!("tuner {index} has no command"))
            })?;
            if template.trim().is_empty() {
                return Err(TunerError::NoCommand(format!(
                    "tuner {index} has no command"
                )));
            }
            // 状態遷移のみロック内。
            s.start_generation = s.start_generation.wrapping_add(1);
            s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
            let start_generation = s.start_generation;
            let start_epoch = self.shutdown_epoch.load(Ordering::Acquire);
            s.state = TunerState::Starting;
            (template, start_generation, start_epoch)
        };

        let (program, args) = match build_tuner_command(&template, physical_channel) {
            Ok(v) => v,
            Err(e) => {
                // 構築失敗は Idle に戻す (Tuning のまま残さない)。
                if let Ok(m) = self.slot_mutex(index) {
                    let mut s = m.lock().await;
                    if s.state == TunerState::Starting
                        && s.start_generation == start_generation
                    {
                        s.state = TunerState::Idle;
                    }
                }
                return Err(TunerError::Other(e));
            }
        };
        // TUNING はチャンネル確定〜配信開始の極短区間。spawn成功で即STREAMING。
        {
            let mut s = self
                .slot_mutex(index)
                .map_err(TunerError::NotFound)?
                .lock()
                .await;
            if self.shutdown.load(Ordering::Acquire)
                || s.state != TunerState::Starting
                || s.start_generation != start_generation
            {
                return Err(TunerError::Disabled(format!(
                    "tuner {index} start was invalidated"
                )));
            }
            s.state = TunerState::Tuning;
        }

        // spawn 自体はロック外 (短時間だが他slotを阻塞させない)。
        match spawn_program(&program, &args).await {
            Ok(proc) => {
                let pid = proc.pid;
                let mut s = self
                    .slot_mutex(index)
                    .map_err(TunerError::NotFound)?
                    .lock()
                    .await;
                let valid = !self.shutdown.load(Ordering::Acquire)
                    && self.shutdown_epoch.load(Ordering::Acquire) == start_epoch
                    && s.state == TunerState::Tuning
                    && s.start_generation == start_generation;
                if !valid {
                    drop(s);
                    let mut proc = proc;
                    if let Err(e) = stop_process(&mut proc).await {
                        tracing::warn!(index, pid, error = %e, "invalidated tuner spawn cleanup failed");
                    }
                    return Err(TunerError::Other(
                        "tuner start completed after shutdown or stop".to_owned(),
                    ));
                }
                s.record_success();
                s.process = Some(proc);
                s.pid = Some(pid);
                // A respawn replaces the OS process but keeps the logical
                // request leases.  The generation returned to HTTP bodies is
                // therefore advanced only for a fresh/takeover stream; old
                // bodies must still be able to release after respawn.
                if !preserve_senders {
                    s.process_generation = s.process_generation.wrapping_add(1);
                }
                s.current_channel = Some(physical_channel.to_owned());
                s.wanted_channel = Some(physical_channel.to_owned());
                if !preserve_senders {
                    s.use_count = s.use_count.saturating_add(1);
                }
                if !preserve_senders {
                    s.request_priorities.clear();
                    s.request_priorities.push(priority);
                }
                s.state = TunerState::Streaming;
                if !preserve_senders {
                    // 新規起動では前回の残骸チャネルを捨て、次に来る
                    // `create_subscription` から作り直す。
                    s.senders.clear();
                    s.scan_senders.clear();
                    s.pump_ready = false;
                }
                tracing::info!(index, pid, channel = %physical_channel, "tuner streaming");
                Ok((pid, s.process_generation))
            }
            Err(e) => {
                tracing::warn!(index, error = %e, "tuner spawn failed");
                let fault = {
                    // slot取得失敗時は Error 扱いで進む (NotFoundは起きない想定)。
                    match self.slot_mutex(index) {
                        Ok(m) => {
                            let mut s = m.lock().await;
                            s.process = None;
                            s.pid = None;
                            s.senders.clear();
                            s.scan_senders.clear();
                            s.pump_ready = false;
                            s.record_failure()
                        }
                        Err(_) => TunerState::Error,
                    }
                };
                // 異常系 release (1000ms) はロック外で待つ。
                // FAULT は端末状態のまま残す。
                release_wait(true).await;
                if fault != TunerState::Fault {
                    if let Ok(m) = self.slot_mutex(index) {
                        let mut s = m.lock().await;
                        if s.state == TunerState::Error {
                            s.state = TunerState::Idle;
                        }
                    }
                }
                if fault == TunerState::Fault {
                    Err(TunerError::Fault(format!(
                        "tuner {index} spawn failed ({e}); FAULT, restart required"
                    )))
                } else {
                    Err(TunerError::SpawnFailed(format!(
                        "tuner {index} spawn failed: {e}"
                    )))
                }
            }
        }
    }

    /// `start` 後に常駐監視タスクを付ける。respawn は既存 watcher が
    /// 引き続き担当するため二重起動しても poll が重なるだけである。
    pub async fn start_monitored(
        self: &Arc<Self>,
        index: usize,
        physical_channel: &str,
    ) -> Result<u32, TunerError> {
        let pid = self.start(index, physical_channel).await?;
        self.spawn_watcher(index);
        Ok(pid)
    }

    /// Start and return the process generation used to guard its stream lease.
    pub async fn start_monitored_with_priority(
        self: &Arc<Self>,
        index: usize,
        physical_channel: &str,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        let (pid, generation) = self.start_with_priority(index, physical_channel, priority).await?;
        self.spawn_watcher(index);
        Ok((pid, generation))
    }

    /// 同一chなら共有 (use_count++)、空きなら起動。別ch稼働中は EBUSY。
    /// 共有判定は呼び出し側で `is_sharing_candidate` + `acquire` の順に行い、
    /// 実際のper-client配信は `create_subscription` のfan-outを使う。
    pub async fn acquire(
        &self,
        index: usize,
        physical_channel: &str,
    ) -> Result<u32, TunerError> {
        self.acquire_with_priority(index, physical_channel, 0)
            .await
            .map(|(pid, _)| pid)
    }

    /// Share an existing physical stream or start a new one, recording the
    /// request priority and returning the process generation for the lease.
    pub async fn acquire_with_priority(
        &self,
        index: usize,
        physical_channel: &str,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        loop {
            let mut s = self
                .slot_mutex(index)
                .map_err(TunerError::NotFound)?
                .lock()
                .await;
            if s.state == TunerState::Stopping {
                let notified = Arc::clone(&s.idle_notify).notified_owned();
                drop(s);
                notified.await;
                continue;
            }
            if s.state == TunerState::Streaming
                && s.current_channel.as_deref() == Some(physical_channel)
                && s.process.is_some()
            {
                s.cancel_idle_stop();
                s.use_count = s.use_count.saturating_add(1);
                s.request_priorities.push(priority);
                s.wanted_channel = Some(physical_channel.to_owned());
                return Ok((s.pid.unwrap_or(0), s.process_generation));
            }
            break;
        }
        self.start_with_priority(index, physical_channel, priority)
            .await
    }

    /// HTTP streams may share only after stdout is attached to the fan-out
    /// pump.  A scan can otherwise expose a Streaming process during setup
    /// with no reader for the HTTP subscriber to consume.
    pub async fn acquire_http_with_priority(
        &self,
        index: usize,
        physical_channel: &str,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        let mut s = self
            .slot_mutex(index)
            .map_err(TunerError::NotFound)?
            .lock()
            .await;
        if s.state == TunerState::Streaming
            && s.current_channel.as_deref() == Some(physical_channel)
            && s.process.is_some()
            && s.pump_ready
        {
            s.cancel_idle_stop();
            s.use_count = s.use_count.saturating_add(1);
            s.request_priorities.push(priority);
            s.wanted_channel = Some(physical_channel.to_owned());
            return Ok((s.pid.unwrap_or(0), s.process_generation));
        }
        if s.state != TunerState::Idle && s.state != TunerState::Error {
            return Err(TunerError::Busy(format!("tuner {index} is busy")));
        }
        drop(s);
        self.start_with_priority(index, physical_channel, priority).await
    }

    /// Atomically evict a lower-priority stream and start the requested
    /// physical channel in the same slot.  The old lease is invalidated by
    /// the next process generation before the process is stopped.
    pub async fn takeover_with_priority(
        self: &Arc<Self>,
        index: usize,
        physical_channel: &str,
        priority: i32,
    ) -> Result<(u32, u64), TunerError> {
        let mut proc = {
            let mut s = self
                .slot_mutex(index)
                .map_err(TunerError::NotFound)?
                .lock()
                .await;
            s.cancel_idle_stop();
            if s.state != TunerState::Streaming
                || s.process.is_none()
                || s.current_channel.as_deref() == Some(physical_channel)
                || priority <= s.stream_priority()
            {
                return Err(TunerError::Busy(format!(
                    "tuner {index} is not eligible for priority takeover"
                )));
            }
            s.start_generation = s.start_generation.wrapping_add(1);
            s.state = TunerState::Stopping;
            s.use_count = 0;
            s.request_priorities.clear();
            s.wanted_channel = None;
            s.current_channel = None;
            s.pid = None;
            s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
            s.senders.clear();
            s.scan_senders.clear();
            s.pump_ready = false;
            s.process.take()
        };

        if let Some(ref mut process) = proc {
            if let Err(error) = stop_process(process).await {
                tracing::warn!(index, %error, "priority takeover stop failed");
            }
        }
        release_wait(false).await;
        {
            let mut s = self.slot_mutex(index).map_err(TunerError::Other)?.lock().await;
            if s.state == TunerState::Stopping && s.process.is_none() {
                s.state = TunerState::Idle;
            }
        }
        self.start_monitored_with_priority(index, physical_channel, priority)
            .await
    }

    /// 明示停止。SIGTERM→6秒→SIGKILL と 100ms release はロック外で行う。
    /// 状態遷移のみロック内。停止時はfan-out先も閉じて全clientへEOFさせる。
    pub async fn stop(&self, index: usize) -> Result<(), String> {
        // process を短時間ロックで取り出し、以降はロック外で停止する。
        let mut proc: SpawnedTuner = {
            let mut s = self.slot_mutex(index)?.lock().await;
            s.cancel_idle_stop();
            s.start_generation = s.start_generation.wrapping_add(1);
            s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
            if (s.state == TunerState::Disabled || s.state == TunerState::Fault)
                && s.process.is_none()
            {
                s.senders.clear();
                return Ok(());
            }
            match s.process.take() {
                None => {
                    if !s.state.is_terminal() {
                        s.state = TunerState::Idle;
                    }
                    s.pid = None;
                    s.current_channel = None;
                    s.wanted_channel = None;
                    s.use_count = 0;
                    s.request_priorities.clear();
                    s.senders.clear();
                    s.scan_senders.clear();
                    s.pump_ready = false;
                    return Ok(());
                }
                Some(p) => {
                    s.state = TunerState::Stopping;
                    // 購読者は停止開始時点でEOFにする。プロセスの終了待ち
                    // (最大6秒) の間もHTTP serverがdrainできるようにする。
                    s.senders.clear();
                    s.scan_senders.clear();
                    s.pump_ready = false;
                    s.use_count = 0;
                    s.request_priorities.clear();
                    p
                }
            }
        };
        // 停止 wait (最大6秒) はロック外。失敗しても IDLE へ進む。
        if let Err(e) = stop_process(&mut proc).await {
            tracing::warn!(index, error = %e, "tuner stop failed");
        }
        release_wait(false).await;
        let mut s = self.slot_mutex(index)?.lock().await;
        s.pid = None;
        s.current_channel = None;
        s.wanted_channel = None;
        s.use_count = 0;
        s.request_priorities.clear();
        s.senders.clear();
        s.scan_senders.clear();
        s.pump_ready = false;
        if !s.state.is_terminal() {
            s.state = TunerState::Idle;
        }
        s.idle_notify.notify_waiters();
        tracing::info!(index, "tuner stopped");
        Ok(())
    }

    /// 需要がなく、かつ指定したプロセス世代・チャンネルがまだ稼働中なら、
    /// 同じslotロックの中でプロセスを取り出して停止状態にする。
    /// checkとtakeの間にacquireが割り込めないことが重要。
    async fn take_idle_process(
        &self,
        index: usize,
        expected_generation: Option<u64>,
        expected_channel: Option<&str>,
    ) -> Result<Option<SpawnedTuner>, String> {
        let mut s = self.slot_mutex(index)?.lock().await;
        let Some(proc_generation) = s.process.as_ref().map(|_| s.process_generation) else {
            return Ok(None);
        };
        if s.use_count != 0
            || expected_generation.is_some_and(|generation| generation != proc_generation)
            || expected_channel.is_some_and(|channel| {
                s.current_channel.as_deref() != Some(channel)
            })
        {
            return Ok(None);
        }
        s.idle_scheduled = false;
        s.idle_generation = s.idle_generation.wrapping_add(1);
        s.idle_notify.notify_one();
        s.state = TunerState::Stopping;
        s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
        s.senders.clear();
        Ok(s.process.take())
    }

    async fn finish_stopped_process(
        &self,
        index: usize,
        mut proc: SpawnedTuner,
    ) -> Result<(), String> {
        if let Err(e) = stop_process(&mut proc).await {
            tracing::warn!(index, error = %e, "tuner stop failed");
        }
        release_wait(false).await;
        let mut s = self.slot_mutex(index)?.lock().await;
        s.pid = None;
        s.current_channel = None;
        s.wanted_channel = None;
        s.use_count = 0;
        s.request_priorities.clear();
        s.senders.clear();
        if !s.state.is_terminal() {
            s.state = TunerState::Idle;
        }
        s.idle_notify.notify_waiters();
        tracing::info!(index, "tuner stopped");
        Ok(())
    }

    /// 全slot停止 (shutdown連携用)。各slot順に止める。
    pub async fn stop_all(&self) {
        // Invalidate starts before waiting for any process. A spawn completion
        // that races with shutdown must reap its child instead of registering it.
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_epoch.fetch_add(1, Ordering::AcqRel);
        for idx in 0..self.len() {
            if let Err(e) = self.stop(idx).await {
                tracing::warn!(tuner = idx, error = %e, "stop_all failed");
            }
        }
    }

    /// 要求を1つ手放す。残存要求がなければ停止する。
    /// 最終要求のプロセスは3秒のidle猶予後に停止する。
    pub async fn release(&self, index: usize) -> Result<(), String> {
        let generation = self.slot_mutex(index)?.lock().await.process_generation;
        self.release_lease(index, generation, None).await
    }

    /// Release one request only if it still belongs to the same process
    /// generation. This makes a body from a preempted stream harmless when it
    /// is dropped after the slot has already been reused.
    pub async fn release_lease(
        &self,
        index: usize,
        generation: u64,
        priority: Option<i32>,
    ) -> Result<(), String> {
        let (process_generation, channel) = {
            let mut s = self.slot_mutex(index)?.lock().await;
            if s.process_generation != generation {
                return Ok(());
            }
            s.use_count = s.use_count.saturating_sub(1);
            s.remove_request_priority(priority);
            if s.use_count > 0 {
                // 残存要求あり: 同一ch継続のためプロセスは残す。
                return Ok(());
            }
            s.wanted_channel = None;
            s.respawn_pending = false;
            if s.process.is_none() {
                s.current_channel = None;
            }
            if matches!(s.state, TunerState::Starting | TunerState::Tuning) {
                s.respawn_pending = false;
                s.start_generation = s.start_generation.wrapping_add(1);
                s.state = TunerState::Idle;
            }
            let process_generation = s.process.as_ref().map(|_| s.process_generation);
            if process_generation.is_some() {
                // Reserve the grace period before releasing the slot lock. The
                // watcher must not observe the short gap and reap the process
                // before the timer task is installed.
                s.idle_generation = s.idle_generation.wrapping_add(1);
                s.idle_scheduled = true;
            }
            (process_generation, s.current_channel.clone())
        };
        if let Some(process_generation) = process_generation {
            if self.self_ref.get().and_then(Weak::upgrade).is_some() {
                self.schedule_idle_stop(index, process_generation, channel.as_deref()).await;
                return Ok(());
            }
        }
        if let Some(proc) = self
            .take_idle_process(index, process_generation, channel.as_deref())
            .await?
        {
            self.finish_stopped_process(index, proc).await?;
        }
        Ok(())
    }

    /// Release a scan lease and synchronously reclaim its process when it was
    /// the final lease.  Normal HTTP releases deliberately use the three
    /// second idle grace, but a scanner must not advance to the next channel
    /// while its old process is still occupying the only tuner slot.
    ///
    /// The process is taken while the use-count transition is protected by the
    /// slot lock.  An HTTP acquire that wins after the transition therefore
    /// either keeps the process alive (use_count > 0) or starts only after the
    /// synchronous stop has made the slot idle.
    pub async fn release_lease_immediately(
        &self,
        index: usize,
        generation: u64,
        priority: Option<i32>,
    ) -> Result<(), String> {
        let process = {
            let mut s = self.slot_mutex(index)?.lock().await;
            if s.process_generation != generation {
                return Ok(());
            }
            s.use_count = s.use_count.saturating_sub(1);
            s.remove_request_priority(priority);
            if s.use_count > 0 {
                return Ok(());
            }

            s.cancel_idle_stop();
            s.wanted_channel = None;
            s.respawn_pending = false;
            if matches!(s.state, TunerState::Starting | TunerState::Tuning) {
                s.start_generation = s.start_generation.wrapping_add(1);
                s.state = TunerState::Idle;
            }

            let Some(process) = s.process.take() else {
                s.current_channel = None;
                s.pid = None;
                if !s.state.is_terminal() {
                    s.state = TunerState::Idle;
                }
                s.idle_notify.notify_waiters();
                return Ok(());
            };

            s.state = TunerState::Stopping;
            s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
            s.senders.clear();
            s.scan_senders.clear();
            s.pump_ready = false;
            process
        };

        self.finish_stopped_process(index, process).await
    }

    /// stdout を pump (fan-out) へ引き渡す。呼び出し後は pump が所有する。
    pub async fn take_stdout(
        &self,
        index: usize,
    ) -> Result<Option<tokio::process::ChildStdout>, String> {
        let mut s = self.slot_mutex(index)?.lock().await;
        Ok(s.process.as_mut().and_then(|p| p.take_stdout()))
    }

    /// 同一物理chで共有可能か (Streaming + 同一ch + プロセスあり)。
    /// 共有キーは物理chのみで `decode` の違いは妨げない (§9)。
    pub async fn is_sharing_candidate(&self, index: usize, phys: &str) -> bool {
        match self.slots.get(index) {
            Some(m) => {
                let s = m.lock().await;
                !s.state.is_terminal()
                    && s.state == TunerState::Streaming
                    && s.current_channel.as_deref() == Some(phys)
                    && s.process.is_some()
            }
            None => false,
        }
    }

    /// per-client購読を作成する (fan-out §10)。
    /// boundedキューでノンブロッキング配信する。
    /// `use_count` は変更しない (呼び出し側の `acquire`/`start` が管理)。
    pub async fn create_subscription(
        &self,
        index: usize,
    ) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let generation = self.slot_mutex(index)?.lock().await.process_generation;
        self.create_subscription_for_generation(index, generation).await
    }

    /// Attach a subscriber only if the process observed by `acquire` is still
    /// the active process generation.  Acquisition and attachment are
    /// separate awaits, so this check must live under the slot lock.
    pub async fn create_subscription_for_generation(
        &self,
        index: usize,
        expected_generation: u64,
    ) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let mut s = self.slot_mutex(index)?.lock().await;
        if s.state.is_terminal() {
            return Err(format!("tuner {index} is terminal"));
        }
        if s.state != TunerState::Streaming
            || s.process.is_none()
            || s.process_generation != expected_generation
        {
            return Err(format!("tuner {index} is not streaming"));
        }
        let (tx, rx) = mpsc::channel(STREAM_QUEUE_LEN);
        s.senders.push(tx);
        s.senders.retain(|tx| !tx.is_closed());
        Ok(rx)
    }

    /// Attach a scan subscriber.  Unlike an HTTP subscriber, this receiver is
    /// closed when the current process reaches EOF so a finite scan fixture or
    /// a failed tuner does not wait for the channel timeout.
    pub async fn create_scan_subscription_for_generation(
        &self,
        index: usize,
        expected_generation: u64,
    ) -> Result<mpsc::Receiver<Vec<u8>>, String> {
        let mut s = self.slot_mutex(index)?.lock().await;
        if s.state.is_terminal() {
            return Err(format!("tuner {index} is terminal"));
        }
        if s.state != TunerState::Streaming
            || s.process.is_none()
            || s.process_generation != expected_generation
        {
            return Err(format!("tuner {index} is not streaming"));
        }
        let (tx, rx) = mpsc::channel(STREAM_QUEUE_LEN);
        s.scan_senders.push(tx);
        s.scan_senders.retain(|tx| !tx.is_closed());
        Ok(rx)
    }

    /// pumpを起動する。tuner stdoutを読み、購読者全員へ `try_send` する。
    /// Full は当該subscriberを切断せず、そのチャンクだけを落とす。
    /// 遅延clientを維持しつつ、overflowはカウンタとログで観測可能にする。
    pub fn spawn_pump(
        self: &Arc<Self>,
        index: usize,
        mut stdout: tokio::process::ChildStdout,
    ) -> JoinHandle<()> {
        let mgr = Arc::clone(self);
        let token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            // Install the token before reading.  All process replacement paths
            // invalidate the previous token while holding the slot lock, so a
            // delayed old pump can only discard its remaining bytes.
            let Some(slot) = mgr.slots.get(index) else { return };
            {
                let mut slot = slot.lock().await;
                if slot.process.is_none() {
                    return;
                }
                if token <= slot.pump_token {
                    return;
                }
                slot.pump_token = token;
                slot.pump_ready = true;
            }
            let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
            loop {
                let n = match tokio::io::AsyncReadExt::read(&mut stdout, &mut buf)
                    .await
                {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                };
                let chunk = buf[..n].to_vec();
                if !mgr.fanout_chunk(index, token, chunk).await {
                    continue;
                }
            }
            if let Some(slot) = mgr.slots.get(index) {
                let mut slot = slot.lock().await;
                if slot.pump_token == token {
                    slot.scan_senders.clear();
                    slot.pump_ready = false;
                }
            }
        })
    }

    async fn fanout_chunk(&self, index: usize, token: u64, chunk: Vec<u8>) -> bool {
        let Some(m) = self.slots.get(index) else { return false };
        let mut slot = m.lock().await;
        if slot.pump_token != token {
            return false;
        }
        let mut overflowed = false;
        slot.senders.retain(|tx| match tx.try_send(chunk.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                overflowed = true;
                STREAM_FANOUT_OVERFLOWS.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(index, "tuner fan-out subscriber queue overflow; chunk dropped");
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
        slot.scan_senders.retain(|tx| match tx.try_send(chunk.clone()) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                STREAM_FANOUT_OVERFLOWS.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(index, "tuner scan fan-out subscriber queue overflow; chunk dropped");
                true
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        });
        !overflowed || !slot.senders.is_empty()
    }

    /// Drop等のawait不可文脈向けの同期ヒント。`try_lock` のみで
    /// `use_count` を減らし、0になれば需要なしを記録する。
    /// 成功時true。失敗時は呼び出し側が非同期 `release` で補う。
    /// これ + `stop_if_idle` + watcher orphan回収でゾンビ化を防ぐ。
    pub fn release_hint_sync(&self, index: usize) -> bool {
        self.release_hint_sync_lease(index, 0, None)
    }

    pub fn release_hint_sync_lease(
        &self,
        index: usize,
        generation: u64,
        priority: Option<i32>,
    ) -> bool {
        if let Some(m) = self.slots.get(index) {
            if let Ok(mut s) = m.try_lock() {
                if priority.is_some() && s.process_generation != generation {
                    return false;
                }
                s.use_count = s.use_count.saturating_sub(1);
                s.remove_request_priority(priority);
                if s.use_count == 0 {
                    s.wanted_channel = None;
                    s.respawn_pending = false;
                    if s.process.is_none() {
                        s.current_channel = None;
                    }
                    if matches!(s.state, TunerState::Starting | TunerState::Tuning) {
                        s.respawn_pending = false;
                        s.start_generation = s.start_generation.wrapping_add(1);
                        s.state = TunerState::Idle;
                    }
                    if s.process.is_some() && !s.idle_scheduled {
                        s.idle_generation = s.idle_generation.wrapping_add(1);
                        s.idle_scheduled = true;
                    }
                }
                return true;
            }
        }
        false
    }

    /// Arm the §10 three-second grace timer after the last lease disappears.
    /// Re-acquiring the same physical channel cancels it under the slot lock.
    pub async fn schedule_idle_stop(
        &self,
        index: usize,
        expected_generation: u64,
        expected_channel: Option<&str>,
    ) {
        let Some(manager) = self.self_ref.get().and_then(Weak::upgrade) else {
            return;
        };
        let Some(mutex) = self.slots.get(index) else { return };
        let (idle_generation, notify) = {
            let mut slot = mutex.lock().await;
            if slot.use_count != 0
                || slot.process_generation != expected_generation
                || expected_channel.is_some_and(|channel| slot.current_channel.as_deref() != Some(channel))
                || slot.process.is_none()
            {
                return;
            }
            if !slot.idle_scheduled {
                slot.idle_generation = slot.idle_generation.wrapping_add(1);
                slot.idle_scheduled = true;
            }
            (slot.idle_generation, Arc::clone(&slot.idle_notify))
        };
        let channel = expected_channel.map(str::to_owned);
        tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(IDLE_GRACE) => {
                    let valid = match manager.slots.get(index) {
                        Some(mutex) => {
                            let slot = mutex.lock().await;
                            slot.idle_scheduled
                                && slot.idle_generation == idle_generation
                                && slot.use_count == 0
                                && slot.process_generation == expected_generation
                                && slot.process.is_some()
                                && channel.as_deref().is_none_or(|value| slot.current_channel.as_deref() == Some(value))
                        }
                        None => false,
                    };
                    if valid {
                        let proc = manager.take_idle_process(index, Some(expected_generation), channel.as_deref()).await;
                        if let Ok(Some(proc)) = proc {
                            let _ = manager.finish_stopped_process(index, proc).await;
                        }
                    }
                }
                _ = notify.notified() => {}
            }
        });
    }

    /// 需要なし (`use_count == 0`) なら停止する。需要ありなら何もしない。
    /// Drop後の非同期停止やwatcherのorphan回収に使う。
    pub async fn stop_if_idle(&self, index: usize) -> Result<(), String> {
        // Take a snapshot first, then repeat all checks under the same slot
        // lock immediately before taking the process. This protects against a
        // release/acquire interleave and documents the generation/channel guard.
        let (generation, channel) = {
            let s = self.slot_mutex(index)?.lock().await;
            (s.process.as_ref().map(|_| s.process_generation), s.current_channel.clone())
        };
        let proc = self
            .take_idle_process(index, generation, channel.as_deref())
            .await?;
        if let Some(proc) = proc {
            self.finish_stopped_process(index, proc).await?;
        }
        Ok(())
    }

    /// Wait until an idle slot has completed its grace-period cleanup.
    /// This is intentionally state/Notify based so callers and tests do not
    /// need to guess how long process reaping will take.
    pub async fn wait_for_idle(&self, index: usize) -> Result<(), String> {
        loop {
            let notified = {
                let slot = self.slot_mutex(index)?.lock().await;
                if slot.use_count == 0
                    && slot.process.is_none()
                    && slot.state == TunerState::Idle
                {
                    return Ok(());
                }
                Arc::clone(&slot.idle_notify).notified_owned()
            };
            notified.await;
        }
    }

    /// 稼働プロセスの終了をノンブロッキング確認する。
    /// 異常終了 + 残存要求があれば同一chで respawn する。
    /// 戻り値は (終了していたか, respawn後のpid)。
    /// release の sleep はロック外で行う。正常終了+残存要求の respawn 前は
    /// Idle へ遷移してから `start` する (EBUSY防止)。
    pub async fn poll_exit(
        &self,
        index: usize,
    ) -> Result<(bool, Option<u32>), String> {
        // 終了確認と回収は短時間ロックで行い、以降の sleep/start はロック外。
        let (success, wanted, use_count, process_generation, current_channel) = {
            let mut s = self.slot_mutex(index)?.lock().await;
            let status = match s.process.as_mut() {
                None => return Ok((false, None)),
                Some(p) => p.try_wait().map_err(|e| e.to_string())?,
            };
            let Some(status) = status else {
                return Ok((false, None));
            };
            let success = status.success();
            tracing::warn!(index, success, %status, "tuner process exited");
            s.pump_token = NEXT_PUMP_TOKEN.fetch_add(1, Ordering::Relaxed);
            s.process = None;
            s.pid = None;
            (
                success,
                s.wanted_channel.clone(),
                s.use_count,
                s.process_generation,
                s.current_channel.clone(),
            )
        };

        if success && use_count == 0 {
            // 正常終了・需要なし: release (100ms) をロック外で待って IDLE へ。
            // wait中は Streaming のまま残し、再利用を状態で抑止する。
            release_wait(false).await;
            {
                let mut s = self.slot_mutex(index)?.lock().await;
                // stop() 等で既に片付いていれば何もしない。
                if s.process.is_none() && s.use_count == 0 {
                    s.record_success();
            s.senders.clear();
            s.scan_senders.clear();
                    s.current_channel = None;
                    s.wanted_channel = None;
                    if !s.state.is_terminal() {
                        s.state = TunerState::Idle;
                    }
                    s.idle_notify.notify_waiters();
                }
            }
            return Ok((true, None));
        }

        if success {
            // 残存要求ありの正常終了 (外部要因) は同一chで respawn。
            // 購読者 (senders) は維持し、新pumpが引き継ぐ (watcherが起動)。
            release_wait(false).await;
            if let Some(ch) = wanted {
                if !self
                    .prepare_respawn(index, process_generation, current_channel.as_deref(), &ch)
                    .await?
                {
                    return Ok((true, None));
                }
                match self.start_inner(index, &ch, true, 0).await {
                    Ok((pid, _)) => return Ok((true, Some(pid))),
                    Err(TunerError::Busy(_)) => return Ok((true, None)),
                    Err(e) => {
                        self.close_senders(index).await;
                        return Err(e.to_string());
                    }
                }
            }
            return Ok((true, None));
        }

        // 異常終了。
        let fault = {
            let mut s = self.slot_mutex(index)?.lock().await;
            let fault = s.record_failure();
            if fault == TunerState::Fault {
                s.senders.clear();
            }
            fault
        };
        if fault == TunerState::Fault {
            return Err(format!(
                "tuner {index} failed {MAX_CONSECUTIVE_ERRORS} times; FAULT, restart required"
            ));
        }
        release_wait(true).await;
        // 残存要求があれば同一chで respawn、なければ IDLE。
        if let Some(ch) = wanted {
            if self
                .prepare_respawn(index, process_generation, current_channel.as_deref(), &ch)
                .await?
            {
                match self.start_inner(index, &ch, true, 0).await {
                    Ok((pid, _)) => return Ok((true, Some(pid))),
                    Err(TunerError::Busy(_)) => return Ok((true, None)),
                    Err(e) => {
                        self.close_senders(index).await;
                        return Err(e.to_string());
                    }
                }
            }
        }
        {
            let mut s = self.slot_mutex(index)?.lock().await;
            if s.state == TunerState::Error {
                s.senders.clear();
                s.current_channel = None;
                s.wanted_channel = None;
                s.state = TunerState::Idle;
            }
        }
        Ok((true, None))
    }

    /// Recheck all respawn prerequisites under the slot lock immediately
    /// before starting a new process. The pending reservation also lets a
    /// release that races after this check invalidate the start generation.
    async fn prepare_respawn(
        &self,
        index: usize,
        expected_generation: u64,
        expected_channel: Option<&str>,
        wanted_channel: &str,
    ) -> Result<bool, String> {
        let mut s = self.slot_mutex(index)?.lock().await;
        let matches_process = s.process_generation == expected_generation
            && s.current_channel.as_deref() == expected_channel
            && s.wanted_channel.as_deref() == Some(wanted_channel)
            && s.use_count > 0
            && s.process.is_none()
            && !s.state.is_terminal();
        if !matches_process {
            s.respawn_pending = false;
            if s.process.is_none() && s.use_count == 0 && !s.state.is_terminal() {
                s.current_channel = None;
                s.wanted_channel = None;
                s.senders.clear();
                s.state = TunerState::Idle;
            }
            return Ok(false);
        }
        // Keep the complete use_count while the respawn is reserved. The
        // respawn start consumes this reservation instead of incrementing it.
        s.state = TunerState::Idle;
        s.respawn_pending = true;
        Ok(true)
    }

    async fn close_senders(&self, index: usize) {
        if let Some(m) = self.slots.get(index) {
            m.lock().await.senders.clear();
        }
    }

    /// spawn直後の常駐監視タスク。wait→poll_exit/respawn を担当する。
    /// 500ms間隔で `poll_exit` し、プロセスなし+需要なしで終了する。
    /// orphan回収 (reaper) も兼ねる: 需要なし (`use_count==0`) なのに
    /// プロセスが残っていれば `stop_if_idle` で確実に片付ける。
    /// Dropの同期ヒントが非同期停止より先に残ってもここで回収されるため、
    /// fire-and-forget単独のゾンビ化は起きない。
    pub fn spawn_watcher(self: &Arc<Self>, index: usize) -> JoinHandle<()> {
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(WATCH_INTERVAL).await;
                // orphan回収: 需要なしなのにプロセス残留 → 停止。
                {
                let orphan = match mgr.slots.get(index) {
                        Some(m) => {
                            let s = m.lock().await;
                            s.use_count == 0
                                && s.wanted_channel.is_none()
                                && !s.idle_scheduled
                                && s.process.is_some()
                        }
                        None => false,
                    };
                    if orphan {
                        let _ = mgr.stop_if_idle(index).await;
                    }
                }
                let should_continue = match mgr.slots.get(index) {
                    None => false,
                    Some(m) => {
                        let s = m.lock().await;
                        if s.state.is_terminal() {
                            // FAULT/DISABLED はプロセスが残っていれば回収のため継続。
                            s.process.is_some()
                        } else {
                            s.process.is_some()
                                || s.state.is_active()
                                || s.state == TunerState::Error
                                || s.use_count > 0
                        }
                    }
                };
                if !should_continue {
                    break;
                }
                match mgr.poll_exit(index).await {
                    Ok((exited, respawned)) => {
                        if exited {
                            if let Some(pid) = respawned {
                                // respawn後の新stdoutを新pumpへ (既存購読者は維持)。
                                match mgr.take_stdout(index).await {
                                    Ok(Some(stdout)) => {
                                        mgr.spawn_pump(index, stdout);
                                    }
                                    Ok(None) => {}
                                    Err(e) => {
                                        tracing::warn!(
                                            index,
                                            error = %e,
                                            "respawn pump attach failed"
                                        );
                                    }
                                }
                                tracing::info!(
                                    index,
                                    pid,
                                    "tuner respawned by watcher"
                                );
                            } else {
                                let still = match mgr.slots.get(index) {
                                    Some(m) => {
                                        let s = m.lock().await;
                                        s.process.is_some() || s.use_count > 0
                                    }
                                    None => false,
                                };
                                if !still {
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            index,
                            error = %e,
                            "tuner watcher exiting"
                        );
                        break;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChannelType;
    use crate::tuner::process::test_fixture_command;
    use std::collections::HashMap;

    fn fixture(mode: &str) -> String {
        test_fixture_command(mode)
    }

    fn tuner_with_command(name: &str, command: Option<&str>) -> Tuner {
        Tuner {
            name: name.to_owned(),
            types: vec![ChannelType::GR],
            command: command.map(|s| s.to_owned()),
            tlv_decoder: None,
            decoder: None,
            extra: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn new_marks_missing_command_disabled() {
        let m = TunerManager::new(vec![
            tuner_with_command("ok", Some("recpt1 <channel> - -")),
            tuner_with_command("ng", None),
            tuner_with_command("blank", Some("   ")),
        ]);
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
        assert_eq!(m.state(1).await.unwrap(), TunerState::Disabled);
        assert_eq!(m.state(2).await.unwrap(), TunerState::Disabled);
    }

    #[tokio::test]
    async fn three_failures_become_fault() {
        let mut slot = TunerSlot::new(
            0,
            &tuner_with_command("t", Some("recpt1 <channel>")),
        );
        assert_eq!(slot.record_failure(), TunerState::Error);
        assert_eq!(slot.record_failure(), TunerState::Error);
        assert_eq!(slot.record_failure(), TunerState::Fault);
        assert!(slot.state.needs_restart());
    }

    #[tokio::test]
    async fn physical_channel_prefers_tuner_channels() {
        let m = TunerManager::new(vec![tuner_with_command(
            "DVB-C-0",
            Some("recdvb <channel>"),
        )]);
        let ch = Channel {
            name: "x".to_owned(),
            channel_type: ChannelType::GR,
            channel: "27".to_owned(),
            serviceId: None,
            tunerChannels: Some(HashMap::from([(
                "DVB-C-0".to_owned(),
                "13".to_owned(),
            )])),
            extra: HashMap::new(),
        };
        assert_eq!(m.physical_channel_for(0, &ch).await.unwrap(), "13");
    }

    #[tokio::test]
    async fn service_channel_uses_logical_target_and_mapping_wins_legacy_extension() {
        let m = TunerManager::new(vec![tuner_with_command(
            "DVB-C-0",
            Some("recdvb <channel>"),
        )]);
        let ch = Channel {
            name: "service".to_owned(),
            channel_type: ChannelType::SKY,
            channel: "CH585:101".to_owned(),
            serviceId: Some(101),
            tunerChannels: Some(HashMap::from([("DVB-C-0".to_owned(), "13".to_owned())])),
            extra: HashMap::from([(String::from("physicalChannel"), serde_json::json!("999"))]),
        };
        assert_eq!(m.physical_channel_for(0, &ch).await.unwrap(), "13");

        let mut without_mapping = ch;
        without_mapping.tunerChannels = None;
        assert_eq!(m.physical_channel_for(0, &without_mapping).await.unwrap(), "999");
        without_mapping.extra.clear();
        assert_eq!(m.physical_channel_for(0, &without_mapping).await.unwrap(), "CH585");
    }

    #[tokio::test]
    async fn start_stop_with_sleep() {
        let m =
            TunerManager::new(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let pid = m.start(0, "13").await.expect("spawn sleep");
        assert!(pid > 0);
        assert_eq!(m.state(0).await.unwrap(), TunerState::Streaming);
        m.stop(0).await.expect("stop");
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
    }

    #[tokio::test]
    async fn acquire_same_channel_shares() {
        let m =
            TunerManager::new(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let a = m.acquire(0, "13").await.unwrap();
        let b = m.acquire(0, "13").await.unwrap();
        assert_eq!(a, b);
        assert_eq!(m.use_count(0).await.unwrap(), 2);
        m.release(0).await.unwrap();
        // 残存要求ありのためプロセス継続。
        assert_eq!(m.state(0).await.unwrap(), TunerState::Streaming);
        m.release(0).await.unwrap();
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
    }

    #[tokio::test]
    async fn stop_all_stops_every_slot() {
        let m = TunerManager::new(vec![
            tuner_with_command("a", Some(&fixture("hold"))),
            tuner_with_command("b", Some(&fixture("hold"))),
        ]);
        m.start(0, "13").await.unwrap();
        m.start(1, "13").await.unwrap();
        m.stop_all().await;
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
        assert_eq!(m.state(1).await.unwrap(), TunerState::Idle);
    }

    #[tokio::test]
    async fn stop_all_rejects_later_starts() {
        let m = TunerManager::new(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        m.stop_all().await;
        assert!(m.start(0, "13").await.is_err());
    }

    #[tokio::test]
    async fn shutdown_cannot_leave_a_racing_start_streaming() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let start = {
            let m = Arc::clone(&m);
            tokio::spawn(async move { m.start(0, "13").await })
        };
        tokio::task::yield_now().await;
        m.stop_all().await;
        let _ = start.await;
        assert_ne!(m.state(0).await.unwrap(), TunerState::Streaming);
    }

    #[tokio::test]
    async fn poll_exit_respawns_on_clean_exit_with_demand() {
        // 正常終了+残存要求でも EBUSY にならず respawn すること (NG1回帰)。
        let m = TunerManager::new(vec![tuner_with_command("t", Some(&fixture("exit-success")))]);
        let pid = m.start(0, "13").await.expect("spawn true");
        assert!(pid > 0);
        let mut respawned: Option<Option<u32>> = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            match m.poll_exit(0).await {
                Ok((false, _)) => continue,
                Ok((true, pid2)) => {
                    respawned = Some(pid2);
                    break;
                }
                Err(e) => panic!("poll_exit should respawn, got Err: {e}"),
            }
        }
        let pid2 = respawned.expect("process should have exited");
        assert!(
            pid2.is_some(),
            "should respawn with remaining demand, got {pid2:?}"
        );
        assert_eq!(m.state(0).await.unwrap(), TunerState::Streaming);
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn poll_exit_does_not_respawn_after_release_during_wait() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("respawn")))]);
        let (_, generation) = m.start_with_priority(0, "13", 0).await.unwrap();

        // Ensure poll_exit observes the exited process and is in its release
        // wait before the final lease is released.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let task_manager = Arc::clone(&m);
        let release_manager = Arc::clone(&m);
        let poll = tokio::spawn(async move { task_manager.poll_exit(0).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        release_manager
            .release_lease(0, generation, Some(0))
            .await
            .unwrap();
        let result = poll.await.unwrap().unwrap();
        assert_eq!(result, (true, None));
        assert_eq!(m.use_count(0).await.unwrap(), 0);
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
        assert_eq!(m.pid(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn busy_and_spawn_failure_are_separate_errors() {
        // High: 文字列contains判定の廃止。BusyとSpawn失敗をenumで分離する。
        let m = TunerManager::shared(vec![
            tuner_with_command("ok", Some(&fixture("hold"))),
            tuner_with_command("ng", Some("/nonexistent-hotarun-cmd-xyz")),
        ]);
        // 稼働中への二重起動はBusy (再試行可)。
        m.start(0, "13").await.expect("spawn sleep");
        let busy = m.start(0, "13").await.expect_err("double start must fail");
        assert!(busy.is_busy(), "expected Busy, got {busy:?}");
        // 存在しないコマンドはSpawn失敗 (即500対象・Busyではない)。
        let spawn_err = m.start(1, "13").await.expect_err("bad command must fail");
        assert!(
            !spawn_err.is_busy(),
            "spawn failure must not be Busy, got {spawn_err:?}"
        );
        assert!(
            matches!(spawn_err, TunerError::SpawnFailed(_)),
            "expected SpawnFailed, got {spawn_err:?}"
        );
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn sharing_candidate_only_for_same_phys_streaming() {
        // High: 同一physは共有/fan-out (acquire使用)。別chは共有しない。
        let m = TunerManager::new(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        m.start(0, "13").await.unwrap();
        assert!(m.is_sharing_candidate(0, "13").await);
        assert!(!m.is_sharing_candidate(0, "27").await);
        // acquire同一chはプロセスを作り直さず共有する。
        let pid_before = m.pid(0).await.unwrap();
        let pid_shared = m.acquire(0, "13").await.unwrap();
        assert_eq!(pid_before, Some(pid_shared));
        assert_eq!(m.use_count(0).await.unwrap(), 2);
        // 別chへのacquireはBusy (同一プロセスで別chは不可)。
        let busy = m.acquire(0, "27").await.expect_err("different ch must be busy");
        assert!(busy.is_busy(), "expected Busy, got {busy:?}");
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn higher_priority_request_takes_over_lower_priority_stream() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let (_, low_generation) = m
            .start_monitored_with_priority(0, "13", 10)
            .await
            .unwrap();
        assert_eq!(m.use_count(0).await.unwrap(), 1);
        assert!(m
            .takeover_with_priority(0, "27", 5)
            .await
            .is_err());

        let (_, high_generation) = m
            .takeover_with_priority(0, "27", 20)
            .await
            .unwrap();
        assert_ne!(low_generation, high_generation);
        assert_eq!(m.current_channel(0).await.unwrap().as_deref(), Some("27"));
        assert!(m
            .takeover_with_priority(0, "13", 20)
            .await
            .is_err());
        assert!(m
            .takeover_with_priority(0, "13", 10)
            .await
            .is_err());
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn fanout_two_subscribers_share_one_process() {
        // §10: 同一Tuner Processを複数clientへfan-outする。
        let m = TunerManager::shared(vec![tuner_with_command(
            "t",
            Some(&fixture("fanout")),
        )]);
        m.start(0, "13").await.unwrap();
        let mut rx1 = m.create_subscription(0).await.expect("sub1");
        let mut rx2 = m.create_subscription(0).await.expect("sub2");
        let stdout = m.take_stdout(0).await.unwrap().expect("stdout");
        m.spawn_pump(0, stdout);
        let got1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
            .await
            .expect("timeout rx1")
            .expect("closed rx1");
        let got2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
            .await
            .expect("timeout rx2")
            .expect("closed rx2");
        assert_eq!(got1, got2);
        assert!(got1.starts_with(b"hello-fanout"));
        // 単一プロセスのまま (spawnし直していない)。
        assert_eq!(m.use_count(0).await.unwrap(), 1);
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn release_hint_and_stop_if_idle_cleanup_without_leak() {
        // High: 切断後リーク防止。Drop相当の同期ヒント+非同期停止で確実にIDLEへ。
        // fire-and-forget単独にせず、ヒントが残ってもstop_if_idle/watcherで回収する。
        let m = TunerManager::new(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        m.start(0, "13").await.unwrap();
        assert_eq!(m.use_count(0).await.unwrap(), 1);
        // 同期ヒント (await不可文脈相当) で需要を減らす。
        assert!(m.release_hint_sync(0));
        assert_eq!(m.use_count(0).await.unwrap(), 0);
        // 非同期停止でプロセスも状態も片付く (Busy残留なし)。
        m.stop_if_idle(0).await.expect("stop_if_idle");
        assert_eq!(m.state(0).await.unwrap(), TunerState::Idle);
        assert_eq!(m.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn stop_if_idle_does_not_stop_a_concurrent_acquire() {
        // stop_if_idle must perform its demand check and process take under one
        // slot lock. If acquire wins that lock, the active process is retained.
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        for _ in 0..10 {
            if m.state(0).await.unwrap() == TunerState::Idle {
                let _ = m.start(0, "13").await.unwrap();
            }
            assert!(m.release_hint_sync(0));
            let stop = m.stop_if_idle(0);
            let acquire = m.acquire(0, "13");
            let (stop_result, acquire_result) = tokio::join!(stop, acquire);
            stop_result.unwrap();
            if acquire_result.is_ok() {
                assert_eq!(m.state(0).await.unwrap(), TunerState::Streaming);
                assert!(m.pid(0).await.unwrap().is_some());
                m.release(0).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn final_release_keeps_process_for_three_second_grace_and_reuses_it() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let (_, generation) = m.acquire_with_priority(0, "13", 0).await.unwrap();
        let pid = m.pid(0).await.unwrap();
        m.release_lease(0, generation, Some(0)).await.unwrap();
        assert_eq!(m.use_count(0).await.unwrap(), 0);
        assert_eq!(m.pid(0).await.unwrap(), pid);
        let (_, reused_generation) = m.acquire_with_priority(0, "13", 0).await.unwrap();
        assert_eq!(reused_generation, generation);
        assert_eq!(m.pid(0).await.unwrap(), pid);
        m.release_lease(0, reused_generation, Some(0)).await.unwrap();
        tokio::time::timeout(IDLE_GRACE + Duration::from_secs(1), m.wait_for_idle(0))
            .await
            .expect("idle grace cleanup timeout")
            .unwrap();
        assert_eq!(m.pid(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn respawn_keeps_all_existing_subscribers() {
        let m = TunerManager::shared(vec![tuner_with_command(
            "t",
            Some(&fixture("respawn"))
        )]);
        m.start(0, "13").await.unwrap();
        let mut rx1 = m.create_subscription(0).await.unwrap();
        let mut rx2 = m.create_subscription(0).await.unwrap();
        let stdout = m.take_stdout(0).await.unwrap().unwrap();
        m.spawn_pump(0, stdout);

        let first1 = tokio::time::timeout(Duration::from_secs(2), rx1.recv())
            .await
            .unwrap()
            .unwrap();
        let first2 = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first1, first2);

        let mut respawned = false;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if let Ok((true, Some(_))) = m.poll_exit(0).await {
                let stdout = m.take_stdout(0).await.unwrap().unwrap();
                m.spawn_pump(0, stdout);
                respawned = true;
                break;
            }
        }
        assert!(respawned);
        let second1 = tokio::time::timeout(Duration::from_secs(2), rx1.recv())
            .await
            .unwrap()
            .unwrap();
        let second2 = tokio::time::timeout(Duration::from_secs(2), rx2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second1, second2);
        assert_eq!(second1, b"respawn-data");
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn respawned_process_keeps_the_original_lease_generation() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("respawn")))]);
        let (_, generation) = m.start_with_priority(0, "13", 0).await.unwrap();
        let mut subscription = m.create_subscription_for_generation(0, generation).await.unwrap();
        let stdout = m.take_stdout(0).await.unwrap().unwrap();
        m.spawn_pump(0, stdout);
        assert_eq!(subscription.recv().await.unwrap(), b"respawn-data");

        let (_, new_pid) = loop {
            if let Ok((true, Some(pid))) = m.poll_exit(0).await {
                let stdout = m.take_stdout(0).await.unwrap().unwrap();
                m.spawn_pump(0, stdout);
                break (true, pid);
            }
            tokio::task::yield_now().await;
        };
        assert!(new_pid > 0);
        assert_eq!(m.use_count(0).await.unwrap(), 1);
        m.release_lease(0, generation, Some(0)).await.unwrap();
        assert_eq!(m.use_count(0).await.unwrap(), 0);
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn subscription_rejects_a_stale_process_generation() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let (_, generation) = m.acquire_with_priority(0, "13", 0).await.unwrap();
        assert!(m
            .create_subscription_for_generation(0, generation.wrapping_add(1))
            .await
            .is_err());
        assert_eq!(m.use_count(0).await.unwrap(), 1);
        m.release_lease(0, generation, Some(0)).await.unwrap();
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn stale_pump_chunks_are_not_delivered_after_takeover() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold"))) ]);
        m.start_with_priority(0, "13", 1).await.unwrap();
        let _old_subscription = m.create_subscription(0).await.unwrap();
        let old_token = m.slots[0].lock().await.pump_token;

        m.takeover_with_priority(0, "27", 2).await.unwrap();
        let mut new_subscription = m.create_subscription(0).await.unwrap();
        assert!(!m.fanout_chunk(0, old_token, b"old-process-residue".to_vec()).await);
        assert!(tokio::time::timeout(Duration::from_millis(50), new_subscription.recv())
            .await
            .is_err());
        m.stop(0).await.unwrap();
    }

    #[tokio::test]
    async fn fault_closes_existing_subscriber() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        let (tx, mut rx) = mpsc::channel(1);
        {
            let mut slot = m.slots[0].lock().await;
            slot.senders.push(tx);
            assert_eq!(slot.record_failure(), TunerState::Error);
            assert_eq!(slot.record_failure(), TunerState::Error);
            assert_eq!(slot.record_failure(), TunerState::Fault);
            slot.senders.clear();
        }
        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn fanout_overflow_is_counted_and_closes_subscriber() {
        let m = TunerManager::shared(vec![tuner_with_command("t", Some(&fixture("hold")))]);
        m.start(0, "13").await.unwrap();
        let mut rx = m.create_subscription(0).await.unwrap();
        let before = STREAM_FANOUT_OVERFLOWS.load(Ordering::Relaxed);
        for _ in 0..=STREAM_QUEUE_LEN {
            let token = m.slots[0].lock().await.pump_token;
            m.fanout_chunk(0, token, vec![0; 1]).await;
        }
        assert!(STREAM_FANOUT_OVERFLOWS.load(Ordering::Relaxed) > before);
        assert!(rx.try_recv().is_ok(), "overflow must not close a delayed subscriber");
        m.stop(0).await.unwrap();
    }
}
