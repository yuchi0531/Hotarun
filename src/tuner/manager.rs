//! チューナースロット群の状態管理 (SPEC §8§9)。
//!
//! - 起動成功は spawn 成功で即時確定 (初回バイト待ちなし)。
//! - 停止は [`crate::tuner::process::stop_process`] (SIGTERM→6秒→SIGKILL)。
//! - release は正常 100ms / 異常 1000ms。
//! - 異常終了時に残存要求 (`use_count > 0`) があれば同一chで respawn。
//! - 3連続失敗で FAULT (要再起動・端末状態)。

use std::time::Duration;

use super::command::build_tuner_command;
use super::process::{SpawnedTuner, spawn_program, stop_process};
use super::state::TunerState;
use crate::config::{Channel, ChannelType, Tuner};

/// 正常 release 後の再利用待ち (§8: 100ms)。
pub const RELEASE_FAST: Duration = Duration::from_millis(100);
/// 異常 release 後の再利用待ち (§8: 1000ms)。
pub const RELEASE_SLOW: Duration = Duration::from_millis(1000);
/// FAULT になる連続失敗回数 (§8: 3連続)。
pub const MAX_CONSECUTIVE_ERRORS: u32 = 3;

/// release 待ち。`after_error=true` で 1000ms、通常は 100ms。
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
    index: usize,
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
    process: Option<SpawnedTuner>,
}

impl TunerSlot {
    fn new(index: usize, tuner: &Tuner) -> Self {
        let usable = command_usable(tuner);
        Self {
            index,
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
            process: None,
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

/// チューナー群。Phase3 から `Arc<Mutex<>>` で共有する。
#[derive(Debug)]
pub struct TunerManager {
    slots: Vec<TunerSlot>,
}

/// 将来の Scheduler / Stream Manager 用の共有型。
pub type SharedTunerManager = std::sync::Arc<tokio::sync::Mutex<TunerManager>>;

impl TunerManager {
    pub fn new(tuners: Vec<Tuner>) -> Self {
        let slots = tuners
            .iter()
            .enumerate()
            .map(|(i, t)| TunerSlot::new(i, t))
            .collect();
        Self { slots }
    }

    pub fn shared(tuners: Vec<Tuner>) -> SharedTunerManager {
        std::sync::Arc::new(tokio::sync::Mutex::new(Self::new(tuners)))
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    fn slot(&self, index: usize) -> Result<&TunerSlot, String> {
        self.slots
            .get(index)
            .ok_or_else(|| format!("no such tuner: {index}"))
    }

    fn slot_mut(&mut self, index: usize) -> Result<&mut TunerSlot, String> {
        self.slots
            .get_mut(index)
            .ok_or_else(|| format!("no such tuner: {index}"))
    }

    pub fn state(&self, index: usize) -> Result<TunerState, String> {
        Ok(self.slot(index)?.state)
    }

    pub fn pid(&self, index: usize) -> Result<Option<u32>, String> {
        Ok(self.slot(index)?.pid)
    }

    pub fn current_channel(&self, index: usize) -> Result<Option<String>, String> {
        Ok(self.slot(index)?.current_channel.clone())
    }

    pub fn consecutive_errors(&self, index: usize) -> Result<u32, String> {
        Ok(self.slot(index)?.consecutive_errors)
    }

    pub fn use_count(&self, index: usize) -> Result<usize, String> {
        Ok(self.slot(index)?.use_count)
    }

    pub fn name(&self, index: usize) -> Result<String, String> {
        Ok(self.slot(index)?.name.clone())
    }

    /// 要再起動 (FAULT) か。
    pub fn needs_restart(&self, index: usize) -> Result<bool, String> {
        Ok(self.slot(index)?.state.needs_restart())
    }

    /// 新規受け付け可能 (IDLE) か。
    pub fn is_usable(&self, index: usize) -> Result<bool, String> {
        Ok(self.slot(index)?.state.can_accept())
    }

    /// 指定の放送種別に対応しているか (§9 条件1)。
    pub fn supports_type(
        &self,
        index: usize,
        channel_type: ChannelType,
    ) -> Result<bool, String> {
        Ok(self.slot(index)?.types.contains(&channel_type))
    }

    /// 空き (IDLE) かつ指定 type に対応し、確保可能なチューナーか
    /// (§9 条件1-3の簡易判定。条件4は物理ch解決が常に可能なため真)。
    pub fn is_available_for(
        &self,
        index: usize,
        channel_type: ChannelType,
    ) -> bool {
        match self.slot(index) {
            Ok(s) => s.state.can_accept() && s.types.contains(&channel_type),
            Err(_) => false,
        }
    }

    /// 稼働中 (確保済みで解放待ち) か。リトライ継続判定用。
    pub fn is_busy(&self, index: usize) -> bool {
        match self.slot(index) {
            Ok(s) => s.state.is_active() || s.state == TunerState::Error,
            Err(_) => false,
        }
    }

    /// `Channel` から物理chを解決する (§9 `resolve_channel`)。
    pub fn physical_channel_for(
        &self,
        index: usize,
        channel: &Channel,
    ) -> Result<String, String> {
        let slot = self.slot(index)?;
        if let Some(map) = channel.tunerChannels.as_ref() {
            if let Some(v) = map.get(&slot.name) {
                return Ok(v.clone());
            }
        }
        Ok(channel.channel.clone())
    }

    /// 手動有効/無効。稼働中の無効化は EBUSY扱いで拒否する。
    pub fn set_disabled(&mut self, index: usize, disabled: bool) -> Result<TunerState, String> {
        let slot = self.slot_mut(index)?;
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
    pub async fn start(
        &mut self,
        index: usize,
        physical_channel: &str,
    ) -> Result<u32, String> {
        let (template, cur_state) = {
            let s = self.slot(index)?;
            (s.command_template.clone(), s.state)
        };
        match cur_state {
            TunerState::Disabled => return Err(format!("tuner {index} is disabled")),
            TunerState::Fault => {
                return Err(format!("tuner {index} is FAULT, restart required"));
            }
            TunerState::Starting
            | TunerState::Tuning
            | TunerState::Streaming
            | TunerState::Stopping => {
                return Err(format!("tuner {index} is busy ({cur_state})"));
            }
            TunerState::Idle | TunerState::Error => {}
        }
        let template =
            template.ok_or_else(|| format!("tuner {index} has no command"))?;
        if template.trim().is_empty() {
            return Err(format!("tuner {index} has no command"));
        }

        {
            let s = self.slot_mut(index)?;
            s.state = TunerState::Starting;
        }

        let (program, args) = build_tuner_command(&template, physical_channel)?;
        // TUNING はチャンネル確定〜配信開始の極短区間。spawn成功で即STREAMING。
        {
            let s = self.slot_mut(index)?;
            s.state = TunerState::Tuning;
        }

        match spawn_program(&program, &args).await {
            Ok(proc) => {
                let pid = proc.pid;
                let s = self.slot_mut(index)?;
                s.record_success();
                s.process = Some(proc);
                s.pid = Some(pid);
                s.current_channel = Some(physical_channel.to_owned());
                s.wanted_channel = Some(physical_channel.to_owned());
                s.use_count = s.use_count.saturating_add(1);
                s.state = TunerState::Streaming;
                tracing::info!(index, pid, channel = %physical_channel, "tuner streaming");
                Ok(pid)
            }
            Err(e) => {
                tracing::warn!(index, error = %e, "tuner spawn failed");
                let fault = {
                    let s = self.slot_mut(index)?;
                    s.process = None;
                    s.pid = None;
                    s.record_failure()
                };
                // 異常系 release (1000ms) 後に再試行可能状態へ戻す。
                // FAULT は端末状態のまま残す。
                release_wait(true).await;
                if fault != TunerState::Fault {
                    if let Ok(s) = self.slot_mut(index) {
                        if s.state == TunerState::Error {
                            s.state = TunerState::Idle;
                        }
                    }
                }
                if fault == TunerState::Fault {
                    Err(format!(
                        "tuner {index} spawn failed ({e}); FAULT, restart required"
                    ))
                } else {
                    Err(format!("tuner {index} spawn failed: {e}"))
                }
            }
        }
    }

    /// 同一chなら共有 (use_count++)、空きなら起動。別ch稼働中は EBUSY。
    pub async fn acquire(
        &mut self,
        index: usize,
        physical_channel: &str,
    ) -> Result<u32, String> {
        let (state, current, pid) = {
            let s = self.slot(index)?;
            (s.state, s.current_channel.clone(), s.pid)
        };
        if state == TunerState::Streaming
            && current.as_deref() == Some(physical_channel)
        {
            let s = self.slot_mut(index)?;
            s.use_count = s.use_count.saturating_add(1);
            s.wanted_channel = Some(physical_channel.to_owned());
            return Ok(pid.unwrap_or(0));
        }
        self.start(index, physical_channel).await
    }

    /// 明示停止。SIGTERM→6秒→SIGKILL 後に 100ms release して IDLE へ。
    pub async fn stop(&mut self, index: usize) -> Result<(), String> {
        let state = self.slot(index)?.state;
        if state == TunerState::Disabled || state == TunerState::Fault {
            return Ok(());
        }
        let has_process = self.slot(index)?.process.is_some();
        if !has_process {
            let s = self.slot_mut(index)?;
            if !s.state.is_terminal() {
                s.state = TunerState::Idle;
            }
            s.pid = None;
            s.current_channel = None;
            s.wanted_channel = None;
            s.use_count = 0;
            return Ok(());
        }

        {
            let s = self.slot_mut(index)?;
            s.state = TunerState::Stopping;
        }
        // process を一旦取り出して停止する (失敗しても IDLE へ進む)。
        let mut proc: SpawnedTuner = {
            let s = self.slot_mut(index)?;
            s.process.take().expect("checked")
        };
        if let Err(e) = stop_process(&mut proc).await {
            tracing::warn!(index, error = %e, "tuner stop failed");
        }
        release_wait(false).await;
        let s = self.slot_mut(index)?;
        s.pid = None;
        s.current_channel = None;
        s.wanted_channel = None;
        s.use_count = 0;
        if !s.state.is_terminal() {
            s.state = TunerState::Idle;
        }
        tracing::info!(index, "tuner stopped");
        Ok(())
    }

    /// 要求を1つ手放す。残存要求がなければ停止する。
    pub async fn release(&mut self, index: usize) -> Result<(), String> {
        let remaining = {
            let s = self.slot_mut(index)?;
            s.use_count = s.use_count.saturating_sub(1);
            if s.use_count > 0 {
                // 残存要求あり: 同一ch継続のためプロセスは残す。
                return Ok(());
            }
            s.wanted_channel = None;
            s.use_count
        };
        let _ = remaining;
        self.stop(index).await
    }

    /// stdout を Stream Manager (Phase3) へ引き渡す。
    pub fn take_stdout(
        &mut self,
        index: usize,
    ) -> Result<Option<tokio::process::ChildStdout>, String> {
        let s = self.slot_mut(index)?;
        Ok(s.process.as_mut().and_then(|p| p.take_stdout()))
    }

    /// 稼働プロセスの終了をノンブロッキング確認する。
    /// 異常終了 + 残存要求があれば同一chで respawn する。
    /// 戻り値は (終了していたか, respawn後のpid)。
    pub async fn poll_exit(
        &mut self,
        index: usize,
    ) -> Result<(bool, Option<u32>), String> {
        let exited_status = {
            let s = self.slot_mut(index)?;
            match s.process.as_mut() {
                None => return Ok((false, None)),
                Some(p) => p.try_wait().map_err(|e| e.to_string())?,
            }
        };
        let Some(status) = exited_status else {
            return Ok((false, None));
        };
        let success = status.success();
        tracing::warn!(index, success, %status, "tuner process exited");

        // 終了済みプロセスを回収する。
        let (wanted, use_count) = {
            let s = self.slot_mut(index)?;
            s.process = None;
            s.pid = None;
            s.current_channel = None;
            (s.wanted_channel.clone(), s.use_count)
        };

        if success && use_count == 0 {
            let s = self.slot_mut(index)?;
            s.record_success();
            if !s.state.is_terminal() {
                s.state = TunerState::Idle;
            }
            release_wait(false).await;
            return Ok((true, None));
        }

        if success {
            // 残存要求ありの正常終了 (外部要因) は同一chで respawn。
            release_wait(false).await;
            if let Some(ch) = wanted {
                // use_count を二重加算しないよう一旦戻して start 相当を行う。
                {
                    let s = self.slot_mut(index)?;
                    if s.state.is_terminal() {
                        return Ok((true, None));
                    }
                    s.use_count = use_count.saturating_sub(1);
                }
                // start() が use_count を +1 するので残存数は維持される。
                match self.start(index, &ch).await {
                    Ok(pid) => return Ok((true, Some(pid))),
                    Err(e) => return Err(e),
                }
            }
            return Ok((true, None));
        }

        // 異常終了。
        let fault = {
            let s = self.slot_mut(index)?;
            s.record_failure()
        };
        if fault == TunerState::Fault {
            return Err(format!(
                "tuner {index} failed {MAX_CONSECUTIVE_ERRORS} times; FAULT, restart required"
            ));
        }
        release_wait(true).await;
        // 残存要求があれば同一chで respawn、なければ IDLE。
        let wanted_now = self.slot(index)?.wanted_channel.clone();
        let demand = self.slot(index)?.use_count;
        if demand > 0 {
            if let Some(ch) = wanted_now {
                {
                    let s = self.slot_mut(index)?;
                    s.state = TunerState::Idle;
                    // start() が +1 するため一旦戻す。
                    s.use_count = demand.saturating_sub(1);
                }
                match self.start(index, &ch).await {
                    Ok(pid) => return Ok((true, Some(pid))),
                    Err(e) => return Err(e),
                }
            }
        }
        let s = self.slot_mut(index)?;
        if s.state == TunerState::Error {
            s.state = TunerState::Idle;
        }
        Ok((true, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChannelType;
    use std::collections::HashMap;

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

    #[test]
    fn new_marks_missing_command_disabled() {
        let m = TunerManager::new(vec![
            tuner_with_command("ok", Some("recpt1 <channel> - -")),
            tuner_with_command("ng", None),
            tuner_with_command("blank", Some("   ")),
        ]);
        assert_eq!(m.state(0).unwrap(), TunerState::Idle);
        assert_eq!(m.state(1).unwrap(), TunerState::Disabled);
        assert_eq!(m.state(2).unwrap(), TunerState::Disabled);
    }

    #[test]
    fn three_failures_become_fault() {
        let mut m =
            TunerManager::new(vec![tuner_with_command("t", Some("recpt1 <channel>"))]);
        let s = m.slot_mut(0).unwrap();
        assert_eq!(s.record_failure(), TunerState::Error);
        assert_eq!(s.record_failure(), TunerState::Error);
        assert_eq!(s.record_failure(), TunerState::Fault);
        assert!(s.state.needs_restart());
    }

    #[test]
    fn physical_channel_prefers_tuner_channels() {
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
        assert_eq!(m.physical_channel_for(0, &ch).unwrap(), "13");
    }

    #[tokio::test]
    async fn start_stop_with_sleep() {
        let mut m =
            TunerManager::new(vec![tuner_with_command("t", Some("sleep 30"))]);
        let pid = m.start(0, "13").await.expect("spawn sleep");
        assert!(pid > 0);
        assert_eq!(m.state(0).unwrap(), TunerState::Streaming);
        m.stop(0).await.expect("stop");
        assert_eq!(m.state(0).unwrap(), TunerState::Idle);
    }

    #[tokio::test]
    async fn acquire_same_channel_shares() {
        let mut m =
            TunerManager::new(vec![tuner_with_command("t", Some("sleep 30"))]);
        let a = m.acquire(0, "13").await.unwrap();
        let b = m.acquire(0, "13").await.unwrap();
        assert_eq!(a, b);
        assert_eq!(m.use_count(0).unwrap(), 2);
        m.release(0).await.unwrap();
        // 残存要求ありのためプロセス継続。
        assert_eq!(m.state(0).unwrap(), TunerState::Streaming);
        m.release(0).await.unwrap();
        assert_eq!(m.state(0).unwrap(), TunerState::Idle);
    }
}
