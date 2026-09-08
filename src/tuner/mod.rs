//! Tuner Manager (Phase2, SPEC §8).
//!
//! - 状態: IDLE / STARTING / TUNING / STREAMING / STOPPING / ERROR / DISABLED + FAULT
//!   (FAULT = 3連続失敗。要再起動)
//! - `spawn(program, args)` のみ。shell 禁止 (`sh -c` を使わない)。
//! - `<channel>` は物理ch解決後に代入。未知変数は空文字。
//! - stdout / stderr 取得・PID 管理。stderr はログのみ。
//! - 停止は SIGTERM → 6秒 → SIGKILL。dvbv5系のみ即KILL。
//! - release は 100ms (正常) / 1000ms (異常)。
//! - 残存要求があれば同一chで respawn。起動成功は spawn 成功で即時確定
//!   (初回バイト待ちをしない)。

pub mod channel;
pub mod command;
pub mod manager;
pub mod process;
pub mod state;

pub use channel::resolve_physical_channel;
pub use command::{
    build_passthrough_command, build_tuner_command, expand_channel_vars, shell_split,
};
pub use manager::{
    MAX_CONSECUTIVE_ERRORS, RELEASE_FAST, RELEASE_SLOW, SharedTunerManager,
    TunerManager, TunerSlot, release_wait,
};
pub use process::{
    STOP_GRACE, SpawnedTuner, is_immediate_kill_program, spawn_program, stop_process,
};
pub use state::TunerState;
