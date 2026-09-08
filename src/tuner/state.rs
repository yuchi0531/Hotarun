//! チューナー状態 (SPEC §8 + FAULT)。
//!
//! ```text
//! IDLE → STARTING → TUNING → STREAMING → STOPPING → IDLE
//!                              ↓ 異常終了 / spawn失敗
//!                             ERROR → (1000ms) → IDLE
//!                               ↓ 3連続失敗
//!                             FAULT (要再起動・端末状態)
//! DISABLED は設定不正・手動無効の端末状態。
//! ```

use std::fmt;
use std::str::FromStr;

/// チューナー状態。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TunerState {
    #[default]
    Idle,
    Starting,
    Tuning,
    Streaming,
    Stopping,
    Error,
    Disabled,
    /// 3連続失敗。サーバー再起動が必要な端末状態。
    Fault,
}

impl TunerState {
    /// Mirakurun互換の文字列表現 (大文字)。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Idle => "IDLE",
            Self::Starting => "STARTING",
            Self::Tuning => "TUNING",
            Self::Streaming => "STREAMING",
            Self::Stopping => "STOPPING",
            Self::Error => "ERROR",
            Self::Disabled => "DISABLED",
            Self::Fault => "FAULT",
        }
    }

    /// 新規ストリームを受け付け可能な状態か。IDLE のみ。
    pub fn can_accept(&self) -> bool {
        matches!(self, Self::Idle)
    }

    /// プロセス稼働中 (排他利用中) か。
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Tuning | Self::Streaming | Self::Stopping
        )
    }

    /// サーバー再起動が必要な端末状態か。
    pub fn needs_restart(&self) -> bool {
        matches!(self, Self::Fault)
    }

    /// 再利用前の release 待ちなしに再開できない端末状態か。
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Disabled | Self::Fault)
    }
}

impl fmt::Display for TunerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TunerState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "IDLE" => Ok(Self::Idle),
            "STARTING" => Ok(Self::Starting),
            "TUNING" => Ok(Self::Tuning),
            "STREAMING" => Ok(Self::Streaming),
            "STOPPING" => Ok(Self::Stopping),
            "ERROR" => Ok(Self::Error),
            "DISABLED" => Ok(Self::Disabled),
            "FAULT" => Ok(Self::Fault),
            other => Err(format!("unknown tuner state: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_strings_roundtrip() {
        for s in [
            TunerState::Idle,
            TunerState::Starting,
            TunerState::Tuning,
            TunerState::Streaming,
            TunerState::Stopping,
            TunerState::Error,
            TunerState::Disabled,
            TunerState::Fault,
        ] {
            assert_eq!(s.to_string(), s.as_str());
            assert_eq!(s.as_str().parse::<TunerState>().unwrap(), s);
        }
    }

    #[test]
    fn only_idle_accepts_and_fault_needs_restart() {
        assert!(TunerState::Idle.can_accept());
        assert!(!TunerState::Streaming.can_accept());
        assert!(!TunerState::Error.can_accept());
        assert!(TunerState::Fault.needs_restart());
        assert!(!TunerState::Idle.needs_restart());
        assert!(TunerState::Fault.is_terminal());
        assert!(TunerState::Disabled.is_terminal());
        assert!(!TunerState::Idle.is_terminal());
    }
}
