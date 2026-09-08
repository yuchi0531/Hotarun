//! 物理チャンネル解決 (SPEC §5§6§9)。
//!
//! ```text
//! tunerChannels[tuner.name] が存在 → その値を物理チャンネルとして使用
//! 存在しない → channel (論理チャンネル) を使用
//! ```

use crate::config::{Channel, Tuner};

/// `resolve_channel(channel, tuner)`。物理チャンネル文字列を返す。
pub fn resolve_physical_channel(channel: &Channel, tuner: &Tuner) -> String {
    if let Some(map) = channel.tunerChannels.as_ref() {
        if let Some(v) = map.get(&tuner.name) {
            return v.clone();
        }
    }
    channel.channel.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ChannelType;
    use std::collections::HashMap;

    fn test_channel() -> Channel {
        Channel {
            name: "sample".to_owned(),
            channel_type: ChannelType::GR,
            channel: "27".to_owned(),
            serviceId: None,
            tunerChannels: Some(HashMap::from([
                ("DVB-C-0".to_owned(), "13".to_owned()),
                ("DVB-C-1".to_owned(), "27".to_owned()),
            ])),
            extra: HashMap::new(),
        }
    }

    fn test_tuner(name: &str) -> Tuner {
        Tuner {
            name: name.to_owned(),
            types: vec![ChannelType::GR],
            command: Some("recpt1 <channel> - -".to_owned()),
            tlv_decoder: None,
            decoder: None,
            extra: HashMap::new(),
        }
    }

    #[test]
    fn mapped_tuner_uses_tuner_channels() {
        let ch = test_channel();
        assert_eq!(
            resolve_physical_channel(&ch, &test_tuner("DVB-C-0")),
            "13"
        );
    }

    #[test]
    fn unmapped_tuner_falls_back_to_logical() {
        let ch = test_channel();
        assert_eq!(
            resolve_physical_channel(&ch, &test_tuner("PT3-0")),
            "27"
        );
    }
}
