use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};

/// 放送種別 (§4§5§6)。
/// `GR|BS|CS|SKY` + 拡張 `BS4K` のみ受け付け、それ以外はデシリアライズ失敗
/// (= ローダ側で当該エントリを skip) する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ChannelType {
    GR,
    BS,
    CS,
    SKY,
    BS4K,
}

/// YAML 上で string / integer / float が混在しうる箇所を string として吸収する。
/// bool は吸収しない (型不正として skip させる)。
fn deserialize_string_coerce<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrInt;
    impl<'de> serde::de::Visitor<'de> for StringOrInt {
        type Value = String;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a string, integer or float")
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<String, E> {
            Ok(v.to_owned())
        }
        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<String, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<String, E> {
            Ok(v.to_string())
        }
        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<String, E> {
            Ok(v.to_string())
        }
    }
    deserializer.deserialize_any(StringOrInt)
}

fn deserialize_string_map_coerce<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<HashMap<String, serde_yaml::Value>> =
        Option::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(m) => {
            let mut out = HashMap::with_capacity(m.len());
            for (k, v) in m {
                let s = match v {
                    serde_yaml::Value::String(s) => s,
                    // Number は int / float ともに吸収する。
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Null => String::new(),
                    // bool は吸収せず型不正として skip させる。
                    serde_yaml::Value::Bool(b) => {
                        return Err(serde::de::Error::invalid_type(
                            serde::de::Unexpected::Bool(b),
                            &"a string, integer or float",
                        ));
                    }
                    other => serde_yaml::to_string(&other)
                        .unwrap_or_default()
                        .trim()
                        .to_owned(),
                };
                out.insert(k, s);
            }
            Ok(Some(out))
        }
    }
}

/// channels.yml の1エントリ (§5)。
/// 必須は `name`, `type`, `channel` (`channel` は string 扱い)。
/// 識別子は `(type, channel)` ペア。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct Channel {
    pub name: String,
    #[serde(rename = "type")]
    pub channel_type: ChannelType,
    #[serde(deserialize_with = "deserialize_string_coerce")]
    pub channel: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub serviceId: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_string_map_coerce"
    )]
    pub tunerChannels: Option<HashMap<String, String>>,
    /// 未知フィールドは保持して素読み返却できるようにする。
    #[serde(flatten, default)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// tuners.yml の1エントリ (§6)。
/// 必須は `name`, `types`。`command` は Hotarun では実質必須だが
/// パース時点では任意とし、型不正エントリのみ skip する。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct Tuner {
    pub name: String,
    pub types: Vec<ChannelType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(
        default,
        rename = "tlvDecoder",
        skip_serializing_if = "Option::is_none"
    )]
    pub tlv_decoder: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decoder: Option<String>,
    /// 未知フィールドは保持して素読み返却できるようにする。
    #[serde(flatten, default)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// 起動時に読み込み、メモリ保持する設定全体 (ホットリロードなし)。
/// `manager` は Scheduler / Stream (Phase3 §8§9§10) 用の共有 TunerManager。
#[derive(Clone)]
pub struct AppState {
    pub channels: Vec<Channel>,
    pub tuners: Vec<Tuner>,
    pub manager: std::sync::Arc<tokio::sync::Mutex<crate::tuner::TunerManager>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // async実行中でもpanicしないよう try_lock のみ使う。
        let manager_len = self.manager.try_lock().map(|m| m.len());
        f.debug_struct("AppState")
            .field("channels", &self.channels)
            .field("tuners", &self.tuners)
            .field("manager_len", &manager_len)
            .finish()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            channels: Vec::new(),
            tuners: Vec::new(),
            manager: std::sync::Arc::new(tokio::sync::Mutex::new(
                crate::tuner::TunerManager::new(Vec::new()),
            )),
        }
    }
}

impl AppState {
    pub fn load_from_dir(dir: &std::path::Path) -> Self {
        let channels =
            load_list::<Channel>(&dir.join("channels.yml"), "channels");
        let tuners = load_list::<Tuner>(&dir.join("tuners.yml"), "tuners");
        let manager = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::tuner::TunerManager::new(tuners.clone()),
        ));
        Self {
            channels,
            tuners,
            manager,
        }
    }

    /// テスト用: channels/tuners から state を組み立てる。
    #[cfg(test)]
    pub fn from_lists(channels: Vec<Channel>, tuners: Vec<Tuner>) -> Self {
        let manager = std::sync::Arc::new(tokio::sync::Mutex::new(
            crate::tuner::TunerManager::new(tuners.clone()),
        ));
        Self {
            channels,
            tuners,
            manager,
        }
    }
}

/// YAML シーケンスを1件ずつパースし、不正エントリは warn + skip する (§6)。
fn load_list<T>(path: &std::path::Path, kind: &str) -> Vec<T>
where
    T: for<'de> Deserialize<'de>,
{
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "{kind} config not found, using empty list");
            return Vec::new();
        }
    };
    if text.trim().is_empty() {
        return Vec::new();
    }
    let raw: Vec<serde_yaml::Value> = match serde_yaml::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "{kind} config parse failed, using empty list");
            return Vec::new();
        }
    };
    let mut out = Vec::with_capacity(raw.len());
    for (i, v) in raw.into_iter().enumerate() {
        match serde_yaml::from_value::<T>(v) {
            Ok(item) => out.push(item),
            Err(e) => {
                tracing::warn!(path = %path.display(), index = i, error = %e, "skip invalid {kind} entry (type check)");
            }
        }
    }
    tracing::info!(path = %path.display(), count = out.len(), "{kind} loaded");
    out
}
