use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Deserializer, Serialize};
use tokio::sync::{Mutex, Notify};
use std::collections::VecDeque;

/// 放送種別 (§4§5§6)。
/// `GR|BS|CS|SKY` + 拡張 `BS4K` のみ受け付け、それ以外はデシリアライズ失敗
/// (= ローダ側で当該エントリを skip) する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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

/// A service stored in a channel's optional `services` extension.  The
/// primary service remains represented by `serviceId`/`name` on `Channel` so
/// existing Mirakurun configuration stays readable; additional services use
/// this compact, flattened representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct ChannelService {
    pub serviceId: i64,
    pub networkId: i64,
    pub name: String,
    #[serde(default, rename = "serviceType")]
    pub service_type: i64,
}

/// Return the services represented by one physical channel.  Invalid or
/// duplicate extension entries are ignored here; the config save validator
/// reports duplicates in the submitted document before it is written.
pub fn channel_services(channel: &Channel) -> Vec<ChannelService> {
    let network_id = numeric_extra(channel, "networkId").unwrap_or(0);
    let service_type = numeric_extra(channel, "serviceType")
        .or_else(|| numeric_extra(channel, "service_type"))
        .unwrap_or(0);
    let mut services = Vec::new();
    if let Some(service_id) = channel.serviceId {
        services.push(ChannelService {
            serviceId: service_id,
            networkId: network_id,
            name: channel.name.clone(),
            service_type,
        });
    }
    if let Some(value) = channel.extra.get("services") {
        if let Ok(additional) = serde_json::from_value::<Vec<ChannelService>>(value.clone()) {
            for service in additional {
                if !services.iter().any(|item| {
                    item.serviceId == service.serviceId && item.networkId == service.networkId
                }) {
                    services.push(service);
                }
            }
        }
    }
    services
}

/// Mirakurun stores both components as unsigned 16-bit ARIB identifiers.
pub fn valid_service_item_component(value: i64) -> bool { (0..=i64::from(u16::MAX)).contains(&value) }

fn numeric_extra(channel: &Channel, key: &str) -> Option<i64> {
    channel.extra.get(key).and_then(|value| match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(value) => value.parse().ok(),
        _ => None,
    })
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

/// Runtime listener and operational settings from `server.yml` (§18).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[allow(non_snake_case)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<String>,
    #[serde(default, rename = "CIDR", alias = "cidr", alias = "cidrs")]
    pub cidr: Vec<String>,
    /// Legacy compatibility setting. It is retained in config/API output but
    /// is not used to authorize TCP clients.
    #[serde(default, rename = "adminCIDR", alias = "adminCidr", alias = "admin_cidr")]
    pub admin_cidr: Vec<String>,
    #[serde(default = "default_log_level", rename = "logLevel", alias = "log_level")]
    pub log_level: i8,
    #[serde(default = "default_max_log_history", rename = "maxLogHistory", alias = "max_log_history")]
    pub max_log_history: usize,
}

fn default_port() -> u16 { 40_772 }
fn default_log_level() -> i8 { 1 }
fn default_max_log_history() -> usize { 1_000 }

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            socket: None,
            cidr: Vec::new(),
            admin_cidr: Vec::new(),
            log_level: default_log_level(),
            max_log_history: default_max_log_history(),
        }
    }
}

impl ServerConfig {
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.socket.as_deref().is_some_and(|socket| socket.trim().is_empty()) {
            errors.push("socket must not be empty".to_owned());
        }
        if self.port == 0 && self.socket.is_none() {
            errors.push("port must be between 1 and 65535 when socket is not set".to_owned());
        }
        if !(-1..=3).contains(&self.log_level) {
            errors.push("logLevel must be between -1 and 3".to_owned());
        }
        for value in &self.cidr {
            if !valid_cidr(value) {
                errors.push(format!("invalid CIDR: {value}"));
            }
        }
        for value in &self.admin_cidr {
            if !valid_cidr(value) {
                errors.push(format!("invalid adminCIDR: {value}"));
            }
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

fn valid_cidr(value: &str) -> bool {
    let Some((address, prefix)) = value.trim().split_once('/') else { return false };
    let Ok(prefix) = prefix.parse::<u8>() else { return false };
    if address.parse::<std::net::Ipv4Addr>().is_ok() {
        prefix <= 32
    } else {
        address.parse::<std::net::Ipv6Addr>().is_ok() && prefix <= 128
    }
}

#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct LogEntry {
    pub timestamp: u64,
    pub level: i8,
    pub message: String,
}

#[cfg(test)]
mod server_tests {
    use super::*;

    #[test]
    fn server_config_validates_ranges_and_cidr() {
        let valid = ServerConfig { port: 40772, socket: None, cidr: vec!["192.168.0.0/16".into()], admin_cidr: vec!["192.168.1.0/24".into()], log_level: 3, max_log_history: 10 };
        assert!(valid.validate().is_ok());
        let invalid = ServerConfig { port: 0, socket: None, cidr: vec!["broken".into()], admin_cidr: vec!["broken".into()], log_level: 4, max_log_history: 0 };
        assert!(invalid.validate().is_err());
        for socket in ["", "   ", "\t\n"] {
            let invalid = ServerConfig { socket: Some(socket.to_owned()), ..ServerConfig::default() };
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn duplicate_channel_identity_is_reported_without_yaml_precedence() {
        let channel = Channel {
            name: "first".into(), channel_type: ChannelType::GR, channel: "27".into(),
            serviceId: None, tunerChannels: None, extra: HashMap::new(),
        };
        let mut second = channel.clone();
        second.name = "second".into();
        let errors = validate_channel_pairs(&[channel, second]).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("duplicate channel identity"));
    }

    #[test]
    fn service_item_components_reject_negative_large_and_arithmetic_collisions() {
        let make = |network_id: i64, service_id: i64| Channel {
            name: "service".into(),
            channel_type: ChannelType::GR,
            channel: service_id.to_string(),
            serviceId: Some(service_id),
            tunerChannels: None,
            extra: HashMap::from([(String::from("networkId"), serde_json::json!(network_id))]),
        };
        assert!(validate_service_item_ids(&[make(1, 2), make(0, 100002)]).is_err());
        assert!(validate_service_item_ids(&[make(-1, 2)]).is_err());
        assert!(validate_service_item_ids(&[make(65536, 2)]).is_err());
        assert!(validate_service_item_ids(&[make(1, 65536)]).is_err());
    }
}

/// 起動時に読み込み、メモリ保持する設定全体 (ホットリロードなし)。
/// `manager` は Scheduler / Stream (Phase3 §8§9§10) 用の共有 TunerManager。
#[derive(Clone)]
pub struct AppState {
    pub channels: Vec<Channel>,
    pub tuners: Vec<Tuner>,
    pub manager: crate::tuner::SharedTunerManager,
    pub decoders: std::sync::Arc<crate::tuner::DecoderRegistry>,
    pub server: ServerConfig,
    pub config_dir: Option<PathBuf>,
    pub restart: Arc<Notify>,
    pub restart_requested: Arc<AtomicBool>,
    pub scan: Arc<crate::scan::ScanManager>,
    /// Serializes channel configuration writes with scan commits.
    pub channel_config_lock: Arc<Mutex<()>>,
    pub logs: Arc<Mutex<VecDeque<LogEntry>>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("channels", &self.channels)
            .field("tuners", &self.tuners)
            .field("manager_len", &self.manager.len())
            .field("server", &self.server)
            .finish()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            channels: Vec::new(),
            tuners: Vec::new(),
            manager: crate::tuner::TunerManager::shared(Vec::new()),
            decoders: crate::tuner::DecoderRegistry::new(),
            server: ServerConfig::default(),
            config_dir: None,
            restart: Arc::new(Notify::new()),
            restart_requested: Arc::new(AtomicBool::new(false)),
            scan: crate::scan::ScanManager::new(),
            channel_config_lock: Arc::new(Mutex::new(())),
            logs: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl AppState {
    pub fn load_from_dir(dir: &std::path::Path) -> Self {
        let mut channels =
            load_list::<Channel>(&dir.join("channels.yml"), "channels");
        reject_duplicate_channel_pairs(&mut channels);
        reject_invalid_service_item_ids(&mut channels);
        reject_duplicate_service_ids(&mut channels);
        let tuners = load_list::<Tuner>(&dir.join("tuners.yml"), "tuners");
        let server = load_server_config(&dir.join("server.yml"));
        let manager = crate::tuner::TunerManager::shared(tuners.clone());
        Self {
            channels,
            tuners,
            manager,
            decoders: crate::tuner::DecoderRegistry::new(),
            server,
            config_dir: Some(dir.to_path_buf()),
            restart: Arc::new(Notify::new()),
            restart_requested: Arc::new(AtomicBool::new(false)),
            scan: crate::scan::ScanManager::new(),
            channel_config_lock: Arc::new(Mutex::new(())),
            logs: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// テスト用: channels/tuners から state を組み立てる。
    pub fn from_lists(channels: Vec<Channel>, tuners: Vec<Tuner>) -> Self {
        let mut channels = channels;
        reject_duplicate_channel_pairs(&mut channels);
        let manager = crate::tuner::TunerManager::shared(tuners.clone());
        Self {
            channels,
            tuners,
            manager,
            decoders: crate::tuner::DecoderRegistry::new(),
            server: ServerConfig::default(),
            config_dir: None,
            restart: Arc::new(Notify::new()),
            restart_requested: Arc::new(AtomicBool::new(false)),
            scan: crate::scan::ScanManager::new(),
            channel_config_lock: Arc::new(Mutex::new(())),
            logs: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Read the current channels file after taking the channel configuration
    /// lock.  AppState remains a startup snapshot by design.
    pub fn reload_channels(&self) -> Vec<Channel> {
        let Some(directory) = self.config_dir.as_deref() else {
            return self.channels.clone();
        };
        load_channels_config(&directory.join("channels.yml"))
    }

    pub async fn record_log(&self, level: i8, message: impl Into<String>) {
        let message = message.into();
        match level {
            0 => tracing::error!("{message}"),
            1 => tracing::info!("{message}"),
            2 => tracing::debug!("{message}"),
            3 => tracing::trace!("{message}"),
            _ => {}
        }
        let mut logs = self.logs.lock().await;
        logs.push_back(LogEntry {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or_default(),
            level,
            message,
        });
        while logs.len() > self.server.max_log_history {
            logs.pop_front();
        }
    }

    pub async fn log_entries(&self) -> Vec<LogEntry> {
        self.logs.lock().await.iter().cloned().collect()
    }
}

pub fn load_server_config(path: &Path) -> ServerConfig {
    match load_server_config_checked(path) {
        Ok(config) => config,
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "server config is invalid; using defaults");
            ServerConfig::default()
        }
    }
}

/// Load the server listener configuration without hiding an invalid value.
/// Startup uses this form so a bad socket cannot reach the bind path and
/// become a panic; the compatibility wrapper above remains useful for the
/// in-memory configuration loader.
pub fn load_server_config_checked(path: &Path) -> Result<ServerConfig, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ServerConfig::default()),
        Err(error) => {
            return Err(format!("server config could not be read: {error}"));
        }
    };
    let config: ServerConfig = serde_yaml::from_str(&text)
        .map_err(|error| format!("server config could not be parsed: {error}"))?;
    config.validate().map_err(|errors| errors.join("; "))?;
    Ok(config)
}

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub fn write_yaml_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), String> {
    let parent = path.parent().ok_or_else(|| "configuration path has no parent".to_owned())?;
    std::fs::create_dir_all(parent).map_err(|error| format!("create config directory: {error}"))?;
    let text = serde_yaml::to_string(value).map_err(|error| format!("serialize config: {error}"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| "configuration path has no file name".to_owned())?;
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&temporary, text).map_err(|error| format!("write temporary config: {error}"))?;
    std::fs::rename(&temporary, path).map_err(|error| format!("replace config: {error}"))
}

/// Load and sanitize the current channels file. The caller serializes this
/// read with channel configuration writes.
pub fn load_channels_config(path: &Path) -> Vec<Channel> {
    let mut channels = load_list::<Channel>(path, "channels");
    reject_duplicate_channel_pairs(&mut channels);
    reject_invalid_service_item_ids(&mut channels);
    reject_duplicate_service_ids(&mut channels);
    channels
}

fn reject_duplicate_service_ids(channels: &mut Vec<Channel>) {
    let mut counts = HashMap::new();
    for channel in channels.iter() {
        for service in channel_services(channel) {
            let Some(id) = crate::routes::api::try_service_item_id(service.networkId, service.serviceId) else { continue };
            *counts
                .entry(id)
                .or_insert(0usize) += 1;
        }
    }
    let duplicates: std::collections::HashSet<_> = counts
        .iter()
        .filter_map(|(key, count)| (*count > 1).then_some(*key))
        .collect();
    if duplicates.is_empty() {
        return;
    }
    tracing::error!(ids = ?duplicates, "duplicate ServiceItemId in channels config; rejecting colliding service records");
    channels.retain(|channel| {
        !channel_services(channel)
            .iter()
            .any(|service| crate::routes::api::try_service_item_id(service.networkId, service.serviceId).is_some_and(|id| duplicates.contains(&id)))
    });
}

fn reject_invalid_service_item_ids(channels: &mut Vec<Channel>) {
    let before = channels.len();
    channels.retain(|channel| {
        let network_valid = channel.extra.get("networkId").is_none_or(|_value| {
            numeric_extra(channel, "networkId").is_some_and(valid_service_item_component)
        });
        let primary_valid = channel.serviceId.is_none_or(valid_service_item_component);
        let additional_valid = channel.extra.get("services").is_none_or(|value| {
            serde_json::from_value::<Vec<ChannelService>>(value.clone()).ok().is_some_and(|services| {
                services.iter().all(|service| valid_service_item_component(service.networkId) && valid_service_item_component(service.serviceId))
            })
        });
        network_valid && primary_valid && additional_valid
    });
    if channels.len() != before {
        tracing::error!(removed = before - channels.len(), "invalid ServiceItemId components in channels config; entries rejected");
    }
}

/// ServiceItemId is globally unique even though several services may share a
/// physical `(type, channel)` record.  Validate the primary service and the
/// optional aggregated services before a config file is written.
pub fn validate_service_item_ids(channels: &[Channel]) -> Result<(), Vec<String>> {
    let mut seen: HashMap<i64, usize> = HashMap::new();
    let mut errors = Vec::new();
    for (channel_index, channel) in channels.iter().enumerate() {
        let mut services = Vec::new();
        let network_id = match channel.extra.get("networkId") {
            None => 0,
            Some(_) => match numeric_extra(channel, "networkId") {
                Some(value) if valid_service_item_component(value) => value,
                _ => {
                    errors.push(format!("invalid networkId at channel index {channel_index}"));
                    continue;
                }
            },
        };
        if let Some(service_id) = channel.serviceId {
            services.push((network_id, service_id));
        }
        if let Some(value) = channel.extra.get("services") {
            match serde_json::from_value::<Vec<ChannelService>>(value.clone()) {
                Ok(additional) => services.extend(additional.into_iter().map(|service| (service.networkId, service.serviceId))),
                Err(_) => errors.push(format!("invalid services list at channel index {channel_index}")),
            }
        }
        for (network_id, service_id) in services {
            if !valid_service_item_component(network_id) || !valid_service_item_component(service_id) {
                errors.push(format!("networkId/serviceId must be between 0 and 65535 at channel index {channel_index}"));
                continue;
            }
            let Some(id) = crate::routes::api::try_service_item_id(network_id, service_id) else {
                errors.push(format!("ServiceItemId arithmetic overflow at channel index {channel_index}"));
                continue;
            };
            if let Some(previous) = seen.insert(id, channel_index) {
                errors.push(format!(
                    "duplicate ServiceItemId {} at channel indexes {} and {}",
                    id, previous, channel_index
                ));
            }
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

/// `(type, channel)` is the channel identity.  There is deliberately no
/// first/last-entry precedence: every colliding entry is rejected so a YAML
/// ordering change cannot silently change the selected command or service.
pub fn validate_channel_pairs(channels: &[Channel]) -> Result<(), Vec<String>> {
    let mut seen = HashMap::new();
    let mut errors = Vec::new();
    for (index, channel) in channels.iter().enumerate() {
        let key = (channel.channel_type, channel.channel.clone());
        if let Some(previous) = seen.insert(key.clone(), index) {
            errors.push(format!(
                "duplicate channel identity ({:?}, {}) at indexes {} and {}",
                key.0, key.1, previous, index
            ));
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors) }
}

fn reject_duplicate_channel_pairs(channels: &mut Vec<Channel>) {
    let mut counts = HashMap::new();
    for channel in channels.iter() {
        *counts
            .entry((channel.channel_type, channel.channel.clone()))
            .or_insert(0usize) += 1;
    }
    let duplicates: std::collections::HashSet<_> = counts
        .into_iter()
        .filter_map(|(key, count)| (count > 1).then_some(key))
        .collect();
    if duplicates.is_empty() {
        return;
    }
    tracing::error!(identities = ?duplicates, "duplicate (type, channel) entries rejected");
    channels.retain(|channel| {
        !duplicates.contains(&(channel.channel_type, channel.channel.clone()))
    });
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
