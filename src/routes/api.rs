//! 一覧系 API (SPEC §11)。設定は起動時に読み込んだスナップショットを返す。

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{Json, Router, extract::{Path, Query, State}, routing::get};
use serde::Serialize;

use crate::{
    config::{AppState, Channel, ChannelType},
    error::ApiError,
    tuner::TunerState,
};

/// Mirakurun の ServiceItemId。networkId と serviceId の混同を避けるため、
/// serviceId そのものではなく networkId + 5桁の serviceId で作る。
pub fn service_item_id(network_id: i64, service_id: i64) -> i64 {
    network_id.saturating_mul(100_000).saturating_add(service_id)
}

#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct ServiceChannel {
    #[serde(rename = "type")]
    pub channel_type: ChannelType,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct ServiceItem {
    pub id: i64,
    pub serviceId: i64,
    pub networkId: i64,
    pub name: String,
    /// 設定にサービス種別がない場合は 0。MVPでは設定された値を素返しする。
    #[serde(rename = "type")]
    pub service_type: i64,
    pub channel: ServiceChannel,
}

fn extra_i64(channel: &Channel, key: &str) -> Option<i64> {
    match channel.extra.get(key) {
        Some(serde_json::Value::Number(n)) => n.as_i64(),
        Some(serde_json::Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

fn channel_type_name(channel_type: ChannelType) -> &'static str {
    match channel_type {
        ChannelType::GR => "GR",
        ChannelType::BS => "BS",
        ChannelType::CS => "CS",
        ChannelType::SKY => "SKY",
        ChannelType::BS4K => "BS4K",
    }
}

fn parse_channel_type(value: &str) -> Option<ChannelType> {
    match value.to_ascii_uppercase().as_str() {
        "GR" => Some(ChannelType::GR),
        "BS" => Some(ChannelType::BS),
        "CS" => Some(ChannelType::CS),
        "SKY" => Some(ChannelType::SKY),
        "BS4K" => Some(ChannelType::BS4K),
        _ => None,
    }
}

fn find_channel(channels: &[Channel], channel_type: ChannelType, channel: &str) -> Option<Channel> {
    channels
        .iter()
        .find(|item| item.channel_type == channel_type && item.channel == channel)
        .cloned()
}

#[derive(Debug, Clone, Serialize)]
pub struct VersionResponse {
    pub current: &'static str,
    pub latest: &'static str,
}

/// GET /api/version。外部照会はせず、current/latestともパッケージ版を返す。
async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        current: env!("CARGO_PKG_VERSION"),
        latest: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusResponse {
    pub status: &'static str,
    pub version: &'static str,
    /// Unix epoch milliseconds。Mirakurunの時刻表現に合わせる。
    pub time: u64,
    pub pid: u32,
    pub tuners: StatusTuners,
    pub streams: StatusStreams,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusTuners {
    pub total: usize,
    pub active: usize,
    pub available: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusStreams {
    pub active: usize,
}

/// GET /api/status。設定と現在の tuner manager から最小限の稼働状況を返す。
async fn status(State(state): State<Arc<AppState>>) -> Result<Json<StatusResponse>, ApiError> {
    let mut active_tuners = 0;
    let mut available_tuners = 0;
    let mut active_streams = 0;

    for index in 0..state.manager.len() {
        let tuner_state = state
            .manager
            .state(index)
            .await
            .map_err(|e| ApiError::new(500, format!("failed to read tuner state: {e}")))?;
        let use_count = state
            .manager
            .use_count(index)
            .await
            .map_err(|e| ApiError::new(500, format!("failed to read tuner users: {e}")))?;

        if tuner_state.is_active() {
            active_tuners += 1;
        }
        if tuner_state.can_accept() {
            available_tuners += 1;
        }
        active_streams += use_count;
    }

    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| ApiError::new(500, format!("failed to read system time: {e}")))?
        .as_millis() as u64;

    Ok(Json(StatusResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
        time,
        pid: std::process::id(),
        tuners: StatusTuners {
            total: state.tuners.len(),
            active: active_tuners,
            available: available_tuners,
        },
        streams: StatusStreams {
            active: active_streams,
        },
    }))
}

/// GET /api/channels/:type/:channel。設定済みの論理チャンネルを返す。
async fn get_channel(
    State(state): State<Arc<AppState>>,
    Path((channel_type_raw, channel)): Path<(String, String)>,
) -> Result<Json<Channel>, ApiError> {
    let Some(channel_type) = parse_channel_type(&channel_type_raw) else {
        return Err(ApiError::not_found(format!(
            "channel not found: {channel_type_raw}/{channel}"
        )));
    };
    find_channel(&state.channels, channel_type, &channel)
        .map(Json)
        .ok_or_else(|| {
            ApiError::not_found(format!("channel not found: {channel_type_raw}/{channel}"))
        })
}

/// GET /api/services/:id。既存の一覧と同じ ServiceItem モデルを返す。
async fn get_service(
    State(state): State<Arc<AppState>>,
    Path(id_raw): Path<String>,
) -> Result<Json<ServiceItem>, ApiError> {
    let id = id_raw
        .parse::<i64>()
        .map_err(|_| ApiError::not_found(format!("service not found: {id_raw}")))?;
    find_service(&state.channels, id)
        .map(|(service, _)| Json(service))
        .ok_or_else(|| ApiError::not_found(format!("service not found: {id}")))
}

/// 現在の channels.yml からサービス一覧を合成する。
/// Hotarunの設定は1チャンネルに最大1つの serviceId を持つため、サービスも1件生成する。
pub fn service_items(channels: &[Channel]) -> Vec<ServiceItem> {
    channels
        .iter()
        .filter_map(|channel| {
            let service_id = channel.serviceId?;
            let network_id = extra_i64(channel, "networkId").unwrap_or(0);
            let service_type = extra_i64(channel, "serviceType")
                .or_else(|| extra_i64(channel, "service_type"))
                .unwrap_or(0);
            Some(ServiceItem {
                id: service_item_id(network_id, service_id),
                serviceId: service_id,
                networkId: network_id,
                name: channel.name.clone(),
                service_type,
                channel: ServiceChannel {
                    channel_type: channel.channel_type,
                    channel: channel.channel.clone(),
                },
            })
        })
        .collect()
}

pub fn find_service(channels: &[Channel], id: i64) -> Option<(ServiceItem, Channel)> {
    let matches: Vec<_> = channels.iter().filter_map(|channel| {
        let item = service_items(std::slice::from_ref(channel)).into_iter().next()?;
        (item.id == id).then(|| (item, channel.clone()))
    }).collect();
    (matches.len() == 1).then(|| matches.into_iter().next().unwrap())
}

fn channel_matches(channel: &Channel, query: &HashMap<String, String>) -> bool {
    query.get("type").is_none_or(|value| {
        channel_type_name(channel.channel_type).eq_ignore_ascii_case(value)
    }) && query
        .get("channel")
        .is_none_or(|value| channel.channel == *value)
        && query
            .get("name")
            .is_none_or(|value| channel.name == *value)
}

/// GET /api/channels
async fn list_channels(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Vec<Channel>> {
    Json(
        state
            .channels
            .iter()
            .filter(|channel| channel_matches(channel, &query))
            .cloned()
            .collect(),
    )
}

fn service_matches(service: &ServiceItem, query: &HashMap<String, String>) -> bool {
    let number_matches = |key: &str, actual: i64| {
        query
            .get(key)
            .is_none_or(|value| value.parse::<i64>().ok() == Some(actual))
    };

    number_matches("serviceId", service.serviceId)
        && number_matches("networkId", service.networkId)
        && number_matches("type", service.service_type)
        && query
            .get("name")
            .is_none_or(|value| service.name == *value)
        && query
            .get("channel.type")
            .is_none_or(|value| {
                channel_type_name(service.channel.channel_type).eq_ignore_ascii_case(value)
            })
        && query
            .get("channel.channel")
            .or_else(|| query.get("channel"))
            .is_none_or(|value| service.channel.channel == *value)
}

/// GET /api/services
async fn list_services(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Vec<ServiceItem>> {
    Json(
        service_items(&state.channels)
            .into_iter()
            .filter(|service| service_matches(service, &query))
            .collect(),
    )
}

#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct TunerStatus {
    pub index: usize,
    pub name: String,
    pub types: Vec<ChannelType>,
    pub command: Option<String>,
    pub pid: Option<u32>,
    pub state: String,
    pub currentChannel: Option<String>,
    pub useCount: usize,
    pub users: Vec<String>,
    pub isAvailable: bool,
    pub isRemote: bool,
    pub isFree: bool,
    pub isUsing: bool,
    pub isFault: bool,
}

async fn tuner_status(state: &AppState, index: usize) -> Result<TunerStatus, ApiError> {
    let tuner = state.tuners.get(index).ok_or_else(|| {
        ApiError::not_found(format!("tuner not found: {index}"))
    })?;
    let tuner_state = state
        .manager
        .state(index)
        .await
        .map_err(|e| ApiError::new(500, format!("failed to read tuner state: {e}")))?;
    let pid = state
        .manager
        .pid(index)
        .await
        .map_err(|e| ApiError::new(500, format!("failed to read tuner pid: {e}")))?;
    let current_channel = state
        .manager
        .current_channel(index)
        .await
        .map_err(|e| ApiError::new(500, format!("failed to read tuner channel: {e}")))?;
    let use_count = state
        .manager
        .use_count(index)
        .await
        .map_err(|e| ApiError::new(500, format!("failed to read tuner users: {e}")))?;
    Ok(TunerStatus {
        index,
        name: tuner.name.clone(),
        types: tuner.types.clone(),
        command: tuner.command.clone(),
        pid,
        state: tuner_state.to_string(),
        currentChannel: current_channel,
        useCount: use_count,
        users: vec!["".to_owned(); use_count],
        isAvailable: !matches!(tuner_state, TunerState::Disabled | TunerState::Fault),
        isRemote: false,
        isFree: tuner_state == TunerState::Idle,
        isUsing: tuner_state.is_active(),
        isFault: tuner_state == TunerState::Fault,
    })
}

/// GET /api/tuners
async fn list_tuners(State(state): State<Arc<AppState>>) -> Result<Json<Vec<TunerStatus>>, ApiError> {
    let mut result = Vec::with_capacity(state.tuners.len());
    for index in 0..state.tuners.len() {
        result.push(tuner_status(&state, index).await?);
    }
    Ok(Json(result))
}

/// GET /api/tuners/:index。負数・小数・空文字は integer >= 0 を満たさないため404。
async fn get_tuner(
    State(state): State<Arc<AppState>>,
    Path(index): Path<String>,
) -> Result<Json<TunerStatus>, ApiError> {
    let index = index
        .parse::<usize>()
        .map_err(|_| ApiError::not_found(format!("tuner not found: {index}")))?;
    Ok(Json(tuner_status(&state, index).await?))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/version", get(version))
        .route("/api/status", get(status))
        .route("/api/channels", get(list_channels))
        .route("/api/channels/{channel_type}/{channel}", get(get_channel))
        .route("/api/services", get(list_services))
        .route("/api/services/{id}", get(get_service))
        .route("/api/tuners", get(list_tuners))
        .route("/api/tuners/{index}", get(get_tuner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn channel() -> Channel {
        Channel {
            name: "NHK".to_owned(),
            channel_type: ChannelType::BS,
            channel: "BS01_0".to_owned(),
            serviceId: Some(101),
            tunerChannels: None,
            extra: HashMap::from([(String::from("networkId"), serde_json::json!(4))]),
        }
    }

    #[test]
    fn service_id_is_not_service_id_alone() {
        let services = service_items(&[channel()]);
        assert_eq!(services[0].id, 400101);
        assert_eq!(services[0].serviceId, 101);
    }

    #[test]
    fn service_filters_support_nested_channel_fields() {
        let service = service_items(&[channel()]).remove(0);
        assert!(service_matches(
            &service,
            &HashMap::from([(String::from("channel.type"), String::from("bs"))])
        ));
        assert!(!service_matches(
            &service,
            &HashMap::from([(String::from("networkId"), String::from("5"))])
        ));
    }

    #[test]
    fn duplicate_service_item_id_is_not_selected() {
        let mut second = channel();
        second.name = "duplicate".to_owned();
        assert!(find_service(&[channel(), second], 400101).is_none());
    }

    #[test]
    fn channel_lookup_uses_type_and_channel_pair() {
        let channels = vec![channel()];
        assert!(find_channel(&channels, ChannelType::BS, "BS01_0").is_some());
        assert!(find_channel(&channels, ChannelType::GR, "BS01_0").is_none());
    }

    #[test]
    fn version_response_uses_package_version() {
        let response = VersionResponse {
            current: env!("CARGO_PKG_VERSION"),
            latest: env!("CARGO_PKG_VERSION"),
        };
        assert_eq!(response.current, env!("CARGO_PKG_VERSION"));
        assert_eq!(response.latest, response.current);
    }
}
