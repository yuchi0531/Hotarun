//! Phase3 TS配信 (SPEC §8§9§10§11)。最初はGR/BS/CS中心だが実装は汎用。
//!
//! - `GET /api/channels/{type}/{channel}/stream`
//! - query `decode` は `0|1` のみ。省略時は `1`。
//! - 要求ヘッダ `X-Mirakurun-Priority` を受け付ける (奪取は将来対応・現状は記録のみ)。
//! - 成功時は `video/MP2T` + `X-Mirakurun-Tuner-User-ID` で tuner stdout をそのまま配信する。
//!   起動成功は spawn 成功で即時確定し、初回バイト待ちはしない (§8)。
//! - 失敗は確保前に Error JSON: 404 (chなし) / 503 no available tuners / 500 (spawn失敗)。
//! - `HEAD` はチューナー確保なしで 200 空返し。

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes, HttpBody},
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    response::Response,
    routing::get,
};
use http_body::Frame;

use crate::{
    config::{AppState, Channel, ChannelType},
    error::ApiError,
    tuner::SharedTunerManager,
};

/// 確保リトライ回数 (§9: 50 x 250ms)。
const ACQUIRE_RETRIES: usize = 50;
const ACQUIRE_WAIT: Duration = Duration::from_millis(250);

fn parse_channel_type(s: &str) -> Option<ChannelType> {
    match s.to_ascii_uppercase().as_str() {
        "GR" => Some(ChannelType::GR),
        "BS" => Some(ChannelType::BS),
        "CS" => Some(ChannelType::CS),
        "SKY" => Some(ChannelType::SKY),
        "BS4K" => Some(ChannelType::BS4K),
        _ => None,
    }
}

fn find_channel(channels: &[Channel], ct: ChannelType, name: &str) -> Option<Channel> {
    channels
        .iter()
        .find(|c| c.channel_type == ct && c.channel == name)
        .cloned()
}

/// query `decode` は `0|1` のみ。省略時は `1` (§11)。
fn parse_decode(query: &HashMap<String, String>) -> Result<bool, ApiError> {
    match query.get("decode") {
        None => Ok(true),
        Some(v) if v == "1" => Ok(true),
        Some(v) if v == "0" => Ok(false),
        Some(v) => Err(ApiError::bad_request(format!(
            "invalid decode value: {v} (expected 0|1)"
        ))),
    }
}

/// 要求ヘッダ `X-Mirakurun-Priority` を受け付ける。欠落・不正時は 0。
fn parse_priority(headers: &HeaderMap) -> i32 {
    headers
        .get("x-mirakurun-priority")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<i32>().ok())
        .unwrap_or(0)
}

/// Scheduler簡易実装 (§9)。
/// - 条件1 type対応 / 2 disabledでない(IDLEのみ) / 3 未使用(IDLEのみ) / 4 物理ch解決可(常に可)
/// - 確保は 50 x 250ms リトライし、尽きたら 503。
/// - `decode` の違いは共有を妨げないため選択に使わない。
/// - `X-Mirakurun-Priority` による奪取は将来対応 (現状は受け付けてログのみ)。
async fn acquire_tuner(
    state: &AppState,
    ct: ChannelType,
    channel: &Channel,
    priority: i32,
) -> Result<(usize, String), ApiError> {
    if priority != 0 {
        tracing::debug!(priority, "X-Mirakurun-Priority accepted (preemption not yet implemented)");
    }
    let mut last_spawn_err: Option<String> = None;

    for _ in 0..ACQUIRE_RETRIES {
        // 空き候補を短時間ロックで収集する。
        let candidates: Vec<(usize, String)> = {
            let mgr = state.manager.lock().await;
            let mut v = Vec::new();
            for idx in 0..mgr.len() {
                if !mgr.is_available_for(idx, ct) {
                    continue;
                }
                match mgr.physical_channel_for(idx, channel) {
                    Ok(phys) => v.push((idx, phys)),
                    Err(e) => {
                        tracing::warn!(tuner = idx, error = %e, "physical channel resolve failed");
                    }
                }
            }
            v
        };

        if candidates.is_empty() {
            // 解放見込みのある busy がいなければ即 503。busy がいれば 250ms 待って再試行。
            let busy_exists = {
                let mgr = state.manager.lock().await;
                let mut busy = false;
                for idx in 0..mgr.len() {
                    let supports = mgr.supports_type(idx, ct).unwrap_or(false);
                    if supports && mgr.is_busy(idx) {
                        busy = true;
                        break;
                    }
                }
                busy
            };
            if !busy_exists {
                break;
            }
            tokio::time::sleep(ACQUIRE_WAIT).await;
            continue;
        }

        for (idx, phys) in candidates {
            let start_res = {
                let mut mgr = state.manager.lock().await;
                mgr.start(idx, &phys).await
            };
            match start_res {
                Ok(pid) => {
                    tracing::info!(tuner = idx, pid, channel = %phys, "stream tuner acquired");
                    return Ok((idx, phys));
                }
                Err(e) => {
                    tracing::warn!(tuner = idx, error = %e, "tuner start failed, trying next");
                    last_spawn_err = Some(e);
                }
            }
        }
        // 候補はあったが全滅 (busy競合 or spawn失敗)。次ラウンド前に少し待つ。
        tokio::time::sleep(ACQUIRE_WAIT).await;
    }

    if let Some(e) = last_spawn_err {
        if e.contains("spawn") || e.contains("command") || e.contains("No such") {
            return Err(ApiError::new(500, format!("failed to start tuner: {e}")));
        }
    }
    Err(ApiError::new(503, "no available tuners"))
}

/// tuner stdout を HTTP へ直結する Body (§10)。
/// Drop時に `release` してチューナーを解放する (最終切断時の停止)。
struct TunerStreamBody {
    stdout: tokio::process::ChildStdout,
    manager: SharedTunerManager,
    index: usize,
    finished: bool,
}

impl Drop for TunerStreamBody {
    fn drop(&mut self) {
        let mgr = self.manager.clone();
        let idx = self.index;
        // Axum実行中は必ずランタイムがある。なければ何もしない。
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let mut m = mgr.lock().await;
                if let Err(e) = m.release(idx).await {
                    tracing::warn!(tuner = idx, error = %e, "stream cleanup release failed");
                } else {
                    tracing::debug!(tuner = idx, "stream tuner released");
                }
            });
        }
    }
}

impl HttpBody for TunerStreamBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        use tokio::io::AsyncRead;

        if self.finished {
            return Poll::Ready(None);
        }
        // 32KBチャンクでノンブロッキングfan-out。遅延者がいても切断しない (§10)。
        let mut buf = [0u8; 32 * 1024];
        let mut read_buf = tokio::io::ReadBuf::new(&mut buf);
        match std::pin::Pin::new(&mut self.stdout).poll_read(cx, &mut read_buf) {
            Poll::Ready(Ok(())) => {
                let filled = read_buf.filled();
                if filled.is_empty() {
                    self.finished = true;
                    Poll::Ready(None)
                } else {
                    let chunk = Bytes::copy_from_slice(filled);
                    Poll::Ready(Some(Ok(Frame::data(chunk))))
                }
            }
            Poll::Ready(Err(e)) => {
                self.finished = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// GET /api/channels/{type}/{channel}/stream
async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path((ctype_raw, channel_raw)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let ct = parse_channel_type(&ctype_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;
    // 映像ヘッダ送出前に validation する。decode不正は 400。
    let _decode = parse_decode(&query)?;
    let priority = parse_priority(&headers);

    let ch = find_channel(&state.channels, ct, &channel_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;

    // 確保 (spawn成功で即時確定・初回バイト待ちなし §8)。
    let (idx, _phys) = acquire_tuner(&state, ct, &ch, priority).await?;

    // stdout引き渡し。なければ 500 (確保後の異常)。
    let stdout = {
        let mut mgr = state.manager.lock().await;
        match mgr.take_stdout(idx) {
            Ok(Some(s)) => s,
            Ok(None) => {
                let _ = mgr.release(idx).await;
                return Err(ApiError::new(
                    500,
                    "tuner has no stdout".to_owned(),
                ));
            }
            Err(e) => {
                let _ = mgr.release(idx).await;
                return Err(ApiError::new(500, format!("failed to take tuner stdout: {e}")));
            }
        }
    };

    // 映像ヘッダはチューナー確保後に送出する (§11)。
    let stream_body = TunerStreamBody {
        stdout,
        manager: state.manager.clone(),
        index: idx,
        finished: false,
    };
    let body = Body::new(stream_body);
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "video/MP2T")
        .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
        .body(body)
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

/// HEAD /api/channels/{type}/{channel}/stream — チューナー確保なしで 200 空返し。
async fn head_stream(
    State(state): State<Arc<AppState>>,
    Path((ctype_raw, channel_raw)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let ct = parse_channel_type(&ctype_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;
    let _decode = parse_decode(&query)?;
    let _priority = parse_priority(&headers);
    find_channel(&state.channels, ct, &channel_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;

    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "video/MP2T")
        .body(Body::empty())
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route(
        "/api/channels/{channel_type}/{channel}/stream",
        get(get_stream).head(head_stream),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_type_parses_case_insensitive() {
        assert_eq!(parse_channel_type("GR"), Some(ChannelType::GR));
        assert_eq!(parse_channel_type("gr"), Some(ChannelType::GR));
        assert_eq!(parse_channel_type("BS"), Some(ChannelType::BS));
        assert_eq!(parse_channel_type("cs"), Some(ChannelType::CS));
        assert_eq!(parse_channel_type("BS4K"), Some(ChannelType::BS4K));
        assert_eq!(parse_channel_type("XXX"), None);
    }

    #[test]
    fn decode_defaults_to_on_and_rejects_invalid() {
        assert_eq!(parse_decode(&HashMap::new()).unwrap(), true);
        assert_eq!(
            parse_decode(&HashMap::from([("decode".to_owned(), "1".to_owned())])).unwrap(),
            true
        );
        assert_eq!(
            parse_decode(&HashMap::from([("decode".to_owned(), "0".to_owned())])).unwrap(),
            false
        );
        assert!(parse_decode(&HashMap::from([("decode".to_owned(), "2".to_owned())])).is_err());
        assert!(
            parse_decode(&HashMap::from([("decode".to_owned(), "foo".to_owned())])).is_err()
        );
    }

    #[test]
    fn priority_missing_or_invalid_is_zero() {
        let empty = HeaderMap::new();
        assert_eq!(parse_priority(&empty), 0);
        let mut h = HeaderMap::new();
        h.insert("x-mirakurun-priority", "5".parse().unwrap());
        assert_eq!(parse_priority(&h), 5);
        let mut bad = HeaderMap::new();
        bad.insert("x-mirakurun-priority", "foo".parse().unwrap());
        assert_eq!(parse_priority(&bad), 0);
    }
}
