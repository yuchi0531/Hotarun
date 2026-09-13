//! Phase3 TS配信 (SPEC §8§9§10§11)。最初はGR/BS/CS中心だが実装は汎用。
//!
//! - `GET /api/channels/{type}/{channel}/stream`
//! - query `decode` は `0|1` のみ。省略時は `1`。
//! - 要求ヘッダ `X-Mirakurun-Priority` による低優先度ストリームの奪取に対応する。
//! - 成功時は `video/MP2T` + `X-Mirakurun-Tuner-User-ID` で tuner stdout を
//!   fan-out配信する。同一物理chは共有し、per-client boundedキューで
//!   ノンブロッキング配信する (overflow時は当該チャンクを落として接続維持 §10)。
//!   起動成功は spawn 成功で即時確定し、初回バイト待ちはしない (§8)。
//! - 失敗は確保前に Error JSON: 404 (chなし) / 503 no available tuners / 500 (spawn失敗)。
//!   Busyのみ 50 x 250ms リトライし、spawn失敗は即500 (文字列判定なし)。
//! - `HEAD` はチューナー確保なしで 200 空返し。

use std::{collections::{HashMap, VecDeque}, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::Duration};

use axum::{
    Router,
    body::{Body, Bytes, HttpBody},
    extract::{Path, Query, State},
    http::{HeaderMap, header},
    response::Response,
    routing::get,
};
use http_body::Frame;
use tokio::sync::mpsc;
use tokio::io::AsyncWriteExt;

use crate::{
    config::{AppState, Channel, ChannelType},
    error::ApiError,
    routes::api::find_service,
    scan::{canonical_cs_logical, canonical_logical_channel},
    tuner::{
        command::build_passthrough_command,
        process::{spawn_decoder_program, RegisteredDecoder},
        SharedTunerManager,
    },
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
    let want = canonical_cs_logical(ct, name);
    channels
        .iter()
        .find(|c| {
            c.channel_type == ct
                && (canonical_logical_channel(c) == want
                    || c.serviceId.is_some_and(|service_id| {
                        c.channel
                            .rsplit_once(':')
                            .is_some_and(|(logical, suffix)| {
                                canonical_cs_logical(ct, logical) == want
                                    && suffix.parse::<i64>().ok() == Some(service_id)
                            })
                    }))
        })
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
/// - 条件1 type対応 / 2 disabledでない(IDLEのみ新規・Streaming同一chは共有)
///   / 3 未使用または同一物理ch共有 / 4 物理ch解決可(常に可)
/// - 共有キーは物理chのみ。`decode` の違いは共有を妨げない。
/// - 同一物理chは `acquire` で共有/fan-outし、毎回spawnしない。
/// - Busyのみ 50 x 250ms リトライし尽きたら503。spawn失敗は即500。
///   文字列 `contains("spawn")` 判定は廃止し [`TunerError`] で分離する。
async fn acquire_tuner(
    state: &AppState,
    ct: ChannelType,
    channel: &Channel,
    priority: i32,
) -> Result<(usize, mpsc::Receiver<Vec<u8>>, u64), ApiError> {

    for _ in 0..ACQUIRE_RETRIES {
        // 1. 共有: 同一物理chでStreaming中があれば acquire (use_count++) して
        //    fan-out購読する。新規spawnはしない。
        for idx in 0..state.manager.len() {
            if !state
                .manager
                .supports_type(idx, ct)
                .await
                .unwrap_or(false)
            {
                continue;
            }
            let phys = match state.manager.physical_channel_for(idx, channel).await
            {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(tuner = idx, error = %e, "physical channel resolve failed");
                    continue;
                }
            };
            if !state.manager.is_sharing_candidate(idx, &phys).await {
                continue;
            }
            match state.manager.acquire_http_with_priority(idx, &phys, priority).await {
                Ok((_pid, generation)) => {
                    match state
                        .manager
                        .create_subscription_for_generation(idx, generation)
                        .await
                    {
                        Ok(rx) => {
                            tracing::info!(tuner = idx, channel = %phys, "stream tuner shared");
                            return Ok((idx, rx, generation));
                        }
                        Err(e) => {
                            tracing::warn!(tuner = idx, error = %e, "share subscribe failed");
                            let _ = state
                                .manager
                                .release_lease(idx, generation, Some(priority))
                                .await;
                            continue;
                        }
                    }
                }
                Err(e) if e.is_busy() => continue,
                Err(e) => {
                    // 共有経路での非Busy (通常は起きないがspawnに落ちた場合) は即500。
                    return Err(ApiError::new(
                        500,
                        format!("failed to start tuner: {e}"),
                    ));
                }
            }
        }

        // 2. 新規: 空き候補を収集する。各slotのみ短時間ロックし、全体ロックはしない。
        let mut candidates: Vec<(usize, String)> = Vec::new();
        for idx in 0..state.manager.len() {
            if !state.manager.is_available_for(idx, ct).await {
                continue;
            }
            match state.manager.physical_channel_for(idx, channel).await {
                Ok(phys) => candidates.push((idx, phys)),
                Err(e) => {
                    tracing::warn!(tuner = idx, error = %e, "physical channel resolve failed");
                }
            }
        }

        if candidates.is_empty() {
            // No free compatible slot: a strictly higher-priority request may
            // take over one existing, different-channel stream. The manager
            // performs the eligibility check and state transition under the
            // slot lock, so this check cannot race with another acquire.
            for idx in 0..state.manager.len() {
                if !state.manager.supports_type(idx, ct).await.unwrap_or(false) {
                    continue;
                }
                let Ok(phys) = state.manager.physical_channel_for(idx, channel).await else {
                    continue;
                };
                match state
                    .manager
                    .takeover_with_priority(idx, &phys, priority)
                    .await
                {
                    Ok((_pid, generation)) => {
                        let rx = match state
                            .manager
                            .create_subscription_for_generation(idx, generation)
                            .await
                        {
                            Ok(rx) => rx,
                            Err(e) => {
                                let _ = state.manager.release_lease(idx, generation, Some(priority)).await;
                                return Err(ApiError::new(500, format!("failed to attach stream: {e}")));
                            }
                        };
                        let stdout = match state.manager.take_stdout(idx).await {
                            Ok(Some(stdout)) => stdout,
                            Ok(None) => {
                                let _ = state.manager.release_lease(idx, generation, Some(priority)).await;
                                return Err(ApiError::new(500, "tuner has no stdout"));
                            }
                            Err(e) => {
                                let _ = state.manager.release_lease(idx, generation, Some(priority)).await;
                                return Err(ApiError::new(500, format!("failed to take tuner stdout: {e}")));
                            }
                        };
                        state.manager.spawn_pump(idx, stdout);
                        tracing::info!(tuner = idx, channel = %phys, priority, "stream tuner taken over");
                        return Ok((idx, rx, generation));
                    }
                    Err(e) if e.is_busy() => continue,
                    Err(e) => return Err(ApiError::new(500, format!("failed to take over tuner: {e}"))),
                }
            }
            if priority != 0 {
                return Err(ApiError::new(503, "no tuner with lower priority available"));
            }
            // 解放見込みのある busy がいなければ即 503。busy がいれば 250ms 待って再試行。
            let mut busy_exists = false;
            for idx in 0..state.manager.len() {
                let supports =
                    state.manager.supports_type(idx, ct).await.unwrap_or(false);
                if supports && state.manager.is_busy(idx).await {
                    busy_exists = true;
                    break;
                }
            }
            if !busy_exists {
                break;
            }
            tokio::time::sleep(ACQUIRE_WAIT).await;
            continue;
        }

        for (idx, phys) in candidates {
            // slot単位ロックのため他slotは阻塞しない。監視タスク付きで起動する。
            match state
                .manager
                .start_monitored_with_priority(idx, &phys, priority)
                .await
            {
                Ok((pid, generation)) => {
                    // 初回購読 + pump起動。失敗時は確保を取り消して500。
                    let rx = match state
                        .manager
                        .create_subscription_for_generation(idx, generation)
                        .await
                    {
                        Ok(rx) => rx,
                        Err(e) => {
                            let _ = state
                                .manager
                                .release_lease(idx, generation, Some(priority))
                                .await;
                            return Err(ApiError::new(
                                500,
                                format!("failed to attach stream: {e}"),
                            ));
                        }
                    };
                    let stdout = match state.manager.take_stdout(idx).await {
                        Ok(Some(s)) => s,
                        Ok(None) => {
                            let _ = state
                                .manager
                                .release_lease(idx, generation, Some(priority))
                                .await;
                            return Err(ApiError::new(
                                500,
                                "tuner has no stdout".to_owned(),
                            ));
                        }
                        Err(e) => {
                            let _ = state
                                .manager
                                .release_lease(idx, generation, Some(priority))
                                .await;
                            return Err(ApiError::new(
                                500,
                                format!("failed to take tuner stdout: {e}"),
                            ));
                        }
                    };
                    state.manager.spawn_pump(idx, stdout);
                    tracing::info!(tuner = idx, pid, channel = %phys, "stream tuner acquired");
                    return Ok((idx, rx, generation));
                }
                Err(e) if e.is_busy() => {
                    // 競合のみ次候補へ。spawn失敗は下で即500。
                    tracing::debug!(tuner = idx, "tuner busy race, trying next");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(tuner = idx, error = %e, "tuner spawn failed");
                    return Err(ApiError::new(
                        500,
                        format!("failed to start tuner: {e}"),
                    ));
                }
            }
        }
        // 候補はあったがBusy競合で全滅。次ラウンド前に少し待つ。
        // (spawn失敗は上で即returnしているためここには来ない)
        tokio::time::sleep(ACQUIRE_WAIT).await;
    }

    Err(ApiError::new(503, "no available tuners"))
}

/// 共有ストリームの Body (§10 fan-out)。
/// pumpが配信するboundedキューを受信する。遅延subscriberは接続を維持し、
/// bounded queue overflow時にチャンクを落とす。overflowはログ/メトリクスで観測する。
/// Drop時は guard/reaper方式で確実に解放する:
/// 同期ヒント (`release_hint_sync`) + 非同期停止 (`stop_if_idle`/`release`)
/// + watcherのorphan回収。fire-and-forget単独にしない。
struct SharedStreamBody {
    rx: mpsc::Receiver<Vec<u8>>,
    manager: SharedTunerManager,
    index: usize,
    generation: u64,
    priority: i32,
}

/// BS4K の decoder 分岐。チューナーの共有は維持しつつ、各要求だけを
/// `TLV -> decoder stdin -> decoder stdout -> HTTP` に通す。
/// decoder の終了は HTTP body の EOF とし、Drop では必ず child を kill/wait
/// して孤児プロセスを残さない。
struct DecodedStreamBody {
    rx: mpsc::Receiver<Vec<u8>>,
    manager: SharedTunerManager,
    index: usize,
    generation: u64,
    priority: i32,
    decoder: RegisteredDecoder,
    input_task: Option<tokio::task::JoinHandle<()>>,
    output_task: Option<tokio::task::JoinHandle<()>>,
    lease_released: Arc<AtomicBool>,
}

/// Per-client MPEG-TS service filter. The tuner remains shared and emits full
/// TS; this body selects PAT, the target PMT, and the PMT's PCR/elementary PIDs.
/// Until PAT/PMT are available, at most 8 MiB of complete packets are held.
struct ServiceStreamBody {
    rx: mpsc::Receiver<Vec<u8>>,
    manager: SharedTunerManager,
    index: usize,
    generation: u64,
    priority: i32,
    target_service_id: u16,
    input: Vec<u8>,
    pending: VecDeque<Vec<u8>>,
    pre_ready: VecDeque<[u8; 188]>,
    pmt_pid: Option<u16>,
    selected_pids: Option<Vec<u16>>,
    pat_psi: PsiAssembler,
    pmt_psi: PsiAssembler,
    lease_released: Arc<AtomicBool>,
}

const SERVICE_PRE_READY_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct PsiAssembler {
    bytes: Vec<u8>,
}

impl PsiAssembler {
    fn feed(&mut self, payload: &[u8], start: bool) -> Vec<Vec<u8>> {
        let mut sections = Vec::new();
        let payload = if start {
            let Some(pointer) = payload.first().copied().map(usize::from) else {
                return sections;
            };
            if pointer + 1 > payload.len() {
                return sections;
            }
            if !self.bytes.is_empty() {
                if pointer > 0 {
                    self.bytes.extend_from_slice(&payload[1..=pointer]);
                    self.take_complete(&mut sections);
                }
                // A PUSI packet starts a new section after the pointer.  Do
                // not let an incomplete previous header determine the
                // length of that new section.
                self.bytes.clear();
            }
            &payload[pointer + 1..]
        } else {
            payload
        };
        self.bytes.extend_from_slice(payload);
        self.take_complete(&mut sections);
        sections
    }

    fn take_complete(&mut self, sections: &mut Vec<Vec<u8>>) {
        loop {
            if self.bytes.len() < 3 {
                return;
            }
            if self.bytes[0] == 0xff {
                self.bytes.clear();
                return;
            }
            let length = (((self.bytes[1] & 0x0f) as usize) << 8)
                | self.bytes[2] as usize;
            let total = length + 3;
            if total > 4096 {
                self.bytes.clear();
                return;
            }
            if self.bytes.len() < total {
                return;
            }
            sections.push(self.bytes.drain(..total).collect());
        }
    }
}

impl ServiceStreamBody {
    fn validate_service_id(service_id: i64) -> Result<u16, ApiError> {
        u16::try_from(service_id).map_err(|_| {
            ApiError::new(
                501,
                "service stream PID filtering is unavailable for this service id",
            )
        })
    }

    fn process_chunk(&mut self, chunk: &[u8]) {
        self.input.extend_from_slice(chunk);
        while self.input.len() >= 188 {
            if self.input[0] != 0x47 {
                if let Some(pos) = self.input.iter().position(|byte| *byte == 0x47) {
                    self.input.drain(..pos);
                } else {
                    self.input.clear();
                    return;
                }
                if self.input.len() < 188 {
                    return;
                }
            }
            let mut packet = [0u8; 188];
            packet.copy_from_slice(&self.input[..188]);
            self.input.drain(..188);
            self.process_packet(packet);
        }
    }

    fn process_packet(&mut self, packet: [u8; 188]) {
        let pid = (((packet[1] & 0x1f) as u16) << 8) | packet[2] as u16;
        let was_ready = self.selected_pids.is_some();
        if pid == 0 {
            // Do not pass through PAT packets while a section is incomplete:
            // doing so leaks the other services. Re-emit a completed target
            // PAT from scratch as valid 188-byte packets instead.
            for section in self.pat_psi.feed(packet_payload(&packet).unwrap_or_default(), packet[1] & 0x40 != 0) {
                if let Some(pmt_pid) = parse_pat_section(&section, self.target_service_id) {
                    self.pmt_pid = Some(pmt_pid);
                }
                if let Some(rewritten) = rewrite_pat_section(&section, self.target_service_id) {
                    self.pending.push_back(psi_packet(0, &rewritten).to_vec());
                }
            }
        } else if self.pmt_pid == Some(pid) {
            for section in self.pmt_psi.feed(packet_payload(&packet).unwrap_or_default(), packet[1] & 0x40 != 0) {
                if let Some(pids) = parse_pmt_section(&section) {
                    self.selected_pids = Some(pids);
                    self.flush_pre_ready();
                }
            }
        }

        if !was_ready && self.selected_pids.is_none() {
            if pid != 0 {
                self.pre_ready.push_back(packet);
            }
            while self.pre_ready.len() * 188 > SERVICE_PRE_READY_LIMIT {
                self.pre_ready.pop_front();
            }
            return;
        }
        // PMT was the packet that made the filter ready; flush_pre_ready
        // handled packets from before it. Include the PMT itself.
        if !was_ready && self.selected_pids.is_some() {
            if pid != 0 {
                self.pending.push_back(packet.to_vec());
            }
            return;
        }
        if self.should_emit(pid) {
            if pid != 0 {
                self.pending.push_back(packet.to_vec());
            }
        }
    }

    fn flush_pre_ready(&mut self) {
        let packets = std::mem::take(&mut self.pre_ready);
        for packet in packets {
            let pid = (((packet[1] & 0x1f) as u16) << 8) | packet[2] as u16;
            if self.should_emit(pid) {
                if pid != 0 {
                    self.pending.push_back(packet.to_vec());
                }
            }
        }
    }

    fn should_emit(&self, pid: u16) -> bool {
        pid == 0 || self.pmt_pid == Some(pid)
            || self.selected_pids.as_ref().is_some_and(|pids| pids.contains(&pid))
    }
}

fn rewrite_pat_section(data: &[u8], service_id: u16) -> Option<Vec<u8>> {
    if data.first().copied()? != 0 || data.len() < 12 {
        return None;
    }
    let pmt_pid = parse_pat_section(data, service_id)?;
    let mut rewritten = vec![
        // The filtered PAT is a standalone one-section table, regardless of
        // which section of the source PAT contained the requested service.
        0x00, 0xb0, 0x0d, data[3], data[4], data[5], 0x00, 0x00,
        (service_id >> 8) as u8, service_id as u8,
        0xe0 | ((pmt_pid >> 8) as u8 & 0x1f), pmt_pid as u8,
    ];
    let crc = mpeg_crc32(&rewritten);
    rewritten.extend_from_slice(&crc.to_be_bytes());
    Some(rewritten)
}

fn psi_packet(pid: u16, data: &[u8]) -> [u8; 188] {
    assert!(data.len() <= 183);
    // A PAT section plus its pointer fits in one payload-only TS packet.  Do
    // not advertise an adaptation field: there is no adaptation payload or
    // PCR in this generated packet.
    let mut packet = [0xff; 188];
    packet[0] = 0x47;
    packet[1] = 0x40 | ((pid >> 8) as u8 & 0x1f);
    packet[2] = pid as u8;
    packet[3] = 0x10;
    packet[4] = 0;
    packet[5..5 + data.len()].copy_from_slice(data);
    packet
}

fn mpeg_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff;
    for byte in bytes {
        crc ^= (*byte as u32) << 24;
        for _ in 0..8 {
            crc = if crc & 0x8000_0000 != 0 {
                (crc << 1) ^ 0x04c1_1db7
            } else {
                crc << 1
            };
        }
    }
    crc
}

impl Drop for ServiceStreamBody {
    fn drop(&mut self) {
        let mgr = self.manager.clone();
        let idx = self.index;
        let generation = self.generation;
        let priority = self.priority;
        if self.lease_released.swap(true, Ordering::AcqRel) {
            return;
        }
        let hinted = mgr.release_hint_sync_lease(idx, generation, Some(priority));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let result = if hinted {
                    let channel = mgr.current_channel(idx).await.ok().flatten();
                    mgr.schedule_idle_stop(idx, generation, channel.as_deref()).await;
                    Ok(())
                } else {
                    mgr.release_lease(idx, generation, Some(priority)).await
                };
                if let Err(error) = result {
                    tracing::warn!(tuner = idx, %error, "service stream cleanup failed");
                }
            });
        }
    }
}

impl Drop for DecodedStreamBody {
    fn drop(&mut self) {
        if let Some(task) = self.input_task.take() {
            task.abort();
        }
        if let Some(task) = self.output_task.take() {
            task.abort();
        }
        let decoder = self.decoder.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                decoder.stop().await;
            });
        }
        if self.lease_released.swap(true, Ordering::AcqRel) {
            return;
        }
        let mgr = self.manager.clone();
        let idx = self.index;
        let generation = self.generation;
        let priority = self.priority;
        let hinted = mgr.release_hint_sync_lease(idx, generation, Some(priority));
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let result = if hinted {
                    let channel = mgr.current_channel(idx).await.ok().flatten();
                    mgr.schedule_idle_stop(idx, generation, channel.as_deref()).await;
                    Ok(())
                } else {
                    mgr.release_lease(idx, generation, Some(priority)).await
                };
                if let Err(error) = result {
                    tracing::warn!(tuner = idx, %error, "decoded stream cleanup failed");
                }
            });
        }
    }
}

impl HttpBody for ServiceStreamBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        loop {
            if let Some(packet) = self.pending.pop_front() {
                return Poll::Ready(Some(Ok(Frame::data(Bytes::from(packet)))));
            }
            match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
                Poll::Ready(Some(chunk)) => self.process_chunk(&chunk),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn packet_payload(packet: &[u8; 188]) -> Option<&[u8]> {
    if packet[3] & 0x10 == 0 {
        return None;
    }
    let adaptation = ((packet[3] >> 4) & 0x3) as usize;
    let mut offset = 4;
    if adaptation == 2 || adaptation == 3 {
        offset += 1 + packet[4] as usize;
    }
    if offset >= 188 {
        return None;
    }
    (offset < 188).then_some(&packet[offset..])
}

#[cfg(test)]
fn section(payload: &[u8], table_id: u8) -> Option<&[u8]> {
    if payload.first().copied()? != table_id || payload.len() < 3 {
        return None;
    }
    let length = (((payload[1] & 0x0f) as usize) << 8) | payload[2] as usize;
    if length + 3 <= payload.len() {
        Some(&payload[..length + 3])
    } else {
        None
    }
}

fn parse_pat_section(data: &[u8], service_id: u16) -> Option<u16> {
    if data.first().copied()? != 0 || data.len() < 12 {
        return None;
    }
    let end = data.len().saturating_sub(4);
    let mut offset = 8;
    while offset + 4 <= end {
        let program = u16::from_be_bytes([data[offset], data[offset + 1]]);
        let pid = (((data[offset + 2] & 0x1f) as u16) << 8) | data[offset + 3] as u16;
        if program == service_id {
            return Some(pid);
        }
        offset += 4;
    }
    None
}

fn parse_pmt_section(data: &[u8]) -> Option<Vec<u16>> {
    if data.first().copied()? != 2 {
        return None;
    }
    if data.len() < 12 {
        return None;
    }
    let pcr_pid = (((data[8] & 0x1f) as u16) << 8) | data[9] as u16;
    let info_len = (((data[10] & 0x0f) as usize) << 8) | data[11] as usize;
    let end = data.len().saturating_sub(4);
    let mut offset = 12 + info_len;
    let mut pids = vec![pcr_pid];
    while offset + 5 <= end {
        let pid = (((data[offset + 1] & 0x1f) as u16) << 8) | data[offset + 2] as u16;
        let descriptors = (((data[offset + 3] & 0x0f) as usize) << 8)
            | data[offset + 4] as usize;
        pids.push(pid);
        offset += 5 + descriptors;
    }
    Some(pids)
}

impl Drop for SharedStreamBody {
    fn drop(&mut self) {
        let mgr = self.manager.clone();
        let idx = self.index;
        let generation = self.generation;
        let priority = self.priority;
        // await不可文脈のためまず同期で需要を減らし、ゾンビBusyを防ぐ。
        let hinted = mgr.release_hint_sync_lease(idx, generation, Some(priority));
        // Axum実行中は必ずランタイムがある。なければwatcherが回収する。
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                // hint済みなら二重減算を避けて停止のみ、未hintならreleaseで減算。
                let res = if hinted {
                    let channel = mgr.current_channel(idx).await.ok().flatten();
                    mgr.schedule_idle_stop(idx, generation, channel.as_deref()).await;
                    Ok(())
                } else {
                    mgr.release_lease(idx, generation, Some(priority)).await
                };
                if let Err(e) = res {
                    tracing::warn!(tuner = idx, error = %e, "stream cleanup failed");
                } else {
                    tracing::debug!(tuner = idx, hinted, "stream tuner released");
                }
            });
        }
    }
}

impl HttpBody for SharedStreamBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(chunk)) => {
                Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk)))))
            }
            // 送信側全滅 (停止・異常終了) はEOFとして閉じる。
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HttpBody for DecodedStreamBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        match std::pin::Pin::new(&mut self.rx).poll_recv(cx) {
            Poll::Ready(Some(chunk)) => Poll::Ready(Some(Ok(Frame::data(Bytes::from(chunk))))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn spawn_decoded_body(
    state: &Arc<AppState>,
    idx: usize,
    generation: u64,
    priority: i32,
    rx: mpsc::Receiver<Vec<u8>>,
    template: &str,
    lease_released: Arc<AtomicBool>,
) -> Result<DecodedStreamBody, ApiError> {
    let (program, args) = match build_passthrough_command(template) {
        Ok(command) => command,
        Err(error) => {
            release_decoded_lease(&state.manager, idx, generation, priority, &lease_released).await;
            return Err(ApiError::new(
                500,
                format!("invalid TLV decoder command: {error}"),
            ));
        }
    };
    let decoder = match spawn_decoder_program(&program, &args).await {
        Ok(decoder) => decoder,
        Err(error) => {
            release_decoded_lease(&state.manager, idx, generation, priority, &lease_released).await;
            return Err(ApiError::new(
                500,
                format!("failed to start TLV decoder: {error}"),
            ));
        }
    };
    let decoder = match state.decoders.register(decoder).await {
        Ok(decoder) => decoder,
        Err(error) => {
            release_decoded_lease(&state.manager, idx, generation, priority, &lease_released).await;
            return Err(ApiError::new(503, error));
        }
    };
    let Some(mut stdin) = decoder.take_stdin() else {
        decoder.stop().await;
        release_decoded_lease(&state.manager, idx, generation, priority, &lease_released).await;
        return Err(ApiError::new(500, "TLV decoder has no stdin"));
    };
    let Some(mut stdout) = decoder.take_stdout() else {
        decoder.stop().await;
        release_decoded_lease(&state.manager, idx, generation, priority, &lease_released).await;
        return Err(ApiError::new(500, "TLV decoder has no stdout"));
    };

    let (output_tx, output_rx) = mpsc::channel(crate::tuner::STREAM_QUEUE_LEN);
    let shutdown = state.decoders.shutdown_notifier();
    let input_shutdown = shutdown.clone();
    let input_task = tokio::spawn(async move {
        let mut rx = rx;
        loop {
            tokio::select! {
                _ = input_shutdown.notified() => break,
                chunk = rx.recv() => {
                    let Some(chunk) = chunk else { break };
                    if stdin.write_all(&chunk).await.is_err() {
                        break;
                    }
                }
            }
        }
        // Closing stdin tells a finite decoder that no more TLV is coming.
        let _ = stdin.shutdown().await;
    });
    let output_task = tokio::spawn(async move {
        let mut buf = vec![0u8; crate::tuner::STREAM_CHUNK_SIZE];
        loop {
            let read = tokio::select! {
                _ = shutdown.notified() => break,
                read = tokio::io::AsyncReadExt::read(&mut stdout, &mut buf) => read,
            };
            match read {
                Ok(0) => break,
                Ok(size) => {
                    if output_tx.send(buf[..size].to_vec()).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    Ok(DecodedStreamBody {
        rx: output_rx,
        manager: state.manager.clone(),
        index: idx,
        generation,
        priority,
        decoder,
        input_task: Some(input_task),
        output_task: Some(output_task),
        lease_released,
    })
}

async fn release_decoded_lease(
    manager: &SharedTunerManager,
    index: usize,
    generation: u64,
    priority: i32,
    released: &AtomicBool,
) {
    if !released.swap(true, Ordering::AcqRel) {
        let _ = manager.release_lease(index, generation, Some(priority)).await;
    }
}

fn tuner_decoder(state: &AppState, idx: usize, ct: ChannelType, decode: bool) -> Option<String> {
    if !decode {
        return None;
    }
    let tuner = state.tuners.get(idx)?;
    if ct == ChannelType::BS4K {
        tuner.tlv_decoder.clone()
    } else {
        tuner.decoder.clone()
    }
}

/// GET /api/channels/{type}/{channel}/stream
async fn get_channel_stream(
    state: Arc<AppState>,
    ct: ChannelType,
    channel: Channel,
    query: HashMap<String, String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // 映像ヘッダ送出前に validation する。decode不正は 400。
    let decode = parse_decode(&query)?;
    let priority = parse_priority(&headers);

    // 確保 (spawn成功で即時確定・初回バイト待ちなし §8)。
    // 共有時は既存tunerのfan-out購読、 신규時は起動+pump済みの購読が返る。
    let (idx, rx, generation) = acquire_tuner(&state, ct, &channel, priority).await?;

    // 映像ヘッダはチューナー確保後に送出する (§11)。
    let body = if let Some(template) = tuner_decoder(&state, idx, ct, decode) {
        // Decoder branches are per-client.  The tuner itself remains shared.
        Body::new(spawn_decoded_body(&state, idx, generation, priority, rx, &template, Arc::new(AtomicBool::new(false))).await?)
    } else {
        Body::new(SharedStreamBody {
            rx,
            manager: state.manager.clone(),
            index: idx,
            generation,
            priority,
        })
    };
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "video/MP2T")
        .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
        .body(body)
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path((ctype_raw, channel_raw)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let ct = parse_channel_type(&ctype_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;
    let channel = find_channel(&state.channels, ct, &channel_raw).ok_or_else(|| {
        ApiError::not_found(format!("channel not found: {ctype_raw}/{channel_raw}"))
    })?;
    get_channel_stream(state, ct, channel, query, headers).await
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
        // HEAD はチューナーを確保しないため、実チューナー番号ではなく
        // 「確保前の応答」であることを示す固定値を返す。
        .header("X-Mirakurun-Tuner-User-ID", "head")
        .body(Body::empty())
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

/// GET/HEAD /api/services/{id}/stream。
/// `id` は serviceId ではなく Mirakurun の ServiceItemId。通常のTSは共有された
/// フルTSからPAT/PMTを解析し、各クライアントへ対象サービスのPIDだけを返す。
/// BS4Kは1TLV 1サービス前提のため、同じTLVをそのまま返す。
async fn service_stream(
    State(state): State<Arc<AppState>>,
    Path(id_raw): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = id_raw
        .parse::<i64>()
        .map_err(|_| ApiError::not_found(format!("service not found: {id_raw}")))?;
    let Some((service, channel)) = find_service(&state.channels, id) else {
        return Err(ApiError::not_found(format!("service not found: {id}")));
    };
    let target_service_id = (channel.channel_type != ChannelType::BS4K)
        .then(|| ServiceStreamBody::validate_service_id(service.serviceId))
        .transpose()?;
    let decode = parse_decode(&query)?;
    let priority = parse_priority(&headers);
    let (idx, rx, generation) = acquire_tuner(&state, channel.channel_type, &channel, priority).await?;
    if let Some(template) = tuner_decoder(&state, idx, channel.channel_type, decode) {
        let Some(target_service_id) = target_service_id else {
            // BS4K/TLV has no MPEG-TS PID stage; keep the existing per-client
            // decoder path for that format.
            let body = Body::new(spawn_decoded_body(&state, idx, generation, priority, rx, &template, Arc::new(AtomicBool::new(false))).await?);
            return Response::builder()
                .status(200)
                .header(header::CONTENT_TYPE, "video/MP2T")
                .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
                .body(body)
                .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")));
        };
        // For TS services, filter the shared full TS before feeding the
        // decoder.  Filtering decoder stdout would corrupt decoder framing
        // and could leak another service's PID set.
        let lease_released = Arc::new(AtomicBool::new(false));
        let filtered = spawn_service_filter(state.manager.clone(), idx, generation, priority, rx, target_service_id);
        let body = Body::new(spawn_decoded_body(&state, idx, generation, priority, filtered, &template, lease_released).await?);
        return Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, "video/MP2T")
            .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
            .body(body)
            .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")));
    }
    if channel.channel_type == ChannelType::BS4K {
        // A BS4K service is one TLV stream/service by definition. Do not run
        // the MPEG-TS PAT/PMT filter or reinterpret its bytes as TS packets.
        let body = if let Some(template) = tuner_decoder(&state, idx, channel.channel_type, decode) {
            Body::new(spawn_decoded_body(&state, idx, generation, priority, rx, &template, Arc::new(AtomicBool::new(false))).await?)
        } else {
            Body::new(SharedStreamBody {
                rx,
                manager: state.manager.clone(),
                index: idx,
                generation,
                priority,
            })
        };
        return Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, "video/MP2T")
            .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
            .body(body)
            .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")));
    }
    let stream_body = ServiceStreamBody {
        rx,
        manager: state.manager.clone(),
        index: idx,
        generation,
        priority,
        target_service_id: target_service_id.expect("validated non-BS4K service id"),
        input: Vec::new(),
        pending: VecDeque::new(),
        pre_ready: VecDeque::new(),
        pmt_pid: None,
        selected_pids: None,
        pat_psi: PsiAssembler::default(),
        pmt_psi: PsiAssembler::default(),
        lease_released: Arc::new(AtomicBool::new(false)),
    };
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "video/MP2T")
        .header("X-Mirakurun-Tuner-User-ID", idx.to_string())
        .body(Body::new(stream_body))
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

fn spawn_service_filter(
    manager: SharedTunerManager,
    index: usize,
    generation: u64,
    priority: i32,
    rx: mpsc::Receiver<Vec<u8>>,
    target_service_id: u16,
) -> mpsc::Receiver<Vec<u8>> {
    let (tx, filtered_rx) = mpsc::channel(crate::tuner::STREAM_QUEUE_LEN);
    tokio::spawn(async move {
        let mut body = ServiceStreamBody {
            rx,
            manager,
            index,
            generation,
            priority,
            target_service_id,
            input: Vec::new(),
            pending: VecDeque::new(),
            pre_ready: VecDeque::new(),
            pmt_pid: None,
            selected_pids: None,
            pat_psi: PsiAssembler::default(),
            pmt_psi: PsiAssembler::default(),
            // The filter task is only a pipeline stage.  The decoder body is
            // the sole owner of the tuner lease for this request.
            lease_released: Arc::new(AtomicBool::new(true)),
        };
        while let Some(chunk) = body.rx.recv().await {
            body.process_chunk(&chunk);
            while let Some(packet) = body.pending.pop_front() {
                if tx.send(packet).await.is_err() {
                    return;
                }
            }
        }
    });
    filtered_rx
}

async fn head_service_stream(
    State(state): State<Arc<AppState>>,
    Path(id_raw): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let id = id_raw
        .parse::<i64>()
        .map_err(|_| ApiError::not_found(format!("service not found: {id_raw}")))?;
    let Some((_service, _channel)) = find_service(&state.channels, id) else {
        return Err(ApiError::not_found(format!("service not found: {id}")));
    };
    let is_tlv = _channel.channel_type == ChannelType::BS4K;
    if !is_tlv {
        ServiceStreamBody::validate_service_id(_service.serviceId)?;
    }
    // HEAD は既存の channel stream と同じく、検証のみでチューナーを確保しない。
    let _decode = parse_decode(&query)?;
    let _priority = parse_priority(&headers);
    Response::builder()
        .status(200)
        .header(header::CONTENT_TYPE, "video/MP2T")
        .header("X-Mirakurun-Tuner-User-ID", "head")
        .body(Body::empty())
        .map_err(|e| ApiError::new(500, format!("failed to build response: {e}")))
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/channels/{channel_type}/{channel}/stream",
            get(get_stream).head(head_stream),
        )
        .route(
            "/api/services/{id}/stream",
            get(service_stream).head(head_service_stream),
        )
        // HTTP adapter used by BonDriver_Mirakurun clients. The actual Windows
        // driver ABI is outside this Rust daemon and is not specified by §10.
        .route(
            "/api/bonDriver/channels/{channel_type}/{channel}/stream",
            get(get_stream).head(head_stream),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppState, Channel, ChannelType, Tuner};
    use crate::tuner::process::test_fixture_command;
    use crate::tuner::TunerManager;
    use std::collections::HashMap;

    fn fixture(mode: &str) -> String {
        test_fixture_command(mode)
    }

    fn bs4k_state(tuner_command: &str, tlv_decoder: Option<&str>) -> Arc<AppState> {
        Arc::new(AppState::from_lists(
            vec![Channel {
                name: "test BS4K".to_owned(),
                channel_type: ChannelType::BS4K,
                channel: "logical".to_owned(),
                serviceId: Some(101),
                tunerChannels: Some(HashMap::from([(
                    "bs4k".to_owned(),
                    "physical-45328".to_owned(),
                ),])),
                extra: HashMap::from([(String::from("networkId"), serde_json::json!(5))]),
            }],
            vec![Tuner {
                name: "bs4k".to_owned(),
                types: vec![ChannelType::BS4K],
                command: Some(tuner_command.to_owned()),
                tlv_decoder: tlv_decoder.map(str::to_owned),
                decoder: None,
                extra: HashMap::new(),
            }],
        ))
    }

    async fn first_body_bytes(response: Response) -> Vec<u8> {
        let mut body = response.into_body();
        let frame = tokio::time::timeout(
            Duration::from_secs(2),
            std::future::poll_fn(|cx| {
                std::pin::Pin::new(&mut body).poll_frame(cx)
            }),
        )
        .await
        .expect("stream body timeout")
        .expect("stream body ended")
        .expect("stream body error");
        frame.into_data().expect("data frame").to_vec()
    }

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

    #[tokio::test]
    async fn bs4k_channel_passthrough_keeps_tlv_bytes_and_resolves_physical_channel() {
        let state = bs4k_state(
            &fixture("raw"),
            None,
        );
        let response = get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::new(),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.headers()[header::CONTENT_TYPE], "video/MP2T");
        assert_eq!(response.headers()["X-Mirakurun-Tuner-User-ID"], "0");
        assert_eq!(first_body_bytes(response).await, b"TLV-raw-45328");
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.current_channel(0).await.unwrap(), None);
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn bs4k_decoder_is_a_per_request_pipe_without_shell_expansion() {
        let state = bs4k_state(
            &fixture("lower"),
            Some(&fixture("decoder-upper")),
        );
        let response = get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::new(),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(first_body_bytes(response).await, b"LOWER");
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn bs4k_decode_zero_bypasses_configured_decoder() {
        let state = bs4k_state(
            &fixture("lower"),
            Some("/definitely/missing-tlv-decoder"),
        );
        let response = get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::from([(String::from("decode"), String::from("0"))]),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(first_body_bytes(response).await, b"lower");
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn bs4k_service_returns_same_tlv_without_ts_pid_filter() {
        let state = bs4k_state(
            &fixture("ts"),
            None,
        );
        let response = service_stream(
            State(Arc::clone(&state)),
            Path(String::from("500101")),
            Query(HashMap::new()),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(first_body_bytes(response).await, [0x47, 0x00, 0x01, b'T', b'L', b'V']);
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn same_bs4k_physical_channel_shares_one_tuner_process() {
        let state = bs4k_state(&fixture("hold"), None);
        let channel = state.channels[0].clone();
        let (idx1, _rx1, generation1) = acquire_tuner(&state, ChannelType::BS4K, &channel, 0)
            .await
            .unwrap();
        let pid = state.manager.pid(idx1).await.unwrap();
        let (idx2, _rx2, generation2) = acquire_tuner(&state, ChannelType::BS4K, &channel, 0)
            .await
            .unwrap();
        assert_eq!(idx1, idx2);
        assert_eq!(state.manager.pid(idx2).await.unwrap(), pid);
        assert_eq!(generation1, generation2);
        assert_eq!(state.manager.use_count(idx1).await.unwrap(), 2);
        state
            .manager
            .release_lease(idx1, generation1, Some(0))
            .await
            .unwrap();
        state
            .manager
            .release_lease(idx2, generation2, Some(0))
            .await
            .unwrap();
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.pid(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn decoder_spawn_failure_releases_tuner_lease() {
        let state = bs4k_state(
            &fixture("bytes"),
            Some("/definitely/missing-tlv-decoder"),
        );
        let error = match get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::new(),
            HeaderMap::new(),
        )
        .await
        {
            Ok(_) => panic!("decoder spawn must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, 500);
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
        assert_eq!(state.manager.pid(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn invalid_service_id_is_rejected_before_tuner_acquisition() {
        for service_id in [-1, 65_536] {
            let state = Arc::new(AppState::from_lists(
                vec![Channel {
                    name: "invalid service".to_owned(),
                    channel_type: ChannelType::BS,
                    channel: "27".to_owned(),
                    serviceId: Some(service_id),
                    tunerChannels: None,
                    extra: HashMap::new(),
                }],
                vec![Tuner {
                    name: "tuner".to_owned(),
                    types: vec![ChannelType::BS],
                    command: Some(fixture("hold")),
                    tlv_decoder: None,
                    decoder: None,
                    extra: HashMap::new(),
                }],
            ));
            let service_item_id = crate::routes::api::service_item_id(0, service_id);
            let error = service_stream(
                State(Arc::clone(&state)),
                Path(service_item_id.to_string()),
                Query(HashMap::new()),
                HeaderMap::new(),
            )
            .await
            .expect_err("invalid service must be rejected");
            assert_eq!(error.code, 501);
            assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
            assert_eq!(state.manager.pid(0).await.unwrap(), None);
            assert_eq!(state.manager.current_channel(0).await.unwrap(), None);
        }

        let state = Arc::new(AppState::from_lists(Vec::new(), Vec::new()));
        let error = service_stream(
            State(Arc::clone(&state)),
            Path(String::from("12345")),
            Query(HashMap::new()),
            HeaderMap::new(),
        )
        .await
        .expect_err("unknown service must be rejected");
        assert_eq!(error.code, 404);
        assert_eq!(state.manager.len(), 0);
    }

    #[test]
    fn service_psi_pusi_zero_discards_an_incomplete_section() {
        let mut assembler = PsiAssembler::default();
        assert!(assembler.feed(&[0, 0x00, 0xb0, 0xff, 0x00], true).is_empty());
        let section = [0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0, 0];
        let mut payload = vec![0];
        payload.extend_from_slice(&section);
        assert_eq!(assembler.feed(&payload, true), vec![section.to_vec()]);
    }

    #[tokio::test]
    async fn decoder_registry_shutdown_ends_body_without_stdin_eof() {
        let state = bs4k_state(
            &fixture("hold"),
            Some(&fixture("decoder-hold")),
        );
        let response = get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::new(),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(state.decoders.active_count(), 1);
        let body = response.into_body();
        state.decoders.stop_all().await;
        let result = tokio::time::timeout(Duration::from_secs(2), axum::body::to_bytes(body, 1024))
            .await
            .expect("decoder shutdown did not end HTTP body")
            .unwrap();
        assert!(result.is_empty());
        assert_eq!(state.decoders.active_count(), 0);
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn bs4k_type_and_missing_channel_are_not_cross_matched() {
        let state = bs4k_state(&fixture("hold"), None);
        let wrong_type = get_stream(
            State(Arc::clone(&state)),
            Path((String::from("BS"), String::from("logical"))),
            Query(HashMap::new()),
            HeaderMap::new(),
        )
        .await
        .expect_err("BS must not match a BS4K channel");
        assert_eq!(wrong_type.code, 404);

        let missing = get_stream(
            State(state),
            Path((String::from("BS4K"), String::from("missing"))),
            Query(HashMap::new()),
            HeaderMap::new(),
        )
        .await
        .expect_err("missing BS4K channel must be 404");
        assert_eq!(missing.code, 404);
    }

    #[tokio::test]
    async fn bs4k_priority_takeover_releases_old_physical_channel() {
        let state = Arc::new(AppState::from_lists(
            vec![
                Channel {
                    name: "first".to_owned(),
                    channel_type: ChannelType::BS4K,
                    channel: "first".to_owned(),
                    serviceId: Some(101),
                    tunerChannels: Some(HashMap::from([(
                        "bs4k".to_owned(),
                        "phys-a".to_owned(),
                    )])),
                    extra: HashMap::new(),
                },
                Channel {
                    name: "second".to_owned(),
                    channel_type: ChannelType::BS4K,
                    channel: "second".to_owned(),
                    serviceId: Some(102),
                    tunerChannels: Some(HashMap::from([(
                        "bs4k".to_owned(),
                        "phys-b".to_owned(),
                    )])),
                    extra: HashMap::new(),
                },
            ],
            vec![Tuner {
                name: "bs4k".to_owned(),
                types: vec![ChannelType::BS4K],
                command: Some(fixture("hold")),
                tlv_decoder: None,
                decoder: None,
                extra: HashMap::new(),
            }],
        ));
        let (_, _, old_generation) =
            acquire_tuner(&state, ChannelType::BS4K, &state.channels[0], 10)
                .await
                .unwrap();
        let (_, _, new_generation) =
            acquire_tuner(&state, ChannelType::BS4K, &state.channels[1], 20)
                .await
                .unwrap();
        assert_ne!(old_generation, new_generation);
        assert_eq!(
            state.manager.current_channel(0).await.unwrap().as_deref(),
            Some("phys-b")
        );
        state.manager.release_lease(0, new_generation, Some(20)).await.unwrap();
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.pid(0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn decoder_exit_after_headers_is_a_clean_eof_without_lease_leak() {
        let state = bs4k_state(
            &fixture("bytes"),
            Some(&fixture("fail")),
        );
        let response = get_channel_stream(
            Arc::clone(&state),
            ChannelType::BS4K,
            state.channels[0].clone(),
            HashMap::new(),
            HeaderMap::new(),
        )
        .await
        .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert!(bytes.is_empty());
        state.manager.wait_for_idle(0).await.unwrap();
        assert_eq!(state.manager.use_count(0).await.unwrap(), 0);
        assert_eq!(state.manager.pid(0).await.unwrap(), None);
    }

    fn psi_packet(pid: u16, section: &[u8]) -> [u8; 188] {
        let mut packet = [0xff; 188];
        packet[0] = 0x47;
        packet[1] = (((pid >> 8) as u8) & 0x1f) | 0x40;
        packet[2] = pid as u8;
        packet[3] = 0x10;
        packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(section);
        packet
    }

    fn split_psi_packets(pid: u16, section: &[u8]) -> Vec<[u8; 188]> {
        let mut packets = Vec::new();
        let first_len = section.len().min(12);
        for (index, part) in std::iter::once(&section[..first_len])
            .chain(section[first_len..].chunks(12))
            .enumerate()
        {
            let mut packet = [0xff; 188];
            packet[0] = 0x47;
            packet[1] = (((pid >> 8) as u8) & 0x1f) | if index == 0 { 0x40 } else { 0 };
            packet[2] = pid as u8;
            let payload_len = part.len() + usize::from(index == 0);
            let adaptation_len = 183 - payload_len;
            packet[3] = 0x30;
            packet[4] = adaptation_len as u8;
            let offset = 5 + adaptation_len;
            if index == 0 {
                packet[offset] = 0;
                packet[offset + 1..offset + 1 + part.len()].copy_from_slice(part);
            } else {
                packet[offset..offset + part.len()].copy_from_slice(part);
            }
            packets.push(packet);
        }
        packets
    }

    #[test]
    fn pat_and_pmt_select_only_target_service_pids() {
        let mut pat = vec![0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00];
        pat.extend_from_slice(&[0x00, 0x65, 0xe1, 0x00]); // service 101 -> PMT 0x100
        pat.extend_from_slice(&[0, 0, 0, 0]); // CRC is not validated by the minimal parser
        let mut body = ServiceStreamBody {
            rx: mpsc::channel(1).1,
            manager: TunerManager::shared(Vec::new()),
            index: 0,
            generation: 0,
            priority: 0,
            target_service_id: 101,
            input: Vec::new(),
            pending: VecDeque::new(),
            pre_ready: VecDeque::new(),
            pmt_pid: None,
            selected_pids: None,
            pat_psi: PsiAssembler::default(),
            pmt_psi: PsiAssembler::default(),
            lease_released: Arc::new(AtomicBool::new(false)),
        };
        body.process_packet(psi_packet(0, &pat));
        assert_eq!(body.pmt_pid, Some(0x100));

        let pmt = vec![
            0x02, 0xb0, 0x17, 0x00, 0x65, 0xc1, 0x00, 0x00, 0xe1, 0x00, 0xf0, 0x00,
            0x1b, 0xe1, 0x01, 0xf0, 0x00, 0x0f, 0xe1, 0x02, 0xf0, 0x00, 0, 0, 0, 0, 0,
        ];
        body.process_packet(psi_packet(0x100, &pmt));
        assert_eq!(body.selected_pids, Some(vec![0x100, 0x101, 0x102]));
        assert!(body.should_emit(0));
        assert!(body.should_emit(0x100));
        assert!(body.should_emit(0x101));
        assert!(!body.should_emit(0x200));
    }

    #[test]
    fn split_pat_and_pmt_sections_make_service_stream_ready() {
        // A deliberately large PAT forces the target section over a TS packet
        // boundary; stuffing is not mistaken for another section.
        let mut pat = vec![0x00, 0xb0, 0xc5, 0x00, 0x01, 0xc1, 0x00, 0x00];
        pat.extend_from_slice(&[0x00, 0x65, 0xe1, 0x00]);
        pat.extend(std::iter::repeat_n(0, 188));
        let pmt = vec![
            0x02, 0xb0, 0x18, 0x00, 0x65, 0xc1, 0x00, 0x00, 0xe1, 0x00, 0xf0, 0x00,
            0x1b, 0xe1, 0x01, 0xf0, 0x00, 0x0f, 0xe1, 0x02, 0xf0, 0x00, 0, 0, 0, 0, 0,
        ];
        let mut body = ServiceStreamBody {
            rx: mpsc::channel(1).1,
            manager: TunerManager::shared(Vec::new()),
            index: 0,
            generation: 0,
            priority: 0,
            target_service_id: 101,
            input: Vec::new(),
            pending: VecDeque::new(),
            pre_ready: VecDeque::new(),
            pmt_pid: None,
            selected_pids: None,
            pat_psi: PsiAssembler::default(),
            pmt_psi: PsiAssembler::default(),
            lease_released: Arc::new(AtomicBool::new(false)),
        };
        let packets = split_psi_packets(0, &pat);
        body.process_packet(packets[0]);
        assert!(body.pending.is_empty(), "incomplete PAT must be withheld");
        for packet in packets.into_iter().skip(1) {
            body.process_packet(packet);
        }
        assert_eq!(body.pmt_pid, Some(0x100));
        for packet in split_psi_packets(0x100, &pmt) {
            body.process_packet(packet);
        }
        assert_eq!(body.selected_pids, Some(vec![0x100, 0x101, 0x102]));
    }

    #[test]
    fn split_pat_is_repacketized_without_other_services() {
        let mut pat = vec![0x00, 0xb0, 0x11, 0x00, 0x01, 0xc1, 0x00, 0x00];
        pat.extend_from_slice(&[
            0x00, 0x65, 0xe1, 0x00, // selected service 101 -> PMT 0x100
            0x00, 0xca, 0xe2, 0x00, // other service 202 -> PMT 0x200
            0, 0, 0, 0,
        ]);
        let mut body = ServiceStreamBody {
            rx: mpsc::channel(1).1,
            manager: TunerManager::shared(Vec::new()),
            index: 0,
            generation: 0,
            priority: 0,
            target_service_id: 101,
            input: Vec::new(),
            pending: VecDeque::new(),
            pre_ready: VecDeque::new(),
            pmt_pid: None,
            selected_pids: None,
            pat_psi: PsiAssembler::default(),
            pmt_psi: PsiAssembler::default(),
            lease_released: Arc::new(AtomicBool::new(false)),
        };
        for packet in split_psi_packets(0, &pat) {
            body.process_packet(packet);
        }

        assert_eq!(body.pmt_pid, Some(0x100));
        assert_eq!(body.pending.len(), 1);
        let output = body.pending.pop_front().unwrap();
        assert_eq!(output.len(), 188);
        let payload = packet_payload(output.as_slice().try_into().unwrap()).unwrap();
        let rewritten = section(&payload[1..], 0).unwrap();
        assert_eq!(parse_pat_section(rewritten, 101), Some(0x100));
        assert_eq!(parse_pat_section(rewritten, 202), None);
        assert!(!output.windows(2).any(|window| window == [0x00, 0xca]));
    }

    #[test]
    fn rewritten_pat_is_standalone_even_when_source_is_section_two() {
        fn pat_section(section_number: u8, service_id: u16, pmt_pid: u16) -> Vec<u8> {
            let mut section = vec![
                0x00, 0xb0, 0x0d, 0x12, 0x34, 0xc1, section_number, 0x01,
                (service_id >> 8) as u8,
                service_id as u8,
                0xe0 | ((pmt_pid >> 8) as u8 & 0x1f),
                pmt_pid as u8,
            ];
            let crc = mpeg_crc32(&section);
            section.extend_from_slice(&crc.to_be_bytes());
            section
        }

        let mut body = ServiceStreamBody {
            rx: mpsc::channel(1).1,
            manager: TunerManager::shared(Vec::new()),
            index: 0,
            generation: 0,
            priority: 0,
            target_service_id: 101,
            input: Vec::new(),
            pending: VecDeque::new(),
            pre_ready: VecDeque::new(),
            pmt_pid: None,
            selected_pids: None,
            pat_psi: PsiAssembler::default(),
            pmt_psi: PsiAssembler::default(),
            lease_released: Arc::new(AtomicBool::new(false)),
        };
        body.process_packet(psi_packet(0, &pat_section(0, 202, 0x200)));
        body.process_packet(psi_packet(0, &pat_section(1, 101, 0x100)));

        assert_eq!(body.pending.len(), 1);
        let output = body.pending.pop_front().unwrap();
        let payload = packet_payload(output.as_slice().try_into().unwrap()).unwrap();
        let rewritten = section(&payload[1..], 0).unwrap();
        assert_eq!(rewritten[5] & 0x3e, 0x00, "version/current-next preserved");
        assert_eq!(rewritten[6], 0, "section_number normalized");
        assert_eq!(rewritten[7], 0, "last_section_number normalized");
        assert_eq!(rewritten.len(), 16);
        assert_eq!(mpeg_crc32(&rewritten), 0, "PAT CRC must cover rewritten section");
    }

    #[test]
    fn generated_pat_packet_has_payload_only_layout() {
        let section = rewrite_pat_section(
            &[0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0, 0],
            101,
        )
        .unwrap();
        let packet = super::psi_packet(0, &section);
        assert_eq!(packet[0], 0x47);
        assert_ne!(packet[1] & 0x40, 0, "PAT must set payload-unit-start");
        assert_eq!(packet[3] & 0x30, 0x10, "PAT must be payload-only");
        assert_eq!(packet[3] & 0x20, 0, "no adaptation field is advertised");
        assert_eq!(packet[4], 0, "pointer_field must be zero");
        assert_eq!(&packet[5..5 + section.len()], section.as_slice());
        assert_eq!(packet.len(), 188);
    }
}
