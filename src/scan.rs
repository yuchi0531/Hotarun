//! Deterministic channel scanning and the small PSI/TLV detector used by the
//! HTTP scan API.  Hardware-specific tuning remains in `TunerManager`, which
//! makes this module usable with the Rust fixture binary in integration tests.

use std::{collections::{HashMap, HashSet}, path::Path, sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};

use serde::Serialize;
use tokio::{io::AsyncReadExt, sync::{mpsc, Mutex, Notify}};

use crate::{config::{write_yaml_atomic, AppState, Channel, ChannelService, ChannelType}, tuner::TunerError};

pub const CHANNEL_TIMEOUT: Duration = Duration::from_secs(20);
pub const SCAN_TIMEOUT: Duration = Duration::from_secs(30 * 60);

#[derive(Debug, Clone, Serialize)]
#[allow(non_snake_case)]
pub struct ChannelScanStatus {
    pub status: String,
    #[serde(rename = "type")]
    pub type_: Option<ChannelType>,
    pub progress: u8,
    pub scanned: usize,
    pub total: usize,
    pub channels: Vec<Channel>,
    pub error: Option<String>,
    pub dryRun: bool,
    pub refresh: bool,
    pub startedAt: Option<u64>,
}

impl Default for ChannelScanStatus {
    fn default() -> Self {
        Self { status: "idle".to_owned(), type_: None, progress: 0, scanned: 0, total: 0,
            channels: Vec::new(), error: None, dryRun: false, refresh: true, startedAt: None }
    }
}

#[derive(Debug)]
pub struct ScanManager {
    state: Mutex<ScanState>,
    cancelled: AtomicBool,
    cancel_notify: Notify,
}

impl Default for ScanManager {
    fn default() -> Self { Self { state: Mutex::new(ScanState::default()), cancelled: AtomicBool::new(false), cancel_notify: Notify::new() } }
}

#[derive(Debug)]
struct ScanState {
    status: ChannelScanStatus,
    task_running: bool,
}

/// Own exactly one scanner lease.  A scan must never stop a process it does
/// not own: the same process may also be serving normal HTTP streams.
struct ScanLease {
    manager: crate::tuner::SharedTunerManager,
    index: usize,
    generation: u64,
    released: bool,
}

impl ScanLease {
    async fn release(&mut self) {
        if !self.released {
            let _ = self.manager.release_lease(self.index, self.generation, Some(0)).await;
            // A scan-only process can be re-tuned immediately.  The idle
            // check is generation/use-count guarded, so a concurrent stream
            // keeps the shared process alive.
            let _ = self.manager.stop_if_idle(self.index).await;
            self.released = true;
        }
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let manager = self.manager.clone();
        let index = self.index;
        let generation = self.generation;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = manager.release_lease(index, generation, Some(0)).await {
                    tracing::warn!(tuner = index, %error, "scan process cleanup failed");
                }
                let _ = manager.stop_if_idle(index).await;
            });
        }
    }
}

impl Default for ScanState {
    fn default() -> Self { Self { status: ChannelScanStatus::default(), task_running: false } }
}

impl ScanManager {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

    pub async fn status(&self) -> ChannelScanStatus { self.state.lock().await.status.clone() }

    pub async fn begin(
        self: &Arc<Self>, state: Arc<AppState>, kind: Option<ChannelType>, dry_run: bool, refresh: bool,
        asynchronous: bool, service_type: Option<i64>,
    ) -> Result<Option<String>, String> {
        let types = kind.map(|value| vec![value]).unwrap_or_else(|| vec![ChannelType::GR, ChannelType::BS, ChannelType::CS, ChannelType::BS4K]);
        let total = types.iter().map(|value| scan_channels(*value).len()).sum();
        {
            let mut current = self.state.lock().await;
            if current.task_running { return Err("channel scan is already running".to_owned()); }
            self.cancelled.store(false, Ordering::Release);
            current.task_running = true;
            current.status = ChannelScanStatus { status: "running".to_owned(), type_: kind, progress: 0,
                scanned: 0, total, channels: Vec::new(), error: None, dryRun: dry_run, refresh,
                startedAt: Some(epoch_seconds()) };
        }
        let manager = Arc::clone(self);
        let work = async move {
            let result = tokio::time::timeout(SCAN_TIMEOUT, run_scan(&state, &manager, &types, dry_run, refresh, service_type))
                .await
                .map_err(|_| "scan timed out".to_owned())
                .and_then(|result| result);
            let mut current = manager.state.lock().await;
            current.task_running = false;
            match &result {
                Ok(channels) => {
                    current.status.status = "complete".to_owned();
                    current.status.progress = 100;
                    current.status.channels = channels.clone();
                }
                Err(error) if current.status.status != "cancelled" => {
                    current.status.status = "error".to_owned();
                    current.status.error = Some(error.clone());
                    let state = state.clone();
                    let error = error.clone();
                    tokio::spawn(async move {
                        state.record_log(0, format!("channel scan failed: {error}")).await;
                    });
                }
                Err(_) => {}
            }
            result
        };
        if asynchronous {
            tokio::spawn(async move {
                let _ = work.await;
            });
            Ok(Some("accepted".to_owned()))
        } else {
            match work.await {
                Ok(_) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    pub async fn cancel(&self) -> Result<bool, String> {
        let mut state = self.state.lock().await;
        if !state.task_running { return Ok(false); }
        self.cancelled.store(true, Ordering::Release);
        self.cancel_notify.notify_waiters();
        state.status.status = "cancelled".to_owned();
        Ok(true)
    }

    pub async fn is_running(&self) -> bool {
        let state = self.state.lock().await;
        state.task_running && state.status.status == "running" && !self.cancelled.load(Ordering::Acquire)
    }
}

pub fn scan_channels(kind: ChannelType) -> Vec<String> {
    match kind {
        ChannelType::GR => (13..=62).map(|n| n.to_string()).collect(),
        ChannelType::BS => (1..=23).flat_map(|n| (0..=3).map(move |slot| format!("BS{n:02}_{slot}"))).collect(),
        ChannelType::CS => (2..=24).map(|n| format!("CS{n}")).collect(),
        ChannelType::BS4K => vec!["BS4K45328".to_owned()],
        ChannelType::SKY => Vec::new(),
    }
}

async fn run_scan(state: &Arc<AppState>, scans: &Arc<ScanManager>, types: &[ChannelType], dry_run: bool, refresh: bool, service_type: Option<i64>) -> Result<Vec<Channel>, String> {
    // An in-memory AppState is used by the unit/API smoke tests without a
    // tuner.  It is not a scan failure: dry-run returns the merged snapshot,
    // while a real save reports the missing configuration directory below.
    if state.config_dir.is_none() && state.manager.is_empty() {
        if dry_run {
            return merge_channels(&state.channels, Vec::new(), types, refresh);
        }
        return Err("configuration directory is unavailable".to_owned());
    }
    if state.config_dir.is_some()
        && !(0..state.manager.len()).any(|index| supports_scan_type(state, index, types))
    {
        return Err("no compatible tuner available".to_owned());
    }
    let mut found = Vec::new();
    let mut found_types = HashSet::new();
    let mut successful_attempts = 0usize;
    let mut failed_attempts = 0usize;
    let mut last_error = None;
    for kind in types {
        for logical in scan_channels(*kind) {
            if !scans.is_running().await { return Err("scan cancelled".to_owned()); }
            // CHANNEL_TIMEOUT is a budget for the complete logical channel,
            // not for each tuner candidate.  In particular, a dead tuner must
            // not give every subsequent candidate another full 20 seconds.
            let channel_deadline = Instant::now() + CHANNEL_TIMEOUT;
            let bytes = match tune_and_read(state, scans, *kind, &logical, channel_deadline).await {
                Ok(bytes) => {
                    successful_attempts += 1;
                    bytes
                }
                Err(error) if error == "scan cancelled" => {
                    return Err(error);
                }
                Err(error) => {
                    if error.starts_with("no valid services:") {
                        // The tuner delivered a complete TS, but its SI did
                        // not satisfy the scan contract.  Keep this as a
                        // successful read so the final error is the useful
                        // "no valid services" result rather than an
                        // infrastructure failure.
                        successful_attempts += 1;
                    } else {
                        failed_attempts += 1;
                    }
                    last_error = Some(error.clone());
                    tracing::warn!(?kind, %logical, %error, "scan channel failed");
                    state.record_log(1, format!("scan channel {kind:?}/{logical} failed: {error}")).await;
                    Vec::new()
                }
            };
            let detected = detect_scan_services(*kind, &logical, &bytes, &state.channels).into_iter().filter(|channel| {
                service_type.is_none_or(|wanted| {
                    channel.extra.get("serviceType").and_then(|value| value.as_i64()) == Some(wanted)
                        || channel.extra.get("services").and_then(|value| value.as_array()).is_some_and(|services| {
                            services.iter().any(|service| service.get("serviceType").and_then(|value| value.as_i64()) == Some(wanted))
                        })
                })
            }).collect::<Vec<_>>();
            if !detected.is_empty() {
                found_types.insert(*kind);
            }
            found.extend(detected);
            let mut current = scans.state.lock().await;
            current.status.scanned += 1;
            current.status.progress = ((current.status.scanned * 100) / current.status.total.max(1)).min(99) as u8;
        }
    }
    if successful_attempts == 0 && failed_attempts > 0 {
        return Err(format!(
            "all tuner scan attempts failed: {}",
            last_error.unwrap_or_else(|| "no successful tuner attempt".to_owned())
        ));
    }
    let missing_types = types
        .iter()
        .filter(|kind| !found_types.contains(kind))
        .map(|kind| format!("{kind:?}"))
        .collect::<Vec<_>>();
    if !missing_types.is_empty() {
        return Err(format!("no valid services detected for {}", missing_types.join(", ")));
    }
    // A scan with no configured tuner must not be reported as a successful
    // configuration save.  Keep the legacy configuration-directory error for
    // in-memory states (it is more useful to callers and preserves the normal
    // save validation order), but reject a real configured directory before
    // writing an empty channels.yml.
    found.sort_by_key(|channel| (channel.channel_type as u8, channel.channel.clone(), channel.serviceId));
    found.dedup_by(|left, right| left.channel_type == right.channel_type && left.channel == right.channel && left.serviceId == right.serviceId);
    let merged = merge_channels(&state.channels, found, types, refresh)?;
    if !dry_run {
        let directory = state.config_dir.as_deref().ok_or_else(|| "configuration directory is unavailable".to_owned())?;
        write_yaml_atomic(&directory.join("channels.yml"), &merged)?;
    }
    Ok(merged)
}

fn supports_scan_type(state: &Arc<AppState>, index: usize, types: &[ChannelType]) -> bool {
    // The manager's type list is immutable after startup.  Keeping this small
    // helper synchronous avoids turning scan preflight into a second async
    // state machine; the async availability check remains authoritative below.
    state.tuners.get(index).is_some_and(|tuner| {
        tuner.types.iter().any(|kind| types.contains(kind)) && tuner.command.as_ref().is_some_and(|command| !command.trim().is_empty())
    })
}

fn remaining(deadline: Instant) -> Result<Duration, String> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| "channel scan timeout".to_owned())
}

async fn release_with_deadline(lease: &mut ScanLease, deadline: Instant) {
    let Ok(budget) = remaining(deadline) else { return };
    let _ = tokio::time::timeout(budget, lease.release()).await;
}

async fn release_generation_with_deadline(
    manager: &crate::tuner::SharedTunerManager,
    index: usize,
    generation: u64,
    deadline: Instant,
) {
    let manager = manager.clone();
    let release = async move {
        let _ = manager.release_lease(index, generation, None).await;
    };
    if let Ok(budget) = remaining(deadline) {
        let _ = tokio::time::timeout(budget, release).await;
    } else {
        tokio::spawn(release);
    }
}

async fn tune_and_read(
    state: &Arc<AppState>,
    scans: &Arc<ScanManager>,
    kind: ChannelType,
    logical: &str,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    let mut last_error = None;
    for index in 0..state.manager.len() {
        if remaining(deadline).is_err() {
            break;
        }
        let supports = match tokio::time::timeout(
            remaining(deadline)?,
            state.manager.supports_type(index, kind),
        )
        .await
        {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                last_error = Some(error);
                continue;
            }
            Err(_) => {
                last_error = Some("channel scan timeout".to_owned());
                break;
            }
        };
        if !supports {
            continue;
        }

        let attempt = tokio::time::timeout(remaining(deadline)?, tune_on_tuner(state, scans, kind, logical, index, deadline)).await;
        match attempt {
            Ok(Ok(bytes)) => return Ok(bytes),
            Ok(Err(error)) if error == "scan cancelled" => return Err(error),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some("channel scan timeout".to_owned());
                break;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| TunerError::Busy("no compatible tuner available".to_owned()).to_string()))
}

async fn tune_on_tuner(
    state: &Arc<AppState>,
    scans: &Arc<ScanManager>,
    kind: ChannelType,
    logical: &str,
    index: usize,
    deadline: Instant,
) -> Result<Vec<u8>, String> {
    enum ScanInput {
        Stdout(tokio::process::ChildStdout),
        Shared(mpsc::Receiver<Vec<u8>>),
    }

    let scan_channel = state.channels.iter()
        .find(|channel| channel.channel_type == kind && channel.channel == logical)
        .cloned()
        .unwrap_or(Channel { name: String::new(), channel_type: kind, channel: logical.to_owned(), serviceId: None, tunerChannels: None, extra: HashMap::new() });
    let physical = tokio::time::timeout(remaining(deadline)?, state.manager.physical_channel_for(index, &scan_channel))
        .await
        .map_err(|_| "channel scan timeout".to_owned())??;

    // Prefer the already-running stream.  `acquire_with_priority` adds a
    // lease atomically; the subscription is the scan's read side of the
    // existing fan-out pump.
    let input = if tokio::time::timeout(remaining(deadline)?, state.manager.is_sharing_candidate(index, &physical))
        .await
        .map_err(|_| "channel scan timeout".to_owned())?
    {
        match tokio::time::timeout(remaining(deadline)?, state.manager.acquire_with_priority(index, &physical, 0))
            .await
            .map_err(|_| "channel scan timeout".to_owned())?
        {
            Ok((_pid, generation)) => match tokio::time::timeout(
                remaining(deadline)?,
                state.manager.create_subscription_for_generation(index, generation),
            )
            .await
            .map_err(|_| "channel scan timeout".to_owned())?
            {
                Ok(receiver) => Some((ScanLease { manager: state.manager.clone(), index, generation, released: false }, ScanInput::Shared(receiver))),
                Err(error) => {
                    release_generation_with_deadline(&state.manager, index, generation, deadline).await;
                    return Err(error);
                }
            },
            Err(error) if error.is_busy() => None,
            Err(error) => return Err(error.to_string()),
        }
    } else {
        None
    };
    let (mut lease, mut input) = if let Some(input) = input {
        input
    } else {
        if !tokio::time::timeout(remaining(deadline)?, state.manager.is_available_for(index, kind))
            .await
            .map_err(|_| "channel scan timeout".to_owned())?
        {
            return Err("tuner is busy".to_owned());
        }
        let started = tokio::time::timeout(
            remaining(deadline)?,
            state.manager.start_monitored_with_priority(index, &physical, 0),
        )
        .await
        .map_err(|_| {
            let manager = state.manager.clone();
            tokio::spawn(async move {
                let _ = manager.stop(index).await;
            });
            "channel scan timeout".to_owned()
        })?;
        let (_pid, generation) = started.map_err(|error| error.to_string())?;
        let mut lease = ScanLease { manager: state.manager.clone(), index, generation, released: false };
        let stdout = match tokio::time::timeout(remaining(deadline)?, state.manager.take_stdout(index)).await {
            Ok(Ok(Some(stdout))) => stdout,
            Ok(Ok(None)) => {
                release_with_deadline(&mut lease, deadline).await;
                return Err("scanner tuner has no stdout".to_owned());
            }
            Ok(Err(error)) => {
                release_with_deadline(&mut lease, deadline).await;
                return Err(error);
            }
            Err(_) => {
                drop(lease);
                return Err("channel scan timeout".to_owned());
            }
        };
        (lease, ScanInput::Stdout(stdout))
    };

    let read_result = tokio::time::timeout(remaining(deadline)?, async {
            let mut bytes = Vec::new();
            let mut ts = (kind != ChannelType::BS4K).then(TsScanBuffer::default);
            let mut buf = [0u8; 8192];
            loop {
                match &mut input {
                    ScanInput::Stdout(stdout) => tokio::select! {
                        _ = scans.cancel_notify.notified() => return Err("scan cancelled".to_owned()),
                        result = stdout.read(&mut buf) => {
                            let count = result.map_err(|e| e.to_string())?;
                            if count == 0 { break; }
                            if let Some(ts) = ts.as_mut() {
                                ts.feed(&buf[..count]);
                            } else {
                                bytes.extend_from_slice(&buf[..count]);
                            }
                        },
                    },
                    ScanInput::Shared(receiver) => tokio::select! {
                        _ = scans.cancel_notify.notified() => return Err("scan cancelled".to_owned()),
                        result = receiver.recv() => {
                            let Some(chunk) = result else { break; };
                            if let Some(ts) = ts.as_mut() {
                                ts.feed(&chunk);
                            } else {
                                bytes.extend_from_slice(&chunk);
                            }
                        }
                    },
                }
                let candidate = ts
                    .as_ref()
                    .map(|buffer| buffer.packets.as_slice())
                    .unwrap_or(bytes.as_slice());
                if !candidate.is_empty() && !detect_scan_services(kind, logical, candidate, &state.channels).is_empty() {
                    break;
                }
                if bytes.len() >= 4 * 1024 * 1024 || ts.as_ref().is_some_and(|buffer| buffer.packets.len() >= 4 * 1024 * 1024) {
                    break;
                }
            }
            if let Some(ts) = ts {
                if !ts.has_packet() {
                    return Err("scanner reached EOF before valid TS packets".to_owned());
                }
                bytes = ts.into_bytes();
            }
            if bytes.is_empty() {
                return Err("scanner reached EOF before data".to_owned());
            }
            if kind != ChannelType::BS4K
                && detect_scan_services(kind, logical, &bytes, &state.channels).is_empty()
            {
                return Err("no valid services: PAT, NIT actual, and SDT actual are incomplete".to_owned());
            }
            Ok::<_, String>(bytes)
        }).await;
    // Cleanup is also bounded by the channel budget. If the process stop is
    // slower than the remaining budget, ScanLease::drop keeps the guarded
    // asynchronous cleanup path as a final backstop.
    release_with_deadline(&mut lease, deadline).await;
    match read_result {
        Ok(Ok(bytes)) => Ok(bytes),
        Ok(Err(error)) => Err(error),
        Err(_) => Err("channel scan timeout".to_owned()),
    }
}

#[derive(Debug, Default)]
struct TsScanBuffer {
    pending: Vec<u8>,
    packets: Vec<u8>,
}

impl TsScanBuffer {
    fn feed(&mut self, chunk: &[u8]) {
        self.pending.extend_from_slice(chunk);
        loop {
            let Some(sync) = self.pending.iter().position(|byte| *byte == 0x47) else {
                let keep = self.pending.len().min(187);
                self.pending.drain(..self.pending.len().saturating_sub(keep));
                return;
            };
            if sync > 0 {
                self.pending.drain(..sync);
            }
            if self.pending.len() < 188 {
                return;
            }
            if self.pending.len() >= 376 && self.pending[188] != 0x47 {
                self.pending.drain(..1);
                continue;
            }
            if !valid_ts_packet(&self.pending[..188]) {
                self.pending.drain(..1);
                continue;
            }
            self.packets.extend_from_slice(&self.pending[..188]);
            self.pending.drain(..188);
        }
    }

    fn has_packet(&self) -> bool { !self.packets.is_empty() }

    fn into_bytes(self) -> Vec<u8> { self.packets }
}

fn valid_ts_packet(packet: &[u8]) -> bool {
    packet.len() == 188
        && packet[0] == 0x47
        && packet[1] & 0x80 == 0
        && packet[3] & 0x30 != 0
        && if packet[3] & 0x20 != 0 {
            usize::from(packet[4]) < 184
        } else {
            true
        }
}

pub fn detect_services(kind: ChannelType, logical: &str, bytes: &[u8], old: &[Channel]) -> Vec<Channel> {
    if kind == ChannelType::BS4K {
        return detect_bs4k_services(logical, bytes, old);
    }
    let sections = assemble_psi(bytes);
    let mut programs = pat_programs(&sections);
    let services = sdt_services(&sections);
    if programs.is_empty() { return Vec::new(); }
    programs.sort_unstable();
    let old_channel = old
        .iter()
        .find(|channel| channel.channel_type == kind && channel.channel == logical);
    let network_id = nit_network_id(&sections).unwrap_or_else(|| {
        old_channel
            .and_then(|channel| channel.extra.get("networkId"))
            .and_then(|value| value.as_i64())
            .unwrap_or(0) as u16
    });
    let mut detected = programs
        .iter()
        .map(|service_id| ChannelService {
            serviceId: i64::from(*service_id),
            networkId: i64::from(network_id),
            name: services
                .get(service_id)
                .map(|value| value.0.clone())
                .unwrap_or_else(|| format!("{kind:?} {service_id}")),
            service_type: services
                .get(service_id)
                .map(|value| value.1)
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();

    // refresh scans must not make a configured primary service disappear just
    // because a short sample did not include its PAT section.  It is still
    // retained in the same physical-channel record, alongside newly detected
    // programs.
    if let Some(existing_service_id) = old_channel.and_then(|channel| channel.serviceId) {
        if !detected.iter().any(|service| service.serviceId == existing_service_id) {
            detected.insert(0, ChannelService {
                serviceId: existing_service_id,
                networkId: old_channel
                    .and_then(|channel| channel.extra.get("networkId"))
                    .and_then(|value| value.as_i64())
                    .unwrap_or(i64::from(network_id)),
                name: old_channel.map(|channel| channel.name.clone()).unwrap_or_default(),
                service_type: old_channel
                    .and_then(|channel| channel.extra.get("serviceType"))
                    .and_then(|value| value.as_i64())
                    .unwrap_or_default(),
            });
        }
    }

    // One physical channel is one Channel record.  Preserve the configured
    // primary service/name and carry all other PAT programs in `extra.services`.
    let Some(primary) = old_channel
        .and_then(|channel| channel.serviceId)
        .and_then(|service_id| detected.iter().find(|service| service.serviceId == service_id).cloned())
        .or_else(|| detected.first().cloned()) else {
        return Vec::new();
    };
    let mut extra = old_channel.map(|channel| channel.extra.clone()).unwrap_or_default();
    extra.insert("networkId".to_owned(), serde_json::json!(primary.networkId));
    extra.insert("serviceType".to_owned(), serde_json::json!(primary.service_type));
    let additional = detected
        .into_iter()
        .filter(|service| service.serviceId != primary.serviceId)
        .collect::<Vec<_>>();
    if additional.is_empty() {
        extra.remove("services");
    } else {
        if let Ok(value) = serde_json::to_value(additional) {
            extra.insert("services".to_owned(), value);
        }
    }
    vec![Channel {
        name: old_channel
            .map(|channel| channel.name.clone())
            .unwrap_or(primary.name),
        channel_type: kind,
        channel: logical.to_owned(),
        serviceId: Some(primary.serviceId),
        tunerChannels: old_channel.and_then(|channel| channel.tunerChannels.clone()),
        extra,
    }]
}

/// The scan path is stricter than the public detector compatibility helper:
/// PAT alone is not a usable channel record.  Require the actual NIT and the
/// actual SDT to describe every PAT service before allowing persistence.
fn detect_scan_services(kind: ChannelType, logical: &str, bytes: &[u8], old: &[Channel]) -> Vec<Channel> {
    if kind != ChannelType::BS4K {
        let sections = assemble_psi(bytes);
        let programs = pat_programs(&sections);
        let services = sdt_services(&sections);
        if programs.is_empty()
            || nit_network_id(&sections).is_none()
            || programs.iter().any(|service_id| !services.contains_key(service_id))
        {
            tracing::debug!(?programs, nit = ?nit_network_id(&sections), ?services, "TS scan SI incomplete");
            return Vec::new();
        }
    }
    detect_services(kind, logical, bytes, old)
}

#[derive(Debug, Clone)]
struct TlvNit {
    network_id: u16,
    streams: HashMap<u16, u16>,
}

#[derive(Debug)]
struct TlvNitSection {
    table_id: u8,
    network_id: u16,
    version: u8,
    current_next: bool,
    section_number: u8,
    last_section_number: u8,
    streams: HashMap<u16, u16>,
}

#[derive(Debug, Clone, PartialEq)]
struct MmtService {
    service_id: u16,
    network_id: u16,
    name: String,
    service_type: i64,
    stream_id: u16,
}

#[derive(Debug, Default)]
struct MmtSiAssembler {
    fragments: HashMap<(u16, u32), MmtFragment>,
}

#[derive(Debug)]
struct MmtFragment {
    packet_sequence: u32,
    bytes: Vec<u8>,
    fragmentation: u8,
}

/// Detect BS4K services from the actual TLV-NIT, MMT PLT and MMT SDT.  The
/// three tables deliberately remain independent until the end: a name or a
/// service id from an unrelated/other table is never enough to create a
/// channel.
fn detect_bs4k_services(logical: &str, bytes: &[u8], old: &[Channel]) -> Vec<Channel> {
    let mut nit_sections: HashMap<(u16, u8, bool), HashMap<u8, TlvNitSection>> = HashMap::new();
    let mut package_ids = HashSet::new();
    let mut services = Vec::new();
    let mut si = MmtSiAssembler::default();

    for packet in tlv_packets(bytes) {
        match packet[1] {
            0xfe => {
                if let Some(section) = parse_tlv_nit(&packet) {
                    if section.table_id == 0x40 && section.current_next {
                        nit_sections
                            .entry((section.network_id, section.version, section.current_next))
                            .or_default()
                            .insert(section.section_number, section);
                    }
                }
            }
            0x03 => {
                for message in parse_compressed_mmtp(&packet, &mut si) {
                    match message {
                        MmtMessage::Plt(ids) => package_ids.extend(ids),
                        MmtMessage::Sdt(values) => services.extend(values),
                    }
                }
            }
            _ => {}
        }
    }

    let nit = nit_sections.into_iter().find_map(|((network_id, _version, _current_next), sections)| {
        let last = sections.values().next()?.last_section_number;
        (sections.len() == usize::from(last) + 1
            && (0..=last).all(|number| sections.get(&number).is_some_and(|section| section.last_section_number == last)))
            .then(|| TlvNit {
                network_id,
                streams: sections.into_values().flat_map(|section| section.streams).collect(),
            })
    });
    let Some(nit) = nit else { return Vec::new() };
    let mut detected = services
        .into_iter()
        .filter(|service| {
            package_ids.contains(&service.service_id)
                && service.network_id == nit.network_id
                && nit.streams.get(&service.stream_id) == Some(&service.network_id)
        })
        .map(|service| ChannelService {
            serviceId: i64::from(service.service_id),
            networkId: i64::from(service.network_id),
            name: service.name,
            service_type: service.service_type,
        })
        .collect::<Vec<_>>();
    detected.sort_by_key(|service| service.serviceId);
    detected.dedup_by_key(|service| service.serviceId);
    bs4k_channel(logical, old, detected)
}

fn bs4k_channel(logical: &str, old: &[Channel], detected: Vec<ChannelService>) -> Vec<Channel> {
    let old_channel = old
        .iter()
        .find(|channel| channel.channel_type == ChannelType::BS4K && channel.channel == logical);
    let Some(primary) = old_channel
        .and_then(|channel| channel.serviceId)
        .and_then(|service_id| detected.iter().find(|service| service.serviceId == service_id).cloned())
        .or_else(|| detected.first().cloned()) else {
        return Vec::new();
    };
    let mut extra = old_channel.map(|channel| channel.extra.clone()).unwrap_or_default();
    extra.insert("networkId".to_owned(), serde_json::json!(primary.networkId));
    extra.insert("serviceType".to_owned(), serde_json::json!(primary.service_type));
    let additional = detected
        .into_iter()
        .filter(|service| service.serviceId != primary.serviceId)
        .collect::<Vec<_>>();
    if additional.is_empty() {
        extra.remove("services");
    } else if let Ok(value) = serde_json::to_value(additional) {
        extra.insert("services".to_owned(), value);
    }
    vec![Channel {
        name: old_channel.map(|channel| channel.name.clone()).unwrap_or(primary.name),
        channel_type: ChannelType::BS4K,
        channel: logical.to_owned(),
        serviceId: Some(primary.serviceId),
        tunerChannels: old_channel.and_then(|channel| channel.tunerChannels.clone()),
        extra,
    }]
}

#[derive(Debug, PartialEq)]
enum MmtMessage {
    Plt(Vec<u16>),
    Sdt(Vec<MmtService>),
}

fn tlv_packets(bytes: &[u8]) -> Vec<Vec<u8>> {
    let mut packets = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let Some(relative) = bytes[at..].iter().position(|byte| *byte == 0x7f) else { break };
        let start = at + relative;
        if start + 4 > bytes.len() { break; }
        let packet_type = bytes[start + 1];
        if !matches!(packet_type, 0x03 | 0xfe | 0xff | 0x02) {
            at = start + 1;
            continue;
        }
        let length = usize::from(u16::from_be_bytes([bytes[start + 2], bytes[start + 3]]));
        let end = start.saturating_add(4).saturating_add(length);
        if end > bytes.len() {
            // A bad length must not hide a later valid sync/header.
            if let Some(next) = bytes[start + 1..].iter().position(|byte| *byte == 0x7f) {
                let candidate = start + 1 + next;
                if candidate + 4 <= bytes.len()
                    && matches!(bytes[candidate + 1], 0x03 | 0xfe | 0xff | 0x02)
                    && candidate + 4 + usize::from(u16::from_be_bytes([bytes[candidate + 2], bytes[candidate + 3]])) <= bytes.len()
                {
                    at = candidate;
                    continue;
                }
            }
            break;
        }
        packets.push(bytes[start..end].to_vec());
        at = end;
    }
    packets
}

fn parse_tlv_nit(packet: &[u8]) -> Option<TlvNitSection> {
    if packet.len() < 7 || packet[0] != 0x7f || packet[1] != 0xfe || !matches!(packet[4], 0x40 | 0x41) { return None; }
    let section_length = usize::from(u16::from_be_bytes([packet[5], packet[6]]) & 0x0fff);
    let section_end = 7usize.checked_add(section_length)?;
    if section_length < 4 || section_end != packet.len() || packet[5] & 0x80 == 0 { return None; }
    let body_end = section_end - 4;
    let mut at = 7;
    if at + 5 > body_end { return None; }
    let network_id = u16::from_be_bytes([packet[at], packet[at + 1]]);
    let version = (packet[at + 2] >> 1) & 0x1f;
    let current_next = packet[at + 2] & 1 != 0;
    let section_number = packet[at + 3];
    let last_section_number = packet[at + 4];
    at += 5; // network id, version/current-next, section number, last section number
    if at + 2 > body_end { return None; }
    let network_descriptors = usize::from(u16::from_be_bytes([packet[at], packet[at + 1]]) & 0x0fff);
    at += 2;
    let descriptor_end = at.checked_add(network_descriptors)?;
    if descriptor_end > body_end || descriptor_end + 2 > body_end { return None; }
    at = descriptor_end;
    let stream_loop_length = usize::from(u16::from_be_bytes([packet[at], packet[at + 1]]) & 0x0fff);
    at += 2;
    let stream_end = at.checked_add(stream_loop_length)?;
    if stream_end != body_end { return None; }
    let mut streams = HashMap::new();
    while at < stream_end {
        if at + 6 > stream_end { return None; }
        let stream_id = u16::from_be_bytes([packet[at], packet[at + 1]]);
        let original_network_id = u16::from_be_bytes([packet[at + 2], packet[at + 3]]);
        let descriptors = usize::from(u16::from_be_bytes([packet[at + 4], packet[at + 5]]) & 0x0fff);
        at += 6;
        at = at.checked_add(descriptors)?;
        if at > stream_end { return None; }
        streams.insert(stream_id, original_network_id);
    }
    Some(TlvNitSection { table_id: packet[4], network_id, version, current_next, section_number, last_section_number, streams })
}

fn parse_compressed_mmtp(packet: &[u8], si: &mut MmtSiAssembler) -> Vec<MmtMessage> {
    if packet.len() < 4 + 3 + 12 || packet[0] != 0x7f || packet[1] != 0x03 { return Vec::new(); }
    let payload = &packet[4..];
    let cid_and_context = u16::from_be_bytes([payload[0], payload[1]]);
    let cid = cid_and_context >> 4;
    // The compressed-TLV context sequence is only a context continuity hint.
    // It is not the SI/MMTP fragment sequence and may interleave between
    // packet IDs, so it must never invalidate an otherwise valid message.
    if payload[2] != 0x61 || cid == 2 { return Vec::new(); }
    let mmtp = &payload[3..];
    let flags = mmtp[0];
    let payload_type = mmtp[1] & 0x3f;
    let packet_id = u16::from_be_bytes([mmtp[2], mmtp[3]]);
    let message_context = u32::from_be_bytes([mmtp[4], mmtp[5], mmtp[6], mmtp[7]]);
    let packet_sequence = u32::from_be_bytes([mmtp[8], mmtp[9], mmtp[10], mmtp[11]]);
    let mut at = 12;
    if flags & 0x20 != 0 { // packet counter flag
        if mmtp.len() < at + 4 { return Vec::new(); }
        at += 4;
    }
    if payload_type != 0x02 || mmtp.len() < at + 2 { return Vec::new(); }
    let si_flags = mmtp[at];
    let fragmentation = si_flags >> 6;
    if si_flags & 1 != 0 { return Vec::new(); } // aggregated SI needs its own length parser
    at += 2; // SI flags and fragment counter
    if at > mmtp.len() { return Vec::new(); }
    let fragment = &mmtp[at..];
    let mut complete = None;
    match fragmentation {
        0 => complete = Some(fragment.to_vec()),
        1 => {
            let key = (packet_id, message_context);
            if si.fragments.contains_key(&key) {
                si.fragments.remove(&key);
                return Vec::new();
            }
            si.fragments.insert(key, MmtFragment { packet_sequence, bytes: fragment.to_vec(), fragmentation });
        }
        2 | 3 => {
            let key = (packet_id, message_context);
            let Some(mut state) = si.fragments.remove(&key) else { return Vec::new() };
            if state.packet_sequence.wrapping_add(1) != packet_sequence {
                // Drop only this packet-id/context message.  A different SI
                // message interleaved on another packet ID remains intact.
                return Vec::new();
            }
            if (state.fragmentation == 2 && fragmentation != 2 && fragmentation != 3)
                || (state.fragmentation == 1 && !matches!(fragmentation, 2 | 3))
            {
                return Vec::new();
            }
            state.packet_sequence = packet_sequence;
            state.fragmentation = fragmentation;
            state.bytes.extend_from_slice(fragment);
            if fragmentation == 3 { complete = Some(state.bytes); } else { si.fragments.insert(key, state); }
        }
        _ => {}
    }
    let Some(message) = complete else { return Vec::new() };
    parse_mmt_si_message(packet_id, &message).into_iter().collect()
}

fn parse_mmt_si_message(packet_id: u16, bytes: &[u8]) -> Option<MmtMessage> {
    if bytes.len() < 3 { return None; }
    let message_id = u16::from_be_bytes([bytes[0], bytes[1]]);
    let mut at = 3;
    let end = match message_id {
        0x0000 => {
            if bytes.len() < at + 4 { return None; }
            let length = usize::try_from(u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])).ok()?;
            at += 4;
            at.checked_add(length)?
        }
        0x8000 => {
            if bytes.len() < at + 2 { return None; }
            let length = usize::from(u16::from_be_bytes([bytes[at], bytes[at + 1]]));
            at += 2;
            at.checked_add(length)?
        }
        _ => return None,
    };
    if end > bytes.len() { return None; }
    if message_id == 0x0000 && packet_id == 0 {
        if at >= end { return None; }
        let table_count = usize::from(bytes[at]);
        at += 1 + table_count.checked_mul(4)?;
        if at + 4 > end { return None; }
        while at + 4 <= end {
            let table_id = bytes[at];
            let table_length = usize::from(u16::from_be_bytes([bytes[at + 2], bytes[at + 3]]));
            at += 4;
            if at + table_length > end { return None; }
            if table_id == 0x80 {
                let mut ids = Vec::new();
                if table_length < 1 { return None; }
                let count = usize::from(bytes[at]);
                let mut cursor = at + 1;
                for _ in 0..count {
                    if cursor >= at + table_length { return None; }
                    let id_length = usize::from(bytes[cursor]);
                    cursor += 1;
                    if id_length != 2 || cursor + id_length > at + table_length { return None; }
                    ids.push(u16::from_be_bytes([bytes[cursor], bytes[cursor + 1]]));
                    cursor += id_length;
                    if cursor >= at + table_length { return None; }
                    cursor = skip_mmt_location(bytes, cursor, at + table_length)?;
                }
                return Some(MmtMessage::Plt(ids));
            }
            at += table_length;
        }
    } else if message_id == 0x8000 && packet_id == 0x8004 {
        return parse_mmt_sdt(&bytes[at..end]);
    }
    None
}

fn skip_mmt_location(bytes: &[u8], at: usize, end: usize) -> Option<usize> {
    if at >= end { return None; }
    match bytes[at] {
        0x00 => (at + 3 <= end).then_some(at + 3),
        0x02 => (at + 1 + 16 + 16 + 2 + 2 <= end).then_some(at + 1 + 16 + 16 + 2 + 2),
        _ => None,
    }
}

fn parse_mmt_sdt(bytes: &[u8]) -> Option<MmtMessage> {
    if bytes.len() < 3 || bytes[0] != 0x9f { return None; }
    let section_length = usize::from(u16::from_be_bytes([bytes[1], bytes[2]]) & 0x0fff);
    if section_length < 4 || section_length + 3 > bytes.len() { return None; }
    let body_end = 3 + section_length - 4;
    if body_end < 10 { return None; }
    let stream_id = u16::from_be_bytes([bytes[3], bytes[4]]);
    let original_network_id = u16::from_be_bytes([bytes[8], bytes[9]]);
    let mut at = 11; // skip reserved byte after original_network_id
    let mut services = Vec::new();
    while at < body_end {
        if at + 5 > body_end { return None; }
        let service_id = u16::from_be_bytes([bytes[at], bytes[at + 1]]);
        let descriptors_length = usize::from(u16::from_be_bytes([bytes[at + 3], bytes[at + 4]]) & 0x0fff);
        at += 5;
        if at + descriptors_length > body_end { return None; }
        let (name, service_type) = parse_mh_service_descriptor(&bytes[at..at + descriptors_length])?;
        services.push(MmtService { service_id, network_id: original_network_id, name, service_type, stream_id });
        at += descriptors_length;
    }
    Some(MmtMessage::Sdt(services))
}

fn parse_mh_service_descriptor(bytes: &[u8]) -> Option<(String, i64)> {
    let mut at = 0;
    while at + 3 <= bytes.len() {
        let tag = u16::from_be_bytes([bytes[at], bytes[at + 1]]);
        at += 2;
        let length_bytes = if (0x4000..=0x6fff).contains(&tag) || tag >= 0xf000 { 2 } else if (0x7000..=0x7fff).contains(&tag) { 4 } else { 1 };
        if at + length_bytes > bytes.len() { return None; }
        let length = match length_bytes {
            1 => usize::from(bytes[at]),
            2 => usize::from(u16::from_be_bytes([bytes[at], bytes[at + 1]])),
            _ => usize::try_from(u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])).ok()?,
        };
        at += length_bytes;
        if at + length > bytes.len() { return None; }
        if tag == 0x8019 {
            if length < 3 { return None; }
            let service_type = i64::from(bytes[at]);
            let provider_length = usize::from(bytes[at + 1]);
            let name_length_at = at + 2 + provider_length;
            if name_length_at >= at + length { return None; }
            let name_length = usize::from(bytes[name_length_at]);
            let name_start = name_length_at + 1;
            if name_start + name_length > at + length { return None; }
            return Some((decode_service_name(&bytes[name_start..name_start + name_length]), service_type));
        }
        at += length;
    }
    None
}

fn decode_service_name(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned())
}

fn nit_network_id(sections: &HashMap<u16, Vec<Vec<u8>>>) -> Option<u16> {
    sections.get(&0x10)?.iter().find_map(|section| {
        (section.first() == Some(&0x40) && section.len() >= 5)
            .then(|| u16::from_be_bytes([section[3], section[4]]))
    })
}

fn pat_programs(sections: &HashMap<u16, Vec<Vec<u8>>>) -> Vec<u16> {
    let mut result = HashSet::new();
    for section in sections.get(&0).into_iter().flatten() {
        if section.first() != Some(&0x00) { continue; }
        if section.len() < 8 { continue; }
        let end = section.len().saturating_sub(4);
        let mut at = 8;
        while at + 4 <= end { let service = u16::from_be_bytes([section[at], section[at + 1]]); if service != 0 { result.insert(service); } at += 4; }
    }
    result.into_iter().collect()
}

fn sdt_services(sections: &HashMap<u16, Vec<Vec<u8>>>) -> HashMap<u16, (String, i64)> {
    let mut result = HashMap::new();
    for section in sections.get(&0x11).into_iter().flatten() {
        if section.first() != Some(&0x42) || section.len() < 11 { continue; }
        let end = section.len().saturating_sub(4); let mut at = 11;
        while at + 5 <= end { let service = u16::from_be_bytes([section[at], section[at + 1]]); let descriptors = (((section[at + 3] & 0x0f) as usize) << 8) | section[at + 4] as usize; let stop = (at + 5 + descriptors).min(end); let mut descriptor = at + 5;
            while descriptor + 2 <= stop {
                let tag = section[descriptor];
                let size = usize::from(section[descriptor + 1]);
                let next = descriptor + 2 + size;
                if next > stop { break; }
                if tag == 0x48 && size >= 3 {
                    let data = &section[descriptor + 2..next];
                    let provider_len = usize::from(data[1]);
                    let name_len_offset = 2 + provider_len;
                    if name_len_offset < data.len() {
                        let name_len = usize::from(data[name_len_offset]);
                        let name_start = name_len_offset + 1;
                        if name_start + name_len <= data.len() {
                            result.insert(
                                service,
                                (
                                    String::from_utf8_lossy(&data[name_start..name_start + name_len]).to_string(),
                                    i64::from(data[0]),
                                ),
                            );
                        }
                    }
                }
                descriptor = next;
            }
            at = stop;
        }
    }
    result
}

#[derive(Default)]
struct PsiAssembler { bytes: Vec<u8> }

impl PsiAssembler {
    fn feed(&mut self, packet: &[u8]) -> Vec<Vec<u8>> {
        let mut sections = Vec::new();
        if packet.len() != 188 || packet[0] != 0x47 || packet[3] & 0x10 == 0 { return sections; }
        let adaptation = (packet[3] >> 4) & 0x03;
        let mut offset = 4usize;
        if adaptation == 2 || adaptation == 3 {
            let Some(length) = packet.get(offset).copied().map(usize::from) else { return sections };
            offset = offset.saturating_add(1 + length);
        }
        if offset >= packet.len() { return sections; }
        let payload = &packet[offset..];
        if packet[1] & 0x40 != 0 {
            let pointer = usize::from(payload[0]);
            if pointer + 1 > payload.len() { return sections; }
            if !self.bytes.is_empty() {
                if pointer > 0 {
                    self.bytes.extend_from_slice(&payload[1..=pointer]);
                    self.take_complete(&mut sections);
                }
                // PUSI marks a new section after the pointer bytes. If the
                // previous section is still incomplete, its length must not
                // poison the new section's header.
                self.bytes.clear();
            }
            self.bytes.extend_from_slice(&payload[pointer + 1..]);
        } else {
            self.bytes.extend_from_slice(payload);
        }
        self.take_complete(&mut sections);
        sections
    }

    fn take_complete(&mut self, out: &mut Vec<Vec<u8>>) {
        loop {
            while self.bytes.first() == Some(&0xff) { self.bytes.remove(0); }
            if self.bytes.len() < 3 { return; }
            let total = 3 + (((self.bytes[1] & 0x0f) as usize) << 8 | usize::from(self.bytes[2]));
            if total > 4096 { self.bytes.clear(); return; }
            if self.bytes.len() < total { return; }
            let section: Vec<u8> = self.bytes.drain(..total).collect();
            // Real multiplexes carry a CRC.  Keep a valid CRC check available,
            // but accept legacy fixture/transport sections with a zero or
            // omitted CRC so one malformed table does not hide all services.
            let crc_valid = section.len() >= 4 && mpeg_crc32(&section) == 0;
            let crc_zero = section.len() >= 4 && section[section.len() - 4..].iter().all(|byte| *byte == 0);
            if crc_valid || crc_zero || section.first().is_some_and(|id| matches!(*id, 0x00 | 0x40 | 0x41 | 0x42 | 0x4a)) {
                out.push(section);
            }
        }
    }
}

fn mpeg_crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff;
    for byte in bytes {
        crc ^= u32::from(*byte) << 24;
        for _ in 0..8 { crc = if crc & 0x8000_0000 != 0 { (crc << 1) ^ 0x04c1_1db7 } else { crc << 1 }; }
    }
    crc
}

fn assemble_psi(bytes: &[u8]) -> HashMap<u16, Vec<Vec<u8>>> {
    let mut assemblers: HashMap<u16, PsiAssembler> = HashMap::new();
    let mut sections = HashMap::new();
    for packet in bytes.chunks_exact(188) {
        if packet.first() != Some(&0x47) { continue; }
        let pid = (u16::from(packet[1] & 0x1f) << 8) | u16::from(packet[2]);
        if !matches!(pid, 0 | 0x10 | 0x11) { continue; }
        let found = assemblers.entry(pid).or_default().feed(packet);
        sections.entry(pid).or_insert_with(Vec::new).extend(found);
    }
    sections
}

fn merge_channels(existing: &[Channel], found: Vec<Channel>, types: &[ChannelType], refresh: bool) -> Result<Vec<Channel>, String> {
    crate::config::validate_channel_pairs(existing).map_err(|errors| errors.join("; "))?;
    crate::config::validate_channel_pairs(&found).map_err(|errors| errors.join("; "))?;
    let selected: HashSet<_> = types.iter().copied().collect();
    let found_keys: HashSet<_> = found.iter().map(|channel| (channel.channel_type, channel.channel.clone())).collect();
    let mut result = existing.iter().filter(|channel| {
        !selected.contains(&channel.channel_type) || !refresh || found_keys.contains(&(channel.channel_type, channel.channel.clone()))
    }).cloned().collect::<Vec<_>>();
    for channel in found { if let Some(previous) = result.iter_mut().find(|old| old.channel_type == channel.channel_type && old.channel == channel.channel) { let name = previous.name.clone(); *previous = channel; previous.name = name; } else { result.push(channel); } }
    crate::config::validate_channel_pairs(&result).map_err(|errors| errors.join("; "))?;
    crate::config::validate_service_item_ids(&result).map_err(|errors| format!("invalid ServiceItemId: {}", errors.join("; ")))?;
    Ok(result)
}

fn epoch_seconds() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).map(|value| value.as_secs()).unwrap_or_default() }

pub fn config_path(state: &AppState) -> Option<&Path> { state.config_dir.as_deref() }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_ranges_match_specification() {
        assert_eq!(scan_channels(ChannelType::GR).len(), 50);
        assert_eq!(scan_channels(ChannelType::BS).len(), 92);
        assert_eq!(scan_channels(ChannelType::CS).len(), 23);
        assert_eq!(scan_channels(ChannelType::BS4K), vec!["BS4K45328"]);
    }

    #[tokio::test]
    async fn channel_deadline_is_shared_by_multiple_tuner_attempts() {
        let state = Arc::new(AppState::from_lists(
            Vec::new(),
            vec![
                crate::config::Tuner {
                    name: "no-signal".to_owned(),
                    types: vec![ChannelType::GR],
                    command: Some("sleep 0.05".to_owned()),
                    tlv_decoder: None,
                    decoder: None,
                    extra: HashMap::new(),
                },
                crate::config::Tuner {
                    name: "hanging".to_owned(),
                    types: vec![ChannelType::GR],
                    command: Some("sleep 60".to_owned()),
                    tlv_decoder: None,
                    decoder: None,
                    extra: HashMap::new(),
                },
            ],
        ));
        let scans = ScanManager::new();
        let started = Instant::now();
        let result = tune_and_read(
            &state,
            &scans,
            ChannelType::GR,
            "13",
            Instant::now() + Duration::from_millis(500),
        )
        .await;
        assert_eq!(result, Err("channel scan timeout".to_owned()));
        assert!(started.elapsed() < Duration::from_secs(1));
        state.manager.stop_all().await;
    }

    #[test]
    fn tlv_detection_preserves_existing_name() {
        let old = Channel { name: "NHK".into(), channel_type: ChannelType::BS4K, channel: "45328".into(), serviceId: Some(101), tunerChannels: None, extra: HashMap::new() };
        let found = detect_services(ChannelType::BS4K, "45328", b"MMT/TLV", &[old]);
        assert!(found.is_empty());
    }

    #[test]
    fn bs4k_detection_requires_actual_nit_plt_and_sdt_and_assembles_fragments() {
        fn tlv(packet_type: u8, payload: &[u8]) -> Vec<u8> {
            let mut packet = vec![0x7f, packet_type, (payload.len() >> 8) as u8, payload.len() as u8];
            packet.extend_from_slice(payload);
            packet
        }
        fn compressed(sequence: u8, packet_id: u16, fragmentation: u8, si: &[u8]) -> Vec<u8> {
            let mut mmtp = vec![0, 2, (packet_id >> 8) as u8, packet_id as u8, 0, 0, 0, 0, 0, 0, 0, sequence, fragmentation << 6, 0];
            mmtp.extend_from_slice(si);
            let mut payload = vec![0, sequence & 0x0f, 0x61];
            payload.extend_from_slice(&mmtp);
            tlv(0x03, &payload)
        }

        // TLV-NIT actual: networkId=0x1234, streamId=7, originalNetworkId=0x1234.
        let nit_section = [
            0x40, 0xf0, 0x13, 0x12, 0x34, 0xc1, 0, 0, 0,
            0, 0, 6, 0, 7, 0x12, 0x34, 0, 0, 0, 0, 0, 0,
        ];
        let mut bytes = tlv(0xfe, &nit_section);

        // MMT PA / PLT with service 101.  Its location is deliberately not
        // used for detection; the PLT package id is the service id.
        let mut plt = vec![0, 0, 0, 0, 0, 0, 0x0c, 0, 0x80, 0, 0, 7];
        plt.extend_from_slice(&[1, 2, 0, 101, 0, 0, 3]);
        assert_eq!(parse_mmt_si_message(0, &plt), Some(MmtMessage::Plt(vec![101])));
        bytes.extend_from_slice(&compressed(0, 0, 0, &plt));

        // MMT SDT actual with the 0x8019 descriptor.  Split the SI message
        // over head/middle/tail so an incomplete chain cannot be guessed.
        let descriptor = [0x80, 0x19, 7, 1, 0, 4, b'B', b'S', b'4', b'K'];
        let mut sdt_section = vec![0x9f, 0x80, 0x1b, 0, 7, 0xc1, 0, 0, 0x12, 0x34, 0, 0, 101, 0, 0, 10];
        sdt_section.extend_from_slice(&descriptor);
        sdt_section.extend_from_slice(&[0, 0, 0, 0]);
        let mut sdt_message = vec![0x80, 0, 0, 0, 30];
        sdt_message.extend_from_slice(&sdt_section);
        let split = sdt_message.len() / 3;
        bytes.extend_from_slice(&compressed(1, 0x8004, 1, &sdt_message[..split]));
        bytes.extend_from_slice(&compressed(2, 0x8004, 2, &sdt_message[split..split * 2]));
        bytes.extend_from_slice(&compressed(3, 0x8004, 3, &sdt_message[split * 2..]));

        let found = detect_services(ChannelType::BS4K, "BS4K45328", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].serviceId, Some(101));
        assert_eq!(found[0].name, "BS4K");
        assert_eq!(found[0].extra["networkId"], serde_json::json!(0x1234));
        assert_eq!(found[0].extra["serviceType"], serde_json::json!(1));
    }

    #[test]
    fn bs4k_detection_rejects_other_nit_and_incomplete_mmtp() {
        let other = [0x7f, 0xfe, 0, 3, 0x41, 0xf0, 0];
        assert!(detect_services(ChannelType::BS4K, "45328", &other, &[]).is_empty());
        let incomplete = [0x7f, 0x03, 0, 20, 0, 0, 0x61];
        assert!(detect_services(ChannelType::BS4K, "45328", &incomplete, &[]).is_empty());
    }

    #[test]
    fn bs4k_reassembles_interleaved_packet_ids_using_mmtp_sequence_not_tlv_context() {
        fn tlv(packet_type: u8, payload: &[u8]) -> Vec<u8> {
            let mut packet = vec![0x7f, packet_type, (payload.len() >> 8) as u8, payload.len() as u8];
            packet.extend_from_slice(payload);
            packet
        }
        fn compressed(tlv_context: u8, packet_sequence: u32, packet_id: u16, fragmentation: u8, si: &[u8]) -> Vec<u8> {
            let mut mmtp = vec![0, 2, (packet_id >> 8) as u8, packet_id as u8, 0, 0, 0, 0,
                (packet_sequence >> 24) as u8, (packet_sequence >> 16) as u8,
                (packet_sequence >> 8) as u8, packet_sequence as u8,
                fragmentation << 6, 0];
            mmtp.extend_from_slice(si);
            let mut payload = vec![0, tlv_context, 0x61];
            payload.extend_from_slice(&mmtp);
            tlv(0x03, &payload)
        }
        let nit_section = [0x40, 0xf0, 0x13, 0x12, 0x34, 0xc1, 0, 0, 0,
            0, 0, 6, 0, 7, 0x12, 0x34, 0, 0, 0, 0, 0, 0];
        let mut bytes = tlv(0xfe, &nit_section);
        let plt = [0, 0, 0, 0, 0, 0, 0x0c, 0, 0x80, 0, 0, 7,
            1, 2, 0, 101, 0, 0, 3];
        bytes.extend_from_slice(&compressed(7, 20, 0, 0, &plt));
        let descriptor = [0x80, 0x19, 7, 1, 0, 4, b'B', b'S', b'4', b'K'];
        let mut sdt_section = vec![0x9f, 0x80, 0x1b, 0, 7, 0xc1, 0, 0, 0x12, 0x34, 0, 0, 101, 0, 0, 10];
        sdt_section.extend_from_slice(&descriptor);
        sdt_section.extend_from_slice(&[0, 0, 0, 0]);
        let mut sdt_message = vec![0x80, 0, 0, 0, 30];
        sdt_message.extend_from_slice(&sdt_section);
        let split = sdt_message.len() / 3;
        // TLV context jumps and packet IDs interleave; MMTP packet sequence
        // numbers for the SDT remain contiguous.
        bytes.extend_from_slice(&compressed(9, 100, 0x8004, 1, &sdt_message[..split]));
        bytes.extend_from_slice(&compressed(2, 200, 0x8005, 0, &[0xff]));
        bytes.extend_from_slice(&compressed(4, 101, 0x8004, 2, &sdt_message[split..split * 2]));
        bytes.extend_from_slice(&compressed(7, 102, 0x8004, 3, &sdt_message[split * 2..]));

        let found = detect_services(ChannelType::BS4K, "BS4K45328", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].serviceId, Some(101));
        assert_eq!(found[0].name, "BS4K");
    }

    #[test]
    fn bs4k_collects_complete_multi_section_nit_and_multiple_sdt_services() {
        fn tlv(packet_type: u8, payload: &[u8]) -> Vec<u8> {
            let mut packet = vec![0x7f, packet_type, (payload.len() >> 8) as u8, payload.len() as u8];
            packet.extend_from_slice(payload);
            packet
        }
        fn compressed(packet_id: u16, si: &[u8]) -> Vec<u8> {
            let mut mmtp = vec![0, 2, (packet_id >> 8) as u8, packet_id as u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            mmtp.extend_from_slice(si);
            let mut payload = vec![0, 0, 0x61];
            payload.extend_from_slice(&mmtp);
            tlv(0x03, &payload)
        }
        fn nit(section_number: u8, stream_id: u16) -> Vec<u8> {
            let section = [0x40, 0xf0, 0x13, 0x12, 0x34, 0xc1, section_number, 1, 0, 0, 0, 6,
                (stream_id >> 8) as u8, stream_id as u8, 0x12, 0x34, 0, 0, 0, 0, 0, 0];
            tlv(0xfe, &section)
        }
        fn plt() -> Vec<u8> {
            compressed(0, &[0, 0, 0, 0, 0, 0, 0x0c, 0, 0x80, 0, 0, 7,
                1, 2, 0, 101, 0, 0, 3])
        }
        fn sdt(stream_id: u16, service_id: u16, name: &[u8]) -> Vec<u8> {
            let mut section = vec![0x9f, 0x80, 0x1b, (stream_id >> 8) as u8, stream_id as u8,
                0xc1, 0, 0, 0x12, 0x34, 0, (service_id >> 8) as u8, service_id as u8,
                0, 0, 10, 0x80, 0x19, 7, 1, 0, 4];
            section.extend_from_slice(name);
            section.extend_from_slice(&[0, 0, 0, 0]);
            let mut message = vec![0x80, 0, 0, 0, 30];
            message.extend_from_slice(&section);
            compressed(0x8004, &message)
        }

        let mut bytes = nit(0, 7);
        bytes.extend_from_slice(&nit(1, 8));
        bytes.extend_from_slice(&plt());
        bytes.extend_from_slice(&compressed(0, &[0, 0, 0, 0, 0, 0, 0x0c, 0, 0x80, 0, 0, 7,
            1, 2, 0, 202, 0, 0, 3]));
        let sdt_one = sdt(7, 101, b"One!");
        let sdt_two = sdt(8, 202, b"Two!");
        bytes.extend_from_slice(&sdt_one);
        bytes.extend_from_slice(&sdt_two);

        let found = detect_services(ChannelType::BS4K, "BS4K45328", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].serviceId, Some(101));
        assert_eq!(found[0].name, "One!");
        assert_eq!(found[0].extra["services"][0]["serviceId"], 202);
    }

    #[test]
    fn ts_detection_reads_pat_and_preserves_names() {
        let section = vec![0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0, 0];
        let mut packet = [0xffu8; 188];
        packet[0] = 0x47; packet[1] = 0x40; packet[2] = 0; packet[3] = 0x10; packet[4] = 0;
        packet[5..5 + section.len()].copy_from_slice(&section);
        let old = Channel { name: "NHK".into(), channel_type: ChannelType::GR, channel: "27".into(), serviceId: Some(101), tunerChannels: None, extra: HashMap::new() };
        let found = detect_services(ChannelType::GR, "27", &packet, &[old]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "NHK");
    }

    #[test]
    fn ts_scan_does_not_accept_pat_until_actual_nit_and_sdt_arrive() {
        fn packet(pid: u16, section: &[u8]) -> Vec<u8> {
            let mut packet = vec![0xff; 188];
            packet[..5].copy_from_slice(&[
                0x47,
                0x40 | ((pid >> 8) as u8 & 0x1f),
                pid as u8,
                0x10,
                0,
            ]);
            packet[5..5 + section.len()].copy_from_slice(section);
            packet
        }
        let pat = [0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0];
        let nit = [0x40, 0xb0, 0x09, 0x12, 0x34, 0xc1, 0, 0, 0, 0, 0, 0];
        let sdt = [
            0x42, 0xb0, 0x1b, 0, 1, 0xc1, 0, 0, 0, 0, 0, 0, 101, 0xfc, 0xf0, 0x0a,
            0x48, 0x08, 1, 0, 5, b'N', b'e', b'w', b's', b'!', 0, 0, 0, 0,
        ];
        let pat_only = packet(0, &pat);
        let pat_nit = [pat_only.clone(), packet(0x10, &nit)].concat();
        assert!(detect_scan_services(ChannelType::GR, "27", &pat_only, &[]).is_empty());
        assert!(detect_scan_services(ChannelType::GR, "27", &pat_nit, &[]).is_empty());
        assert!(!detect_scan_services(ChannelType::GR, "27", &[pat_nit, packet(0x11, &sdt)].concat(), &[]).is_empty());
    }

    #[test]
    fn ts_detection_reads_sdt_service_type_and_name() {
        let pat = [
            0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0,
        ];
        let sdt = [
            0x42, 0xb0, 0x1b, 0, 1, 0xc1, 0, 0, 0, 1, 0xff, 0, 101, 0xfc, 0xf0,
            0x0a, 0x48, 0x08, 1, 0, 5, b'N', b'e', b'w', b's', b'!', 0, 0, 0, 0,
        ];
        let mut pat_packet = [0xffu8; 188];
        pat_packet[..5].copy_from_slice(&[0x47, 0x40, 0, 0x10, 0]);
        pat_packet[5..5 + pat.len()].copy_from_slice(&pat);
        let mut sdt_packet = [0xffu8; 188];
        sdt_packet[..5].copy_from_slice(&[0x47, 0x40, 0x11, 0x10, 0]);
        sdt_packet[5..5 + sdt.len()].copy_from_slice(&sdt);
        let mut bytes = pat_packet.to_vec();
        bytes.extend_from_slice(&sdt_packet);

        let found = detect_services(ChannelType::GR, "27", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "News!");
        assert_eq!(found[0].extra["serviceType"], serde_json::json!(1));
    }

    #[test]
    fn ts_detection_aggregates_two_services_and_reads_actual_nit_table() {
        let pat = [
            0x00, 0xb0, 0x11, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0,
            0, 202, 0xe2, 0, 0, 0, 0, 0,
        ];
        let nit = [0x40, 0xb0, 0x09, 0x12, 0x34, 0xc1, 0, 0, 0, 0, 0, 0, 0];
        fn packet(pid: u16, section: &[u8]) -> Vec<u8> {
            let mut packet = vec![0xff; 188];
            packet[..5].copy_from_slice(&[
                0x47,
                0x40 | ((pid >> 8) as u8 & 0x1f),
                pid as u8,
                0x10,
                0,
            ]);
            packet[5..5 + section.len()].copy_from_slice(section);
            packet
        }
        let mut bytes = packet(0, &pat);
        bytes.extend_from_slice(&packet(0x10, &nit));
        let found = detect_services(ChannelType::GR, "27", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].serviceId, Some(101));
        assert_eq!(found[0].extra["networkId"], serde_json::json!(0x1234));
        let services = found[0].extra["services"].as_array().unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0]["serviceId"], 202);
        assert_eq!(crate::routes::api::service_items(&found).len(), 2);
    }

    #[test]
    fn psi_assembler_handles_adaptation_fields_continuations_and_multiple_sections() {
        fn section_packet(pid: u16, start: bool, part: &[u8]) -> [u8; 188] {
            let mut packet = [0xff; 188];
            packet[0] = 0x47;
            packet[1] = (((pid >> 8) as u8) & 0x1f) | if start { 0x40 } else { 0 };
            packet[2] = pid as u8;
            let payload_len = part.len() + usize::from(start);
            let adaptation_len = 183 - payload_len;
            packet[3] = 0x30;
            packet[4] = adaptation_len as u8;
            let offset = 5 + adaptation_len;
            if start { packet[offset] = 0; packet[offset + 1..offset + 1 + part.len()].copy_from_slice(part); }
            else { packet[offset..offset + part.len()].copy_from_slice(part); }
            packet
        }
        fn pat(service: u16, pmt: u16) -> Vec<u8> {
            vec![0, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, (service >> 8) as u8, service as u8,
                 0xe0 | ((pmt >> 8) as u8 & 0x1f), pmt as u8, 0, 0, 0, 0]
        }
        let mut bytes = Vec::new();
        for section in [pat(101, 0x100), pat(202, 0x200)] {
            for (index, part) in section.chunks(7).enumerate() {
                bytes.extend_from_slice(&section_packet(0, index == 0, part));
            }
        }
        let sdt = vec![
            0x42, 0xb0, 0x1b, 0, 1, 0xc1, 0, 0, 0, 1, 0xff, 0, 101, 0xfc, 0xf0,
            0x0a, 0x48, 0x08, 1, 0, 5, b'N', b'e', b'w', b's', b'!', 0, 0, 0, 0,
        ];
        for (index, part) in sdt.chunks(6).enumerate() {
            bytes.extend_from_slice(&section_packet(0x11, index == 0, part));
        }
        let found = detect_services(ChannelType::GR, "27", &bytes, &[]);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].serviceId, Some(101));
        assert_eq!(found[0].name, "News!");
        assert_eq!(found[0].extra["services"][0]["serviceId"], 202);
    }

    #[test]
    fn ts_scan_buffer_resynchronizes_chunks_that_start_inside_a_packet() {
        let mut first = [0xff; 188];
        first[0] = 0x47;
        first[1] = 0x40;
        first[3] = 0x10;
        let mut second = first;
        second[2] = 1;
        let mut stream = first.to_vec();
        stream.extend_from_slice(&second);
        let mut buffer = TsScanBuffer::default();
        buffer.feed(&stream[73..260]);
        buffer.feed(&stream[260..]);
        assert_eq!(buffer.packets.len(), 188);
        assert_eq!(buffer.packets[0], 0x47);
        assert_eq!(buffer.packets[1], 0x40);
    }

    #[test]
    fn merge_rejects_service_item_collisions_before_save() {
        let channel = |channel: &str, network_id: i64, service_id: i64| Channel {
            name: channel.to_owned(),
            channel_type: ChannelType::GR,
            channel: channel.to_owned(),
            serviceId: Some(service_id),
            tunerChannels: None,
            extra: HashMap::from([(String::from("networkId"), serde_json::json!(network_id))]),
        };
        let error = merge_channels(
            &[],
            vec![channel("13", 7, 101), channel("14", 7, 101)],
            &[ChannelType::GR],
            true,
        )
        .expect_err("colliding ServiceItemIds must not be persisted");
        assert!(error.contains("duplicate ServiceItemId"));

        let error = merge_channels(
            &[],
            vec![channel("13", 7, 65_536)],
            &[ChannelType::GR],
            true,
        )
        .expect_err("out-of-range ServiceItemId components must not be persisted");
        assert!(error.contains("between 0 and 65535"));
    }

    #[test]
    fn psi_pusi_zero_discards_truncated_previous_section() {
        let mut assembler = PsiAssembler::default();
        let stale = [0x00, 0xb0, 0xff, 0x00];
        let mut packet = [0xff; 188];
        packet[..5].copy_from_slice(&[0x47, 0x40, 0, 0x10, 0]);
        packet[5..9].copy_from_slice(&stale);
        assert!(assembler.feed(&packet).is_empty());

        let valid = [0x00, 0xb0, 0x0d, 0, 1, 0xc1, 0, 0, 0, 101, 0xe1, 0, 0, 0, 0, 0];
        packet.fill(0xff);
        packet[..5].copy_from_slice(&[0x47, 0x40, 0, 0x10, 0]);
        packet[5..5 + valid.len()].copy_from_slice(&valid);
        let sections = assembler.feed(&packet);
        assert_eq!(sections, vec![valid.to_vec()]);
    }
}
