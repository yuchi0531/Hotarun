# Hotarun
## Lightweight Mirakurun / MMirakurun Compatible Tuner Server
### Software Specification v0.1

## 1. 概要

Hotarunは、Mirakurun / MMirakurun互換のAPI・設定形式を持つ、軽量なチューナーサーバーである。

主目的は、チューナーデバイスや外部チューナープログラムから取得した放送ストリームをHTTP経由で配信することである。

EPG・録画管理・トランスコード等は実装せず、チューナーサーバーとして必要な機能に限定する。

### 対応

- Mirakurun互換 `channels.yml`
- Mirakurun互換 `tuners.yml`
- Mirakurun互換HTTP API
- Rivarun
- BonDriver_Mirakurun
- GR
- BS
- CS
- BS4K
- MPEG-2 TS
- BS4K TLV/MMT
- チューナー自動選択
- ストリーム共有
- チャンネルスキャン
- チューナー固有チャンネルマッピング
- 最小限のWeb UI

### 対象外

- EPG収集
- 番組データベース
- 録画管理
- HLS/DASH
- 動画デコード
- 動画エンコード
- トランスコード
- DLNA
- メディアサーバー機能


## 2. 設計方針

- 軽量
- 単一バイナリ
- 外部依存を最小化
- ストリームを可能な限りそのまま転送
- チューナープログラムを外部プロセスとして利用
- Mirakurun/MMirakurunとの互換性を優先
- 設定ファイルはYAML
- APIはHTTP/JSON
- Web UIは単純なHTML/CSS/JavaScript
- Dockerを必須としない


## 3. 技術構成

### Backend

- Rust
- Tokio
- Axum
- Serde
- serde_yaml
- serde_json
- tracing

### Frontend

- HTML
- CSS
- JavaScript

フロントエンドフレームワークは使用しない。

### 実行形式

```text
/usr/bin/hotarun
/etc/hotarun/channels.yml
/etc/hotarun/tuners.yml
/var/log/hotarun/
```

systemdによる常駐実行を基本とする。


## 4. 放送種別

| Type | Stream |
|---|---|
| GR | MPEG-2 TS |
| BS | MPEG-2 TS |
| CS | MPEG-2 TS |
| SKY | MPEG-2 TS |
| BS4K | TLV/MMT |

TS/TLVをチューナーごとに手動指定する設定は持たない。

`channel.type` によりストリーム形式を決定する。


## 5. channels.yml

基本的にMirakurun/MMirakurun形式に準拠する。

- 必須は `name`, `type`, `channel`。`channel` は string。
- `type` は `GR|BS|CS|SKY` + 拡張 `BS4K`。
- 識別子は `(type, channel)` ペアで判定する。衝突時の優先規則は持たない。
- スキャンによる更新時は `name` を上書きしない。

```yaml
- name: NHK BS
  type: BS
  channel: BS01_0
  serviceId: 101

- name: NHK BS4K
  type: BS4K
  channel: BS01_0
  serviceId: 101
```

### tunerChannels

チューナーによって物理チャンネル番号が異なる環境に対応するため、Hotarunでは `tunerChannels` をサポートする。

```yaml
- name: NHK BS
  type: BS
  channel: BS01_0
  serviceId: 101

  tunerChannels:
    DVB-C-0: "13"
    DVB-C-1: "27"
```

解決規則：

```text
tunerChannels[tuner.name] が存在
    ↓
その値を物理チャンネルとして使用

存在しない
    ↓
channel を使用
```

`channel` に対する通常のMirakurun/MMirakurun動作は維持する。

つまり `tunerChannels` がない場合は通常の設定と同じ動作になる。


## 6. tuners.yml

- 必須は `name`, `types`。`command` は本家では任意だが、Hotarunでは実質必須。
- `types` は `GR|BS|CS|SKY` + 拡張 `BS4K`。
- ホットリロードなし。設定変更の保存のみでは反映されず、反映には再起動が必要。

```yaml
- name: PT3-0
  types:
    - GR
    - BS
    - CS
  command: recpt1 --device /dev/pt3video0 <channel> - -

- name: DVB-C-0
  types:
    - GR
    - BS
    - BS4K
  command: recdvb --dev 0 <channel> - -
```

### command

`<channel>` 等はHotarunが実際に使用する物理チャンネルへ置換する。

- 未知変数は空文字として展開する。
- `shell: true` は禁止。`spawn(program, args)` で起動する。
- バリデーションは型チェック + 不正時の skip のみ。
- LAN限定継承。

例：

```text
論理チャンネル
BS01_0

        ↓

tunerChannels

        ↓

DVB-C-0 = 13

        ↓

recdvb --dev 0 13 - -
```

### TLVDecoder

BS4KについてはMMirakurunを基準としたTLV/MMT処理および `TLVDecoder` 設定をサポートする。

具体的な設定形式・引数はMMirakurunの実装仕様に合わせ、互換性を優先する。

- `tlvDecoder?: string` はBS4K専用。`decoder` はCAS用。
- 両方とも変数展開なし。shell分割で `spawn` する。
- パイプは `tuner → Filter → Decoder → HTTP`。
- 共有時はチューナーを共有し、デコード分岐は per-client に行う。
- `TLVDecoder` が指定されない場合は、TLVをそのまま配信する。
- 未指定 + `?decode=0` は素通し。素通しも `Content-Type: video/MP2T`。


## 7. 内部アーキテクチャ

```text
                    ┌───────────────┐
                    │    Rivarun    │
                    └───────┬───────┘
                            │ HTTP
                            ▼
┌──────────────────────────────────────────┐
│                  Hotarun                 │
│                                          │
│  ┌────────────┐      ┌───────────────┐  │
│  │ HTTP API   │─────▶│   Scheduler   │  │
│  └────────────┘      └───────┬───────┘  │
│                              │          │
│                      ┌───────▼───────┐  │
│                      │ Tuner Manager │  │
│                      └───────┬───────┘  │
│                              │          │
│                      ┌───────▼───────┐  │
│                      │ Stream Manager │  │
│                      └───────┬───────┘  │
│                              │          │
│              ┌───────────────┼────────┐ │
│              ▼               ▼        ▼ │
│           tuner0           tuner1   tuner2
│              │               │        │
└──────────────┼───────────────┼────────┼─┘
               ▼               ▼        ▼
          外部チューナープログラム
```

主要コンポーネント：

```text
API
Config
Channel Manager
Scheduler
Tuner Manager
Stream Manager
Channel Scanner
TLV/MMT
Web UI
```


# 8. Tuner Manager

チューナーごとに外部プロセスを管理する。

### 状態

```text
IDLE
STARTING
TUNING
STREAMING
STOPPING
ERROR
DISABLED
```

### 必須機能

- プロセス起動
- `<channel>` 展開
- stdout取得
- stderr取得
- PID管理
- プロセス終了検出
- SIGTERM
- 必要時SIGKILL
- 異常終了検出
- 再接続処理

- 起動成功は spawn 成功で即時確定する。初回バイト待ちはしない。
- 停止は SIGTERM → 6秒 → SIGKILL。dvbv5-zap系のみ即KILL。
- stderrはログのみに使用する。
- releaseは 100ms / 1000ms。
- 残存要求がある場合は respawn する。3連続失敗で FAULT とし、要再起動。


# 9. Scheduler

ストリーム要求を受けた場合、使用可能なチューナーを選択する。

選択条件：

```text
1. channel.typeに対応している
2. disabledではない
3. 現在使用されていない
4. 必要な物理チャンネルを設定できる
```

選択順序は、共有 → 空き → 再利用 → 優先度奪取。

- 共有キーは物理chのみ。`decode` の違いは共有を妨げない。
- `tunerChannels` 解決後の `(tuner, 物理ch)` で判定する。
- 確保は 50 x 250ms リトライし、尽きたら 503。
- `X-Mirakurun-Priority` による奪取あり。

チューナー固有チャンネルがある場合：

```text
resolve_channel(channel, tuner)
```

によって物理チャンネルを決定する。


# 10. Stream Manager

外部チューナープロセスのstdoutをHTTPレスポンスへ接続する。

```text
Tuner stdout
    │
    ▼
Stream Manager
    │
    ├── Client 1
    ├── Client 2
    └── Client 3
```

### ストリーム共有

同一チャンネルへの複数要求について、可能な場合は同一チューナーを共有する。

```text
Client A ─┐
Client B ─┼─> 同一Tuner Process
Client C ─┘
```

- 別 `serviceId` でも同一TSは共有し、per-client TSFilterで分離する。
- PATは単一化する。ready前は8MBで古い方を破棄する。
- channel指定はフルTS。

最後のクライアントが切断した場合、チューナープロセスを停止する。

- 最終切断から3秒猶予後に停止する。
- backpressureはノンブロッキングfan-out。遅延者がいても切断しない。
- socketはHWM 16MB相当。


# 11. HTTP API

最低限、以下を実装する。

```http
GET /api/config/channels
GET /api/config/tuners

GET /api/channels
GET /api/tuners
GET /api/services

GET /api/tuners/:id

GET /api/channels/:type/:channel/stream
GET /api/services/:serviceId/stream

PUT /api/config/channels/scan
```

- `GET /api/tuners/:index` の `:index` は integer >= 0。
- `GET /api/services/:id/stream` の `:id` は ServiceItemId。`serviceId` と混同しないこと。
- 推奨追加: `GET /api/version`, `GET /api/status`, `GET /api/channels/:type/:channel`, `GET/DELETE /api/config/channels/scan`。

### stream

- query `decode` は `0|1` のみ。省略時は `1` (デコードON)。`?decode=0` のみOFF。
- 要求ヘッダ `X-Mirakurun-Priority` を受け付ける。
- 応答は `video/MP2T` + `X-Mirakurun-Tuner-User-ID`。HEAD対応。
- 映像ヘッダはチューナー確保後に送出する。
- 未実装のEPG等は 404 JSON。501にしない。
- 504はなし。

### エラー

- 形式は `{"code","reason","errors":[]}`。
- `404` / `503 no available tuners` / `500` / `409 scan多重` / `400 設定不正`。

### scan API

- `PUT /api/config/channels/scan` は query のみ。
- syncは `200 text/plain`、asyncは `202 accepted`。
- `GET scan` で ChannelScanStatus 進捗を返す。
- `DELETE` で中止 (`206/404/409`)。
- 範囲外削除・他type保持。保存後の反映には再起動が必要 (RESTART REQUIRED)。

必要に応じてMirakurun/MMirakurun互換APIを追加する。


# 12. チャンネルスキャン

対応：

```text
GR
BS
CS
BS4K
```

処理：

```text
scan request
    ↓
対応チューナー確保
    ↓
物理チャンネル走査
    ↓
ストリーム取得
    ↓
サービス検出
    ↓
結果生成
    ↓
channels.yml更新
```

- 内蔵パーサは PAT/NIT/SDT + MMT/TLV。
- ch毎20s、全体30分。
- 既定範囲は GR13-62 BS01_0-BS23_3/101-256 CS2-24 BS4K45328。
- service種別フィルタ等あり。`refresh=false` は引継ぎ。
- 範囲外削除・他type保持。保存後の反映には再起動が必要。

Web UIからも実行可能とする。


# 13. BS4K / TLV / MMT

BS4Kは通常のMPEG-2 TSとは異なるTLV/MMTストリームとして扱う。

```text
BS4K tuner
    ↓
TLV/MMT
    ↓
TLVDecoder
    ↓
Hotarun
    ↓
client
```

MMirakurunのBS4K/TLV実装を互換性の基準とする。

`TLVDecoder` が指定されない場合は、TLVをそのまま配信する。

- `tlvDecoder` はBS4K専用。詳細は§6参照。
- 未指定 + `?decode=0` は素通し。素通しも `Content-Type: video/MP2T`。


# 14. Web UI

Web UIは機能を最小限にする。

```text
Dashboard
├── Server Status
├── Tuner Status
└── Active Streams

Tuners
├── Tuner List
├── State
└── Current Channel

Channels
├── Channel List
└── Channel Type

Scan
├── GR
├── BS
├── CS
└── BS4K

Configuration
├── channels.yml
└── tuners.yml
```

Web UIは最後に実装する。


# 15. 実装フェーズ

## Phase 1 — 基盤

実装：

```text
Rust project
Tokio
Axum
Config loader
YAML parser
Logging
Error handling
```

完成条件：

```http
GET /api/config/channels
GET /api/config/tuners
```

が動作する。


## Phase 2 — Tuner Manager

実装：

```text
command spawn
<channel> replacement
stdout
stderr
PID
process termination
state management
```

外部チューナーを起動できる状態にする。


## Phase 3 — TS Streaming

最初はGR/BS/CSのみ。

```text
HTTP request
    ↓
Scheduler
    ↓
Tuner Manager
    ↓
external tuner
    ↓
stdout
    ↓
HTTP response
```

ここで初めて実際のストリームを流す。


## Phase 4 — Rivarunテスト

テストクライアント：

```text
Rivarun
```

テスト対象サーバー：

```text
192.168.100.111
```

基本テスト：

```text
Rivarun
   ↓
192.168.100.111
   ↓
Hotarun
   ↓
Tuner
   ↓
MPEG-2 TS
```

確認項目：

```text
Server discovery
Channel list
Tuner list
GR stream
BS stream
CS stream
Tuner selection
Tuner conflict
Disconnect
Reconnect
Process termination
```

ここを最初の実用上のマイルストーンとする。


## Phase 5 — Scheduler完成

実装：

```text
type matching
tuner availability
tuner selection
tunerChannels
physical channel resolution
```

異なるチューナーで同じ論理チャンネルを受信できるようにする。


## Phase 6 — Stream Sharing

同一チャンネルの複数クライアントに対してストリーム共有を実装する。

```text
Client A ─┐
Client B ─┼─> Stream Session ─> Tuner
Client C ─┘
```

最後のクライアント切断時に停止。


## Phase 7 — BS4K / TLV

実装：

```text
BS4K channel type
TLV/MMT handling
TLVDecoder
BS4K stream
BS4K service handling
```

MMirakurunとの互換性を確認する。


## Phase 8 — Channel Scan

実装：

```text
GR scan
BS scan
CS scan
BS4K scan
service detection
scan progress
result
channels.yml update
```

## Phase 9 — API Compatibility

Rivarun等が実際に要求するAPIを確認しながら互換APIを追加する。

優先順位：

```text
Channel API
Tuner API
Service API
Stream API
Config API
Scan API
```

## Phase 10 — BonDriver_Mirakurun

BonDriver_Mirakurunから接続し、

```text
Channel switching
Tuner allocation
Stream reception
```

を確認する。


## Phase 11 — Web UI

最後に管理画面を実装する。

優先順位：

```text
Dashboard
Tuner status
Channel list
Scan
Configuration
```

# 16. MVP

最初の完成目標は以下。

```text
✓ channels.yml
✓ tuners.yml
✓ YAML parser
✓ tuner process spawn
✓ tuner selection
✓ GR
✓ BS
✓ CS
✓ MPEG-2 TS streaming
✓ Mirakurun-compatible API
✓ Rivarun
```

MVP完成後：

```text
tunerChannels
    ↓
stream sharing
    ↓
BS4K / TLV
    ↓
channel scan
    ↓
BonDriver_Mirakurun
    ↓
Web UI
```

の順番で拡張する。


# 17. 最終構成

```text
Hotarun
├── API
│   ├── channels
│   ├── tuners
│   ├── services
│   ├── streams
│   ├── config
│   └── scan
│
├── Config
│   ├── channels.yml
│   └── tuners.yml
│
├── Channel Manager
│
├── Scheduler
│
├── Tuner Manager
│
├── Stream Manager
│
├── Channel Scanner
│
├── TLV/MMT
│
└── Web UI
```

 設計上の最重要原則は、Hotarun自身が放送データを解析・変換するのではなく、「論理チャンネル → 適切な物理チューナー → 外部チューナープロセス → ストリーム」という経路を軽量に管理することである。

# 18. 運用

- port既定は40772。unix socket可。HTTPのみ。
- 認証なし + CIDR制限 + Origin/Referer検査 + CORS。LAN限定。
- `logLevel` は -1..3 + `maxLogHistory`。
- healthは `version` + `status`。

### server設定

- `port` / `socket` / `CIDR` / `logLevel` 等は `server.yml` に格納する。
- `channels.yml` / `tuners.yml` と同ディレクトリに置く。