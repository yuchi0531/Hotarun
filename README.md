# Hotarun

##mirakcをフォークした方がどう考えてもいいと気づいたのでやめる

Rust/Axumで実装した、Mirakurun/MMirakurun互換の軽量チューナーサーバーです。
チューナープログラムの標準出力を受け取り、HTTPストリームとして配信します。
Hotarun自身は録画、EPG収集、映像変換を行いません。

## 概要

実装済みの主な機能は次のとおりです。

- `channels.yml`、`tuners.yml`、`server.yml` のYAML設定
- GR / BS / CS / SKYのMPEG-2 TS配信
- BS4KのTLV/MMT配信
- チューナーの自動選択、物理チャンネルのチューナー別マッピング
- 同じ物理チャンネルのストリーム共有とノンブロッキングfan-out
- `X-Mirakurun-Priority` による優先度の高い要求へのtakeover
- TS service streamのPAT / PMT / PCR / elementary PIDフィルタリング
- 1物理チャンネル内の複数サービス検出と`ServiceItemId`列挙
- TS用`decoder`、BS4K用`tlvDecoder`のクライアント単位の外部プロセス
- Mirakurun互換を目指した設定・一覧・状態・ストリームAPI
- Mirakurun互換のチャンネルスキャンAPI（GR / BS / CS / SKY / BS4K、同期・非同期・進捗・中止・dryRun）
- 設定保存APIと再起動要求API
- 互換用CIDR設定、Origin / Referer検査、CORS
- Unix socket、peer credential検証、socket mode `0660`
- ログファイル、管理イベント履歴、health / status API
- 素のHTML/CSS/JavaScriptによるWeb UI
- systemd用のhardening例
- BonDriver_Mirakurun向けHTTPストリームアダプター

設計上の基準は [SPECIFICATION.md](SPECIFICATION.md) です。このREADMEは現在のコードの実装状況を説明するもので、`SPECIFICATION.md`は変更していません。

## リリース

バージョンは [SemVer](https://semver.org/) に準拠します。リリースタグは `vX.Y.Z` 形式です。現在のバージョンは `0.0.3` です。

Linux向けのリリースバイナリは、リリース番号によらない固定ファイル名で配布します。

| 対応環境 | バイナリ | SHA-256 |
|---|---|---|
| Linux x86_64 | [`hotarun-linux-x86_64`](https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-x86_64) | [`hotarun-linux-x86_64.sha256`](https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-x86_64.sha256) |
| Linux ARM32 (ARMv7 hard-float) | [`hotarun-linux-arm32`](https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-arm32) | [`hotarun-linux-arm32.sha256`](https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-arm32.sha256) |

例えばx86_64版は次のように取得・検証できます。

```sh
curl -fL -o hotarun-linux-x86_64 https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-x86_64
curl -fL -o hotarun-linux-x86_64.sha256 https://github.com/yuchi0531/Hotarun/releases/latest/download/hotarun-linux-x86_64.sha256
sha256sum -c hotarun-linux-x86_64.sha256
chmod +x hotarun-linux-x86_64
```

`v*`タグのリリースは [.github/workflows/release.yml](.github/workflows/release.yml) でx86_64とARM32をビルドし、上記の固定名assetをGitHub Releaseへ追加・置換します。

## 対応放送

| Type | 配信形式 | スキャン |
|---|---|---|
| `GR` | MPEG-2 TS | 対応 |
| `BS` | MPEG-2 TS | 対応 |
| `CS` | MPEG-2 TS | 対応 |
| `SKY` | MPEG-2 TS | 対応（設定済み識別子を走査） |
| `BS4K` | TLV/MMT | 対応 |

## アーキテクチャ

```text
HTTP client / Rivarun / HTTP adapter
                 │
                 ▼
          Axum HTTP API / Web UI
                 │
                 ▼
    scheduler・チューナー選択・scan lease
                 │
                 ▼
          Tuner Manager
                 │
                 ▼
      外部チューナープログラム
                 │ stdout
                 ▼
       Stream Manager / fan-out
          │                  │
          │                  └─ per-client decoder
          └─ TS filter       └─ BS4K TLV decoder
                 │
                 ▼
             HTTP response
```

- 設定は起動時に読み込み、メモリ上のスナップショットとして使用します。
- 同一物理チャンネルの要求は、可能な場合に同じチューナープロセスを共有します。
- チューナーstdoutはクライアントごとのbounded queueへfan-outします。遅いクライアントがいても他のクライアントを待たせず、queueが満杯のチャンクはそのクライアント向けに破棄します。
- 最後のクライアント切断後は3秒の猶予を置いてチューナーを停止します。
- 外部コマンドはshellを介さず、プログラムと引数に分割して直接起動します。

## 必要環境

- Rust 1.85以降のtoolchain（`cargo`）
- Rustバイナリと外部チューナープログラムを実行できるLinux等の環境
- 標準出力へストリームを書き出すチューナープログラム（例: `recpt1`、`recdvb`）
- BS4Kでデコードする場合は、`tlvDecoder`に指定する外部プログラム

実機チューナーのデバイスアクセス権は、Hotarunを起動するユーザーに付与してください。

## ビルドと起動

```sh
cargo build --release
./target/release/hotarun
```

既定値は次のとおりです。

| 項目 | 既定値 |
|---|---|
| 設定ディレクトリ | `/etc/hotarun` |
| TCPポート | `40772` |
| ログファイル | `/var/log/hotarun/hotarun.log` |

設定ディレクトリとTCPポートは、コマンドライン引数または環境変数で変更できます。コマンドライン引数が優先されます。

```sh
./target/release/hotarun \
  --config-dir ./fixtures \
  --port 40772
```

- `--port`は`-p`でも指定できます。
- `HOTARUN_CONFIG_DIR`は設定ディレクトリの初期値です。
- `PORT`はTCPポートの初期値です。
- `/var/log/hotarun`を作成できない場合、tracingの出力は標準エラーへフォールバックします。
- 設定ファイルの変更はホットリロードされません。保存後に再起動してください。

`server.yml`で`socket`を指定した場合はTCPではなくUnix socketで待ち受けます。

## 設定

サンプルは [fixtures/channels.yml](fixtures/channels.yml)、[fixtures/tuners.yml](fixtures/tuners.yml)、[fixtures/server.yml](fixtures/server.yml) にあります。

### `channels.yml`

`name`、`type`、`channel`が必須です。`type`は`GR`、`BS`、`CS`、`SKY`、`BS4K`のいずれかです。`channel`は文字列として扱います。

```yaml
- name: NHK BS
  type: BS
  channel: BS01_0
  serviceId: 101
  networkId: 4

- name: NHK BS4K
  type: BS4K
  channel: BS01_0
  serviceId: 101
  networkId: 5
  tunerChannels:
    DVB-C-0: BS4K45328

- name: sample GR
  type: GR
  channel: "27"
  serviceId: 1024
  networkId: 1
```

`tunerChannels`を指定すると、`tuner.name`ごとに物理チャンネルを上書きします。指定がなければ`channel`を使用します。

CSの論理チャンネルはMirakurun互換の`ND2`、`ND4`、…、`ND24`（110度CSの12トランスポンダ、偶数のみ）が正規です。旧来の`CS2`のような`CS<n>`表記はAPIのlookupとスキャン突合では`ND<n>`と同一視しますが、表示・スキャン生成は`NDxx`に統一します。既存の`CSxx`設定は`refresh=true`での再スキャンか、`channel`を`NDxx`へ手動変更して移行してください。

スキャンで複数サービスを検出した場合は、同じ`(type, channel)`のレコードに主サービスを`serviceId` / `name`として保存し、追加サービスを拡張フィールド`services`に保存します。

### `tuners.yml`

`name`と`types`が必須です。ストリーム配信とスキャンには`command`が必要です。

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
  tlvDecoder: tlvdecoder --arg1
  decoder: cas-decoder --arg1
```

- `command`の`<channel>`は、`tunerChannels`解決後の物理チャンネルへ置換します。CSは論理`NDxx`がそのまま渡ります。`recpt1`のように`CS2`形式を要求するチューナーでは、`tunerChannels`で`ND2: CS2`のようにマッピングしてください。
- 未知の`<...>`は空文字になります。
- 引用符とバックスラッシュによる引数分割に対応しますが、shellは起動しません。
- `tlvDecoder`は`BS4K`専用です。`decoder`はGR / BS / CS / SKYのTS用です。
- `tlvDecoder`と`decoder`には変数展開を行いません。指定した文字列を引数分割して直接起動します。

### `server.yml`

`channels.yml`、`tuners.yml`と同じ設定ディレクトリに置きます。

```yaml
port: 40772
# socket: /run/hotarun/hotarun.sock
CIDR:
  - 192.168.0.0/16
  - 10.0.0.0/8
adminCIDR:
  - 192.168.1.0/24
logLevel: 1
maxLogHistory: 1000
```

- `port`の既定値は`40772`です。`socket`を指定するとUnix socketを使用します。
- `CIDR`と`adminCIDR`は既存設定との互換性のため読み書きしますが、TCPクライアントの接続可否判定には使用しません。任意のTCP peerから接続できます。
- `logLevel`は`-1`から`3`です。`maxLogHistory`は`/api/log`で返す管理イベント履歴の上限です。
- Unix socketはbind時にmode `0660`となり、要求時にpeerのUIDまたはGIDがHotarunプロセスの有効UID/GIDと一致することを確認します。

設定保存APIは一時ファイルを経由してYAMLを置換しますが、実行中の設定は更新しません。保存後に再起動が必要です。

## BS4K / TLV / MMT

`type: BS4K`はMPEG-2 TSとして解析せず、TLVストリームとして扱います。

| 条件 | 動作 |
|---|---|
| `tlvDecoder`未指定 | TLVをそのままHTTPへ中継 |
| `tlvDecoder`指定、`decode=1`（省略時の既定） | 要求ごとに`TLV → tlvDecoder stdin → tlvDecoder stdout → HTTP`で中継 |
| `decode=0` | `tlvDecoder`の指定にかかわらず、TLVをそのまま中継 |

いずれの場合もレスポンスの`Content-Type`は正確に`video/MP2T`です。BS4Kのchannel stream、service streamでは、TSのPAT / PMT / PIDフィルタリングを行いません。BS4K service streamは、対応するTLVストリームをそのまま返します。

通常のTSでは、`decoder`を指定して`decode=1`にすると次のように動作します。

- channel stream: フルTSをdecoder stdinへ渡し、decoder stdoutをHTTPへ中継
- service stream: 対象サービスのPAT / PMT / PCR / elementary PIDだけをdecoder stdinへ渡し、decoder stdoutをHTTPへ中継
- `decode=0`: 外部decoderを使わず、channel streamはフルTS、service streamは通常のservice filter結果を返す

BS4Kのサービス検出は、fixtureとテストで次を実装しています。

- actual TLV-NIT
- actual MMT PLT / SDT
- MMTメッセージのfragment再構成
- packet sequenceを使ったinterleave処理
- 複数section、複数serviceの集約
- NIT、PLT、SDTの整合性確認

## HTTP API

成功するストリームレスポンスは`Content-Type: video/MP2T`と`X-Mirakurun-Tuner-User-ID`を返します。`HEAD`はチューナーを確保せず、空のレスポンスを返します。

### 一覧・状態

```text
GET /api/version
GET /api/status
GET /api/health
GET /api/log

GET /api/config/channels
GET /api/config/tuners
GET /api/config/server

GET /api/channels
GET /api/channels/:type/:channel
GET /api/services
GET /api/services/:id
GET /api/tuners
GET /api/tuners/:index
```

- `/api/channels`は`type`、`channel`、`name`で絞り込めます。
- `/api/services`は`serviceId`、`networkId`、`type`、`name`、`channel.type`、`channel.channel`で絞り込めます。
- serviceの`:id`は`serviceId`単体ではありません。`networkId * 100000 + serviceId`で作るMirakurunの`ServiceItemId`です。
- `/api/status`はサーバー、チューナー、アクティブストリーム、スキャンの状態を返します。
- `/api/log`は`maxLogHistory`件までの設定保存・再起動・スキャン失敗などの管理イベントを返します。外部プロセスのstderr等はtracingの出力先へ送ります。

### ストリーム

```text
GET  /api/channels/:type/:channel/stream
HEAD /api/channels/:type/:channel/stream
GET  /api/services/:id/stream
HEAD /api/services/:id/stream

GET  /api/bonDriver/channels/:type/:channel/stream
HEAD /api/bonDriver/channels/:type/:channel/stream
```

ストリームqueryの`decode`は`0`または`1`だけを受け付け、省略時は`1`です。`X-Mirakurun-Priority`を指定すると、空きがない場合に、より高い優先度の要求が別チャンネルのストリームをtakeoverできます。

エラーは次のJSON形式です。

```json
{
  "code": 404,
  "reason": "not found",
  "errors": []
}
```

未実装のパスは404、対応していないメソッドは405、チューナー枯渇は503を返します。

### 設定・再起動

```text
PUT/POST /api/config/channels
PUT      /api/config/tuners
GET/PUT  /api/config/server
POST     /api/config/restart
```

設定APIは管理用エンドポイントです。保存成功時は`restartRequired: true`を返します。`/api/config/restart`は再起動要求を受け付け、daemonのgraceful shutdown後に設定を読み直します。

## チャンネルスキャン

```text
PUT    /api/config/channels/scan
GET    /api/config/channels/scan
DELETE /api/config/channels/scan
```

`PUT`のqueryで次を指定できます。

| query | 既定値 | 説明 |
|---|---:|---|
| `type` | 全種類 | `GR`、`BS`、`CS`、`BS4K`。`channels.yml`にSKYが設定されている場合は`SKY`も含む |
| `dryRun` | `false` | `channels.yml`へ保存しない |
| `refresh` | `false` | 既存の対象チャンネルを再走査せず保持。`true`は対象を再走査して置換 |
| `async` | `false` | `true`なら202を返してバックグラウンド実行 |
| `serviceType` | なし | 検出サービス種別で絞り込む |

- 同期スキャンは完了後に`200 text/plain; charset=utf-8`でスキャンログ（検出数・引き継ぎ数を含む）を返します。最終チャンネル結果はGETの`result`/`channels`で確認します。
- 非同期スキャンは`202`を返し、GETでMirakurun互換の`status`（`not_started`、`scanning`、`completed`、`cancelled`、`error`）、`isScanning`、`progress`、`currentChannel`、`scanLog`、`result`を確認できます。Hotarun固有の`scanned`、`total`、`channels`、`error`も併せて返します。
- DELETEは実行中スキャンを中止し、成功時は`206`と`{"status":"stopping",...}`を返します。実行中でなければ`404`、停止要求済みの競合は`409`です。
- 1論理チャンネルのタイムアウトは20秒、全体のタイムアウトは30分です。
- TSはPAT、actual NIT、actual SDTが揃うまで有効なサービスとして保存しません。
- `scanMode=Channel`（GRの既定）は1論理チャンネルに主サービスと追加サービスをまとめ、`scanMode=Service`（BS/CS/SKY/BS4Kの既定）はサービスごとに`<論理チャンネル>:<serviceId>`のエントリを生成します。Service modeでも`physicalChannel`と論理チャンネルAPI lookupを維持し、サービスの`ServiceItemId`を検証します。CSの論理チャンネルは`ND2`、`ND4`、…、`ND24`です。
- CSの既定走査範囲は`ND2`-`ND24`の偶数12波です（GR50 + BS248 + CS12 + BS4K1 = 311、SKY設定時は+SKY数）。旧`CSxx`連番23波とは異なり、存在しない奇数トランスポンダを走査しません。
- SKYはBS4Kとは異なりMPEG-2 TS経路で走査します。MirakurunにはSKY用の固定走査範囲がないため、`channels.yml`に設定済みのSKY識別子（例: `CH585`、`ATXHD`）を走査対象とし、推測した数値範囲は追加しません。Mirakurun互換のサービス種別（`0x01`、`0x02`、`0xA1`、`0xA4`、`0xA5`、`0xAD`、`0xC0`）だけを登録します。
- BS4Kはactual TLV-NITとMMT PLT / SDTを検出し、fixtureでfragment、interleave、複数serviceを検証しています。
- スキャンは既存の共有ストリームを利用でき、スキャンが所有していないチューナープロセスを停止しません。
- 保存後の反映には再起動が必要です。

## Web UI

管理画面は `/ui/` です。次の画面を含みます。

- Dashboard: server status、tuner status、active streams
- Tuners: チューナー一覧と状態
- Channels: チャンネル一覧
- Configuration: `channels`、`tuners`、`server`の表示・保存

UIはRustバイナリに埋め込んだ素のHTML/CSS/JavaScriptです。スキャン操作はWeb UIには置かず、Mirakurun互換のHTTP APIだけで行います。

## systemd

[fixtures/hotarun.service.example](fixtures/hotarun.service.example)にサービス例があります。

```sh
install -Dm755 target/release/hotarun /usr/bin/hotarun
install -d -o hotarun -g hotarun /etc/hotarun /var/log/hotarun
cp fixtures/channels.yml fixtures/tuners.yml fixtures/server.yml /etc/hotarun/
cp fixtures/hotarun.service.example /etc/systemd/system/hotarun.service
systemctl daemon-reload
systemctl enable --now hotarun
```

実運用では、fixtureのチューナーコマンドを実際の環境に合わせて変更してください。サービス例は`NoNewPrivileges`、`PrivateTmp`、`ProtectSystem`、`ProtectHome`を有効にし、必要な書き込み先だけを`ReadWritePaths`に指定しています。

## セキュリティとネットワーク

Hotarunには認証機能がありません。TCPリスナーは`0.0.0.0`で待ち受け、クライアントIP/CIDRによる接続拒否は行いません。インターネットへ直接公開せず、必要に応じてファイアウォールやリバースプロキシで接続元を制限してください。

- `CIDR`と`adminCIDR`は旧設定を壊さないために保持されますが、TCPのアクセス制御には影響しません。管理APIもクライアントIPでは拒否しません。
- `Origin`はHTTP scheme、host、portが`Host`と一致する場合だけ許可します。
- `Referer`がある場合はhostが`Host`と一致する必要があります。
- CORSは許可済みのOriginに対してだけ応答ヘッダーを付けます。
- Unix socketではmode `0660`とpeerのUID/GID検証を併用します。
- `command`、`decoder`、`tlvDecoder`は設定から外部プロセスを起動します。設定ファイルを信頼できる管理者だけが変更できるようにしてください。
- インターネットへ直接公開しないでください。必要に応じてファイアウォールやリバースプロキシで接続元を制限してください。

## テストと検証状況

次の検証を実施済みです。

```sh
/home/yuchi0531/.cargo/bin/cargo build
/home/yuchi0531/.cargo/bin/cargo check --all-targets
/home/yuchi0531/.cargo/bin/cargo test --no-fail-fast
git diff --check
```

- `cargo test --no-fail-fast`: **135 passed**
- HTTP API、ストリーム共有、priority takeover、decoder、scan lifecycle、設定保存、任意TCP peer、Unix socket、Origin/Referer/CORS、Web UIを統合テストで検証
- Rust製`hotarun-test-fixture`でTS/TLV、scan入力、decoder入出力を再現
- BS4K MMT/TLV discoveryはfixture/testでactual TLV-NIT、MMT PLT/SDT、fragment/interleave、複数serviceを検証

## 既知の制限

「実装済みだが実機未確認」と「未実装」は次のように分かれます。

### 実装済みだが実機未確認

- 実機チューナーを使ったGR / BS / CS / SKYのTS配信
- 実機BS4KチューナーからのTLV入力
- 実機の外部`tlvDecoder`との接続
- 実機環境でのRivarun接続
- BonDriver_MirakurunクライアントからのHTTP adapter接続
- 実機のスキャン結果、チューナーのデバイス固有動作

BS4K MMT/TLV discovery自体はfixture/testで実装・検証済みですが、実機のBS4K/TLV decoderでの動作は未確認です。

### 未実装または対象外

- native BonDriver Windows DLL ABI

  BonDriverのnative DLL ABIやwire protocolは`SPECIFICATION.md`で定義されていません。そのため、推測によるWindows DLL実装は行わず、実装済みなのは`/api/bonDriver/channels/:type/:channel/stream`のHTTP adapterだけです。

- 仕様で定義されていないMMTの細部
- EPG収集、番組データベース、録画管理
- HLS / DASH、DLNA、トランスコード、メディアサーバー機能
- 設定ファイルのホットリロード
- SKYの固定数値範囲スキャン（Mirakurun本家にも固定範囲がないため、設定済み識別子の走査を使用）

`cargo fmt -- --check`は実行しましたが、既存および今回の変更を含む多数の整形差分が検出されました。無関係な広範囲の自動整形を避けるため、リリース前には自動整形を適用していません。

## ライセンス

[MIT License](LICENSE)
