# Hotarun

Rust/Axumで実装した、Mirakurun互換の軽量チューナーサーバーです。
外部チューナープログラムの標準出力をHTTPで配信します。

## 概要

現在の実装では、次の機能を提供します。

- `channels.yml` と `tuners.yml` の読み込み
- GR / BS / CS のMPEG-2 TS配信
- BS4KのTLV配信
- チューナーの自動選択
- 同じ物理チャンネルへのストリーム共有とfan-out
- `X-Mirakurun-Priority` による優先度の高い要求への切り替え
- 通常のTSストリームに対するservice単位のPAT / PMTフィルタリング
- BS4Kの `tlvDecoder` を使用した要求ごとの外部decoder pipe
- Mirakurun形式の設定・一覧・ストリームAPI
- LAN向けのアクセス制御とOrigin / Referer検査

Hotarun自身は録画や映像変換を行いません。チューナーと、必要に応じて外部decoderを起動・管理し、ストリームを中継します。

### 対象外・未実装

現行コードには次の機能は実装されていません。

- チャンネルスキャンAPI
- `server.yml` の読み込みや設定反映
- Web UI
- EPG収集、番組データベース、録画管理
- HLS / DASH、DLNA、トランスコード
- BonDriverやRivarunとの実機接続検証

詳細な設計上の方針は [SPECIFICATION.md](SPECIFICATION.md) を参照してください。仕様書に記載されていても、上記のように現行コードで未実装の機能があります。

## 必要環境

- Rust toolchain（`cargo`）
- Linuxなど、Rustと外部チューナープログラムを実行できる環境
- 使用するチューナープログラム（例: `recpt1`、`recdvb`）
- BS4Kでdecoderを使う場合は、`tlvDecoder` に指定する外部プログラム

外部チューナープログラムは、標準出力へストリームを書き出す必要があります。Hotarunはshellを介さず、プログラムと引数を直接起動します。

## ビルド

```sh
cargo build --release
```

生成されるバイナリは `target/release/hotarun` です。

## 設定

既定の設定ディレクトリは `/etc/hotarun` です。起動時に次のファイルを読み込みます。

```text
/etc/hotarun/channels.yml
/etc/hotarun/tuners.yml
```

設定は起動時に読み込まれ、ホットリロードされません。ファイルを変更した場合は再起動してください。ファイルがない場合や、型が不正なエントリがある場合は警告を出して空のリスト、または有効なエントリだけで起動します。

サンプルは [fixtures/channels.yml](fixtures/channels.yml) と [fixtures/tuners.yml](fixtures/tuners.yml) にあります。

### `channels.yml`

`name`、`type`、`channel` が必須です。`type` は `GR`、`BS`、`CS`、`SKY`、`BS4K` のいずれかです。`channel` は文字列として扱われます。

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

`tunerChannels` を指定すると、チューナー名ごとに物理チャンネルを上書きできます。指定がなければ `channel` の値を使用します。

### `tuners.yml`

`name` と `types` が必須です。ストリーム要求を処理するには `command` も必要です。

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
```

チューナーの `command` にある `<channel>` は、`tunerChannels` の解決後に物理チャンネルへ置換されます。未知の `<...>` は空文字になります。引用符とバックスラッシュによる引数分割には対応しますが、shellは起動しません。

`tlvDecoder` はBS4K専用です。decoderコマンドには変数展開を行わず、指定されたプログラムを直接起動します。`decoder` フィールドは設定形式として保持されますが、現行のBS4Kストリーム経路では `tlvDecoder` を使用します。

## 起動

```sh
./target/release/hotarun
```

設定ディレクトリとポートは起動引数で変更できます。

```sh
./target/release/hotarun \
  --config-dir ./fixtures \
  --port 40772
```

`--port` は `-p` と書くこともできます。既定値は次のとおりです。

| 項目 | 既定値 |
|---|---|
| 設定ディレクトリ | `/etc/hotarun` |
| HTTPポート | `40772` |

環境変数 `HOTARUN_CONFIG_DIR` と `PORT` でも初期値を指定できます。コマンドライン引数が指定された場合は引数が優先されます。

サーバーは `0.0.0.0` で待ち受けます。実際のチューナーを使う場合は、外部コマンドが実行可能で、対象デバイスへアクセスできるユーザーで起動してください。

## BS4K / TLV

`type: BS4K` のチャンネルは、通常のMPEG-2 TSとしてPAT / PMTフィルタリングせず、TLVストリームとして扱います。

- `tlvDecoder` 未指定: TLVをそのままクライアントへ配信
- `tlvDecoder` 指定かつ `decode=1`（既定）: 要求ごとに `TLV -> tlvDecoder stdin -> tlvDecoder stdout -> HTTP` として配信
- `decode=0`: `tlvDecoder` の指定があってもdecoderを通さず、TLVをそのまま配信
- いずれの場合もレスポンスの `Content-Type` は `video/MP2T`
- BS4Kのservice streamでもTSのPAT / PMTフィルタリングは行わない

decoderはクライアントごとに起動されます。一方、元のチューナーストリームは通常のストリームと同じく共有されます。

## HTTP API

設定・一覧APIはGETです。ストリームAPIはGETとHEADに対応します。

### 設定・一覧

```text
GET /api/config/channels
GET /api/config/tuners

GET /api/channels
GET /api/channels/:type/:channel
GET /api/services
GET /api/services/:id
GET /api/tuners
GET /api/tuners/:index
```

`/api/channels` は `type`、`channel`、`name` で絞り込めます。`/api/services` は `serviceId`、`networkId`、`type`、`name`、`channel.type`、`channel.channel` で絞り込めます。

serviceの `:id` は `serviceId` 単体ではありません。`networkId * 100000 + serviceId` で作られるMirakurunのServiceItemIdです。

### バージョン・状態

```text
GET /api/version
GET /api/status
```

`/api/version` はパッケージのバージョンを `current` と `latest` に返します。`/api/status` はサーバー状態、プロセスID、チューナー数、利用可能数、アクティブストリーム数などを返します。

### ストリーム

```text
GET  /api/channels/:type/:channel/stream
HEAD /api/channels/:type/:channel/stream

GET  /api/services/:id/stream
HEAD /api/services/:id/stream
```

クエリパラメータ `decode` は `0` または `1` のみ受け付けます。省略時は `1` です。`decode=0` はBS4Kの外部decoderを回避する指定で、通常のTS経路のPAT / PMTフィルタリングを無効にする指定ではありません。

要求ヘッダー `X-Mirakurun-Priority` を指定すると、より高い優先度の要求が別チャンネルのストリームを引き継げます。成功時は次のヘッダーを返します。

```text
Content-Type: video/MP2T
X-Mirakurun-Tuner-User-ID: <tuner index>
```

HEADはチューナーを確保せず、空のレスポンスを返します。エラーは次の形式のJSONです。

```json
{
  "code": 404,
  "reason": "not found",
  "errors": []
}
```

## テスト

単体テストとHTTP統合テストを実行します。

```sh
cargo test
```

ビルドのみ確認する場合は次を実行します。

```sh
cargo build
```

テストにはRust製の `hotarun-test-fixture` バイナリを使用するため、実チューナーや実機のBS4K/TLV decoderは必要ありません。

## セキュリティとネットワーク

Hotarunには認証機能がありません。サーバーは全インターフェースで待ち受けますが、TCP接続元はループバック、プライベートアドレス、リンクローカルアドレスに制限されます。OriginまたはRefererが送られた場合は、リクエストのHostと一致する必要があります。

したがって、インターネットへ直接公開しないでください。ファイアウォールなどでもLAN内の必要なクライアントだけに制限してください。`command` と `tlvDecoder` は設定ファイルから外部プロセスを起動するため、信頼できる管理者だけが設定ファイルを書き換えられるようにしてください。

## 既知の制限

- 実機チューナーでのGR / BS / CS配信は、このリポジトリの自動テスト対象ではありません。
- 実機BS4K/TLVチューナーと外部 `tlvDecoder` の接続は未確認です。
- 設定変更はホットリロードされません。反映には再起動が必要です。
- チャンネルスキャン、Web UI、EPG、録画機能はありません。
- `server.yml` は読み込まれません。ポートと設定ディレクトリは起動引数または環境変数で指定します。
- HTTP APIはMirakurun互換を目指した最小実装です。未実装のパスは404、対応していないメソッドは405を返します。

## ライセンス

[MIT License](LICENSE)
