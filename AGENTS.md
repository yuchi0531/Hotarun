# Hotarun 開発メモ

## まず確認すること

- 実装と検証の一次情報は `Cargo.toml`、`src/`、`tests/`、`.github/workflows/`。`SPECIFICATION.md` のPhase/MVPや実装順序は最初の設計計画なので、現在の挙動の根拠にしない。
- `README.md` は現在の実装状況と運用例、`fixtures/*.yml` は設定例、`fixtures/hotarun.service.example` はsystemd例。仕様の全量をこのファイルに複製しない。
- `SPECIFICATION.md` は変更しない。既存の未コミット変更を上書きせず、作業前後に `git status` で確認する。

## 検証コマンド

Rust 1.85以上（edition 2021）が必要。通常の確認は次の順で行う。

```sh
cargo build
cargo check --all-targets
cargo test --no-fail-fast
git diff --check
```

- 統合テストだけなら `cargo test --test http_stream` または `cargo test --test admin_api`、個別テストなら末尾にテスト名を追加する。
- 統合テストは `hotarun-test-fixture` を外部子プロセスとして使うため、fixtureを省略した独自の実行方法で判断しない。
- `cargo fmt` / `cargo clippy` はリポジトリのCI必須手順としては設定されていない。利用可能なら変更内容に応じて実行するが、必須検証と混同しない。
- リリースCIは通常のテストではなく、`cargo build --locked --release` のx86_64/ARMv7クロスビルド。ARM側は `CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc` を使う。

## 構造と変更境界

- `src/main.rs` はCLI/env、ログ、TCP/Unix listener、shutdown/restart。`src/lib.rs` は全Axum routerとmiddlewareの組み立て。機能追加時にdaemon起動処理へroute実装を混ぜない。
- `src/config.rs` はYAMLモデル、validation、atomic write、起動時 `AppState`。`src/routes/` はHTTP/API/UI、`src/scan.rs` はscan orchestrationとTS/TLV検出、`src/tuner/` は外部プロセス・lease・fan-out・状態機械を担当する。
- 設定は起動時スナップショットで、設定保存APIはファイルを更新するだけ。保存後は `/api/config/restart` またはプロセス再起動が必要で、保存成功を即時反映と解釈しない。
- `stream` のresponse bodyのDropがtuner leaseを解放する。stream、sharing、takeover、respawnを変更するときは `src/routes/stream.rs` だけでなく `src/tuner/manager.rs` のgeneration/use-count/idle停止も確認する。

## 実装上の落とし穴

- `channels.yml` のチャンネル識別子は `(type, channel)`。`GR/27` と `BS/27` は別物で、同一組み合わせの重複は順序で解決せずrejectする。物理チャンネルは `tunerChannels[tuner.name]` があればそれを優先する。
- `ServiceItemId` は `serviceId` 単体ではなく `networkId * 100000 + serviceId`。service API/streamのID変更時にこの互換形式を壊さない。
- `command`、`decoder`、`tlvDecoder` はshell経由ではなく直接spawnされる。shellの `|`、`>`、`&&`、`$VAR` 等を設定に書いても動かない。`<channel>` 展開があるのはtuner `command`だけで、decoder系は変数展開しない。
- 通常のGR/BS/CS/SKYはMPEG-2 TSとしてPAT/PMT/PCR/PIDを扱うが、BS4KはTLV/MMTでありTSのPID filteringを適用しない。BS4Kのdecoderは `tlvDecoder`、通常TSは `decoder`。
- SKYのscan対象は推測した数値範囲ではなく、現在の `channels.yml` に設定されたSKY識別子（例: `CH585`）だけ。scanは共有streamを利用でき、所有していないtuner processを停止してはいけない。1チャンネル20秒、全体30分のtimeout。
- TCP listenerは `0.0.0.0` で、`CIDR`/`adminCIDR` はTCPアクセス制御に使われない。認証もないため外部公開せず、必要な制限はfirewall/reverse proxyで行う。Unix socket利用時だけmode `0660` とpeer UID/GID検証がある。
