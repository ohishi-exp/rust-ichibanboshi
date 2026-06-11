---
name: rust-ichibanboshi-map
generated-from: rust-ichibanboshi:a866c377c770e9f305c21db5b818c9839887d86d
paths: [src/]
description: rust-ichibanboshi (一番星 SQL Server CAPE#01 の売上データを tiberius で読み REST API 提供する Rust/Axum サービス) の構造ナビゲーション。sales 集計エンドポイント / tiberius+bb8 接続 / 売上集計ロジック (税抜カラム・請求K) / musl deploy + Cloudflare Tunnel の gotcha を 1 枚にまとめる。トリガー:「rust-ichibanboshi」「一番星」「CAPE#01」「tiberius」「SQL Server 売上」「月計テーブル」「請求K」「税抜金額」「傭車」「Cloudflare Tunnel」「CF Access Service Token」等。
---

# rust-ichibanboshi-map — rust-ichibanboshi 構造ナビゲーション

一番星の SQL Server (CAPE#01) から売上データを tiberius (+bb8 pool) で読み、Axum で
REST API 提供するサービス。`nuxt-ichibanboshi` (CF Workers) → Cloudflare Tunnel
(`rust-ichiban.mtamaramu.com`) → systemd で稼働する本サービス、という経路。

> ここは索引。集計ロジックの完全な条件・カラム名は CLAUDE.md と repo 側が正。
> frontmatter の `generated-from` が現 tree-sha とズレたら hook が再生成を促す。

## 区画 (module)

| ファイル | 役割 |
|---|---|
| `src/main.rs` | clap entrypoint。`--console` で console、Windows では `service::run_service`、それ以外は console |
| `src/lib.rs` | crate ルート (`rust_ichibanboshi`) — 各 module re-export |
| `src/server.rs` | `run()` — pool 生成 + Axum Router 組み立て + graceful shutdown |
| `src/config.rs` | `Config` / `AppArgs` (clap) / TOML 読み込み / `addr()` |
| `src/db.rs` | `create_pool` — bb8-tiberius pool (named instance / NotSupported 暗号化) |
| `src/repo.rs` | `TiberiusRepo` / `DynRepo` trait — SQL クエリ本体 (DB 層) |
| `src/auth.rs` | `JwtSecret` / JWT 検証 |
| `src/service.rs` | `windows-service` 統合 (cfg(windows) のみ) |
| `src/routes/health.rs` | `/health` |
| `src/routes/sales.rs` | `/api/sales/*` 売上集計ハンドラ群 (下記) |
| `src/routes/schema.rs` | `/api/schema/*` tables/columns/sample (デバッグ用 schema 探索) |

## entrypoint / Axum router (`src/server.rs::run`)

- `/health`
- `/api/sales/*`: `monthly` / `by-department` / `by-customer` / `yoy` / `daily` /
  `customer-trend` / `customer-yoy` / `customer-yoy-by-dept` / `departments` / `customer-detail`
- `/api/schema/*`: `tables` / `columns` / `sample`
- layer: CORS (allowed_origins) + TraceLayer + `Extension(DynRepo)` + `Extension(JwtSecret)`
- repo は `Arc<TiberiusRepo>` を `DynRepo` として Extension 注入 → test は MockRepo に差し替え可能

## gotcha (CLAUDE.md / Cargo.toml 由来)

- **売上集計は税抜カラムで**: 月計テーブルと一致させるには `税抜金額+税抜割増+税抜実費-値引`
  (傭車は `税抜傭車*`)。`金額` カラムは実費の内税/外税で消費税の含み方が変わるため使わない。
  日付は `売上年月日`、請求区分 `請求K IN ('0','2')`、自車/傭車判定は `傭車先C = '000000'`。
- **月計テーブルは締め時点スナップショット** — 締め後の遡り修正は再集計されないため日報明細と差が出る。
- **SQL Server 接続**: host `172.18.21.102`、名前付きインスタンス `softec`
  (`using_named_connection()` 必須)、DB `CAPE#01`、`EncryptionLevel::NotSupported`、文字コード Shift_JIS。
- **Windows / Linux 両対応の罠**: Cargo.toml description は "Windows Service"、`wix/main.wxs` +
  `windows-service` dep (cfg(windows)) があるが、**実運用は Linux systemd** (CLAUDE.md / deploy.sh)。
  `--console` 無し起動は OS により分岐 (Windows=service, それ以外=console)。
- **CF Access Service Token 必須**: Tunnel 経由のリクエストは `CF-Access-Client-Id/Secret` 無しだと 403。

## CI / deploy から見た立ち位置

- **`./deploy.sh`**: `cargo build --release --target x86_64-unknown-linux-musl` (GLIBC 不一致回避の
  static link) → scp で `/tmp` → `mv` (atomic) で `/opt/ichibanboshi/`。systemd `ichibanboshi-watcher.path`
  (PathModified) がバイナリ変更を検知して自動 restart。実行先 `ohishi-data.tailea945d.ts.net`。
- `.github/workflows/`: `ci.yml` / `release.yml` / `tag-release.yml`。
- `coverage_100.toml`: `auth.rs` / `config.rs` / `routes/{health,sales,schema}.rs` を 100% 維持
  (全て MockRepo / 純粋関数テストで DB 不要)。`scripts/check_coverage_100.sh` で検証。
- `deploy/` に `ichibanboshi.service` / `.toml`、`config/ichibanboshi.default.toml`。

## 関連 skill

- `coverage-test-patterns` — tiberius + bb8 向け DB/ロジック分離・broken pool・Axum oneshot テスト
- `coverage-check` — 未カバー行抽出
- `cross-repo-symbol-index` — この per-repo map の鮮度 hook 運用方針
