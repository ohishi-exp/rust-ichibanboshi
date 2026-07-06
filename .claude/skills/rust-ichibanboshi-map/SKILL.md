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
| `src/routes/surcharge.rs` | `/api/surcharge/base` 燃料サーチャージ基礎データ (請求のみ行 → 県/車種/請求日 展開、#12) |

## entrypoint / Axum router (`src/server.rs::run`)

- `/health`
- `/api/sales/*`: `monthly` / `by-department` / `by-customer` / `yoy` / `daily` /
  `customer-trend` / `customer-yoy` / `customer-yoy-by-dept` / `departments` / `customer-detail`
- `/api/surcharge/base`: 燃料サーチャージ基礎データ。`運転日報明細` の請求のみ行 (`請求K`='1'、
  `kind=transport`/`all` で切替) を 得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定)
  に展開。県正規化 (`地域N` → 都道府県) は `normalize_prefecture` 純粋関数。残マスタ (燃費/距離/軽油価格/
  対象得意先) は scope 外 (#12 残課題)。
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

## CLAUDE.md から移設 (2026-07-06)

## 担当者別売上 (Phase 2、Refs #762) のデータ流れと status

```
SQL Server (CAPE#01、172.18.21.102)
    ↓ recalc 実行 (rust の POST /api/uriage/recalc)
[ohishi-data:3100 (rust host)]
  ├─ SQLite (/opt/ichibanboshi/data.db): uriage_person_daily / recalc_jobs / verify_jobs
  └─ disk (/opt/ichibanboshi/raw/{month}/eigyosho-{id}.ndjson.gz): R2 投入用 raw NDJSON.gz
    ↓ R2 sync (nuxt の /api/uriage/r2-sync = rust から raw を fetch して R2 へ put)
[Cloudflare R2 bucket]
  └─ uriage/{month}/eigyosho-{id}.ndjson.gz
    ↓ Worker から read
[ブラウザ UI (nuxt /admin/*)]
```

`recalc_jobs.status` の意味:

| status | SQLite | disk raw | R2 | UI から見える? |
|---|---|---|---|---|
| `r2_synced` (✅ R2 同期済) | ある | ある | ある | 見える |
| `computed` (🟡 計算済、R2 同期待ち) | ある | ある | **無い** | 見えない |
| `failed` (❌ 失敗) | failed 記録のみ | 無し | 無し | 見えない |

タイムスタンプ列の意味:

| 列 | 意味 |
|---|---|
| `created_at` | 行が初めて作られた時刻 (= 初回 recalc 時) |
| `computed_at` | **最後に recalc が走った時刻** (fingerprint が変わったかどうかに関わらず更新) |
| `fingerprint_changed_at` | **fingerprint が実際に変化した時刻** (= data が変わった時刻、fingerprint 不変な再 recalc では更新されない) |
| `r2_synced_at` | 最後に R2 同期に成功した時刻 (= R2 オブジェクトが最新化された時刻) |

要点:

- **`computed` は「rust host にはデータがあるが、R2 にはまだ転送していない」状態**。
  ブラウザから直接 SQLite は見えないので、R2 同期しないと UI 側には反映されない。
- `record_recalc_computed` は **fingerprint が同じ再 recalc** では status / `r2_synced_at` /
  `fingerprint_changed_at` をすべて維持する。**fingerprint 変化時のみ** `status='computed'`
  + `r2_synced_at=NULL` + `fingerprint_changed_at=now` を立てて R2 再送信を促す
  (user 2026-06-30: 「ｒ２同期待ちおかしくね?」「finger verified のほうがよくね?」)。
- `computed_at` は不変/変化に関わらず毎回更新される (= 最終 recalc 試行時刻)。なので
  「`computed_at` が新しいのに `fingerprint_changed_at` が古い」 = 「最近 recalc は走ったが
  data は変わっていない」と読める。
- verify (PHP vs Rust) は `verify_jobs` (PK: unko_date, eigyosho_id, cal) に upsert。
  `verify_coverage` view が `(month, eigyosho_id)` で集計し、`r2_pending` view と
  `list_recalc_jobs` が LEFT JOIN して `verified_count / ok / ng` を露出する。
- `r2_pending` view の条件: `status='computed' AND r2_synced_at IS NULL AND raw_path IS NOT NULL
  AND (fingerprint_before IS NULL OR fingerprint_before != fingerprint_after)`。
  fingerprint 変化なし (= データ同じ) なら R2 への再送信は不要。

## 燃料サーチャージ基礎データ (`/api/surcharge/base`、Refs #12)

調査 #12 で確定した「`運転日報明細` の単一行に完了条件の全項目が揃う」結論に基づく
基礎データ endpoint。請求のみ行 (`請求K`='1') を中心に、各行を
**得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定)** に展開して返す。

- query: `from` / `to` (売上年月 YYYY-MM、半開区間)、`kind` (`billing_only` default / `transport` / `all`)、`limit` (1..=10000、default 2000)
- 県正規化: `地域ﾏｽﾀ.地域N` の先頭を都道府県へ。`北海道` のみ 4 文字、他は最初の `県`/`府`/`都` まで。
  未マップ (`発地域C`='000000' 等) は `"?"`。ロジックは `routes/surcharge.rs::normalize_prefecture` (純粋関数)
- 運賃 = `金額 + 割増 + 実費` (#12 確定式。月計一致用の税抜カラムとは別物なので混同しない)
- **残課題は scope 外** (新規構築/外部取込が必要): 燃費 km/L マスタ / 県庁間距離マスタ (47×47) /
  週次全国平均軽油価格の取込 / サーチャージ対象得意先の識別。これらは本 endpoint では扱わない

## 一番星 売上データ集計ロジック（検証済み）

### 月計テーブルとの完全一致条件

月計テーブル（種別別月計、部門別月計、得意先別月計）の `自車売上 + 傭車売上` は、
運転日報明細から以下の条件で集計した値と **全得意先で完全一致** する。

| 項目 | 値 |
|------|-----|
| **日付カラム** | `売上年月日`（運行年月日・管理年月日ではない） |
| **請求区分** | `請求K IN ('0', '2')`（請求+非請求。請求のみ(1)は除外） |
| **自車売上** | `税抜金額 + 税抜割増 + 税抜実費 - 値引` |
| **傭車売上** | `税抜傭車金額 + 税抜傭車割増 + 税抜傭車実費 - 傭車値引` |

### 注意: `金額` カラムは使わない

`金額` カラムは実費の税処理（内税/外税/非課税）によって消費税の含み方が異なるため、
単純な `金額+割増+実費-値引` では月計と一致しない。
必ず `税抜金額 + 税抜割増 + 税抜実費 - 値引` を使うこと。

### 実費の税処理K（実費内訳ﾏｽﾀ）

| 実費K | 名称 | 税処理K | 説明 |
|-------|------|---------|------|
| 1 | 高速料金 | 3 | 内税（税込） |
| 2 | 保険料 | 1 | 非課税 |
| 3 | 高速（課税） | 0 | 外税 |
| 4 | 高速料金調整 | 0 | 外税 |
| 5 | 橋通行料 | 3 | 内税 |
| 6 | フェリー代 | 3 | 内税 |
| 7 | 手数料 | 0 | 外税 |
| 8 | ﾌｪﾘｰ代調整金 | 3 | 内税 |
| 9 | 計量代 | 0 | 外税 |

### 消費税調整

- `基本事項.消費税調整K = 1` — 得意先単位で消費税を再計算・丸め調整
- 調整行: 品名C=9003「消費税調整」、品名C=9998「端数調整(消費税調整の為)」
- 一括調整行: 品名N「※　請求一括調整明細　※」「※　傭車一括調整明細　※」（金額0、消費税カラムのみ）

### 自車/傭車の判定

- `傭車先C = '000000'` → 自車
- `傭車先C ≠ '000000'` → 傭車
- ※ `傭車先C` は空白ではなく `'000000'`（6桁ゼロ）

### 月計テーブルと日報明細のずれ

- 月計テーブルは締め処理時点のスナップショット
- 締め後に日報明細が遡り修正されても月計は再集計されない
- 例: 2026年1月分の日報が3月に修正 → 月計との差額 ~412万円
- 2月（締め直後）は差額 ~5,500円（消費税丸めのみ）

### 基本事項

| 項目 | 値 |
|------|-----|
| 運用月 | `基本事項.運用月`（現在の処理月） |
| 輸送締日 | 31（月末） |
| 消費税調整K | 1（得意先単位丸め） |
| インボイス適用 | 2023-09-01 |

## SQL Server 接続

- **ホスト**: `172.18.21.102` (ohishi-srv)
- **インスタンス**: `softec`（名前付きインスタンス、`using_named_connection()` 必須）
- **データベース**: `CAPE#01`
- **ユーザー**: `pbi` / `test`
- **暗号化**: `EncryptionLevel::NotSupported`（PHP の `encrypt=optional` 相当）
- **文字コード**: Shift_JIS (CP932)

## デプロイ

### CI 自動デプロイ (推奨、Refs #14)

`main` への merge で `ci.yml` の `deploy` job が musl binary を build →
**Cloudflare Tunnel SSH 経由**で `ohishi-data` に deploy → PathModified 自動 restart →
`/health` 200 を確認する。GitHub Actions runner は Tailscale 網に居ないため、
Tailscale ではなく `cloudflared access ssh` を `ProxyCommand` にした SSH で到達し、
**CF Access service token** で認証する。

- trigger: `push: branches: [main]` (= merge で本番反映)、`needs: [test]`
- 実 deploy ロジックは手動 `deploy.sh` と `scripts/deploy-remote.sh` を共有
  (host 名・proxy・鍵を env 化)
- deploy 失敗 (build / SSH / health != 200) は job が **loud fail** する

必要な GitHub secrets / variables (ohishi-exp repo or org):

| 名前 | 種別 | 用途 |
|---|---|---|
| `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET` | secret | CF Access service token (SSH 経路の認証) |
| `DEPLOY_SSH_KEY` | secret | CI 専用 SSH 秘密鍵 (host の authorized_keys に公開鍵を登録) |
| `DEPLOY_SSH_HOST` | variable | `ssh-rust-ichiban.mtamaramu.com` 等 (CF Tunnel SSH ingress hostname) |

host 側 (一度きり): `cloudflared` Tunnel に SSH ingress
(`ssh-rust-ichiban.mtamaramu.com` → `ssh://localhost:22`) を追加 → CF Access app +
Service Auth ポリシーで保護 (CI 専用 token のみ許可) → deploy ユーザー
(`ubuntu`) の `~/.ssh/authorized_keys` に CI 公開鍵を登録。

### 手動 fallback (Tailscale 経路)

```bash
# musl build → Tailscale SSH で deploy → 自動 restart
./deploy.sh
```

- **実行先**: `ohishi-data.tailea945d.ts.net` (ubuntu / Ohishi55)
- **インストール先**: `/opt/ichibanboshi/`（ubuntu 所有、sudo 不要）
- **サービス**: `systemctl status ichibanboshi`
- **自動再起動**: `ichibanboshi-watcher.path` (systemd PathModified) がバイナリ変更を検知 → 自動 restart
- **ビルド**: musl スタティックリンク（GLIBC バージョン不一致回避）
- `deploy.sh` / `scripts/deploy-remote.sh` の流れ: `cargo build --release --target x86_64-unknown-linux-musl` → `scp /tmp` → `mv`（アトミック） → PathModified で自動 restart → `/health` 疎通確認

## Cloudflare Access

- **トンネル**: `rust-ichiban.mtamaramu.com` → `http://172.18.21.35:3100`
- **Service Token**: CF-Access-Client-Id / CF-Access-Client-Secret
- トークンなしのリクエストは 403

## フロントエンド

- **リポジトリ**: `ohishi-exp/nuxt-ichibanboshi`
- **URL**: `https://nuxt-ichibanboshi.m-tama-ramu.workers.dev`
- **テナント制限**: `NUXT_ALLOWED_TENANT_ID` (wrangler secret)
- **認証**: rust-alc-api の Google OAuth → JWT

## ワークスペース構成

- `nuxt-ichibanboshi/` → `/home/yhonda/js/nuxt-ichibanboshi` (symlink)
- `.vscode/settings.json` で `git.scanRepositories` にフロントエンドリポジトリを登録
