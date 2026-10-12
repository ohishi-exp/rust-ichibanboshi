---
name: rust-ichibanboshi-map
generated-from: rust-ichibanboshi:a866c377c770e9f305c21db5b818c9839887d86d
paths: [src/]
description: rust-ichibanboshi (一番星 SQL Server CAPE#01 の売上データを tiberius で読み REST API 提供する Rust/Axum サービス) の構造ナビゲーション。sales 集計エンドポイント / tiberius+bb8 接続 / 売上集計ロジック (税抜カラム・請求K) / musl deploy + Cloudflare Tunnel の gotcha を 1 枚にまとめる。トリガー:「rust-ichibanboshi」「一番星」「CAPE#01」「tiberius」「SQL Server 売上」「月計テーブル」「請求K」「税抜金額」「傭車」「Cloudflare Tunnel」「CF Access Service Token」「rdp-relay」「RDP 中継」「RemoteApp」「RDCleanPath」「Cf-Access-Jwt-Assertion」等。
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
| `src/main.rs` | clap entrypoint。`--console` の有無に関わらず console 起動 (フラグは互換のため残置) |
| `src/lib.rs` | crate ルート (`rust_ichibanboshi`) — 各 module re-export |
| `src/server.rs` | `run()` — pool 生成 + Axum Router 組み立て + graceful shutdown |
| `src/config.rs` | `Config` / `AppArgs` (clap) / TOML 読み込み / `addr()` |
| `src/db.rs` | `create_pool` — bb8-tiberius pool (named instance / NotSupported 暗号化) |
| `src/repo.rs` | `TiberiusRepo` / `DynRepo` trait — SQL クエリ本体 (DB 層) |
| `src/auth.rs` | `JwtSecret` / JWT 検証 |
| `src/routes/health.rs` | `/health` (`backends` は `sqlserver` だけ) |
| `src/routes/uriage.rs` + `src/sqlite.rs` + `src/cakephp.rs` | 担当者別売上 `/api/uriage/*` (純粋部分は共有 crate `workers/uriage/logic`)。`cakephp.rs` は masters・editable-months・print-json の client (エラー型等は `kintai-logic` から借りる) |
| `workers/kintai/` | **勤怠 (kintai・restraint・dtako・timecard・wage・shift 等) は勤怠 Worker だけが持つ。** オンプレ版の勤怠の route・モジュール・テスト・設定・バッチ (`push`/`recalc`/`sync`)・build.rs の etag の仕組みは撤去済み (#322)。詳細は `workers/kintai/README.md` |
| `workers/ichiban/` | 一番星 (CAPE#01) の管理画面向け 6 本 (`/health`・`/api/employees`・`/api/vehicles`・`/api/sales/departments`・`/api/sales/vehicle-daily`・`/api/costs/vehicle-daily`) は **Cloudflare Worker `ichibanboshi-ichiban`** に移った (#322)。`logic/` (`ichiban-logic`) に SQL 文・型・絞り込み判定・`build_*` を置く (6 本の SQL を直すのはここ。`worker/src/rows.rs` と列の並びが 1 対 1)。**オンプレ版の 5 本は削除済み。6 本は Worker だけが提供** (オンプレの `/health` は他系統の監視用に残す。`src/repo.rs` は `ichiban-logic` に依存せず、`HEALTH_SQL` を同じ文字列で持つ)。売上・運賃・燃料サーチャージ・schema の口も Worker に移り、オンプレ版は削除済み (#322)。gate は `workers/ichiban/coverage_100.toml` (worker-ichiban.yml)。詳細は `workers/ichiban/README.md` |
| `workers/kyuyo/` | 給与大臣 `/kyuyo/*` の読み出しは **Cloudflare Worker `ichibanboshi-kyuyo`** に移った (#322)。オンプレ版 (`src/kyuyo/`・`src/routes/kyuyo.rs`・`/api/kyuyo/*`) は撤去済み。純粋ロジック `logic/src/payroll.rs`・SQL 文 `logic/src/sql.rs`・応答型 `logic/src/api.rs`・store の DDL `logic/src/store_keys.rs`、DO `KyuyoState`、認可は auth-worker の `KyuyoAuthEntrypoint`。**coverage 100% gate は `workers/kyuyo/coverage_100.toml` (worker-kyuyo.yml が判定)**。詳細は `workers/kyuyo/README.md` |

## entrypoint / Axum router (`src/server.rs::run`)

- `/health`
- 売上 9 本・運賃 6 本・燃料サーチャージ・schema 3 本は一番星 Worker (`workers/ichiban/`) に移った (#322)。オンプレ版は削除済み
- `/api/uriage/*` (担当者別売上、下記「担当者別売上」節)
- layer: CORS (allowed_origins) + TraceLayer + `Extension(DynRepo)` 等
- repo は `Arc<TiberiusRepo>` を `DynRepo` として Extension 注入 → test は MockRepo に差し替え可能

## gotcha (CLAUDE.md / Cargo.toml 由来)

- **売上集計は税抜カラムで**: 月計テーブルと一致させるには `税抜金額+税抜割増+税抜実費-値引`
  (傭車は `税抜傭車*`)。`金額` カラムは実費の内税/外税で消費税の含み方が変わるため使わない。
  日付は `売上年月日`、請求区分 `請求K IN ('0','2')`、自車/傭車判定は `傭車先C = '000000'`。
- **月計テーブルは締め時点スナップショット** — 締め後の遡り修正は再集計されないため日報明細と差が出る。
- **SQL Server 接続**: host `172.18.21.102`、名前付きインスタンス `softec`
  (`using_named_connection()` 必須)、DB `CAPE#01`、`EncryptionLevel::NotSupported`、文字コード Shift_JIS。
- **実運用は Linux systemd (オンプレ) と勤怠 Worker (`workers/kintai`)。** Windows サービスと MSI・Cloud Run への deploy (image の build・smoke・deploy の workflow) は #322 で撤去済み。オンプレ版の勤怠も撤去済み (#322)。
- **オンプレの `ichibanboshi.toml` に勤怠の節が残っていても起動する** — `Config` は `deny_unknown_fields` を付けず知らない節を読み飛ばす。`tests/config_test.rs` の `ONPREM_TOML` が固定。付けると起動が落ちて売上ごと止まる (deploy に巻き戻しは無い)
- **CF Access Service Token 必須**: Tunnel 経由のリクエストは `CF-Access-Client-Id/Secret` 無しだと 403。

## CI / deploy から見た立ち位置

- **`./deploy.sh`**: `cargo build --release --target x86_64-unknown-linux-musl` (GLIBC 不一致回避の
  static link) → scp で `/tmp` → `mv` (atomic) で `/opt/ichibanboshi/`。systemd `ichibanboshi-watcher.path`
  (PathModified) がバイナリ変更を検知して自動 restart。実行先 `ohishi-data.tailea945d.ts.net`。
- `.github/workflows/`: `ci.yml` / `tag-release.yml`。
- `coverage_100.toml`: `auth.rs` / `config.rs` / `routes/health.rs` 等を 100% 維持
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
  ├─ SQLite (/opt/ichibanboshi/state.db): uriage_person_daily / recalc_jobs / verify_jobs
  └─ disk (/opt/ichibanboshi/raw/{month}/eigyosho-{id}.ndjson.gz): R2 投入用 raw NDJSON.gz
    ↓ R2 sync (nuxt の /api/uriage/r2-sync = rust から raw を fetch して R2 へ put)
[Cloudflare R2 bucket]
  └─ uriage/{month}/eigyosho-{id}.ndjson.gz
    ↓ Worker から read
[ブラウザ UI (nuxt /admin/*)]
```

**`state.db` は 3 つの derived store とは別扱い** (`[sqlite] path`、Refs #205 の G4)。
`uriage_person_*` は SQL Server から作り直せるが、`recalc_jobs` は fingerprint と
R2 同期状態の**唯一の記録**で、消えると差分再計算の基準が失われる。**空文字で無効宣言**
でき、その instance では `/api/uriage/*` が 503 になる — SQL Server を持たない形
(Cloud Run 版だった形) は `SQLITE_PATH=""` を入れること。入れないと揮発 FS に空の `state.db` が生えて
「壊れている」ではなく「0 件」を返す。オンプレは既定のままで挙動不変。

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

## 給与 (給与大臣) の読み出し — Worker `workers/kyuyo/` (旧 `/api/kyuyo/*`)

読み出しは **Cloudflare Worker `ichibanboshi-kyuyo`** (`workers/kyuyo/`) が担う。オンプレ版の `/api/kyuyo/*`
は撤去済み (#322)。一番星とは**別の SQL Server** (給与大臣、年度ごとに `KYDATA{会社}_{年度}` の DB が分かれる)。
認可は**ブラウザ JWT** を auth-worker の `KyuyoAuthEntrypoint.authorize(token)` で判定する — 金額を返すので
Service Token の口とは扱いが違う。以下は列と項目番号の知識 (実装は `workers/kyuyo/logic/`)。

**項目マスタ `KOUMOKU.TAIKEIKOUNO` = 体系コード(2桁) + 項目番号(3桁)** で、`KYUYO` の列と
項目番号帯が対応する。**帯ごとに列がまったく別**なのが要点:

| 項目番号 | 内容 | `KYUYO` の列 | 組み立て |
| --- | --- | --- | --- |
| 001〜017 | **勤怠** (出勤日数・公休日数・有休日数・欠勤日数・残業時間・遅刻回数 等) | `KINDATA0000, KINDATA0100, .., KINDATA1600` (17列・100刻み) | `kintai_taikeikouno(taikei, n)` = `1 + n` |
| 018〜097 | 支給・控除 | `MONEY00 〜 MONEY79` (80列) | `taikeikouno(taikei, n)` = `18 + n` |

- **`KINDATA*` は全社・全項目で `raw / 100` = 実数** (Refs #103、実データ 4 社で確定)。
  **「回数」系も例外ではない** — 遅刻回数の生値 `800` は 8 回で、割らないと 800 回になる。
  実測: 残業時間 `5900`=59.00h / 有休日数 `50`,`150`,`250`=0.5,1.5,2.5 日 (半休も整合) /
  残業時間 `13750`=137.50h
- **`KOUMOKU.NUMMODE` (1〜4) は参照しない。** 「表示モード」という名前でスケール区分に見えるうえ、
  同じ項目 (残業時間) でも会社によって 2 だったり 4 だったりするが、これは**給与大臣 UI 側の
  表示書式**で格納スケールとは無関係
- 応答は `payments` (支給) / `deductions` (控除) / `attendance` (勤怠、除算済みの実数) の 3 本立て。
  **勤怠を金額に混ぜない** — 混ぜると `payments` 合計と `SHUKEI1.SOSHIKYU` の自己突合が壊れる
- 支給/控除の分け方は **`KAZEI`** (1/2=支給・0=控除)。`KUBUN` は両者が混在していて使えない (#93)。
  `MEISAI=1` は単価項目でどちらにも入れない。`GENGAKU=1` は支給側でも符号反転 (#87)
- 0 の項目は落とす — 体系によって未定義の項目番号があり、拾うと「項目マスタ未解決」warning が全社で毎回出る
- スキーマの一次情報は `docs/kyuyo-daijin-schema.md`

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

- **実行先**: `ohishi-data.tailea945d.ts.net` (ubuntu / ***REMOVED***)
- **インストール先**: `/opt/ichibanboshi/`（ubuntu 所有、sudo 不要）
- **サービス**: `systemctl status ichibanboshi`
- **自動再起動**: `ichibanboshi-watcher.path` (systemd PathModified) がバイナリ変更を検知 → 自動 restart
- **ビルド**: musl スタティックリンク（GLIBC バージョン不一致回避）
- `deploy.sh` / `scripts/deploy-remote.sh` の流れ: `cargo build --release --target x86_64-unknown-linux-musl` → `scp /tmp` → `mv`（アトミック） → PathModified で自動 restart → `/health` 疎通確認

## migration (`migrations/` — kintai スキーマ / Supabase)

売上は SQL Server で migration を持たない。`migrations/` は **#205 の勤怠 (kintai)
スキーマ専用**で、相手は別 DB (専用 Supabase / secret `kintai-database-url`)。

適用は `make kintai-migrate` (= `scripts/migrate_kintai.sh`、psql)。**ledger は
sqlx-postgres 0.8 と同形の `_sqlx_migrations`** (version / description / SHA-384
checksum) なので、postgres client を入れた後は `sqlx::migrate!` がそのまま引き継げる
— どちらで適用しても状態が割れない。捨てクレートで `sqlx migrate run` を走らせて
checksum 一致・無再適用を実証済み。

### 本番へ当たるのは CI のジョブ (merge = 反映ではない)

本番の適用は手で流すものではなく、**CI の `Apply kintai schema (Supabase)` ジョブ**
(`ci.yml`、main への push で `scripts/migrate_kintai.sh` を回す) が行う。
**merge = 本番反映ではなく、このジョブが success になった時点が反映。**
merge 後は必ずこのジョブの結果を確認する — 2026-07-31 に Supabase pooler への
接続タイムアウトでこのジョブだけが落ちた実例がある (再実行で復旧)。

**適用済み migration はコメント 1 文字でも変えない。** checksum は
`migrate_kintai.sh` がファイル内容の **SHA-384** で照合するので、変えると以後の適用が
全部止まる。直すときは必ず新しい連番で (2026-07-31 の #205 の 20 で、merge 済みの
005 のコメントを直そうとして踏みかけた)。

env var は `DATABASE_URL` ではなく **`KINTAI_DATABASE_URL`**。`DATABASE_*` は
`src/config.rs` で既に SQL Server の名前空間 (`DATABASE_ENABLED` / `DATABASE_HOST` …)
なので、そこに `DATABASE_URL` を置くと「売上 SQL Server の URL」と読める。

規範は `rust-alc-api` と同じ:

- **適用済み migration は絶対に変更しない** (checksum 照合で loud fail)。修正は
  新規ファイルの追加で行う。`make kintai-rls-verify` がこの fail も検証している
- `SECURITY DEFINER` 関数を作るなら `SET search_path = kintai` を必須にする
- RLS policy に `WITH CHECK (true)` を書かない。`FOR ALL` で `WITH CHECK` を省略すれば
  `USING` 式がそのまま適用されるので、そちらに倒す
- 既存データへの `INSERT` / `UPDATE` をハードコードしない (`WHERE EXISTS` で書く)
- **パスワードを migration に書かない。** ロールは認証情報無しで作り、付与は Supabase
  側で `ALTER ROLE ... PASSWORD` → secret へ

**RLS は `postgres` では素通りする**ので、検証は必ず `kintai_reader` (`NOBYPASSRLS`)
で繋いで行う。`make kintai-rls-verify` が使い捨ての docker postgres でこれをやり、
CI (`ci.yml` の migration job) が毎 PR で同じ検証を回す。

**002 (`day_summaries_per_shift`)**: `day_summaries` の PK に `shift_start_at` を足して
勤務 1 本 = 1 行にした。`daily_summary()` は同じ暦日に複数の勤務行を返す (フェリーが
休息イベントなので勤務が割れる。実測 1726 / 2026-03-14 は 4 勤務) ため、001 の
`(tenant_id, driver_cd, date)` では潰れる。`shifts` への FK は `ON DELETE CASCADE`。

未解決 (003 以降の候補): `GRANT ... ON ALL TABLES` はその時点の表しか対象にしないので
`ALTER DEFAULT PRIVILEGES` に寄せるべき / `CREATE ROLE` は cluster-scoped で
`IF NOT EXISTS` が無い / Supabase の automatic RLS が独自 policy を足さないか未確認。

## RDP 中継 (`rdp-relay`) — ブラウザ ↔ RDS (Refs #296)

**同じ repo の 2 本目の binary。** `cargo build` が `[[bin]]` を全部作るので、
売上 API (`ichibanboshi`) と同じ musl release から拾って同じ SSH deploy で運ぶ
(`ci.yml` の `DEPLOY_EXTRA_BINARY` / `DEPLOY_EXTRA_NAME`)。`ohishi-data` の上で
**2 つの systemd サービスが並走**する。

| | `ichibanboshi.service` | `rdp-relay.service` |
|---|---|---|
| ポート | 3100 (LAN) | `RDP_RELAY_BIND` (既定 loopback、site では LAN IP) |
| 実行ユーザー | `root` | `ubuntu` + `ProtectSystem=strict` 等 |
| 再起動 | `ichibanboshi-watcher` | `rdp-relay-watcher` (binary の mtime を見て restart) |

**deploy 後の health チェックは 3100 しか見ていない** (`DEPLOY_EXTRA_HEALTH_PORT` 未設定)。
中継が起動に失敗しても main の deploy は緑のままなので、`rdp-relay` を触った PR の後は
自分で確かめる (`systemctl is-active rdp-relay` / `curl <bind>/health`)。

### 経路と認証

```
ブラウザ (IronRDP/WASM) ──wss──> rdp.ippoan.org (Cloudflare Access)
                                → localTunnel → 中継 → RDS:3389
```

`--auth` (env `RDP_RELAY_AUTH`) が入口を決める:

- **`cf-access`** (現行) … 中継自身が `Cf-Access-Jwt-Assertion` を team の JWKS で検証する
  (`src/cf_access.rs`)。Access の設定漏れやヘッダ偽装で素通りさせないための二段目の壁。
  **Access の IdP は auth-worker の OIDC surface** (`auth.ippoan.org/oidc/*`) なので、
  管理画面に既にログインしている利用者は追加ログイン無しで通る
- **`vpc`** … 公開ホスト名を持たず Workers VPC binding からだけ届かせる。認証はアプリ側の
  Worker が持つ。`nuxt-dtako-admin` の `/ws/rdp` がこれだったが #705 で撤去済み

`/health` はどちらでも素通し (deploy の疎通確認が localhost から叩くため)。

### 口

| path | 中身 |
|---|---|
| `GET /health` | `ok`。認証なし |
| `GET /rdp` | WebSocket。RDCleanPath を 1 往復して RDS と TLS を張り、以降は素の RDP を流す |
| `GET /defaults` | 画面が入力欄の初期値に使う値。`{destination, domain, remote_app}` |

`/defaults` の宛先は **`--allow` の先頭をそのまま返す** — 画面が同じ値を別に持つと
二重管理になり、ズレた瞬間に「許可されていない宛先」で閉じられるため、権威をここに集める。
ドメインと RemoteApp は RDP セッションの中身で中継からは見えないので `RDP_RELAY_DEFAULT_*` で受ける。
別オリジンから cookie 付きで読まれるので `RDP_RELAY_CORS_ORIGIN` に挙げた origin だけを echo する
(**credentials 付きに `*` は使えない**。前段の Access は origin 応答に CORS ヘッダを足さず、
preflight は 403 で落とす — 実測)。

### ファイル

| ファイル | 役割 |
|---|---|
| `src/bin/rdp_relay.rs` | I/O とルーティング。`probe` サブコマンドは X.224→TLS の実機確認用 |
| `src/rdcleanpath.rs` | RDCleanPath (DER) の解釈 |
| `src/rdp_nego.rs` | X.224 接続要求/確認・TPKT |
| `src/cf_access.rs` | Access JWT の検証 (JWKS 取得だけが I/O、wiremock でテスト) |
| `src/rdp_defaults.rs` | `/defaults` の組み立てと CORS 判断 (純粋関数) |
| `deploy/rdp-relay.service` / `-watcher.{path,service}` | systemd |
| `deploy/rdp-relay.env.example` | `/etc/ichibanboshi/rdp-relay.env` の雛形。**この env が無いと起動しない** (`EnvironmentFile` に `-` を付けていない — 無認証で上がるより落ちる方を選ぶ) |

**site 固有の値は repo に置かない**: `CF_ACCESS_TEAM_DOMAIN` / `CF_ACCESS_AUD` /
`RDP_RELAY_BIND` / `RDP_RELAY_DEFAULT_*` / `RDP_RELAY_CORS_ORIGIN` は env ファイル側。
**CI は env を触らない**ので、新しい変数を足したら人が入れて `systemctl restart rdp-relay`。

ブラウザ側 (画面・cookie の確保・`/defaults` の使い方) は `nuxt-dtako-admin-map` skill。


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
