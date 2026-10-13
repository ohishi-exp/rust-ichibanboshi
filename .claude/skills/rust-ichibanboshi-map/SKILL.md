---
name: rust-ichibanboshi-map
generated-from: rust-ichibanboshi:a866c377c770e9f305c21db5b818c9839887d86d
paths: [src/]
description: rust-ichibanboshi (一番星 SQL Server CAPE#01 の売上・勤怠・給与の読みを提供する Cloudflare Worker 群 `workers/` と、root package の RDP 中継 `rdp-relay`) の構造ナビゲーション。Worker の区画 / 売上集計ロジック (税抜カラム・請求K) / rdp-relay の musl deploy の gotcha を 1 枚にまとめる。トリガー:「rust-ichibanboshi」「一番星」「CAPE#01」「tiberius」「SQL Server 売上」「月計テーブル」「請求K」「税抜金額」「傭車」「Cloudflare Tunnel」「CF Access Service Token」「rdp-relay」「RDP 中継」「RemoteApp」「RDCleanPath」「Cf-Access-Jwt-Assertion」等。
---

# rust-ichibanboshi-map — rust-ichibanboshi 構造ナビゲーション

一番星の SQL Server (CAPE#01) 等の読みは Cloudflare Worker 群 (`workers/`) が提供する。
root package (`src/`) は **RDP 中継 `rdp-relay` だけ** (オンプレの systemd)。
オンプレ版の売上 API `ichibanboshi` (旧 :3100) は Worker へ移して撤去した (#322)。

> ここは索引。集計ロジックの完全な条件・カラム名は CLAUDE.md と repo 側が正。
> frontmatter の `generated-from` が現 tree-sha とズレたら hook が再生成を促す。

## 区画 (module)

| ファイル | 役割 |
|---|---|
| `src/lib.rs` | crate ルート (`rust_ichibanboshi`) — rdp-relay の部品 4 つ (`cf_access`・`rdcleanpath`・`rdp_defaults`・`rdp_nego`) |
| `src/bin/rdp_relay.rs` | root package の唯一の binary。下記「RDP 中継」節 |
| `workers/uriage/` | 担当者別売上 `/api/uriage/*` は **Cloudflare Worker** に移った (#322)。集計・SQL・raw ndjson と fingerprint の純粋部分は `logic/` (`uriage-logic`)、store は D1/R2。オンプレ版 (`src/routes/uriage.rs`・`sqlite.rs`・`cakephp.rs`) は撤去済み。詳細は `workers/uriage/README.md` |
| `workers/kintai/` | **勤怠 (kintai・restraint・dtako・timecard・wage・shift 等) は勤怠 Worker だけが持つ。** オンプレ版の勤怠の route・モジュール・テスト・設定・バッチ (`push`/`recalc`/`sync`)・build.rs の etag の仕組みは撤去済み (#322)。詳細は `workers/kintai/README.md` |
| `workers/ichiban/` | 一番星 (CAPE#01) の管理画面向け 6 本 (`/health`・`/api/employees`・`/api/vehicles`・`/api/sales/departments`・`/api/sales/vehicle-daily`・`/api/costs/vehicle-daily`) は **Cloudflare Worker `ichibanboshi-ichiban`** に移った (#322)。`logic/` (`ichiban-logic`) に SQL 文・型・絞り込み判定・`build_*` を置く (6 本の SQL を直すのはここ。`worker/src/rows.rs` と列の並びが 1 対 1)。**オンプレ版は削除済み。6 本は Worker だけが提供**。売上・運賃・燃料サーチャージ・schema の口も Worker に移り、オンプレ版は削除済み (#322)。gate は `workers/ichiban/coverage_100.toml` (worker-ichiban.yml)。詳細は `workers/ichiban/README.md` |
| `workers/kyuyo/` | 給与大臣 `/kyuyo/*` の読み出しは **Cloudflare Worker `ichibanboshi-kyuyo`** に移った (#322)。オンプレ版 (`src/kyuyo/`・`src/routes/kyuyo.rs`・`/api/kyuyo/*`) は撤去済み。純粋ロジック `logic/src/payroll.rs`・SQL 文 `logic/src/sql.rs`・応答型 `logic/src/api.rs`・store の DDL `logic/src/store_keys.rs`、DO `KyuyoState`、認可は auth-worker の `KyuyoAuthEntrypoint`。**coverage 100% gate は `workers/kyuyo/coverage_100.toml` (worker-kyuyo.yml が判定)**。詳細は `workers/kyuyo/README.md` |

## gotcha (CLAUDE.md / Cargo.toml 由来)

- **売上集計は税抜カラムで**: 月計テーブルと一致させるには `税抜金額+税抜割増+税抜実費-値引`
  (傭車は `税抜傭車*`)。`金額` カラムは実費の内税/外税で消費税の含み方が変わるため使わない。
  日付は `売上年月日`、請求区分 `請求K IN ('0','2')`、自車/傭車判定は `傭車先C = '000000'`。
- **月計テーブルは締め時点スナップショット** — 締め後の遡り修正は再集計されないため日報明細と差が出る。
- **SQL Server 接続**: 名前付きインスタンス `softec`、DB `CAPE#01`、暗号化なし、文字コード Shift_JIS
  (Worker からの接続部品は `workers/shared/`)。
- **実運用は Cloudflare Worker 群 (`workers/`) と、オンプレの systemd で動く `rdp-relay` だけ。** Windows サービスと MSI・Cloud Run への deploy、オンプレ版の売上・勤怠 (`ichibanboshi`) は #322 で撤去済み。root に売上・勤怠を戻さない

## CI / deploy から見た立ち位置

- `ci.yml`: root package (rdp-relay) の fmt・clippy・test・100% gate、kintai migration の検証と適用、
  main push で `deploy` (rdp-relay だけをオンプレへ。下記「デプロイ」節)。
- Worker ごとの workflow は `worker-<名前>.yml` (Worker の deploy はタグで、merge では出ない)。
- `coverage_100.toml`: `cf_access.rs` / `rdp_defaults.rs` / `rdp_nego.rs` を 100% 維持
  (wiremock / 純粋関数テストで DB 不要)。`scripts/check_coverage_100.sh` で検証。
- `deploy/` は rdp-relay の unit (`rdp-relay.service`・`rdp-relay-lan.service`・`rdp-relay-watcher.{path,service}`) と env の雛形。

## 関連 skill

- `coverage-test-patterns` — tiberius + bb8 向け DB/ロジック分離・broken pool・Axum oneshot テスト
- `coverage-check` — 未カバー行抽出
- `cross-repo-symbol-index` — この per-repo map の鮮度 hook 運用方針

## 担当者別売上 — Worker `workers/uriage/`

オンプレ版の `/api/uriage/*` (state.db の SQLite と disk の raw NDJSON.gz、:3100 の rust host) は撤去済み (#322)。
口・store (D1/R2)・recalc / verify / R2 同期の status と fingerprint の扱いは `workers/uriage/README.md` が正。

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

- **インスタンス**: `softec`（名前付きインスタンス、`using_named_connection()` 必須）
- **データベース**: `CAPE#01`
- **暗号化**: `EncryptionLevel::NotSupported`（PHP の `encrypt=optional` 相当）
- **文字コード**: Shift_JIS (CP932)

## デプロイ

### CI 自動デプロイ (推奨、Refs #14)

`main` への merge で `ci.yml` の `deploy` job が **rdp-relay の** musl binary
(`cargo build --release --target x86_64-unknown-linux-musl --locked --bin rdp-relay`) を build →
**Cloudflare Tunnel SSH 経由**で `ohishi-data` に deploy → PathModified 自動 restart →
rdp-relay の `/health` (:3390、listen しているアドレスを `ss` で引く) 200 を確認する。
GitHub Actions runner は Tailscale 網に居ないため、`cloudflared access ssh` を `ProxyCommand` にした
SSH で到達し、**CF Access service token** で認証する。

- trigger: `push: branches: [main]` (= merge で本番反映)、`needs: [test]`
- 実 deploy ロジックは手動 `deploy.sh` と `scripts/deploy-remote.sh` を共有
  (host 名・proxy・鍵を env 化。binary・unit 名・health port の既定は rdp-relay)
- deploy 失敗 (build / SSH / health != 200) は job が **loud fail** する
- **オンプレの `ichibanboshi.service` には触らない** (撤去後の停止・退避は人が手で行う)

必要な GitHub secrets / variables (ohishi-exp repo or org):

| 名前 | 種別 | 用途 |
|---|---|---|
| `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET` | secret | CF Access service token (SSH 経路の認証) |
| `DEPLOY_SSH_KEY` | secret | CI 専用 SSH 秘密鍵 (host の authorized_keys に公開鍵を登録) |
| `DEPLOY_SSH_HOST` | variable | CF Tunnel SSH ingress hostname |

### 手動 fallback (Tailscale 経路)

```bash
# rdp-relay の musl build → Tailscale SSH で deploy → 自動 restart → health
./deploy.sh
```

- **インストール先**: `/opt/ichibanboshi/rdp-relay`（ubuntu 所有、sudo 不要）
- **サービス**: `systemctl status rdp-relay` (LAN 用 `rdp-relay-lan` も同じ binary)
- **自動再起動**: `rdp-relay-watcher.path` (systemd PathModified) がバイナリ変更を検知 → 両インスタンスを restart
- **ビルド**: musl スタティックリンク（GLIBC バージョン不一致回避）

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

**root package の唯一の binary。** `ci.yml` の `deploy` job が musl release を build して
SSH deploy で運ぶ。`ohishi-data` の上で `rdp-relay.service` (cf-access) と
`rdp-relay-lan.service` (vpc、LAN 向け) が同じ binary で動く。

| | `rdp-relay.service` |
|---|---|
| ポート | `RDP_RELAY_BIND` (既定 loopback :3390、site では LAN IP) |
| 実行ユーザー | `ubuntu` + `ProtectSystem=strict` 等 |
| 再起動 | `rdp-relay-watcher` (binary の mtime を見て restart) |

**deploy 後の health チェックは rdp-relay (:3390) を見る** — 中継が起動に失敗すると deploy job が赤くなる。

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

- nuxt-ichibanboshi → Worker の口は CF Access Service Token (`CF-Access-Client-Id/Secret`) 付き。無ければ 403
- rdp-relay の入口は Access の JWT (`Cf-Access-Jwt-Assertion`) を中継自身も検証する (上の「経路と認証」)

## フロントエンド

- **リポジトリ**: `ohishi-exp/nuxt-ichibanboshi`
- **URL**: `https://nuxt-ichibanboshi.m-tama-ramu.workers.dev`
- **テナント制限**: `NUXT_ALLOWED_TENANT_ID` (wrangler secret)
- **認証**: rust-alc-api の Google OAuth → JWT

## ワークスペース構成

- `nuxt-ichibanboshi/` → `/home/yhonda/js/nuxt-ichibanboshi` (symlink)
- `.vscode/settings.json` で `git.scanRepositories` にフロントエンドリポジトリを登録
