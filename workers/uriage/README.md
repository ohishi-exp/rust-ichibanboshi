# workers/uriage — 担当者別売上 (`/api/uriage/*`) の Worker 化

Refs ohishi-exp/rust-ichibanboshi#322。オンプレ版 rust の売上の口を Cloudflare Worker `ichibanboshi-uriage` に移す。
純粋部分の crate `uriage-logic` (`logic/`、オンプレ版も path 依存で使う) と Worker の crate `uriage-worker` (`worker/`)。
**いまの Worker は D1 を読む口 6 本と、`POST /api/uriage/recalc`・`POST /api/uriage/verify`・`GET /api/uriage/verify-debug`・
`POST /api/uriage/admin/{delete,rebuild}`** (下記)。by-person・raw・ack は移さない (404)。

## uriage-logic

root (オンプレ版) の `src/routes/uriage.rs`・`src/repo.rs`・`src/cakephp.rs`・`src/sqlite.rs` から移した。
root は path 依存で `pub use` する (写さない)。tiberius・worker・axum・reqwest・rusqlite に依存せず、時計も読まない。

| ファイル | 中身 |
|---|---|
| `compute.rs` | 担当者振替 (PHP `computePersonSum()` の写経。**リファクタ禁止**)・日次と取引先別の集計・`cal_total` 等 |
| `sqlserver.rs` | 一番星 SQL Server の `[運転日報明細]` を読む SQL (`uriage_rows_sql`)・列番号・`Cells` と `uriage_row` |
| `raw.rs` | raw ndjson と fingerprint (sha256)・gzip・ファイル名と R2 の key |
| `period.rs` | 月 (`YYYY-MM`) の月初・月末・日数 |
| `verify.rs` | PHP `print-json.sum` の読み方と PHP / Rust の差分 |
| `cakephp.rs` | 社内 CakePHP (`masters-json`・`editable-months`・`print-json`) の応答の型・Worker 用の path・VPC の origin・応答の読み (`read_json`、2xx 以外は `Status`)・失敗の種類 (通信は呼び手) |
| `recalc.rs` | recalc の段取り (月・masters・job の上限 `MAX_JOBS`)・bucket の D1 の文 (DELETE 4 + `json_each` の INSERT 4 + recalc_jobs の upsert)・put を省く判定・時刻の形式 |
| `write_auth.rs` | 書き込みの口の token の照合 (workers/kintai の写し) |
| `api.rs` | 口の Query と応答の型・store から読んだ行の型・r2/pending の verify gate |
| `d1.rs` | D1 の表の定義 (`SCHEMA_SQL` = `worker/migrations/0001_uriage.sql`)・読みの口 6 本の SELECT (オンプレ版の rusqlite の文と同じ文字列)・入力の検査 (400)・`raw()` の行の読み取り・応答の組み立て |
| `routes.rs` | Worker の経路の判定 (404 / 405) と応答の形 (`Reply`、失敗の本文) |

## Worker の口 (この段)

Service Binding 専用 (route・workers.dev・preview 無し)。読みの口は認可なし (一番星 Worker・勤怠 Worker の読みの口と同じ。関門は呼び手の側)。
**path・Query・応答の JSON (キーの順まで) と 400 はオンプレ版と同じ。**

| 口 | D1 に流す文 | 応答 |
|---|---|---|
| `GET /api/uriage/daily` (`month`・`eigyosho_id`・`cal`) | 1 文 | `{month, eigyosho_id, cal, rows}` |
| `GET /api/uriage/person-monthly-totals` (`from`・`to`・`cal`) | 1 文 | `{from, to, cal, rows}` |
| `GET /api/uriage/person-partner-totals` (`person`・`from`・`to`・`cal`) | 2 文 (得意先・傭車先) | `{person, from, to, cal, customers, subcontractors}` |
| `GET /api/uriage/r2/pending` (`from`・`to` 任意) | 1 文 | `{count, items}` (verify gate 付き) |
| `GET /api/uriage/recalc-jobs` (`from`・`to` 任意) | 1 文 | `{from, to, count, jobs}` |
| `GET /api/uriage/verify-history` (`from`・`to`・`eigyosho_id` 任意) | 1 文 | `{from, to, eigyosho_id, count, rows}` |
| `GET /health` | `SELECT 1` | `{"status":"ok"}` |
| `POST /probe` | `SELECT 1` → SQL Server にログインして `SELECT 1` → CakePHP の editable-months | `{"ok":true,"d1":"ok","sqlserver":"ok","cakephp":"ok"}` (最初に落ちた段の失敗を返す) |
| `POST /api/uriage/recalc` (`month`・`eigyosho_id` 任意。header `X-Uriage-Write-Token` 必須) | bucket ごとに SELECT 1 + batch 9 + (put 後) UPDATE 1 | オンプレ版と同じ `{source_table, months, editable_months_count, calculated_at, jobs}` |
| `POST /api/uriage/verify?id=&date=&cal=` (header `X-Uriage-Write-Token` 必須) | masters → (bumon 空は skipped) → print-json → SQL Server 1 本 (単日) → verify_jobs の UPSERT 1 | オンプレ版と同じ `{office_id, date, cal, php_sum, rust_sum, diff, ok, row_count, elapsed_ms, skipped_reason?}` |
| `GET /api/uriage/verify-debug?id=&date=&cal=` (認可なし・書かない) | masters → print-json → SQL Server 1 本 | オンプレ版と同じ `{office_id, date, cal, bumon, persons, other, rows, php_sum, rust_sum, diff}` |
| `POST /api/uriage/admin/delete?month=&eigyosho_id=` (token 必須) | 1 batch の DELETE 3 (日次・取引先別・recalc_jobs。verify_jobs は消さない) | `{month, eigyosho_id, daily_deleted, jobs_deleted}`。month が空は本文なしの 400 |
| `POST /api/uriage/admin/rebuild` (token 必須) | 1 batch の DELETE 4 (VIEW は残す) | `{rebuilt_at}` |

読みの口の検査の順は **Query → 入力 (400) → binding (503) → D1 (500)**。書き込みの口は **認可 (403 / 503) → 入力 → binding → I/O**。
verify・verify-debug の bumon が空の営業所は、verify は検証せず 200 + `skipped_reason: "no_bumon"` (verify_jobs にも ok = 1 で記録。
無いと r2_pending が埋まらない)。verify-debug は止めずに集計して返す。verify_jobs の UPSERT の失敗は握りつぶす (応答は返す)。

### recalc

段取りはオンプレ版 (root の `src/routes/uriage.rs` の `recalc`) と同じ: editable months → 月ごとに masters (月末日) →
job (month × 営業所、直列) ごとに SQL Server 1 本 → 集計 → D1 の 1 batch (原子的) → R2 に raw を put → `r2_synced`。

- 検査の順は **認可 (403 / 503) → Query (400) → binding (D1 = 503 `binding:missing`、他は 503 `binding:<binding 名>`) → I/O**
- editable months の失敗は 502 `cakephp:<request|timeout|status:N|json>`。SQL を流す job が 12 を超えたら 422 `jobs:too_many`
  (masters を全部取ってから数え、SQL Server・D1 には触らない)
- job の中の失敗 (masters・SQL Server・D1) は 200 の `jobs[].status = "failed"` (オンプレ版と同じ) と recalc_jobs の failed。
  error は `stage:kind` だけ (`cakephp:status:500`・`sqlserver:login:server:18456/14/1`・`d1:batch` 等)
- TDS の接続は 1 リクエスト 1 本を job 間で使い回し、失敗した後の job は張り直す
- R2: key は `uriage/{month}/eigyosho-{id}.ndjson.gz` (D1 の `raw_path` にも入れる)。customMetadata に fingerprint と computed_at。
  **fingerprint 不変かつ `r2_synced` なら put を省く**。put の失敗は failed にしない (computed のまま。次の recalc で置き直す)
- **fingerprint はオンプレ版とバイト単位で同じ** (`raw.rs`、`logic/tests/golden.rs` が固定)。移送したデータと一致しないと全 bucket が「変化あり」になる

### オンプレ版との違い

| | オンプレ版 (SQLite の `LocalStore`) | Worker (D1) |
|---|---|---|
| 置き場が無い | `[sqlite] path` が空 = 503 (本文なし) | `URIAGE_DB` の binding が無い = 503 `{"ok":false,"stage":"binding","kind":"missing"}` |
| 置き場の失敗 | 500 (daily 等は本文なし、recalc-jobs・verify-history は失敗の文言入りの平文) | 500 `{"ok":false,"stage":"bind\|query\|rows","kind":"failed\|malformed"}` (D1 の message は出さない) |
| recalc-jobs・verify-history で置き場が無い | 500 (Disabled も 500 になっていた) | 503 (上と同じ) |
| 404 / 405 | axum の既定 (本文なし) | `{"ok":false}`。`by-person`・`raw`・`ack` は移さないので 404 |
| verify の method | GET | **POST** + header `X-Uriage-Write-Token` (D1 に書くため。GET は 405)。Query (`id`・`date`・`cal`) は同じ |
| verify の失敗の本文 | 生の文言 (`CakePHP status 500: …`・`SQL Server fetch failed (status=N)`) | `stage:kind` だけ (`cakephp:status:500`・`sqlserver:login:…`・`cakephp:sum`)。date 不正 (400)・営業所なし (404) は同じ平文 |
| verify の SQL Server の失敗 | 接続 = 503、クエリ = 500 (本文は固定の文言) | 同じ status。本文は `{"ok":false,"stage":"sqlserver","kind":"<段>:<種類>"}` |
| admin の method | POST (認可なし) | POST + token |
| admin/rebuild | 全 table・VIEW を DROP → 再 migrate | **DROP しない。4 表 (日次・取引先別・recalc_jobs・verify_jobs) を DELETE する** (VIEW・schema は残る) |
| 修復 | 起動のたびに VIEW を作り直し、列の後付けと修復の UPDATE 2 本を流す | 持たない (表は migration で 1 回だけ。移送するデータは修復した後の形で入れる) |
| recalc の raw | disk に書き、nuxt の r2-sync が `raw` / `ack` で R2 に運ぶ | recalc が R2 に put し、同じリクエストで `r2_synced` にする |
| recalc の失敗の error | 生の文言 (`sqlserver: QueryError(…)` 等) | `stage:kind` だけ |
| recalc で binding / token が無い | CakePHP の base_url が空 = 503 (平文) | 503 `{"ok":false,"stage":…,"kind":…}`。token が無い・違う = 403 |
| recalc の job 数 | 上限なし | 12 を超えたら 422 |
| 成功した recalc で既存行が failed・fingerprint 不変 | upsert の ON CONFLICT が failed を保つ (r2_pending からも外れたまま) | computed に戻す (put が通れば r2_synced)。保つのは computed / r2_synced だけ |

**確かめ方**: `logic/tests/d1.rs` が native の SQLite に migration と固定の入力 (`logic/tests/fixtures/d1_fixture.sql`) を流し、
固定の要求の列 (`d1_requests.txt`、27 本。成功・空・400 の 3 種) を Worker と同じ順・同じ部品で流した応答 (status・content-type・本文) が
`d1_expected.txt` と一致することを確かめる。期待値はオンプレ版 (root の axum の handler + `LocalStore`) に同じ行と要求を与えて得たもので、
Worker (`worker-build` → `wrangler dev --local` + local D1) を同じ要求で叩いた応答とも一致した。root は売上の撤去で消えるので、比べる相手を外して値を固定している
(`workers/kintai/pg/tests/restraint_d1_parity.rs` と同じ形)。D1 は数を JS の number で返すので、整数を浮動小数で渡しても同じ応答になることも見る。

## binding (`worker/wrangler.toml`)

- `URIAGE_DB` — D1 `ichibanboshi-uriage` (apac。`migrations_dir = "migrations"`)。**トップレベルにだけ置く**。無ければ全部の口が 503
- `URIAGE_R2` — R2 bucket `ichibanboshi-uriage` (raw ndjson.gz)
- `URIAGE_SQL_VPC` — SQL Server への口 (TCP)。一番星 Worker の `ICHIBAN_VPC` と同じ VPC Service。資格情報は `ICHIBAN_SQL` (同じ secret、SELECT のみ)
- `URIAGE_CAKEPHP_VPC` — 社内 CakePHP への口 (HTTP)。勤怠 Worker の `KINTAI_CAKEPHP_VPC` と同じ VPC Service。origin は `http://localhost:120`、3xx は追わない
- `URIAGE_WRITE_TOKEN` — 書き込みの口の共有 secret (Secrets Store、`ICHIBAN_SQL` と同じ store)
- `CF_VERSION_METADATA` — 版の元
- `[limits] cpu_ms = 60000`
- ローカル検証 (`wrangler dev`) は `.dev.vars` に `LOCAL_SQL_ADDR` (`host:port`) と `LOCAL_URIAGE_SQL_JSON` (`{"user":…,"pass":…}`) を置くと VPC と Secrets Store を迂回する。**wrangler.toml の vars には置かない**
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし / d1_databases・r2_buckets・vpc_services・secrets_store_secrets は
  トップレベル以外に無い / services を持たない / `LOCAL_` で始まるキーが無い。`scripts/check-exposure.sh` が CI で検査し、`scripts/check-exposure-test.sh` が陰性対照

## D1 の migration

- 表の定義の正本は `worker/migrations/0001_uriage.sql` (4 表・index・VIEW 3 本。オンプレ版の `src/sqlite.rs` の migrate と同じ定義で、後付けの列は表の中に並べた)。
  **適用済みの migration は変えない** (表を変えるなら `0002_*.sql` を足す)
- **本番 D1 への適用は人が手で打つ**: `cd workers/uriage/worker && npx wrangler d1 migrations apply URIAGE_DB --remote`。
  deploy job は `wrangler d1 migrations list --remote` で未適用が 0 件であることだけを確かめ、残っていれば deploy しない。PR の job は runner の中の local D1 に当てる

## ローカル検証

```
cd workers/uriage/worker && worker-build --release
npx wrangler d1 migrations apply URIAGE_DB --local --persist-to <dir>
npx wrangler d1 execute URIAGE_DB --local --persist-to <dir> --file ../logic/tests/fixtures/d1_fixture.sql
npx wrangler dev --local --persist-to <dir>
```

## 本番 deploy

タグ `worker-uriage-v*` の push で `.github/workflows/worker-uriage.yml` の deploy job が `wrangler deploy --tag <タグ> --message <git SHA>`
を打つ (org の secret `CLOUDFLARE_API_TOKEN`)。main への merge では本番に出ない。

## 変えてはいけないもの

- **SQL の文字列** — `logic/tests/golden.rs` が代表的な入力の sha256 を固定している
- **raw ndjson のバイト列と fingerprint** — 同じく固定。オンプレ版が `recalc_jobs` に記録した fingerprint と
  Worker の再計算が一致しないと、全 bucket が「変化あり」になる
- **D1 の読みの文** — オンプレ版の rusqlite の文と同じ文字列 (`d1.rs`)。行は列の番号で読む (`raw()`)
- **`uriage_rows_sql` の呼び方の順序** — 部門が空かは接続の前、`None` (英数字に絞って空) は接続の後に見る

## 試験と gate

```
cargo test --manifest-path workers/uriage/Cargo.toml -p uriage-logic
cargo llvm-cov --manifest-path workers/uriage/Cargo.toml -p uriage-logic --text > cov.txt
bash scripts/check_coverage_100.sh --config workers/uriage/coverage_100.toml --use-cache cov.txt
cargo build --manifest-path workers/uriage/Cargo.toml --target wasm32-unknown-unknown
bash workers/uriage/scripts/check-exposure.sh && (cd workers/uriage && bash scripts/check-exposure-test.sh)
```

CI は `.github/workflows/worker-uriage.yml`。`compute.rs` は到達不能な 1 行があるので登録簿に載せない (理由は
`coverage_100.toml`)。root の HTTP の試験は `tests/uriage_test.rs` に残る。
