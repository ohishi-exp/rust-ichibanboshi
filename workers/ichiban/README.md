# workers/ichiban

一番星 (CAPE#01) SQL Server の読み出し Worker (Refs #322)。管理画面が使う 6 本を、旧オンプレ版と同じ path・クエリ・応答で返す
(オンプレ版の 5 本は削除済みで、6 本はこの Worker だけが提供する。オンプレの `/health` は他系統の監視用に残っている)。Workers VPC (TCP のみ) → 既存の Tunnel → SQL Server に
TDS でログインする。1 リクエスト = 1 接続。

| 口 | 中身 |
|---|---|
| `GET /health` | `SELECT 1` を流して 200 `{"status":"ok"}`。オンプレ版の `commit` 等は返さない (shadow 比較の対象外) |
| `GET /api/employees` | 社員ﾏｽﾀ。`{"source_table":…,"data":[{"employee_code","employee_name","employee_r"}…]}` |
| `GET /api/vehicles` | 車種ﾏｽﾀ。`data` は `{"vehicle_code","vehicle_name"}` |
| `GET /api/sales/departments` | 部門ﾏｽﾀ。`data` は `{"department_code","department_name"}` |
| `GET /api/sales/vehicle-daily` | `?from=&to=&vehicle=&driver=&customer=&origin=&dest=&limit=`。絞り込み 0 件・`from`/`to` 欠け・読めないクエリは 400 (本文なし) |
| `GET /api/costs/vehicle-daily` | `?from=&to=&vehicle=&driver=&kind=&limit=`。400 の判定は同上 |
| `POST /probe` | 到達の切り分け用。ログインして `SELECT 1`。200 `{"ok":true}` |

**オンプレ版に残っていた一番星系の 15 本は、すべて Worker に移った (#322)。** オンプレとの応答の比較 (#322 のコメント) とタグ `worker-ichiban-v0.2.0` での本番 deploy は済み、呼び手も切り替わった (nuxt-ichibanboshi#134・nuxt-ichibanboshi-seikyu#90)
(method 違いは 405):

| 領域 | 口 (すべて GET) |
|---|---|
| `sales_monthly` | `/api/sales/monthly`・`/api/sales/by-department`・`/api/sales/by-customer`・`/api/sales/yoy` |
| `sales_daily` | `/api/sales/daily`・`/api/sales/customer-trend`・`/api/sales/customer-detail` |
| `sales_yoy` | `/api/sales/customer-yoy`・`/api/sales/customer-yoy-by-dept` |
| `unchin` | `/api/unchin/candidates`・`/api/unchin/summary`・`/api/unchin/customer-net`・`/api/unchin/customer-net-detail` |
| `surcharge` | `/api/surcharge/base` |
| `schema` | `/api/schema/columns` |
| `leave` | `/api/leave/days`・`/api/leave/employees` (rust-leave-worker#1。休暇入力の行と、入社日などに絞った社員) |

- 応答 JSON・400 の判定・limit の丸め (1..=5000、既定 500) はオンプレ版と同じ (`logic/` を共有)
- SQL Server までの失敗はどの口も 502 `{"ok":false,"stage":"secret|connect|login|query","kind":"…"}`。エラー本文・ホスト・ユーザー名は出さない
- method 違いは 405、他の path は 404 (本文 `{"ok":false}`)

## 到達面と認可

**認可なし (ユーザー決定 2026-10-09)。** Service Binding 専用 (route・workers.dev・preview 無し) で、関門は呼び手 (管理画面の proxy) の
requireAuth と path allowlist。同じアカウントで Worker を deploy できる者は binding で読める (社員名・売上・経費) — 承知のうえ。

## binding (`worker/wrangler.toml`)

- `ICHIBAN_VPC` — Workers VPC の VPC Service (TCP)。宛先 host:port は Service 側で固定。`service_id` は VPC Service `ichibanboshi-ichiban-sql` の id
- `ICHIBAN_SQL` — Secrets Store の secret。JSON `{"user":…,"pass":…}`
- `CF_VERSION_METADATA` — 版の元 (workers/kyuyo と同じ)
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし。`scripts/check-exposure.sh` が CI で検査し、`check-exposure-test.sh` が陰性対照

tiberius は `EncryptionLevel::NotSupported`・`database("CAPE#01")`。`port` / `instance_name` は呼ばない
(SQL Browser の UDP は Worker から出せない)。

## 構成

`logic/` (`ichiban-logic`): 管理画面が使う 6 本 (`/health`・`/api/employees`・`/api/vehicles`・`/api/sales/departments`・
`/api/sales/vehicle-daily`・`/api/costs/vehicle-daily`) の SQL 文 (`sql.rs`)・応答の型 (`api.rs`)・絞り込みの判定と行の組み立て
(`vehicle_daily.rs`・`costs_daily.rs`)。tiberius にも worker にも依存しない。使うのは Worker だけ (オンプレ版の 5 本と path 依存は削除済み。
オンプレの `src/repo.rs` には `/health` の `SELECT 1` と `customer_yoy_by_dept` が使う部門一覧の SQL だけが同じ文字列で残る)。
`tiberius::Row` から `Raw*Row` を詰める関数は `worker/src/rows/` にある — 列の並びは `sql.rs` の定数と 1 対 1 なので、変えるときは両方直す。
`period.rs` は期間の計算 (旧オンプレ `src/routes/sales.rs` の `calc_prev_period`・`calc_next_month`・`calc_months` を同じ挙動で写したもの)。
100% 行カバレッジ gate は `coverage_100.toml` (worker-ichiban.yml が判定)。

**移した 15 本は領域ごとに 3 か所のファイルを持つ** (領域名は上の表): `logic/src/<領域>.rs` (SQL・Raw 型・応答型・Query・組み立て) /
`worker/src/routes/<領域>.rs` (`handle(env, route, query) -> Result<String, Failure>`) / `worker/src/rows/<領域>.rs`
(`tiberius::Row` → Raw 型。列の読み方は `rows/mod.rs` の `decode_cp932`・`get_i64`・`get_f64`・`get_i32`・`get_datetime`)。
領域の子はこの 3 つと `logic/tests/`・`coverage_100.toml` の自分の行だけを触る。空の `logic/src/<領域>.rs` は実行行 0 なので、中身が入るまで gate に登録しない。

`worker/src/`: `lib.rs` (fetch) / `routes/mod.rs` (経路の振り分けと 7 本の本体。1 リクエスト 1 接続) / `routes/<領域>.rs` / `rows/mod.rs` (`tiberius::Row` → logic の型。
旧オンプレ版の `src/repo.rs` の `decode_cp932`・`get_i64`・`get_f64`・`get_i32`・`rows_to_*` を列番号まで同じに写したもの) / `rows/<領域>.rs` / `repo.rs` (資格情報・ログイン) /
`transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern) / `probe_logic.rs` (経路・stage・応答・資格情報の検証)。
`routes/mod.rs` の `fetch_rows` は 1 本流すごとに接続し直す。`fetch_rows_many` は 1 接続で複数の (SQL, bind) を順に流して結果セットを同じ順で返す
(1 リクエストで 2〜3 本流す口用。bind が空なら `simple_query`、あれば `query` の規則は同じ)。
接続・経路・応答の部品は `workers/kyuyo` から**意図して写している** (特に公開範囲の検査スクリプト 2 本は kyuyo と片方だけ直さないこと)。独立した workspace (repo ルートの package からは参照されない)。

## ローカル検証

`workers/kyuyo/README.md` のローカル検証の節に準ずる。`.dev.vars` (commit しない) に `LOCAL_ICHIBAN_SQL_JSON` と `LOCAL_SQL_ADDR` を置く。
`wrangler.toml` の vars には置かない。`wrangler.toml` のまま `wrangler dev` を打たない (`vpc_services` が remote で API に繋ぎにいく)。
実接続は VPC Service・`ICHIBAN_SQL`・宛先の FW を用意してから `wrangler dev --remote` で `POST /probe` と 6 本。

## 本番 deploy

タグ `worker-ichiban-v*` の push で `.github/workflows/worker-ichiban.yml` の deploy job が `wrangler deploy --tag <タグ> --message <git SHA>`
を打つ (org の secret `CLOUDFLARE_API_TOKEN`。repo 単位の secret は作らない)。main への merge では本番に出ない。`v*.*.*` タグには当てない。

## 罠

- FW が cloudflared のホストを許していないと `stage=login` / `kind=io:Other` になり、資格情報の誤りに見える
- オンプレ版は instance 名 + SQL Browser でポートを引いている。VPC Service のポートが SQL Server の実ポートと違う・動的ポートだと `stage=connect` か `login` で落ちる
