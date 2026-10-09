# workers/ichiban

一番星 (CAPE#01) SQL Server の読み出し Worker (Refs #322)。管理画面が使う 6 本をオンプレ版と同じ path・クエリ・応答で返す
(並走期間は管理画面の proxy が shadow で両方を叩いて応答を比べる)。Workers VPC (TCP のみ) → 既存の Tunnel → SQL Server に
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
(`vehicle_daily.rs`・`costs_daily.rs`)。tiberius にも worker にも依存しない。**オンプレ版 (repo ルート) も path 依存で同じ定義を使う**
(並走期間に応答を比べるため。オンプレ版の 6 本を削除するときに path 依存も外す)。`tiberius::Row` から `Raw*Row` を詰める関数は
オンプレ (`src/repo.rs`) と Worker に別々に持つ — 列の並びは `sql.rs` の定数と 1 対 1 なので、変えるときは両方直す。
100% 行カバレッジ gate は `coverage_100.toml` (worker-ichiban.yml が判定)。

`worker/src/`: `lib.rs` (fetch) / `routes.rs` (7 本の本体。1 リクエスト 1 接続) / `rows.rs` (`tiberius::Row` → logic の型。
**オンプレ `src/repo.rs` の `decode_cp932`・`get_i64`・`get_f64`・`rows_to_*` を列番号まで同じに写している**) / `repo.rs` (資格情報・ログイン) /
`transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern) / `probe_logic.rs` (経路・stage・応答・資格情報の検証)。
接続・経路・応答の部品は `workers/kyuyo` から**意図して写している** (特に公開範囲の検査スクリプト 2 本は kyuyo と片方だけ直さないこと)。独立した workspace (repo ルートの package からは `logic` だけを path 依存で借りる)。

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
