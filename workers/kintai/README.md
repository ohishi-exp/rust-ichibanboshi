# workers/kintai

勤怠 (kintai) の読み出し Worker `ichibanboshi-kintai` (Refs #322)。口は 2 系統:

- 社内 MariaDB (打刻・デジタコの生行) — 到達の確認 (PoC) の `POST /probe` だけ
- Supabase の勤怠スキーマ (`kintai.*`) を**読むだけ**の `GET /api/kintai/*` の 5 本 (Cloud Run 版から移した。下記)

## 到達の経路

Workers VPC (TCP 3306) → 既存の Tunnel → 社内 MariaDB。MySQL プロトコルを**自作のクライアント** (`mysql/` = `kintai-mysql`) で話す。

- **TLS なし** (平文で話す。CLIENT_SSL は立てない)。社内 LAN 区間の平文は一番星の TDS と同じ扱い
- 認証は **mysql_native_password** だけ。サーバーが Auth Switch Request で別のプラグインを求めたら `auth` 段の失敗 (`auth_plugin`)
- 文字コードは utf8mb4 (charset 45)
- 1 リクエスト = 1 接続。接続 → handshake → 認証 → `SET SESSION max_statement_time=60` (convoy 対策。オンプレ版と同じ) → クエリ → `COM_QUIT`
- 社内 MariaDB へは SELECT だけ

Hyperdrive の MySQL は JS ドライバ専用で TLS が必須、wasm32 で動く既製の MySQL クライアントも無かった
(`mysql_common` は `default-features = false` で wasm32 の build が flate2 の backend 未選択で落ちる) ので自作した。

## `POST /probe`

接続して `SELECT 1, VERSION(), @@character_set_connection, CURRENT_USER()` を流す。

| 結果 | status | 本文 |
|---|---|---|
| 届いた | 200 | `{"ok":true,"version":"…","charset":"utf8mb4","user_matches":true,"elapsed_ms":…}` |
| 資格情報の secret が無い・読めない | 503 | `{"ok":false,"stage":"secret","kind":"missing"}` |
| MariaDB までの途中で失敗 | 502 | `{"ok":false,"stage":"connect\|handshake\|auth\|query","kind":"…"}` |
| 他の path / method 違い | 404 / 405 | `{"ok":false}` |

- `user_matches` は `CURRENT_USER()` の `@` より前が secret の `user` と一致するかの真偽だけ (DB のユーザー名そのものは返さない)
- `kind` は種別の名前だけ (`timeout`・`transport`・`closed`・`io:<ErrorKind>`・`truncated`・`malformed`・`capability`・`auth_plugin`・
  `server:<エラー番号>` 等)。パスワード・接続先・サーバーのエラー本文は応答にもログにも出さない

Service Binding を持つ Worker からだけ呼べる。実機の確認は `wrangler dev --remote` (下記)。

## `GET /api/kintai/*` (Supabase を読む 5 本)

Cloud Run 版 (root の `src/routes/`) が Supabase を読むだけで答えていた口を移した。**応答 (JSON の形・キー・数値の型)・
入力の検査・400 / 502 / 503 の条件は元と同じ**にしてある (呼び手の relay は応答をそのまま返すため)。呼び手の切替はまだ。

| 口 | 引数 | 読む表 |
|---|---|---|
| `day-summaries` | `month` 必須・`driver` 任意 | `kintai.day_summaries` |
| `shift-overlaps` | `month` 必須 | `kintai.shifts` (自己結合) |
| `shift-days` | `month`・`driver` 必須 | `kintai.shifts` + `day_summaries` + `day_parts` |
| `change-log` | `from`・`to` 必須 (両端含む・400 日まで)・`driver` 任意 | `kintai.event_changes` |
| `wage-range` | `comp`・`from`・`to` 必須・`source` (既定 gcp)・任意の現行版 | `kintai.wage_snapshot` |

| 結果 | status | 本文 |
|---|---|---|
| 成功 | 200 | 元と同じ JSON (`application/json`) |
| 入力不正 | 400 | 元と同じ文言 (平文)。Query として読めない (`change-log` の `driver=abc` 等) は axum と同じ `Failed to deserialize query string: …` |
| `KINTAI_HYPERDRIVE` が無い | 503 | `[KINTAI_HYPERDRIVE] が無効です (読み先がありません)` (元の `[kintai_push] が無効です` に当たる) |
| `KINTAI_TENANT_ID` が空・UUID でない・nil | 503 | `読み先のテナントが決まりません (KINTAI_TENANT_ID を設定してください)` |
| DB・接続の失敗 | 502 | 元の文言の頭 + `failed: <kind>` (`kind` は SQLSTATE か固定の語。DB の message・接続先は出さない) |

失敗の本文はすべて `text/plain; charset=utf-8`。検査の順は元の handler と同じ (入力 → binding → テナント → DB)。

- **テナントは設定 pin (`KINTAI_TENANT_ID`)。`X-Tenant-ID` は読まない。** 接続ロールは BYPASSRLS なので、SQL の
  `WHERE tenant_id = $1` (`$1` = pin の UUID) が他テナントを見せない唯一の担保。`logic/tests/common.rs` が
  6 本の SQL 定数すべての `tenant_id = $1` と、各口の第 1 引数が `Type::UUID` の pin であることを確かめる
- DB へは `alc-worker-db` (ippoan/alc-worker-kit、rev は直下の `Cargo.toml` の 1 か所) の `PgClient::tenant_tx` の中で
  `query_typed` / `query_typed_one` だけを流す (名前付き prepared statement は Hyperdrive で接続が切れる)。全 `$n` に型を付ける。
  kit の `SET_TENANT` は `search_path = alc_api` にするが、5 本の SQL は全部 `kintai.` で修飾しているのでそのまま動く
- 移していないもの: `stale-months` (`logic_version` を Worker で同じ値に作れない)・`unko-gaps` (alc の etags の掃引を含む)・
  `wage-snapshot` の保存。どれも書き込み側を移す段で一緒に移す

### 元 (root の src/) と写し (logic/) の対応 — **撤去までは片方を直したらもう片方も直す**

純粋部分は root の crate から共有せず、`logic/` (`kintai-logic`) に写した (共有にすると root の crate を触り、
Cloud Run 版の勤怠の再 deploy と応答の比較が要るため)。**Cloud Run 版の勤怠はこの移行の最後の段 (呼び手を
切り替えた後) で撤去するので、二重管理はそれまでの期限付き。** それまでは、下の左を直したら右も、右を直したら左も直す。

| 元 (root) | 写し (`workers/kintai/logic/`) |
|---|---|
| `src/routes/kintai_day_summaries.rs` (SQL・Query・検査・行 → JSON) | `src/day_summaries.rs` |
| `src/routes/shift_overlaps.rs` | `src/shift_overlaps.rs` |
| `src/routes/shift_days.rs` | `src/shift_days.rs` |
| `src/routes/change_log.rs` | `src/change_log.rs` |
| `src/routes/wage_snapshot.rs` の `wage_range`・`RangeQuery`・`SELECT_RANGE_SQL`・`to_fetched`・`to_buckets` (読み出しだけ) | `src/wage_range.rs` |
| `src/wage_snapshot.rs` (丸ごと。テスト 39 本も) | `src/wage_snapshot.rs` |
| `src/routes/kintai.rs` の `is_valid_month`・`parse_driver` | `src/common.rs` |
| 4 つの `read_tenant_of` / `tenant_of` (`[kintai_events]` → `[kintai_push]` の pin) | `src/common.rs` の `tenant_of` 1 つ (`KINTAI_TENANT_ID`) |
| `month_date_bounds` (DATE) と `month_bounds` (JST の TIMESTAMPTZ。`kintai_push::jst_day_bounds`) | `src/common.rs` の `month_bounds` + `jst_midnight` 1 つずつ |
| 4 つの `store` (`[kintai_push]` が無効なら 503) | `src/common.rs` の `no_db` (`KINTAI_HYPERDRIVE` が無ければ 503) |

テストは `logic/tests/` (DB 不要)。元の単体テストのうちテナント・月の境界・store の写しは `tests/common.rs` に畳み、
handler を叩いていたものは同じ入力を `parse` に通す形に書き直した。DB を要する元の `tests/*_pg_test.rs` は写していない。

## 到達面と認可

**認可なし (ユーザー決定 2026-10-10、一番星と同じ)。** Service Binding 専用 (route・workers.dev・preview 無し) で、
関門は呼び手の側 (relay の共有 secret、kyuyo-mcp の OAuth)。資格情報は Secrets Store の binding で読み、呼び手の cookie・Authorization は受け取らない。

## binding (`worker/wrangler.toml`)

- `KINTAI_MARIADB_VPC` — Workers VPC の VPC Service (TCP 3306)。宛先 host:port は Service 側で固定。`service_id` は VPC Service `ichibanboshi-kintai-mariadb` の id
- `KINTAI_MARIADB` — Secrets Store の secret。JSON `{"user":…,"password":…,"database":…}` (どれも空でない文字列)。未投入なら `/probe` は 503
- `KINTAI_HYPERDRIVE` — Supabase への Hyperdrive (分割 worker と共有の実行用ロールの設定)。**トップレベルにだけ置く**。無ければ `GET /api/kintai/*` は 503
- `KINTAI_TENANT_ID` (`[vars]`) — 読み先のテナントの UUID。本番は deploy 時に repo variable `KINTAI_EVENTS_TENANT_ID` (Cloud Run 版と同じ) を `--var` で渡す (git 履歴に UUID を焼かない)。ここは空のままで、空の間は `GET /api/kintai/*` は 503
- `CF_VERSION_METADATA` — 版の元
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし / `LOCAL_*` の var なし /
  hyperdrive はトップレベル以外に無い。`scripts/check-exposure.sh` が CI で検査し、`check-exposure-test.sh` が陰性対照

## 構成

- `mysql/` (`kintai-mysql`): I/O を持たない純粋なコーデック。`packet.rs` (枠・length-encoded の値) / `handshake.rs` (Initial Handshake v10・
  HandshakeResponse41・mysql_native_password・Auth Switch) / `response.rs` (OK / ERR / EOF・COM_QUERY・テキストの結果セット)。
  CLIENT_DEPRECATE_EOF は立てない (結果セットは EOF で区切られる形に固定)。テストは `mysql/tests/codec.rs`、100% 行カバレッジ gate は `coverage_100.toml`
- `logic/` (`kintai-logic`): Supabase を読む 5 本の口の純粋部分 (上の対応表)。100% 行カバレッジ gate は `coverage_100.toml`
- `worker/` (`kintai-worker`): `lib.rs` (fetch・段ごとの打ち切り時間) / `conn.rs` (socket とコーデックの間) / `probe.rs` (経路・段・応答・資格情報の検証) /
  `reads.rs` (Hyperdrive への接続・テナント・`tenant_tx` の中の `query_typed`・行の詰め直し) /
  `transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern)。`tcp.rs`・`transport.rs` は `workers/ichiban` から写した (共有 crate に畳むのは本実装の段で)

独立した workspace (repo ルートの package・`workers/ichiban`・`workers/kyuyo` からは参照されない)。

## ローカル検証

`cargo test -p kintai-mysql -p kintai-logic` (DB 不要)。Worker は `cargo build --target wasm32-unknown-unknown` と clippy まで。
ローカルで VPC や Secrets Store を迂回する var は持たないので、実接続は VPC Service と `KINTAI_MARIADB` を用意してから
`wrangler dev --remote` で `POST /probe`。Supabase の 5 本も同じく `wrangler dev --remote` (Hyperdrive の経路は CI では通せない)。
`--var "KINTAI_TENANT_ID:<UUID>"` を渡すと 5 本が 503 ではなく答える。

## 本番 deploy

タグ `worker-kintai-v*` の push で `.github/workflows/worker-kintai.yml` の deploy job が `wrangler deploy --tag <タグ> --message <git SHA>`
を打つ (org の secret `CLOUDFLARE_API_TOKEN`)。main への merge では本番に出ない。
