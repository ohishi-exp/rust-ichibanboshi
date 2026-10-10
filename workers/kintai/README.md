# workers/kintai

勤怠 (kintai) の社内 MariaDB (打刻・デジタコの生行) の読み出し Worker `ichibanboshi-kintai` (Refs #322)。
いまは土台と到達の確認 (PoC) だけで、口は `POST /probe` の 1 本。

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

## 到達面と認可

**認可なし (ユーザー決定 2026-10-10、一番星と同じ)。** Service Binding 専用 (route・workers.dev・preview 無し) で、
関門は呼び手の側 (relay の共有 secret、kyuyo-mcp の OAuth)。資格情報は Secrets Store の binding で読み、呼び手の cookie・Authorization は受け取らない。

## binding (`worker/wrangler.toml`)

- `KINTAI_MARIADB_VPC` — Workers VPC の VPC Service (TCP 3306)。宛先 host:port は Service 側で固定。`service_id` は VPC Service `ichibanboshi-kintai-mariadb` の id
- `KINTAI_MARIADB` — Secrets Store の secret。JSON `{"user":…,"password":…,"database":…}` (どれも空でない文字列)。未投入なら `/probe` は 503
- `CF_VERSION_METADATA` — 版の元
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし / `LOCAL_*` の var なし。
  `scripts/check-exposure.sh` が CI で検査し、`check-exposure-test.sh` が陰性対照
- Hyperdrive の binding はまだ無い

## 構成

- `mysql/` (`kintai-mysql`): I/O を持たない純粋なコーデック。`packet.rs` (枠・length-encoded の値) / `handshake.rs` (Initial Handshake v10・
  HandshakeResponse41・mysql_native_password・Auth Switch) / `response.rs` (OK / ERR / EOF・COM_QUERY・テキストの結果セット)。
  CLIENT_DEPRECATE_EOF は立てない (結果セットは EOF で区切られる形に固定)。テストは `mysql/tests/codec.rs`、100% 行カバレッジ gate は `coverage_100.toml`
- `worker/` (`kintai-worker`): `lib.rs` (fetch・段ごとの打ち切り時間) / `conn.rs` (socket とコーデックの間) / `probe.rs` (経路・段・応答・資格情報の検証) /
  `transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern)。`tcp.rs`・`transport.rs` は `workers/ichiban` から写した (共有 crate に畳むのは本実装の段で)

独立した workspace (repo ルートの package・`workers/ichiban`・`workers/kyuyo` からは参照されない)。

## ローカル検証

`cargo test -p kintai-mysql` (DB 不要)。Worker は `cargo build --target wasm32-unknown-unknown` と clippy まで。
ローカルで VPC や Secrets Store を迂回する var は持たないので、実接続は VPC Service と `KINTAI_MARIADB` を用意してから
`wrangler dev --remote` で `POST /probe`。

## 本番 deploy

タグ `worker-kintai-v*` の push で `.github/workflows/worker-kintai.yml` の deploy job が `wrangler deploy --tag <タグ> --message <git SHA>`
を打つ (org の secret `CLOUDFLARE_API_TOKEN`)。main への merge では本番に出ない。
