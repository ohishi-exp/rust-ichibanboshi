# workers/kyuyo

Cloudflare Worker (Refs #322)。給与大臣 (`/api/kyuyo/*`) を Worker + Workers VPC + 既存の Tunnel へ移す前提として、
**wasm32 の Worker から tiberius で給与大臣の SQL Server に TDS を張れること**を確かめる PoC。
いまは `POST /probe` (ログインと `SELECT 1` だけ) しか持たない。実 API は後続の PR で足す。

## 構成

| パス | 中身 |
|---|---|
| `logic/` | crate `kyuyo-logic`。Worker に依存しない純粋ロジック (`/probe` の経路判定・失敗の stage と応答の写像・資格情報 JSON の検証)。std のみで `cargo test` できる |
| `worker/` | crate `kyuyo-worker` (cdylib)。`#[event(fetch)]` (`POST /probe`) を持つ Worker 本体 |
| `worker/src/tcp.rs` | VPC binding の JS `connect()` を呼ぶ extern |
| `worker/src/transport.rs` | socket を開いて `tokio_util::compat` で tiberius に渡せる形にする |
| `scripts/check-exposure.sh` | `worker/wrangler.toml` の公開範囲の検査 (CI で毎回) |
| `scripts/check-exposure-test.sh` | 上の陰性対照 |

repo ルートの package (オンプレ / Cloud Run の本体) とは独立した workspace (`workers/kyuyo/Cargo.toml`)。

```text
Service Binding を宣言した worker
  └─ ichibanboshi-kyuyo (fetch `POST /probe` だけ)
       ├─ KYUYO_SQL (Secrets Store): JSON {"user","pass"}
       └─ KYUYO_VPC (Workers VPC の VPC Service) ── Tunnel ── 社内の給与大臣 SQL Server
             tiberius 0.12 (default-features = false、tds73 + chrono)、平文 TDS
```

1 回の流れ: 資格情報を読む → `KYUYO_VPC` から TCP を開く → tiberius でログイン
(`EncryptionLevel::NotSupported`、database `master`) → `SELECT 1`。

- 成功: 200 `{"ok":true}`
- 失敗: 502 `{"ok":false,"stage":"secret|connect|login|query"}` (写像は `logic/` の `reply_for_probe`)
  - `secret` — JSON が読めない・キー欠け・空
  - `connect` — TCP が開けない、または 20 秒以内にログインまで終わらない
  - `login` — TDS のログインが拒否された
  - `query` — `SELECT 1` が失敗した・10 秒で返らない・1 が返らない
- パスが `/probe` 以外は 404、`/probe` で POST 以外は 405

tiberius の `rustls` feature と `bb8-tiberius` は wasm32 で落ちる (getrandom / mio / rustls-native-certs) ので入れない。
社内 LAN 区間の平文 TDS は許可済み (repo ルートの `src/kyuyo/repo.rs` と同じ `EncryptionLevel::NotSupported`)。

## 設定 (値はこの repo に書かない)

| 種別 | 名前 | 中身 |
|---|---|---|
| Secrets Store | binding `KYUYO_SQL` | JSON。キー `user` / `pass` (両方必須の非空文字列)。SQL Server 認証の資格情報 |
| binding | `KYUYO_VPC` | VPC Service (TCP、宛先は給与大臣 SQL Server のポート)。`service_id` は作成後に入れる |
| binding | `CF_VERSION_METADATA` | ログに出る版の元 |

`worker/wrangler.toml` の `service_id` と `secrets_store_secrets` の `store_id` / `secret_name` はプレースホルダ。

ローカル検証専用の `LOCAL_SQL_ADDR` (host:port) と `LOCAL_KYUYO_SQL_JSON` (`KYUYO_SQL` と同じ JSON。
あるときだけそれを読み、Secrets Store は見ない) は `.dev.vars` にだけ置く。どちらも `vars` に置かない。

応答にもログにも、エラーの生文言・ホスト・ポート・ユーザー名を出さない。ログは stage と所要ミリ秒だけ。

## 公開範囲

外から届く口を持たない。`scripts/check-exposure.sh` が CI で毎回 `worker/wrangler.toml` を検査する:

- トップレベルに `workers_dev = false` と `preview_urls = false` が明示されている
- `route` / `routes` が無い、`env` 表が無い (トップレベルだけで運用する)
- SQL Server への口 `vpc_services` の `KYUYO_VPC` はトップレベルにある
- `vars` に `LOCAL_SQL_ADDR` と `LOCAL_KYUYO_SQL_JSON` が無い
- `service_id` がプレースホルダのままなら warning (fail にはしない)

fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。同一アカウントで binding を宣言した
worker は誰でも叩けるが、効果は給与大臣 SQL Server への 1 回のログインと `SELECT 1` だけで、データは返さない。
認可は後続の実 API で auth-worker 経由に入れる。

`scripts/check-exposure-test.sh` は wrangler.toml を読んだ dict を 1 か所ずつ崩して書き戻し、各検査が exit 1 になることを確かめる
(特定の表の直前に行を挿す作りにはしない — 末尾に表が足されると検出できなくなる)。

## ビルドと検査

```sh
cargo test --manifest-path workers/kyuyo/Cargo.toml -p kyuyo-logic                      # repo ルートで
cargo build --manifest-path workers/kyuyo/Cargo.toml --target wasm32-unknown-unknown
bash scripts/check-exposure.sh worker/wrangler.toml                                     # workers/kyuyo で
bash scripts/check-exposure-test.sh worker/wrangler.toml
cd worker
cargo install worker-build@0.8.7 --locked
worker-build --release
npx -y wrangler@4.144.0 deploy --dry-run            # login 不要
```

CI は `.github/workflows/worker-kyuyo.yml` (PR と main への push で同じことをする。deploy はしない)。

## ローカル検証 (wrangler dev + docker の SQL Server)

1. 使い捨ての SQL Server を 127.0.0.1 のエフェメラルポートで起動する (パスワードは使い捨ての値):

   ```sh
   docker run -d --name mssql-kyuyo -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=<使い捨て> \
     -p 127.0.0.1::1433 mcr.microsoft.com/mssql/server:2022-latest
   docker port mssql-kyuyo 1433
   ```

2. `worker/wrangler.toml` の `vpc_services` は `remote = true` なので、そのまま `wrangler dev` すると
   Cloudflare API に繋ぎにいく。ローカル専用のコピー (`vpc_services` と `secrets_store_secrets` と `[build]` を外し、
   `main` をビルド済みの `worker/build/index.js` の絶対パスに向けたもの) を repo の外に作り、その隣に `.dev.vars` を置く:

   ```sh
   LOCAL_SQL_ADDR=127.0.0.1:<port>
   LOCAL_KYUYO_SQL_JSON={"user":"sa","pass":"<使い捨て>"}
   ```

3. `npx wrangler@4.144.0 dev --port <port>` を起動し、`curl -s -X POST http://127.0.0.1:<port>/probe` が
   `{"ok":true}` を返すことを見る。陰性対照: pass を誤らせると 502 `stage":"login"`、`LOCAL_SQL_ADDR` を閉じたポートに
   すると `stage":"connect"`、`GET /probe` は 405、`POST /x` は 404
4. `docker rm -f mssql-kyuyo`

`.dev.vars` は `.gitignore` 済み。

## 本番への切り替え (未着手)

1. VPC Service (TCP、宛先は給与大臣 SQL Server のポート) を既存の Tunnel に作る
2. `worker/wrangler.toml` の `service_id` を入れる
3. Secrets Store に資格情報の JSON (`{"user":"…","pass":"…"}`) を入れ、`store_id` / `secret_name` を入れる
4. deploy の経路 (job・タグ) を足す。いまの workflow は build と dry-run だけで deploy しない
5. 呼び出し側 worker に Service Binding を宣言し、`POST /probe` が `{"ok":true}` を返すことを見る
6. 実 API (`/api/kyuyo/*`) を足し、認可を auth-worker 経由で入れる
