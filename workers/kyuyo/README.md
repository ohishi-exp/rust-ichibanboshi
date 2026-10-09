# workers/kyuyo

Cloudflare Worker (Refs #322)。給与大臣 (`/api/kyuyo/*`) を Worker + Workers VPC + 既存の Tunnel へ移す先。
オンプレ版 (repo ルートの `src/routes/kyuyo.rs`) と並走させて応答を比べてから切り替える。

| 口 | 状態 |
|---|---|
| `POST /probe` | ログインと `SELECT 1` だけ (認可なし。データは返さない) |
| `GET /kyuyo/access` | 実装済み。認可の結果の email で `{"allowed":true,"email":…}`、`Cache-Control: no-store`。SQL Server を開かない |
| `GET /kyuyo/synced-months` | 実装済み。DO の SQLite の sync 済み月 (`{"entries":[…]}`、オンプレ版と同じ形)。SQL Server を開かない |
| `GET /kyuyo/databases` / `companies` | 実装済み。毎回 SQL Server を読む (オンプレ版と同じ応答) |
| `GET /kyuyo/employees` / `payroll` (`?company=&month=`) | 実装済み。read-through: DO の SQLite にあれば SQL Server を開かず `source:"cache"`、無ければ読んで保存して `source:"live"` |
| `POST /kyuyo/sync` (`?company=&month=`) | 実装済み。payroll と employees を読み直して保存。保存に失敗したら 500 (sync 成功 = 保存が最新) |

パスに `/api` は付かない (呼び出し側の binding から見た path)。method が違えば 405、どの口でもなければ 404 (本文 `{"ok":false}`)。

## 構成

| パス | 中身 |
|---|---|
| `logic/` | crate `kyuyo-logic`。Worker に依存しない純粋ロジック (経路判定・失敗の stage と応答の写像・資格情報 JSON の検証)。std のみで `cargo test` できる |
| `logic/src/api.rs` | `/api/kyuyo/*` の応答型。**オンプレ版と Worker が同じ定義を使う** (JSON のキーと順は unit test が固定) |
| `logic/src/store_keys.rs` | derived store の DDL (3 表)・版・scope の鍵。**オンプレ版の `src/kyuyo/store.rs` と DO が共有** |
| `logic/src/auth.rs` | auth-worker の `authorize` の戻りの読み方 (200 だけ通す・fail-closed) と `/kyuyo/*` の応答 |
| `logic/src/payroll.rs` | 給与明細を組み立てる純粋ロジック (オンプレ版も借りている) |
| `logic/src/sql.rs` | 給与大臣に流す SQL 文と DB 名の検証。**オンプレ版の `src/kyuyo/repo.rs` と Worker が同じ文字列を流す** (CAST / CONVERT(…,120) はここ) |
| `logic/src/service.rs` | SQL Server を開く 5 口の純粋部分: company / month の検証 (400)、失敗の写像 (オンプレ版の `map_repo_err` / `map_db_open_err`)、行の組み立て、store の行の encode / decode |
| `worker/` | crate `kyuyo-worker` (cdylib)。fetch は認可してから全リクエストを DO へ転送する |
| `worker/src/state.rs` | Durable Object `KyuyoState` (SQLite)。SQL Server を開く処理はこの中のロックの中だけ |
| `worker/src/auth.rs` | auth-worker の `KyuyoAuthEntrypoint.authorize(token)` (binding `AUTH_KYUYO`) への RPC |
| `worker/src/probe.rs` | `POST /probe` の本体 (DO のロックの中で走る) |
| `worker/src/repo.rs` | 資格情報・TCP・ログイン (`connect`、`/probe` と共用) と、`sql.rs` の文を流して `Raw*Row` に詰める読み取り (型が合わなければ空文字 / 0、オンプレ版と同じ) |
| `worker/src/routes.rs` | 5 口の本体 (DO のロックの中で走る。1 リクエスト = 1 接続) |
| `worker/src/store.rs` | DO の SQLite の derived store の読み書き (オンプレ版 `src/kyuyo/store.rs` と同じ表・同じ中身) |
| `worker/src/tcp.rs` | VPC binding の JS `connect()` を呼ぶ extern |
| `worker/src/transport.rs` | socket を開いて `tokio_util::compat` で tiberius に渡せる形にする |
| `scripts/check-exposure.sh` | `worker/wrangler.toml` の公開範囲の検査 (CI で毎回) |
| `scripts/check-exposure-test.sh` | 上の陰性対照 |

repo ルートの package (オンプレ / Cloud Run の本体) とは独立した workspace (`workers/kyuyo/Cargo.toml`)。

```text
Service Binding を宣言した worker
  └─ ichibanboshi-kyuyo (fetch)
       ├─ /kyuyo/* は先に AUTH_KYUYO (auth-worker の KyuyoAuthEntrypoint).authorize(token)
       └─ 全リクエストを DO KyuyoState (idFromName("kyuyo") の 1 インスタンス) へ転送
            ├─ SQLite: kyuyo_payroll / kyuyo_employees / kyuyo_sync_state + schema_version
            └─ ロックの中だけで SQL Server を開く
                 ├─ KYUYO_SQL (Secrets Store): JSON {"user","pass"}
                 └─ KYUYO_VPC (Workers VPC の VPC Service) ── Tunnel ── 社内の給与大臣 SQL Server
                       tiberius 0.12 (default-features = false、tds73 + chrono)、平文 TDS
```

## 認可 (`/kyuyo/*`)

fetch は `Authorization: Bearer <token>` を取り出し (無ければ空文字)、auth-worker の `KyuyoAuthEntrypoint.authorize(token)`
を呼ぶ。戻りは throw しない `{status, body, contentType}`:

- 200 `{"allowed":true,"email":…}` → email を内部ヘッダ `x-kyuyo-authorized-email` に載せて DO へ転送する。DO への
  リクエストは method と URL だけから新しく組み立てるので、**外から来た同名ヘッダ (や他のヘッダ・本文) は DO に届かない**。
  200 でも body から email が取れなければ 503 `{"error":"server_error"}` (fail-closed)
- 200 以外 (401 `unauthorized` / 403 `forbidden` / 503 `kyuyo_allowlist_unset`・`server_error`) → **その status と body をそのまま返す**
- binding が無い・RPC が落ちた → 503 `{"error":"server_error"}`

認可を差し替えるローカル専用の var や分岐はコードに置かない (fail-open になる)。ローカル検証はスタブの auth worker を
Service Binding で繋ぐ (下の「ローカル検証」)。

### 認可の注意

- **allowlist の正本は auth-worker の KV `kyuyo-allowed-emails` だけ** (オンプレ版の口は撤去済み)。
  空なら 503 (`kyuyo_allowlist_unset`)
- **認可で弾いた応答の body は auth-worker のもの** (`{"error":"unauthorized"}` 等)。オンプレ版の `ErrorBody` とキー
  (`error`) は同じだが文言は違う。**並走比較では認可で弾いた応答は status だけを比べる**

## DO `KyuyoState`

- インスタンスは `idFromName("kyuyo")` の 1 つだけ。`POST /probe` を含む全リクエストがここを通る
- SQLite に `store_keys` の DDL で 3 表 (オンプレ版の derived store と同じ形) と自前の `schema_version` 表
  (`PRAGMA user_version` は使わない)。版 (`store_keys::SCHEMA_VERSION`) が違えば 3 表を drop → 再作成する
  (derived なので migration しない。源泉から作り直せる)
- DO は await 中に次のリクエストが割り込むので、SQL Server を開く区間は `tokio::sync::Mutex` で直列化する
  (給与大臣 PC への同時接続を増やさない。オンプレ版の `KyuyoLimiter` と同じ役目)。`/probe` と 5 口がこのロックを通る
- `access` と `synced-months` は SQL Server を開かない (ロックを取らない)
- 5 口はロックの中で走る (employees / payroll のキャッシュ命中もロックの中なので、sync の保存と読みが交差しない)。
  保存は await を挟まずに文を続けて流すので、DO の暗黙のトランザクションで一度に確定する。途中の文が失敗したら
  その scope の `kyuyo_sync_state` を消して、行の欠けたキャッシュを命中させない

## 5 口 (`databases` / `companies` / `employees` / `payroll` / `sync`)

挙動はオンプレ版 `src/routes/kyuyo.rs` の各ハンドラと同じ。応答 JSON は `logic/src/api.rs` の型、SQL は `logic/src/sql.rs`。

| 失敗 | status | 本文 `error` (オンプレ版と同じ文言) |
|---|---|---|
| company が `0100/0200/0300/0400` 以外・month が `YYYY-MM` でない | 400 | `company は … のいずれかで指定してください` / `month は YYYY-MM で指定してください` |
| company / month が無い | 400 | `company と month を指定してください` (オンプレ版は axum の `Query` が返す平文の 400) |
| 資格情報が読めない | 503 | `給与 DB 接続が未設定です ([kyuyo] config)` |
| TCP・ログインの失敗 (20 秒) | 503 | `給与 DB に接続できません (給与大臣 PC の稼働を確認してください)` |
| employees / payroll の本体クエリで SQL Server error **4060** | 404 | `{DB 名} を開けません (…)` |
| それ以外のクエリの失敗・60 秒の時間切れ | 500 | `給与 DB クエリに失敗しました` |
| sync の保存の失敗 | 500 | `キャッシュへの保存に失敗しました (payroll)` / `(employees)` |

- 404 の判定はオンプレ版 (文言 "Cannot open database" か "4060") と同じことをエラー番号 4060 で行う。
  **SQL Server 2022 (docker) では、存在しない年度 DB は 208 (Invalid object name)、権限の無い DB は 916 になり、
  どちらも 4060 ではない** ので、オンプレ版も Worker も 500 になる (ローカルで両方を叩いて一致を確認済み)
- `companies` は会社名マスタ (`KYCOMSTD`) が読めなくても warning を付けて一覧を返す (employees の会社名も同じ)
- 失敗ログは 1 行 `kyuyo payroll: failed at query (6 ms) kind=server:208/16/1` (口の名前・stage・種類・ミリ秒だけ)。
  成功は `kyuyo payroll: ok cache (0 ms)` / `ok live (22 ms)` — 2 回目の読みが SQL Server を開いていないことはこの行で見える
- `synced_at` はオンプレ版と同じ RFC3339 (`…+00:00`)。精度はミリ秒 (オンプレ版はナノ秒)
- 給与大臣の varchar (照合順序 `Japanese_CI_AS` = CP932) と nvarchar の日本語は tiberius がそのまま読む
  (`encoding_rs` の Shift_JIS。feature の追加は要らない)。ローカルの docker で氏名・所属・会社名・項目名が化けないことを確認済み

## `POST /probe`

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
| binding | `KYUYO_VPC` | VPC Service (TCP、宛先は給与大臣 SQL Server のポート)。`service_id` は `wrangler.toml` に入れてある |
| binding | `CF_VERSION_METADATA` | ログに出る版の元 |
| Service Binding | `AUTH_KYUYO` | auth-worker の named entrypoint `KyuyoAuthEntrypoint` (`authorize(token)` だけ) |
| Durable Object | `KYUYO_STATE` | class `KyuyoState` (SQLite、migration `v1` の `new_sqlite_classes`) |

`worker/wrangler.toml` の `service_id` (VPC Service) と `store_id` (Secrets Store) は実 id が入っている。どちらも資格情報でも宛先でもないので public repo に置く (smb-watch と同じ扱い)。宛先の IP・Tunnel ID・account ID は書かない。

ローカル検証専用の `LOCAL_SQL_ADDR` (host:port) と `LOCAL_KYUYO_SQL_JSON` (`KYUYO_SQL` と同じ JSON。
あるときだけそれを読み、Secrets Store は見ない) は `.dev.vars` にだけ置く。どちらも `vars` に置かない。

応答にもログにも、エラーの生文言・ホスト・ポート・ユーザー名を出さない。ログは stage・失敗の種類・所要ミリ秒だけ。

失敗ログは 1 行 `kyuyo probe: failed at login (5019 ms) kind=io:Other`。`kind` は `logic/` の `ErrKind` (`timeout` / `transport` / `io:<ErrorKind の名前>` / `server:<code>/<class>/<state>` / `protocol` / `encoding` / `tls` / `routing` / `other`)。tiberius のエラーからは種類・番号・状態だけを取り、`message` や表示文言は読まない。

## 公開範囲

外から届く口を持たない。`scripts/check-exposure.sh` が CI で毎回 `worker/wrangler.toml` を検査する:

- トップレベルに `workers_dev = false` と `preview_urls = false` が明示されている
- `route` / `routes` が無い、`env` 表が無い (トップレベルだけで運用する)
- SQL Server への口 `vpc_services` の `KYUYO_VPC` はトップレベルにある
- `vars` に `LOCAL_SQL_ADDR` と `LOCAL_KYUYO_SQL_JSON` が無い
- `services` の `AUTH_KYUYO` が 1 つだけあり、`service = "auth-worker"` / `entrypoint = "KyuyoAuthEntrypoint"` を指す
  (トップレベル。別の worker・entrypoint へ差し替えると allowlist を通らずに給与が読める)
- `durable_objects.bindings` の `KYUYO_STATE` が `class_name = "KyuyoState"` で `script_name` を持たない (この Worker 自身の DO)、
  `migrations` の `new_sqlite_classes` に `KyuyoState` がある
- `service_id` がプレースホルダのままなら warning (fail にはしない。実 id を入れた今は出ない)

fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。同一アカウントで binding を宣言した
worker は誰でも叩ける: `POST /probe` の効果は給与大臣 SQL Server への 1 回のログインと `SELECT 1` だけでデータは返さない。
`/kyuyo/*` は auth-worker の認可 (上) を通ったときだけ DO へ届く。

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

CI は `.github/workflows/worker-kyuyo.yml`。PR と main への push は build と dry-run だけ (secret も token も使わない)。タグ `worker-kyuyo-v*` の push だけが本番へ `wrangler deploy` する (`v*.*.*` には当てない)。

## ローカル検証 (wrangler dev + docker の SQL Server)

1. 使い捨ての SQL Server を 127.0.0.1 のエフェメラルポートで起動する (パスワードは使い捨ての値):

   ```sh
   docker run -d --name mssql-kyuyo -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=<使い捨て> \
     -p 127.0.0.1::1433 mcr.microsoft.com/mssql/server:2022-latest
   docker port mssql-kyuyo 1433
   ```

2. **元の `worker/wrangler.toml` のまま `wrangler dev` しない** (`vpc_services` が `remote = true` なので
   Cloudflare API に繋ぎにいく)。ローカル専用のコピー `worker.toml` (`vpc_services` と `secrets_store_secrets` と `[build]` を外し、
   `main` をビルド済みの `worker/build/index.js` の絶対パスに向けたもの。`services` と DO はそのまま) を repo の外に作り、
   その隣に `.dev.vars` を置く:

   ```sh
   LOCAL_SQL_ADDR=127.0.0.1:<port>
   LOCAL_KYUYO_SQL_JSON={"user":"sa","pass":"<使い捨て>"}
   ```

3. 同じ場所にスタブの auth worker を置く。名前は `auth-worker`、`KyuyoAuthEntrypoint.authorize(token)` を持ち、
   token が `ok` なら 200 `{"allowed":true,"email":"stub@example.com"}`、`deny` なら 403 `{"error":"forbidden"}`、
   それ以外は 401 `{"error":"unauthorized"}` を返すだけのもの:

   ```js
   // stub.js (stub.toml: name = "auth-worker" / main = "stub.js" / compatibility_date は worker と同じ)
   import { WorkerEntrypoint } from "cloudflare:workers";
   const reply = (status, body) => ({ status, body: JSON.stringify(body), contentType: "application/json" });
   export class KyuyoAuthEntrypoint extends WorkerEntrypoint {
     async authorize(token) {
       if (token === "ok") return reply(200, { allowed: true, email: "stub@example.com" });
       if (token === "deny") return reply(403, { error: "forbidden" });
       return reply(401, { error: "unauthorized" });
     }
   }
   export default { fetch: () => new Response("stub", { status: 404 }) };
   ```

4. `npx wrangler@4.144.0 dev -c worker.toml -c stub.toml --port <port>` (1 つ目が主。2 つ目が Service Binding の相手) を起動して見る:
   - `GET /kyuyo/access` — `Bearer ok` で 200 `{"allowed":true,…}` (`Cache-Control: no-store`)、`Bearer deny` で 403、無しで 401。
     `x-kyuyo-authorized-email` を外から付けても応答の email は変わらない
   - `GET /kyuyo/synced-months` — 空なら 200 `{"entries":[]}`
   - 5 口は下の「5 口のローカル検証」
   - `POST /probe` — `{"ok":true}`。陰性対照: pass を誤らせると 502 `stage":"login"`、`LOCAL_SQL_ADDR` を閉じたポートに
     すると `stage":"connect"`、`GET /probe` は 405、`POST /x` は 404
   - `-c stub.toml` を外して起動すると `/kyuyo/*` は 503 `{"error":"server_error"}` (認可に届かなければ通さない)
5. `docker rm -f mssql-kyuyo`

### 5 口のローカル検証

上の構成に、給与大臣と同じ形の最小のテーブルを作って流す (照合順序は `Japanese_CI_AS`、ダミーデータは実在の人名・社名を使わない):

- DB `KYCOMSTD` (`SELDATA`: `KCODE smallint`, `CONAME1 varchar`) と `KYDATA{会社}_{年度}C` (`KYUYO` (`MONEY00..79` /
  `KINDATA0000..1600`) / `SHAIN1` / `SHOZOKU` (`NAME1` / `NAME2` は nvarchar) / `SHAIN2` / `SHAIN3` / `KOUMOKU` / `SHUKEI1`)
- sa ではなく読み取り専用ログイン (`db_datareader`) を作り、年度 DB の 1 つだけ権限を付けない (`companies` の warning を見るため)
- 見るもの: 5 口が 200、2 回目の employees / payroll が `source:"cache"` (ログ `ok cache`)、コンテナを止めても
  キャッシュ済みの月は 200 で `databases` は 503、存在しない年度は 500 (上の 404 の注)、不正な company / month は 400、
  sync の後に `synced-months` に月が出る
- オンプレ版との突き合わせ: オンプレ版の口は撤去済みなので、この手順はできない。比べたいときは撤去前の commit (84f0377) をビルドして使う

`.dev.vars` は `.gitignore` 済み。

## 本番への切り替え

1. [x] VPC Service `ichibanboshi-kyuyo-sql` (TCP) を既存の Tunnel に作り、`service_id` を入れた
2. [x] Secrets Store に資格情報の JSON (`{"user":"…","pass":"…"}`、secret 名 `KYUYO_SQL`) を入れ、`store_id` / `secret_name` を入れた
3. [x] 実接続確認: `wrangler dev --remote` で `POST /probe` が `{"ok":true}` を返す
4. [ ] タグ deploy: `worker-kyuyo-v*` を push (`wrangler deploy --tag`。org secret `CLOUDFLARE_API_TOKEN`)
5. [ ] 呼び出し側 worker に Service Binding を宣言し、`POST /probe` が `{"ok":true}` を返すことを見る
6. [x] 認可 (auth-worker の `KyuyoAuthEntrypoint`)・DO `KyuyoState`・`/kyuyo/access`・`/kyuyo/synced-months` を足した
7. [x] 残りの 5 口 (`databases` / `companies` / `employees` / `payroll` / `sync`) を DO のロックの中に実装した。
   SQL 文と DB 名の検証はオンプレ版と共有 (`logic/src/sql.rs`)。ローカルでオンプレ版と応答が一致することを確認済み
8. [ ] 並走: 呼び出し側から両方を叩いて応答を比べ、一致を見てから切り替える。**並走中の給与大臣 PC への同時接続は
   最大 2 本** (オンプレ版の `KyuyoLimiter` = Semaphore(1) と Worker の DO ロックが独立に 1 本ずつ)。給与大臣 PC の
   上限 (同時 2 接続) に収まるが、並走中に別の接続元 (手作業の SSMS 等) を足すと超える

### 到達面の判断

本番 deploy 後も fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。同一アカウントで binding を
宣言した worker は誰でも叩けるが、`POST /probe` の効果は給与大臣 SQL Server への 1 回のログインと `SELECT 1` だけで
データは返さない。`/kyuyo/*` は auth-worker の認可を通ったときだけ応答する。

### 罠

- 宛先側のファイアウォールが cloudflared ホストからの接続を許しているか先に確かめる。TCP が捨てられると
  `stage=login` で `kind=io:Other` (Network connection lost) になり、資格情報の誤りに見える
