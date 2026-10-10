# workers/kintai

勤怠 (kintai) の Worker `ichibanboshi-kintai` (Refs #322)。口は 2 系統:

- 社内 MariaDB (打刻・デジタコの生行) — 到達の確認 (PoC) の `POST /probe` と、直接読む GET の 10 本 (`/api/kintai/*` の 9 本と `/api/dtako/worktime`。オンプレ版から移した。下記)
- Supabase の勤怠スキーマ (`kintai.*`) — 読む `GET /api/kintai/*` の 6 本と、書く `POST /api/kintai/{timecard,wage-snapshot}` の 2 本 (Cloud Run 版から移した。下記)

## 到達の経路

Workers VPC (TCP 3306) → 既存の Tunnel → 社内 MariaDB。MySQL プロトコルを**自作のクライアント** (`mysql/` = `kintai-mysql`) で話す。

- **TLS なし** (平文で話す。CLIENT_SSL は立てない)。社内 LAN 区間の平文は一番星の TDS と同じ扱い
- 認証は **mysql_native_password** だけ。サーバーが Auth Switch Request で別のプラグインを求めたら `auth` 段の失敗 (`auth_plugin`)
- 文字コードは utf8mb4 (charset 45)
- 1 リクエスト = 1 接続。接続 → handshake → 認証 → `SET SESSION max_statement_time=60` (convoy 対策。オンプレ版と同じ) → クエリ → `COM_QUIT`。
  kosoku-daily・version は同じ 1 接続でクエリを続けて流す (遡り起点の 2 本 → 本体。worker の `open` → `query` × n → `Session::quit`)。
  クエリの打ち切り時間 (65 秒) は 1 本ごと
- 社内 MariaDB へは SELECT だけ
- **connect・handshake の段 (認証パケットを送る前) の失敗だけ、新しい接続で 2 回までやり直す** (200ms 空ける)。間を空けずに再接続を続けると
  handshake を読む前に閉じられることがある (`handshake:closed`、20 回連続で 2〜3 回)。認証以降・クエリ以降の失敗はやり直さない。
  判断は `mysql/src/retry.rs` (`should_retry`)。切断は成否によらず COM_QUIT を送ってから閉じる

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

## `GET /api/kintai/*` (社内 MariaDB を直接読む 4 本)

オンプレ版 (root の `src/routes/kintai.rs`) が社内 MariaDB を直接読んで答えていた口を移した。**path・応答 (JSON のキー・null 扱い・
数値の型)・入力の検査の順・400 / 502 / 503 の条件は元と同じ** (呼び手の切替はまだ)。SQL 文と突合・引き当て・末尾検知の純粋ロジックは
共有 crate `kintai-kosoku` をオンプレ版と同じものを使う (写さない)。`/probe` と同じ接続・認証・`SET SESSION max_statement_time=60` の上で
1 本のクエリを流す (1 リクエスト 1 接続)。

| 口 | 引数 | 読む SQL (`kintai_kosoku::sql`) | 窓 | 応答 |
|---|---|---|---|---|
| `events` | `month` (`is_valid_month`)・`driver` **必須** | `EVENTS_SQL` | `month_range` = `[月初, 翌月 2 日)` | `{rows}` (7 キー) |
| `rest-diff` | `month`・`driver` 任意 | `REST_EVENTS_SQL` | `month_range` | `kintai_rest_diff::rest_diff` の結果 + `month`・`driver`・`from`・`to`・`max_items` |
| `reading-dates` | `month`・`driver` 任意 | `OPERATION_READING_DATES_SQL` | `month_range` | `kintai_reading_dates::reading_dates` の結果 + 同上 |
| `tail-gap-probe` | `month`・`driver` 任意 | `ALL_EVENTS_SQL` (乗務員の絞りは Rust 側) | `exact_month_range` = `[月初, 翌月初)` | `kintai_tail_gap_probe::tail_gap_probe` の結果。`expected` = min(月末, JST の今日 − 1 日)。今日は Worker が `Date.now()` から作って渡す |

| 結果 | status | 本文 (平文) |
|---|---|---|
| `month` が不正 (events は `is_valid_month`、他 3 本は窓が作れない) | 400 | `month は YYYY-MM で指定してください` |
| `driver` が不正 (events は省略も、他 3 本は `driver=` の空も) | 400 | `driver は乗務員CD (数字) で指定してください` |
| Query として読めない (同じ欄が 2 回等) | 400 | axum と同じ `Failed to deserialize query string: …` |
| `KINTAI_MARIADB` が無い・読めない・JSON 不正 | 503 | `MariaDB 接続設定が未設定` (元の `NotConfigured` と同じ) |
| 接続・handshake・認証・クエリ・行の読み取りの失敗 | 502 | `MariaDB query failed: <段>:<種別>` (`connect:timeout`・`query:server:1146`・`rows:int` 等。DB の message・接続先は出さない) |

検査の順は元の handler と同じ (`month` → `driver` → 資格情報 → DB)。

- **値の埋め込み**: 自作コーデックは COM_QUERY (テキストプロトコル) だけなので、SQL の名前付き引数 (`:from`・`:to`・`:driver`) を
  `kintai_mysql::bind::expand` がリテラルに展開する。受ける値は**整数 (`u64`)・日時 (`'YYYY-MM-DD HH:MM:SS'`)・NULL の 3 種だけ**で、
  任意の文字列を受ける口は無い (`month` も `driver` も検査済みの値から作る)。引用符・バッククォートの中は飛ばし、未知の名前・使われない
  名前・コメントはエラー (502 `bind_*`)。4 本の SQL の名前の集合と渡す名前の集合の一致はテスト (`mysql/tests/bind.rs`・`logic/tests/mariadb.rs`) で固定
- **行 → JSON**: テキストプロトコルの各列 (バイト列か NULL) を、元の mysql_async のタプルと同じ型で読む。元で `String` の列の NULL・
  整数の列の読めない値・不正な UTF-8 は 502 (`rows:null`・`rows:int`・`rows:utf8`)。NULL は JSON の null (0 や空文字にしない)
- オンプレ版の同時実行キャップ (`KOSOKU_DB_PERMITS` の 4) は持たない (Worker はリクエストごとに独立)。`max_statement_time=60` は同じ

## `GET /api/kintai/day-events`・`GET /api/dtako/worktime` (社内 MariaDB を直接読む 2 本)

オンプレ版 (root の `src/routes/dtako_day.rs`・`src/routes/dtako_worktime.rs`) の口を移した。上の 4 本と同じ接続・資格情報・
400 / 502 / 503 の流儀 (`MariaDB 接続設定が未設定` / `MariaDB query failed: <段>:<種別>`) で、**path・応答・入力の検査の順・
400 の文言は元と同じ** (呼び手の切替はまだ)。日の窓・運行への畳み方・リンクの組み立て・層 A の秒数は共有 crate `kintai-dtako`
(`dtako/`) をオンプレ版と同じものを使う (写さない)。検査・SQL の引数・応答は `logic/src/dtako_reads.rs`。

| 口 | 引数 (検査の順) | 読む SQL (`kintai_kosoku::sql`) | 窓 | 応答 |
|---|---|---|---|---|
| `day-events` | `driver` **必須** → `date` **必須** (`YYYY-MM-DD`、実在する日) | `EVENTS_SQL` | `[date 00:00:00, 翌日 00:00:00)` | `{driver_cd, date, operations, events}` (`kintai_dtako::day`) |
| `dtako/worktime` | `month` (`exact_month_range` が作れること) → `driver` 任意 (`driver=` の空は 400) | `driver` あり `EVENTS_SQL` / なし `ALL_EVENTS_SQL` | `exact_month_range` = `[月初, 翌月初)` | `kintai_dtako::worktime::Aggregate::to_json` (`month`・`driver`・`from`・`to`・`layer_a_states`・`days`・捨てた行の数) |

| 結果 | status | 本文 (平文) |
|---|---|---|
| `driver` が無い・不正 | 400 | `driver は乗務員CD (数字) で指定してください` |
| `date` が無い・不正 (day-events) | 400 | `date は YYYY-MM-DD で指定してください` |
| `month` が不正 (worktime) | 400 | `month は YYYY-MM で指定してください` |
| Query として読めない (同じ欄が 2 回等) | 400 | axum と同じ `Failed to deserialize query string: …` |
| `KINTAI_MARIADB` が無い・読めない | 503 | `MariaDB 接続設定が未設定` |
| 接続・クエリ・行の読み取りの失敗 | 502 | `MariaDB query failed: <段>:<種別>` |

- **day-events のリンク**: base URL は `[vars]` の `KINTAI_RYOHI_BASE_URL` (社内 nginx) と `KINTAI_DTAKO_BASE_URL` (dtako-admin)。
  オンプレ版の `[dtako_day_links]` と同じ意味で、**空ならその項目を `null`** にする。wrangler.toml は空のまま (社内ホスト名を
  repo に書かない)、本番は deploy 時に同名の repo variable を `--var` で渡す。deploy は値が `http://` か `https://` で始まらなければ
  (空も) 値を出さずに止まる (本番ではリンクを必ず出す前提)
- **worktime の行は 1 本ずつ畳む**: テキストプロトコルの行を 1 本ずつ JSON にして `Aggregate::add_row` に足す (全乗務員の 1 か月 =
  約 10 万行の JSON を並べない)。結果セットの生の行 (バイト列) は `kintai-mysql` が全部読んでから渡すので、そこは 4 本と同じ
- オンプレ版の worktime の同時実行の絞り (`DTAKO_WORKTIME_PERMITS` の 2) は持たない (Worker はリクエストごとに独立)

## `GET /api/kintai/{kosoku-daily,version,timecard/drivers,timecard/events}` (社内 MariaDB を直接読む 4 本)

オンプレ版 (root の `src/routes/kintai.rs` の `kosoku_daily`・`src/routes/kintai_version.rs`・`src/routes/kintai_timecard.rs` の
`drivers`・`window_events`) の口を移した。**path・応答 (JSON のキー・null 扱い・数値の型)・入力の検査の順・400 / 502 / 503 の条件は
元と同じ** (呼び手の切替はまだ)。応答を組む部分 (日別の整形・view・乗務員ごとの組み立て・etag の畳み方・ページの切り方・月の検査) は
共有 crate `kintai-kosoku` (`kosoku_daily`・`kintai_version`・`kintai_timecard`) をオンプレ版と同じものを使う (写さない)。
検査・SQL の引数・応答は `logic/src/kosoku_reads.rs`。

| 口 | 引数 (検査の順) | 1 接続で流す SQL (`kintai_kosoku::sql`) | 応答 |
|---|---|---|---|
| `kosoku-daily` | `month` (`is_valid_month`) → `driver` 任意 (`driver=` の空は 400)・`view` (`compare`/`timecard`、他は全項目) | `HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL` (窓 `month_range`) → 単一版 `EVENTS_SQL` / 全員版 `ALL_EVENTS_SQL` (窓の始端は遡り起点、`read_window`) → `FERRY_SQL` (窓は月ちょうど、全員版の `driver` は NULL) | 単一版 `{month, driver, days, …}` / 全員版 `{drivers, month}` (`kosoku_daily` の形) |
| `version` | `month` (`is_valid_month`) | `HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL` → `VERSION_SQL` (範囲は `version_ranges`) | `{month, etag}` + 同じ値の `ETag` ヘッダ |
| `timecard/drivers` | `month` (`is_valid_month`)・`after_driver_cd`・`max_drivers` (既定 50、1〜100 に丸める) | `TIMECARD_DRIVERS_SQL` (窓は月ちょうど) | `{month, drivers, next_after_driver_cd, elapsed_ms}` |
| `timecard/events` | `months` (カンマ区切り、`parse_months`) | `TIMECARD_WINDOW_SQL` (窓は最初の月初〜最後の翌月初) | `{months, drivers, events, elapsed_ms}` |

| 結果 | status | 本文 (平文) |
|---|---|---|
| `month` が不正 (kosoku-daily・version・timecard/drivers) | 400 | `month は YYYY-MM で指定してください` |
| `driver` が不正 (kosoku-daily) | 400 | `driver は乗務員CD (数字) で指定してください` |
| `months` が空・不正 (timecard/events) | 400 | `months は YYYY-MM をカンマ区切りで指定してください` / `month は YYYY-MM です: <最初の不正な月>` |
| Query として読めない (同じ欄が 2 回・`max_drivers=x` 等) | 400 | axum と同じ `Failed to deserialize query string: …` |
| `KINTAI_MARIADB` が無い・読めない (kosoku-daily・version) | 503 | `MariaDB 接続設定が未設定` |
| 接続・クエリ・行の読み取りの失敗 (kosoku-daily・version) | 502 | `MariaDB query failed: <段>:<種別>` |
| `KINTAI_MARIADB` が無い・接続・クエリ・行の失敗 (timecard の 2 本) | **502** | `kintai events read failed: MariaDB 接続設定が未設定` / `kintai events read failed: MariaDB query failed: <段>:<種別>` (元の `map_diff_err` は読みの失敗を全部 502 にする。資格情報が無いときも) |
| kosoku-daily の全員版の本文が 32MB を超えた | 503 | `kosoku-daily の応答が上限を超えました` (オンプレ版には無い。途中まで書いた応答を返さない) |

- **フェリーの失敗は 502 にしない** (元と同じ): `FERRY_SQL` が落ちた・行が読めないときは控除 0 で続ける (`ferry_or_empty`)
- **全員版のメモリ**: 1 か月で生行 約 6.4 万行・応答 約 2.2MB (オンプレ版で 11 秒)。結果セットの生の行 (バイト列) を乗務員ごとに束ね、
  1 人ぶんずつ JSON にして `kintai_kosoku::kosoku_daily::for_each_driver` に渡し、1 人ぶんずつ直列化して本文のバイト列へ書く
  (応答全体の JSON の木も、全員の行の JSON も持たない)。書き出しは共有 crate の `write_all_drivers` (全行を一度に渡す版。オンプレ版の
  応答と同じバイト列) と同じバイト列になることを `logic/tests/kosoku.rs` で固定している。結果セットの生の行は `kintai-mysql` が全部
  読んでから渡すので、そこは他の口と同じ
- **CPU**: `[limits] cpu_ms = 120000` (既定 30000。理由は wrangler.toml のコメント)。合否は `wrangler dev --remote` で最大の月・full・全員を測って決める
- **`KosokuParams` は既定値** (オンプレ版も設定ファイルに `[kosoku]` 節が無く既定値で動いている)
- **version の etag の版は Worker の版**: `worker/build.rs` が `KINTAI_WORKER_OUTPUT_SHA` を焼く。対象は workers/kintai の
  `kosoku`・`logic`・`mysql`・`worker` の src の全 .rs (応答を組むコードが worker の src にもあるため worker も入れる。dtako は
  kosoku-daily と無関係なので入れない)。畳み方は repo ルートの build.rs の `KINTAI_OUTPUT_SHA` と同じ関数 (`output_sha.rs` を両方が
  `include!`。どちらの glob にも入らない場所に置く)。必ず拾うファイルの一覧 (`REQUIRED`) から 1 つでも欠けたらビルドが落ちる。
  **オンプレ版の etag とは値が違う** (対象のファイルが違う) — 呼び手を一斉に切り替えるので、切替時に relay のキャッシュが 1 回捨てられるだけ
- `max_drivers` は元が `usize` (64 bit) なので `u64` で受ける (wasm32 の `usize` は 32 bit で、そのまま受けると元が通す値を 400 にする)

### 持たないもの

- オンプレ版の同時実行の絞り (`KOSOKU_DB_PERMITS` の 4。kosoku-daily・events 系の DB 読みと畳みをまとめて 4 本に絞る) は持たない
  (Worker はリクエストごとに独立)。社内 MariaDB の convoy を防ぐ役目なので、呼び手の切替の段で扱う
- `POST /api/kintai/timecard/diff`・`POST /api/kintai/timecard/window` (書き込み側・突き合わせ) は移していない

### 後の段に残したもの

- `timecard/diff` (POST)

## `GET /api/kintai/*` (Supabase を読む 6 本)

Cloud Run 版 (root の `src/routes/`) が Supabase を読むだけで答えていた口を移した。**応答 (JSON の形・キー・数値の型)・
入力の検査・400 / 502 / 503 の条件は元と同じ**にしてある (呼び手の relay は応答をそのまま返すため)。呼び手の切替はまだ。

| 口 | 引数 | 読む表 |
|---|---|---|
| `day-summaries` | `month` 必須・`driver` 任意 | `kintai.day_summaries` |
| `shift-overlaps` | `month` 必須 | `kintai.shifts` (自己結合) |
| `shift-days` | `month`・`driver` 必須 | `kintai.shifts` + `day_summaries` + `day_parts` |
| `change-log` | `from`・`to` 必須 (両端含む・400 日まで)・`driver` 任意 | `kintai.event_changes` |
| `wage-range` | `comp`・`from`・`to` 必須・`source` (既定 gcp)・任意の現行版 | `kintai.wage_snapshot` |
| `timecard/signatures` | `month`・`driver_cd` 必須 | `kintai.kintai_events` (日別の署名。下の書き込みの節) |

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
- 移していないもの: `stale-months` (`logic_version` を Worker で同じ値に作れない)・`unko-gaps` (alc の etags の掃引を含む)。
  window (`fold` 付きで畳み直す)・fold・recalc と一緒に後の段で移す

## Supabase に書く口 (`POST /api/kintai/timecard`・`POST /api/kintai/wage-snapshot`) と `GET /api/kintai/timecard/signatures`

Cloud Run 版 (root の `src/routes/kintai_timecard.rs` の `receive`・`signatures`、`src/routes/wage_snapshot.rs` の `put_wage_snapshot`) を
移した。**入力の検査・400 の条件と文言・応答の JSON・書く表の中身は元と同じ** (呼び手の切替はまだ)。

| 口 | 認可 | 入力の検査 (順) | 書く表 (1 transaction) | 応答 |
|---|---|---|---|---|
| `POST /api/kintai/timecard` | `X-Kintai-Write-Token` | 本文 (axum の `Json`: 415 / 413 / 400 / 422) → `month` (400) | 旧 events を読む → `kintai.event_changes` (変わった日の前後) → `kintai.kintai_events` の DELETE → INSERT (2000 行ごと) | `TimecardBatchResult` |
| `POST /api/kintai/wage-snapshot` | `X-Kintai-Write-Token` | 本文 (同上) → `validate_snapshot` (400) → `payroll_synced_at` (400) | 既存を読む → 同じなら書かない (`skipped_unchanged: true`) / 違えば `kintai.wage_snapshot` の DELETE → INSERT | `{saved, skipped_unchanged, …}` |
| `GET /api/kintai/timecard/signatures` | なし (読みの口) | Query (400) → `month` (400) → `driver_cd` (400) | (読むだけ) `STORED_SIGNATURES_SQL` | `{month, driver_cd, signatures}` |

| 結果 | status | 本文 (平文) |
|---|---|---|
| `X-Kintai-Write-Token` が無い・違う (書き込みの 2 本) | 403 | `書き込みの口には正しい X-Kintai-Write-Token が要ります` (固定。元には無い) |
| `KINTAI_WRITE_TOKEN` の binding が無い・読めない・空 (書き込みの 2 本) | 503 | `書き込みの認可の設定 (KINTAI_WRITE_TOKEN) が読めません` (元には無い) |
| 本文の Content-Type が JSON でない / 2MB 超 / JSON として読めない / 型に合わない | 415 / 413 / 400 / 422 | axum の `Json` と同じ文言 (`Expected request with …` / `Failed to buffer the request body: length limit exceeded` / `Failed to parse the request body as JSON: …` / `Failed to deserialize the JSON body into the target type: …`) |
| 入力不正 | 400 | 元と同じ文言 |
| `KINTAI_HYPERDRIVE` が無い | 503 | `[KINTAI_HYPERDRIVE] が無効です (書き先がありません)` (元の `[kintai_push] が無効です (書き先がありません)` に当たる。signatures も元が書き先の store を使っていたので同じ) |
| `KINTAI_TENANT_ID` が空・UUID でない・nil | 503 | `読み先のテナントが決まりません (KINTAI_TENANT_ID を設定してください)` |
| DB・接続の失敗 | 502 | `kintai push db failed: <kind>` (timecard・signatures) / `kintai.wage_snapshot access failed: <kind>` (wage-snapshot)。DB の message は出さない |

検査の順は **認可 → 入力 → binding → テナント → DB** (認可の前に本文を読んで 400 を返さない)。

- **テナントは `KINTAI_TENANT_ID` の設定 pin。`X-Tenant-ID` は読まない。** 元の timecard・signatures は `X-Tenant-ID` を読み、設定の pin と
  食い違えば 403・どちらも無ければ 400 だった。Worker は pin だけで決めるので、その 403 / 400 は無い (pin が無ければ 503)。
  呼び手 (relay) が名乗るテナントと pin が違っても pin に書く — 単一テナントの運用が前提
- 書き込みの部品は `pg/` (`kintai-pg`): kit の `PgClient::tenant_tx` の中で、共有 crate (`kintai_kosoku::kintai_push`・
  `kintai_logic::{change_log, wage_write}`) の SQL と Vec の束を `query_typed` / `execute_typed` に `Type::*_ARRAY` 付きで渡す。
  型なしの `ANY($5)` は SQL を書き換えず `Type::TEXT_ARRAY` を渡す。全 SQL の `$1` は pin の UUID (`tenant_id = $1`、
  `kosoku/tests/pg_write_snapshot.rs`・`logic/tests/pg_write_snapshot.rs` が全 SQL について確かめる)
- **`statement_timeout`**: 元は接続ごとに `SET statement_timeout = 300000`。Worker は transaction の頭で
  `set_config('statement_timeout', '300000', true)` (transaction の中だけ効く)。Hyperdrive 越しで効くかは CI では確かめられない
  (native の postgres では効く)。効かなくても Workers の CPU / 実行時間の上限の方が先に来る
- **元との一致の確かめ方**: `pg/tests/root_parity.rs` が root の sqlx の経路 (`apply_timecard_batch`・`put_wage_snapshot`・
  `stored_day_signatures`) と `kintai-pg` に同じ入力を与え、`kintai.kintai_events`・`event_changes`・`wage_snapshot` の中身と応答が
  一致することを実 postgres で確かめる (worker-kintai.yml の `pg-parity` job)。int8[]・timestamptz[]・text[]・jsonb[] (NULL 入り)・
  date[]・int2[]・int4[] (NULL 入り)・bool[] がここを通る。入力の検査は `pg/tests/axum_parity.rs` が root の axum の handler と
  同じ status・本文になることを確かめる (DB 不要)
- 元の `apply_timecard_batch` の `deduped` の数え方には、misplaced を含む日の後に日が続くと引き算が負になる不具合がある (#361)。
  Worker も同じ式 (共有 crate) なので一致する

### 元 (root の src/) と写し (logic/) の対応 — **撤去までは片方を直したらもう片方も直す**

純粋部分は root の crate から共有せず、`logic/` (`kintai-logic`) に写した (共有にすると root の crate を触り、
Cloud Run 版の勤怠の再 deploy と応答の比較が要るため)。**Cloud Run 版の勤怠はこの移行の最後の段 (呼び手を
切り替えた後) で撤去するので、二重管理はそれまでの期限付き。** それまでは、下の左を直したら右も、右を直したら左も直す。

| 元 (root) | 写し (`workers/kintai/logic/`) |
|---|---|
| `src/routes/kintai_day_summaries.rs` (SQL・Query・検査・行 → JSON) | `src/day_summaries.rs` |
| `src/routes/shift_overlaps.rs` | `src/shift_overlaps.rs` |
| `src/routes/shift_days.rs` | `src/shift_days.rs` |
| `src/routes/kintai.rs` の `parse_driver` | `src/common.rs` (`is_valid_month` は写しをやめ、共有 crate の `kintai_kosoku::window::is_valid_month` を再 export) |
| 4 つの `read_tenant_of` / `tenant_of` (`[kintai_events]` → `[kintai_push]` の pin) | `src/common.rs` の `tenant_of` 1 つ (`KINTAI_TENANT_ID`) |
| `month_date_bounds` (DATE) と `month_bounds` (JST の TIMESTAMPTZ。`kintai_push::jst_day_bounds`) | `src/common.rs` の `month_bounds` + `jst_midnight` 1 つずつ |
| 4 つの `store` (`[kintai_push]` が無効なら 503) | `src/common.rs` の `no_db` (`KINTAI_HYPERDRIVE` が無ければ 503) |
| `src/routes/kintai.rs` の `events`・`rest_diff`・`reading_dates`・`tail_gap_probe` (検査・窓・応答の JSON) | `src/mariadb_reads.rs` |
| `src/routes/kintai.rs` の `map_repo_err` (`MariaDB 接続設定が未設定` / `MariaDB query failed: `) | `src/common.rs` の `mariadb_unconfigured`・`mariadb_fail` |
| `src/kintai_repo.rs` の `row_to_json` (`EVENTS_SQL` の 7 列) | `src/mariadb_rows.rs` の `event_row` |
| `src/kintai_repo.rs` の `all_row_to_json` (`ALL_EVENTS_SQL` の 5 列。`unko_no`・`vehicle` はキーごと出さない) | `src/mariadb_rows.rs` の `all_event_row` |
| `src/kintai_repo.rs` の `rest_row_to_json` (`REST_EVENTS_SQL` の 6 列。`vehicle` 無し) | `src/mariadb_rows.rs` の `rest_row` |
| `src/kintai_repo.rs` の `reading_date_row_to_json` (`OPERATION_READING_DATES_SQL` の 6 列) | `src/mariadb_rows.rs` の `reading_date_row` |
| `src/kintai_http_repo.rs` の `today_jst` | `src/mariadb_reads.rs` の `jst_today` (Worker が `Date.now()` を渡す) |
| `src/routes/dtako_day.rs` の `day_events` (検査の順・`day_range`・`build_operations` に base URL を渡す) | `src/dtako_reads.rs` の `DtakoRead::DayEvents` |
| `src/routes/dtako_worktime.rs` の `worktime` (検査の順・SQL の選び方・`aggregate`) | `src/dtako_reads.rs` の `DtakoRead::Worktime` |
| `src/routes/kintai.rs` の `kosoku_daily`・`kosoku_daily_all` (検査の順・起点 → 窓 → 生イベント → フェリーの順・フェリーの失敗を控除 0 にする) | `src/kosoku_reads.rs` の `parse_kosoku_daily`・`DailyRequest` |
| `src/routes/kintai_version.rs` の `version` と `src/kintai_version.rs` の `fetch_markers` (起点 → `VERSION_SQL`) | `src/kosoku_reads.rs` の `parse_version`・`version_sql`・`version_respond` |
| `src/routes/kintai_timecard.rs` の `drivers`・`src/kintai_diff.rs` の `drivers_page` (読みの部分) | `src/kosoku_reads.rs` の `parse_timecard_drivers`・`DriversRequest` |
| `src/routes/kintai_timecard.rs` の `window_events` | `src/kosoku_reads.rs` の `parse_timecard_events`・`WindowRequest` |
| `src/routes/kintai_timecard.rs` の `map_diff_err` (読みの失敗は全部 502、`kintai events read failed: `) | `src/kosoku_reads.rs` の `timecard_fail` |
| `src/kintai_repo.rs` の `mariadb_month_head_anchors` の運行の行 `(i64, String)` (`HEAD_RUN_ENDS_SQL` の 2 列) | `src/mariadb_rows.rs` の `head_run_end_row` |
| `src/kintai_repo.rs` の `HeadPunchRow` → `head_punch` (`HEAD_PUNCHES_SQL` の 4 列) | `src/mariadb_rows.rs` の `head_punch_row` |
| `src/kintai_repo.rs` の `FerryRow` → `ferry_row_to_json` (`FERRY_SQL` の 3 列) | `src/mariadb_rows.rs` の `ferry_row` |
| `src/kintai_version.rs` の `MarkerRow` (`VERSION_SQL` の 3 列、全部 CHAR) | `src/mariadb_rows.rs` の `version_row` |
| `src/kintai_repo.rs` の `fetch_timecard_driver_cds_between` の `u64` (`TIMECARD_DRIVERS_SQL` の 1 列) | `src/mariadb_rows.rs` の `timecard_driver_row` |
| `src/kintai_repo.rs` の `fetch_timecard_window` の行 (`TIMECARD_WINDOW_SQL` の 7 列 = `row_to_json`) | `src/mariadb_rows.rs` の `event_row` (events と共有) |

**写しをやめて共有にしたもの** (Refs #322、Supabase への書き込みの部品の段): `change_log` (読みの SQL・期間の検査と、
書きの `build_changes`・SQL・bind の束)・`wage_range` (SQL・検査・詰め直し・応答)・`wage_snapshot` (丸ごと)・`wage_write`
(保存の SQL・検査・「前回と同じなら書かない」の判定・応答・bind の束) は `kintai-logic` が正本で、root (`src/change_log.rs`・
`src/routes/change_log.rs`・`src/wage_snapshot.rs`・`src/routes/wage_snapshot.rs`) は path 依存でそれを使う (sqlx の bind と
handler だけを持つ)。打刻と畳んだ 3 表の書き込みの純粋部分 (生行の写し・重複・署名・差分の計画・SQL・bind の束) は
`kintai-kosoku` の `kintai_push`・`kintai_fold` (root の `src/kintai_push.rs`・`src/kintai_fold.rs` は再 export と I/O だけ)。
移す前と SQL の文字列・bind の束が同じことは `kosoku/tests/pg_write_snapshot.rs`・`logic/tests/pg_write_snapshot.rs` が
基点 (a06a4d0) の sha256 で縛る。Worker の書き込みの口はこの部品の上に載る (上の節)。

day-events・worktime の純粋部分そのもの (日の窓・畳み方・リンク・層 A の秒数) は写しではなく、オンプレ版と Worker が同じ
共有 crate `kintai-dtako` を使う。上の 2 行は handler の部分 (検査の順・どの SQL を読むか) だけの対応。
kosoku-daily・version・timecard の 2 本も同じで、応答を組む部分は共有 crate `kintai-kosoku` (オンプレ版と同じもの)、
上の `kosoku_reads.rs` の行は handler の部分だけの対応。

テストは `logic/tests/` (DB 不要)。元の単体テストのうちテナント・月の境界・store の写しは `tests/common.rs` に畳み、
handler を叩いていたものは同じ入力を `parse` に通す形に書き直した。DB を要する元の `tests/*_pg_test.rs` は写していない。

## 到達面と認可

Service Binding 専用 (route・workers.dev・preview 無し)。資格情報は Secrets Store の binding で読み、呼び手の cookie・Authorization は受け取らない。

- **読みの口 (GET 全部。`timecard/signatures` を含む) は認可なし** (ユーザー決定 2026-10-10、一番星と同じ)。関門は呼び手の側
  (relay の共有 secret、kyuyo-mcp の OAuth)
- **書き込みの口 (`POST /api/kintai/timecard`・`POST /api/kintai/wage-snapshot`) は Worker が共有 secret を照合する** (ユーザー決定
  2026-10-10)。読みのために binding を持つ呼び手が POST を転送しても書けないようにするため。ヘッダー `X-Kintai-Write-Token` を
  Secrets Store の `KINTAI_WRITE_TOKEN` と照合し (両方を sha256 にして 32 バイトを定数時間で比べる。`logic/src/write_auth.rs`)、
  無い・違う = 403 (固定文言)、binding が無い・読めない・空 = 503。値は repo に書かない (GCP の Secret Manager が正本)

## binding (`worker/wrangler.toml`)

- `KINTAI_MARIADB_VPC` — Workers VPC の VPC Service (TCP 3306)。宛先 host:port は Service 側で固定。`service_id` は VPC Service `ichibanboshi-kintai-mariadb` の id
- `KINTAI_MARIADB` — Secrets Store の secret。JSON `{"user":…,"password":…,"database":…}` (どれも空でない文字列)。未投入なら `/probe` と MariaDB の口は 503
  (timecard の 2 本だけは元と同じく 502)
- `KINTAI_WRITE_TOKEN` — Secrets Store の secret (書き込みの口の共有 secret。store は `KINTAI_MARIADB` と同じ)。無ければ書き込みの 2 本は 503
- `KINTAI_HYPERDRIVE` — Supabase への Hyperdrive (分割 worker と共有の実行用ロールの設定)。**トップレベルにだけ置く**。無ければ Supabase の口 (読み 6 本・書き 2 本) は 503
- `KINTAI_RYOHI_BASE_URL`・`KINTAI_DTAKO_BASE_URL` (`[vars]`) — day-events のリンクの base URL。空 = そのリンクを省く。本番は deploy 時に同名の repo variable を `--var` で渡す (社内ホスト名を repo に書かない)
- `KINTAI_TENANT_ID` (`[vars]`) — 読み先のテナントの UUID。本番は deploy 時に repo variable `KINTAI_EVENTS_TENANT_ID` (Cloud Run 版と同じ) を `--var` で渡す (git 履歴に UUID を焼かない)。ここは空のままで、空の間は `GET /api/kintai/*` は 503
- `CF_VERSION_METADATA` — 版の元
- `[limits] cpu_ms = 120000` — kosoku-daily の全員版のため (上記)
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし / `LOCAL_*` の var なし /
  hyperdrive・secrets_store_secrets はトップレベル以外に無い / 書き込みの口 (`worker/src` の `Route::Write`) があるなら
  `KINTAI_WRITE_TOKEN` の binding がある。`scripts/check-exposure.sh` が CI で検査し、`check-exposure-test.sh` が陰性対照

## 構成

- `mysql/` (`kintai-mysql`): I/O を持たない純粋なコーデック。`packet.rs` (枠・length-encoded の値) / `handshake.rs` (Initial Handshake v10・
  HandshakeResponse41・mysql_native_password・Auth Switch) / `response.rs` (OK / ERR / EOF・COM_QUERY・テキストの結果セット) /
  `bind.rs` (名前付き引数を整数・日時・NULL のリテラルに展開) / `retry.rs` (接続のやり直しの判断)。
  CLIENT_DEPRECATE_EOF は立てない (結果セットは EOF で区切られる形に固定)。テストは `mysql/tests/codec.rs`、100% 行カバレッジ gate は `coverage_100.toml`
- `dtako/` (`kintai-dtako`): day-events と dtako/worktime の純粋部分 (`day.rs`・`worktime.rs`)。**repo ルートの package も path 依存で使う共有 crate**
  (root の 2 つの route は handler だけ)。依存は serde_json・chrono・kintai-kosoku だけ。root の `build.rs` の勤怠の版 (`KINTAI_OUTPUT_SHA`) の glob の外
  (元の route と同じ分類。`kintai-kosoku` に入れると版が変わり、`kintai-logic` に入れると postgres-types 等が root に入るので別 crate)。100% 行カバレッジ gate は `coverage_100.toml`
- `logic/` (`kintai-logic`): Supabase を読む 6 本と社内 MariaDB を読む 10 本の口の純粋部分 (上の対応表)、Supabase への書き込みの
  部品 (変更履歴 `change_log`・賃金スナップショット `wage_write`・timecard の検査 `timecard_write`・認可 `write_auth`、本文の読み方
  `common::parse_json`)。**repo ルートの package も path 依存で使う** (書き込みの部品と
  `change_log`・`wage_range`・`wage_snapshot`。root の build.rs の勤怠の版の glob の外)。100% 行カバレッジ gate は `coverage_100.toml`
- `worker/` (`kintai-worker`): `lib.rs` (fetch・段ごとの打ち切り時間・MariaDB の 10 本の往復。1 接続を開く `open` とクエリ 1 本の `query`) /
  `build.rs` (version の etag の版 `KINTAI_WORKER_OUTPUT_SHA`) / `conn.rs` (socket とコーデックの間) / `probe.rs` (経路・段・応答・資格情報の検証) /
  `reads.rs` (Hyperdrive への接続・テナント・`tenant_tx` の中の `query_typed`・行の詰め直し) / `writes.rs` (書き込みの 2 本: 認可 → 本文 → `kintai-pg`) /
  `transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern)。`tcp.rs`・`transport.rs` は `workers/ichiban` から写した (共有 crate に畳むのは本実装の段で)

- `pg/` (`kintai-pg`): Supabase への書き込み (`stored_day_signatures`・`replace_window`・`apply_timecard_batch`・`put_wage_snapshot`)。
  kit の `tenant_tx` の中の `query_typed` / `execute_typed` だけで、native でも動く (Worker は Hyperdrive の接続を、テストは native の
  tokio-postgres の接続を渡す)。実 DB が要るので 100% gate には入れず、`pg-parity` job が root と突き合わせる。dev-dependency に
  repo ルートの package (`rust-ichibanboshi`) を持つ (比べる相手。wasm のビルドには入らない)

- `output_sha.rs`: 版の畳み方 (`fold_output_sha`)。repo ルートの build.rs と `worker/build.rs` が `include!` する (crate の src の外 = どちらの版の glob にも入らない)

独立した workspace (repo ルートの package・`workers/ichiban`・`workers/kyuyo` からは参照されない)。
ただし repo ルートの package は `kintai-logic`・`kintai-kosoku`・`kintai-dtako` に path 依存するので、**これらの crate の依存を変えたら root の `Cargo.lock` も同じ PR で更新する** (`cargo metadata --format-version 1`。忘れると main の GCP image が `--locked` で落ちる。`worker-kintai.yml` が PR で検査する)。

## ローカル検証

`cargo test -p kintai-mysql -p kintai-logic -p kintai-kosoku -p kintai-dtako` (DB 不要)。`kintai-pg` は
`KINTAI_TEST_DATABASE_URL` (使い捨ての postgres) を渡して `cargo test -p kintai-pg` (無ければ `root_parity` は失敗する)。Worker は `cargo build --target wasm32-unknown-unknown` と clippy まで。
ローカルで VPC や Secrets Store を迂回する var は持たないので、実接続は VPC Service と `KINTAI_MARIADB` を用意してから
`wrangler dev --remote` で `POST /probe` と MariaDB の 10 本 (day-events のリンクを出すなら `--var "KINTAI_RYOHI_BASE_URL:…"` 等)。Supabase の 5 本も同じく `wrangler dev --remote` (Hyperdrive の経路は CI では通せない)。
`--var "KINTAI_TENANT_ID:<UUID>"` を渡すと Supabase の口が 503 ではなく答える。書き込みの 2 本は `KINTAI_WRITE_TOKEN` の値を
`X-Kintai-Write-Token` に載せて叩く (Supabase に書くので、書いてよい月・乗務員で)。

## 本番 deploy

タグ `worker-kintai-v*` の push で `.github/workflows/worker-kintai.yml` の deploy job が `wrangler deploy --tag <タグ> --message <git SHA>`
を打つ (org の secret `CLOUDFLARE_API_TOKEN`)。main への merge では本番に出ない。
