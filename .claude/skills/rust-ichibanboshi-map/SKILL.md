---
name: rust-ichibanboshi-map
generated-from: rust-ichibanboshi:a866c377c770e9f305c21db5b818c9839887d86d
paths: [src/]
description: rust-ichibanboshi (一番星 SQL Server CAPE#01 の売上データを tiberius で読み REST API 提供する Rust/Axum サービス) の構造ナビゲーション。sales 集計エンドポイント / tiberius+bb8 接続 / 売上集計ロジック (税抜カラム・請求K) / musl deploy + Cloudflare Tunnel の gotcha を 1 枚にまとめる。トリガー:「rust-ichibanboshi」「一番星」「CAPE#01」「tiberius」「SQL Server 売上」「月計テーブル」「請求K」「税抜金額」「傭車」「Cloudflare Tunnel」「CF Access Service Token」等。
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
| `src/main.rs` | clap entrypoint。`--console` で console、Windows では `service::run_service`、それ以外は console |
| `src/lib.rs` | crate ルート (`rust_ichibanboshi`) — 各 module re-export |
| `src/server.rs` | `run()` — pool 生成 + Axum Router 組み立て + graceful shutdown |
| `src/config.rs` | `Config` / `AppArgs` (clap) / TOML 読み込み / `addr()` |
| `src/db.rs` | `create_pool` — bb8-tiberius pool (named instance / NotSupported 暗号化) |
| `src/repo.rs` | `TiberiusRepo` / `DynRepo` trait — SQL クエリ本体 (DB 層) |
| `src/auth.rs` | `JwtSecret` / JWT 検証 |
| `src/service.rs` | `windows-service` 統合 (cfg(windows) のみ) |
| `src/routes/health.rs` | `/health` |
| `src/routes/sales.rs` | `/api/sales/*` 売上集計ハンドラ群 (下記) |
| `src/routes/schema.rs` | `/api/schema/*` tables/columns/sample (デバッグ用 schema 探索) |
| `src/routes/surcharge.rs` | `/api/surcharge/base` 燃料サーチャージ基礎データ (請求のみ行 → 県/車種/請求日 展開、#12) |
| `src/routes/kintai.rs` | `/api/kintai/daily` (CakePHP 中継) / `/api/kintai/events` (MariaDB 直読み) / `/api/kintai/kosoku-daily` (日別サマリ、`driver` 省略で全乗務員) (#99 / #116 / #118 / #125、下記) |
| `src/kintai_repo.rs` | 勤怠の生イベント読み取り — 社内 MariaDB (mysql_async) の `UNION ALL` 1 本 (#116)。1 名分の `EVENTS_SQL` と全乗務員の `ALL_EVENTS_SQL` (#125) |
| `src/kintai_version.rs` + `src/routes/kintai_version.rs` | `/api/kintai/version` 月別バージョン (ETag) — `daily`/`kosoku-daily` の全ソーステーブル (11 個) の COUNT+CRC32 マーカーを `VERSION_SQL` 1 本で取り sha256 に畳む (#184、下記) |
| `src/kosoku.rs` | 拘束時間の日別サマリ**純粋ロジック** (イベント列 → 日別、乗務員ごとの分割)。DB も HTTP も触らない。**coverage 100% 対象** (#118) |
| `src/kosoku_paper.rs` | 紙のタイムカード表 (社内 CakePHP) の日別拘束の**再現** — 突合用に `paper_drift_by_date` (cause `rounding`) / `paper_outside_by_date` (紙だけが数える分: 勤務外・運行行欠けの対二重・イベント重複の二重、cause `paper-outside`) / `ours_outside_by_date` (こちらだけが数える分: 紙の材料に無い拘束、cause `ours-outside`) を `kosoku-daily?view=compare` に載せる (nuxt-dtako-admin#501/#546、#182)。**coverage 100% 対象** |
| `src/routes/kyuyo.rs` | `/api/kyuyo/*` 給与大臣 DB の読み出し (下記) |
| `src/kyuyo/logic.rs` | 給与の純粋ロジック (項目マッピング・行組み立て)。**coverage 100% 対象** |
| `src/kyuyo/repo.rs` | 給与大臣 SQL Server への SELECT (別 pool・別 trait)。DB 層 |
| `src/kyuyo/introspect.rs` | `/api/kyuyo/*` の in-service gate (auth-worker introspect + email allowlist) |

## entrypoint / Axum router (`src/server.rs::run`)

- `/health`
- `/api/sales/*`: `monthly` / `by-department` / `by-customer` / `yoy` / `daily` /
  `customer-trend` / `customer-yoy` / `customer-yoy-by-dept` / `departments` / `customer-detail`
- `/api/surcharge/base`: 燃料サーチャージ基礎データ。`運転日報明細` の請求のみ行 (`請求K`='1'、
  `kind=transport`/`all` で切替) を 得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定)
  に展開。県正規化 (`地域N` → 都道府県) は `normalize_prefecture` 純粋関数。残マスタ (燃費/距離/軽油価格/
  対象得意先) は scope 外 (#12 残課題)。
- `/api/kintai/daily?month=YYYY-MM`: 社内 CakePHP (`yhonda-ohishi/nginx`) のタイムカード日別
  データを Cloudflare Worker (nuxt-dtako-admin の dtako-scraper-relay) へ**中継するだけ**。
  詳細は下記「勤怠の中継」節。
- `/api/kintai/events?month=YYYY-MM&driver=N`: **打刻と運行イベントの生時系列**
  (#114 / #116、拘束時間の打刻基準化 Phase 1)。ここだけ CakePHP を経由せず
  **社内 MariaDB を直読み**する。`driver` 必須・キャッシュ無し・未設定は 503。
- `/api/kintai/kosoku-daily?month=YYYY-MM[&driver=N]`: **打刻基準の日別サマリ** (#118、Phase 2)。
  `events` の生行を `src/kosoku.rs` の純粋ロジックで畳む。**金額は含めない**。
  **`driver` 省略で全乗務員** (#125、`{month, drivers:[{driver, days}]}`)。下記「日別サマリ」節。
- `/api/kintai/version?month=YYYY-MM`: **月別バージョン (ETag)** (#184)。relay の条件付き
  再検証キャッシュ用。下記「月別バージョン」節。
- `/api/schema/*`: `tables` / `columns` / `sample`
- layer: CORS (allowed_origins) + TraceLayer + `Extension(DynRepo)` + `Extension(JwtSecret)`
- repo は `Arc<TiberiusRepo>` を `DynRepo` として Extension 注入 → test は MockRepo に差し替え可能

## gotcha (CLAUDE.md / Cargo.toml 由来)

- **売上集計は税抜カラムで**: 月計テーブルと一致させるには `税抜金額+税抜割増+税抜実費-値引`
  (傭車は `税抜傭車*`)。`金額` カラムは実費の内税/外税で消費税の含み方が変わるため使わない。
  日付は `売上年月日`、請求区分 `請求K IN ('0','2')`、自車/傭車判定は `傭車先C = '000000'`。
- **月計テーブルは締め時点スナップショット** — 締め後の遡り修正は再集計されないため日報明細と差が出る。
- **SQL Server 接続**: host `172.18.21.102`、名前付きインスタンス `softec`
  (`using_named_connection()` 必須)、DB `CAPE#01`、`EncryptionLevel::NotSupported`、文字コード Shift_JIS。
- **Windows / Linux 両対応の罠**: Cargo.toml description は "Windows Service"、`wix/main.wxs` +
  `windows-service` dep (cfg(windows)) があるが、**実運用は Linux systemd** (CLAUDE.md / deploy.sh)。
  `--console` 無し起動は OS により分岐 (Windows=service, それ以外=console)。
- **CF Access Service Token 必須**: Tunnel 経由のリクエストは `CF-Access-Client-Id/Secret` 無しだと 403。

## CI / deploy から見た立ち位置

- **`./deploy.sh`**: `cargo build --release --target x86_64-unknown-linux-musl` (GLIBC 不一致回避の
  static link) → scp で `/tmp` → `mv` (atomic) で `/opt/ichibanboshi/`。systemd `ichibanboshi-watcher.path`
  (PathModified) がバイナリ変更を検知して自動 restart。実行先 `ohishi-data.tailea945d.ts.net`。
- `.github/workflows/`: `ci.yml` / `release.yml` / `tag-release.yml`。
- `coverage_100.toml`: `auth.rs` / `config.rs` / `routes/{health,sales,schema}.rs` を 100% 維持
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
  ├─ SQLite (/opt/ichibanboshi/data.db): uriage_person_daily / recalc_jobs / verify_jobs
  └─ disk (/opt/ichibanboshi/raw/{month}/eigyosho-{id}.ndjson.gz): R2 投入用 raw NDJSON.gz
    ↓ R2 sync (nuxt の /api/uriage/r2-sync = rust から raw を fetch して R2 へ put)
[Cloudflare R2 bucket]
  └─ uriage/{month}/eigyosho-{id}.ndjson.gz
    ↓ Worker から read
[ブラウザ UI (nuxt /admin/*)]
```

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

## 勤怠 (タイムカード) の中継 — `/api/kintai/daily` (Refs #99, ohishi-exp/nuxt-dtako-admin#424)

nuxt-dtako-admin の給与比較を**本社事務員等 (デジタコに乗らない人)** へ広げるため、勤務実績を
社内 CakePHP (`yhonda-ohishi/nginx` の `GET /time-card/daily-json?month=`) から供給する経路。
CakePHP は LAN 内にしか居ないので、**同一ホストで動く本サービスが橋渡し**する。

- **`[cakephp] base_url` は `http://127.0.0.1:120`** (loopback の plain HTTP)。
  `https://ohishi-dev.ohishi.local` は使っていない — **DNS も TLS も経路に入らない 1 hop**
  (`ohishi-dev.ohishi.local` は同ホスト上でも名前解決できない。443/TLS へ移すなら別途手当てが要る)
- **中継だけ。解釈も変換もしない。** 行は `serde_json::Value` のまま素通しし、トップレベルの
  未知フィールドも `#[serde(flatten)]` で拾って復元する — 上流が項目を足しても型を触らずに済む
- **ID 変換・突合はしない** — CakePHP の `drivers.id` は乗務員CD (= 一番星 `社員ﾏｽﾀ.社員C`) と
  同一番号体系で、本社事務員も含まれる。受け手がそのまま引き当てる
- **認可は CF Access Service Token (edge)** で `/employees` と同じ扱い。前例のコピーではなく
  **データの ACL で選んでいる**: 応答は識別情報と時刻だけで金額を含まず、消費者が
  Cloudflare Worker の DO なのでブラウザ JWT を持てないため。**金額を足すなら `/kyuyo/*` と同じ
  in-service gate へ移すこと**
- `month` は `YYYY-MM` 必須 (上流の `HolidaysTrait` が月単位 API のため)。不正は 400、
  `base_url` 未設定は 503、上流の非 2xx / 非 JSON / 到達不能は 502
- **上流が非 JSON (ログイン画面の HTML) を返したら 502 `parse failed`** — CakePHP 側の
  `AppController::addUnauthenticatedActions` に action 名を足し忘れた時にこうなる (yhonda-ohishi/nginx#773)
- テストは `tests/kintai_test.rs` (wiremock で CakePHP を stub)。`src/routes/kintai.rs` は
  coverage_100 登録済み

### 生イベントの直読み — `/api/kintai/events` (Refs #114 → #116)

拘束時間管理表の残業を**タイムカードの打刻を正として**計算し直すことになり (設計:
claude.ai/code/artifact/db46b3b2)、規則を決める前に実データで各パターン (同日 2 運行・
打刻と運行のズレ・細切れ休憩・8h 未満の休息) が何件あるかを数えるための読み出し口。

- **ここだけ CakePHP 中継ではなく社内 MariaDB の直読み** (`src/kintai_repo.rs`)。
  返すのが解釈しない生行なので ORM を挟む意味が薄く、挟めば「CakePHP の解釈」と
  「本サービスの中継」の 2 段になる。上流に相当エンドポイントも無かった (#116)
- **`/api/kintai/daily` は CakePHP 中継のまま** — あちらは休日判定・セッション組み立てと
  いう解釈が上流にあり、直読みで再現すると二重実装になる。混同しないこと
- 接続は `[mariadb]` (既定 `127.0.0.1:3306`、user `kintai_reader`)。同一ホストの docker
  `db` コンテナ (172.18.21.35 をマスタにしたレプリカ) へ loopback 1 hop。**password /
  database はデプロイ先の toml でのみ設定**し、未設定なら 503 fail-closed
  (`DisabledKintaiEventsRepo` — 空配列を返して「0 件」に見せない)
- **乗務員は `対象乗務員CD` で引く (`乗務員CD1` ではない)。** 2 名乗務・交替の運行では
  運行まるごとが `乗務員CD1` = 別の乗務員のまま記録されるため、`乗務員CD1` で引くと
  他人の運行を拾い、引かれた側は取りこぼす (実測: 2026-06 の 1740 は 休息+休憩が
  `乗務員CD1` 114 件 / `対象乗務員CD` 83 件)。速度も `対象乗務員CD` は
  `idx_driver_datetime` の covering index が効いて 0.2 秒 → 0.4 ミリ秒 (#126)
- 読むのは 5 テーブル: `time_card_dstate` (打刻 30/31) / `time_card_dtako` (運行の確定
  イベント 10/11/20、`unko_no` 付き) / `time_card_dtako_state` (名称マスタ) /
  `dtako_events` (デジタコ生イベント、区間持ち) / `dtako_cars` (車番)。`UNION ALL` して
  `datetime` 昇順。**SELECT のみを GRANT した専用アカウント**で読む
- 範囲は `[月初, 翌月+1日)` (`month_range`) — 月で切ると日跨ぎ勤務の終業が消える。
  上流 `daily-json` の `queryEnd` と同じ考え方
- 日時は SQL の `DATE_FORMAT` で文字列にして取り出す — driver の時刻型と timezone 解釈を
  経路に持ち込まない
- **`/events` の `driver` は必須** (省略・非数字・負値・桁溢れは 400)。生イベントは日別サマリより
  1 桁多く、全乗務員を返す用途がここには無い (`kosoku-daily` は #125 で省略可になったが、
  **こちらは必須のまま**)
- **キャッシュを持たない**。調査用途で頻度が低く常に最新の打刻が要るため
- 認可は `daily` と同じ CF Access Service Token (edge)。応答は識別情報と時刻・車番だけで
  **金額を含まない**。金額を足すなら `/kyuyo/*` の in-service gate へ移すこと
- テストは `tests/kintai_events_test.rs` — `KintaiEventsApi` の mock を挿して **DB 無し**で
  回す。打刻のみ / 運行のみ / 同日 2 運行 / 日跨ぎ の 4 ケースが「解釈されずそのまま通る」
  ことを固定する (解釈は Phase 2 の `kosoku-daily` 側の担当)

### 打刻基準の日別サマリ — `/api/kintai/kosoku-daily` (Refs #118)

`events` の生行を日別に畳む。**規則は #118 で 4 点とも確定済み** (2026-07-27)。畳み込みは
`src/kosoku.rs` の純粋関数 `daily_summary(rows, month, params)` にあり、DB も HTTP も
触らないので実データ無しでテストできる。

- **就業時間**: 打刻 (`始業`/`終業`) があればそれを使い、無ければ**休息イベント**
  (休息の終了 = 始業、休息の開始 = 終業)。**運行では切らない** — 実測で運行の継ぎ目は
  4〜112 分 (中央 8 分) しかなく勤務の切れ目ではない。打刻と休息は補完関係
  (日跨ぎの長距離は打刻が無く休息がある / 日帰りは打刻があり休息が無い)
- **時間区分**: 所定 7.5h (450 分) / 7.5〜8h は法定内残業 (割増 1.0) / 8h 超が法定時間外 (1.25)
- **法定休日 = 日曜**。祝日は割増に使わない (所定休日で固有の割増が無い)。祝日は表示用に
  別 API へ (#119)。賃金側も日曜のみで判定していることを 685 乗務員月で実測確認済み
- **深夜 (22:00〜05:00) は 所定内/時間外 × 平日/法定休日 の 4 区分**。法定外休日は持たない。
  法定内残業の深夜は所定内と同じ枠 (どちらも基礎 1.0)。**深夜と時間外深夜は排他**
- **休憩は閾値 (既定 10 分) 以上のイベントのみ**。拘束からは外さない (実働 = 拘束 − 休憩)
- **拘束 24 時間超は打ち切らない** (Refs #152)。`over_24h` を立てるだけで値は実測のまま。
  帰宅日の混入は `merge_shifts` (打刻と重なる休息由来を捨てる) と `split_by_run_gaps`
  (運行終了→次の運行開始が 8 時間以上空けば割る) で既に潰れており、残る 24h 超は本物。
  実測 1674 / 2026-04-07 01:08→04-08 16:22 は 39.2 時間で、中の休憩は最長 152 分・
  運行の空きは 36 分・打刻なし。**打ち切ると改善基準違反がその分だけ小さく見える**
- **秒は紙と同じ「区分ごとの切り捨て」で落とす** (既定 `paper_per_segment`、`RestraintRounding`、
  Refs #182)。勤務の境界を秒のまま持ち回り、拘束を日別へ落とす瞬間に運行の境界で区分に割って
  紙の流儀で丸める (`paper_minutes_by_date`): 打刻↔運行境界の対 = 経過秒切り捨て、運行の中 =
  端点床、日跨ぎは深夜 0 時割り (前半は跨ぐイベントの開始秒で決まる)。実働・休憩・診断は
  従来どおり分の格子 (summarize の walk) で出すため、**`拘束 = 実働 + 休憩` は ±1 分ずれ得る**。
  `kosoku.restraint_rounding` で `truncate_elapsed` (勤務単位の経過切り捨て = #182 の従来方式。
  打刻の対だけなら紙と一致: 始業 09:00:30 / 終業 17:00:20 → 479 分) や `floor_endpoints`
  (両端床、同 480 分) に TOML だけで戻せる。区分丸めにしたことで紙とのずれ (cause `rounding`)
  は日別 ±1 分以内 = 突合の許容誤差内に収まる (Refs ohishi-exp/nuxt-dtako-admin#501)
- **運行の中かどうかは `in_run_at`** — 直前の運行境界が `運行開始` なら中。ただし**運行が月を
  またいで続く**と当月に頭が無く、境界が 1 つも見つからない (実測 1041 / 2026-03: 2/28 に
  始まった運行が 03-07 00:02 の `運行終了` まで続く)。そこを「外」に倒すと丸めが端点床から
  経過床へ入れ替わり、**区分 1 つあたり 1 分ずつ紙より小さくなる** — 03-01〜03-06 が全日
  cause `rounding` として残っていた原因。頭が見えないときは**後ろに最初に来る境界**で決める
  (`運行終了` なら手前は運行の中)
- **紙だけが数える勤務外の分は `paper_outside_by_date`** (view=compare、`paper_outside_by_date`
  #182 フォローアップ)。紙は打刻に縛られずイベントを数え続けるので、終業後も続くイベント
  (状態切り忘れの夜通し「積み」・構内ミニ運行) が勤務の外に残ると紙だけ大きくなる — relay の
  cause `paper-outside` (nuxt-dtako-admin#546) の実額。「外」= 勤務 span + run_head
  (`shift_cover`)。休息の終わりと**同時刻の運行終了 (点) は勤務を残す証拠にしない**
  (`has_operation_strictly_after_start`) — 点だけを証拠に何も無い休日が勤務 41 時間になっていた
  (実測 1418 / 2026-01-22)
- **運行に出ていない勤務は昼休憩 (12:00-13:00) を引く** (ユーザー決定 2026-07-28、`has_operation`)。
  休憩イベントはデジタコにしか残らないので、事務・作業・整備や乗務員の構内作業日は休憩が
  1 分も出ず 実働 = 拘束 になっていた (実測 2026-04: 事務 1065 は 23 日すべて休憩 0 で
  拘束 = 実働 293.8h、乗務 1021 は 26 日で休憩 63.1h)。**運行の証拠は `timecard` 以外の
  イベント**で、`休息` は除く (勤務の外側の境界であって運行ではない)。イベント由来の休憩と
  昼休憩は**排他** — 休憩イベントが 1 件でもあればその勤務は運行扱いになる
- **昼の窓に 1 分も掛からない勤務 (夜勤・夕方だけ) は、拘束 6 時間超なら 1 時間を勤務の
  まん中に置く** (ユーザー指示 2026-07-28、`off_hours_break`)。昼休憩は時計の窓なので夜勤には
  当たらず、作業員 1196 の 23:45 → 08:00 の夜勤 18 日が全日 休憩 0 のまま残っていた。
  6 時間以下を除くのは 19:00 → 00:00 の 5 時間勤務 (事務 1706 / 1707) に引かないため
  (労基法 34 条の最初の閾値に合わせる)。**まん中に置くのは深夜の内訳を偏らせないため** —
  端に寄せると 22:00-05:00 のどこを削るかで night / overtime_night が動く
- **24 時間超の勤務は中の休息で切り直す** (#133)。長距離は打刻が運行 1 本 (数日) を
  まるごと挟むため、打刻の対だけで組むと 6 日間 1 勤務になる (実測: 乗務員 1021 の
  2026-04-03 06:33 → 04-09 15:16)。打刻優先 (#118) は維持し、**24 時間を超えた勤務だけ**
  休息で割る (通常の勤務は休息があっても割らない)。社内 CakePHP も運行単位に区切ってから
  暦日へ配っており、打刻と運行イベントを同列に扱っていない
- **それでも 24 時間を超える勤務は最後の `運行終了` で終わらせる** (#135)。休息由来の勤務は
  「次の休息の開始」で終わるが、**運行が終わって帰宅している間は休息イベントが出ない**ので
  勤務が終わらない (実測: 乗務員 1021 は 2026-04-28 08:14 に運行終了して帰宅したのに、次の
  運行 5/1 まで休息が無く 24 時間打ち切りになっていた)。切るのは**最後の**運行終了だけで、
  途中の継ぎ目では切らない (#123)
- **それでも 24 時間を超える区間は 24 時間で打ち切り** `over_24h` を立てる。改善基準告示に
  照らして違反で、正確に積む意味がないため (実測の最長は 38.1 時間)
- **月境界の罠**: 勤務は**始業日**で当月に振り分ける。月初の勤務は前月末の休息が要るので、
  月初の勤務の始業は前月末に始まって月初に終わる休息の**終わり**で決まるので、
  `EVENTS_SQL` は `dtako_events` を **2 ブランチ**で読む — 「期間内に始まる区間」と
  「期間内に終わる区間 (開始は期間より前)」。拾い漏らすと**毎月 1 日目が静かに欠ける**。
  **`COALESCE(終了日時, 開始日時) >= :from` で 1 本にまとめてはいけない** — 関数適用で
  索引が効かず `type=ALL` の全表走査 (427 万行) になり、実機で 0.2 秒が 4 分超になった
  (#121 → #122 で revert)。`開始日時` と `終了日時` はそれぞれ索引があり、条件を分ければ
  各 0.2 秒で済む (#124)
- パラメータは `[kosoku]` (`break_threshold_minutes` / `prescribed_minutes` / `legal_minutes`)。
  就業規則が変わったら再ビルドせず toml で追随する
- **`punches` に勤務を構成した打刻をそのまま添える** (#128)。`start`/`end` は勤務としての
  解釈 (分に丸め・24 時間で打ち切り) が入るが、`punches` は生の打刻 (秒つき)。
  **打ち切り前の区間から拾う** — 24 時間で切ると切った先の終業打刻が落ちる
  (実測: 乗務員 1194 の 2026-04-01 始業 → 04-03 16:47 終業)。休息由来の勤務は空。
  社内タイムカード表 (`TimeCardController::createPdf`) は**打刻を日ごとに並べただけ**で
  勤務という単位を持たないので、同じ表を作る側にはこの生の時刻が要る
- **`parts` に暦日按分の内訳を添える** (#130)。行の各分数は勤務を**始業日へ丸ごと寄せた**
  値だが、現行の拘束時間管理表 (社内 CakePHP) は拘束を**暦日へ配る**。同じ基準で読みたい
  消費者は `parts` を足し合わせる (月合計は寄せ方によらず一致、日別と月境界だけ変わる)。
  **1 日で終わる勤務では空** — 内訳が行そのものになるため。法定休日の判定は勤務単位
  (始業日) なので、月曜へこぼれた分も `legal_holiday_minutes` のまま
- 認可は `events` と同じ CF Access Service Token (edge) — **応答に金額を含めない**ため
- テストは `src/kosoku.rs` の unit test (規則の網羅) と `tests/kosoku_daily_test.rs`
  (route の配線・検証・失敗の写し方) の 2 段

#### `driver` 省略 = 全乗務員 (#125)

画面 (nuxt-dtako-admin のタイムカード表) は全乗務員ぶんが要る。1 名ずつ叩くと
**96 名で約 3 秒**かかるので 1 リクエストにまとめた (**実測 0.25 秒**)。

| 呼び方 | 応答 |
|---|---|
| `driver=1051` | `{month, driver, days}` — **既存の形は変えない** |
| 省略 | `{month, drivers: [{driver, days}]}` (乗務員CD 昇順) |
| `driver=` (空) | **400**。省略ではなく不正として扱う — front の入れ忘れで約 1 MB を返さない |

- **一括の SQL は `ALL_EVENTS_SQL`** (`src/kintai_repo.rs`)。`EVENTS_SQL` から乗務員の
  絞り込みを外し、**`運行NO` / `車輌名` と `dtako_cars` の JOIN を落とした**もの。
  ここが速度の支配項だった — 内訳計測で スキャンのみ 0.215 秒 / 現行の形 1.201 秒 /
  `DATE_FORMAT` を外しても 1.224 秒 / **列と JOIN を落とすと 0.247 秒 (約 5 倍)**。
  `DATE_FORMAT` はほぼ効いていない。日別サマリはどちらの列も使わないので値は変わらない
- **`dtako_events` はイベント名で絞る** (`休息` / `休憩` / `運行開始` / `運行終了`)。
  105,771 行 → 22,092 行 (1/3)。休息と休憩だけで畳めるが、**運行開始・終了も読む** —
  同日の継ぎ目を「作業」と判定した根拠 (#123) を後から確かめられなくなるため。
  **`/api/kintai/events` は絞らない** (#116 の「解釈しない読み出し口」を崩さない)
- **畳む前に `split_by_driver` (純粋関数、`src/kosoku.rs`) で乗務員ごとに分ける。**
  `daily_summary` は乗務員を知らないので、混ぜたまま渡すと**後から来た始業が前の始業を
  上書きして人ぶんの拘束が丸ごと落ちる**。`driver_id` が無い / 数でない / 負の行は捨てる
- **勤務が 1 日も組めなかった乗務員は応答から落とす** (退職者・内勤で応答を膨らませない)。
  受け手にとって空配列と「居ない」は同じ
- 応答サイズの見積り: 1 名 1 か月で約 10 KB → 96 名で約 1 MB

### 月別バージョン (ETag) — `/api/kintai/version` (Refs #184)

nuxt-dtako-admin の relay (dtako-scraper-relay) が上流応答キャッシュ (DO SQLite) の
**条件付き再検証**に使う数十バイトの読み出し口。鮮度要件は「古い値は一切返さない」。

- 応答: `{"month":"YYYY-MM","etag":"…"}` + 同じ値を `ETag` ヘッダに。etag は
  **HTTP の quoted ETag そのもの** (`"…"` 込み) — relay は文字列比較だけ
- **唯一の危険点はソーステーブルの列挙漏れ** — `kosoku-daily` (MariaDB 直読み:
  `time_card_dstate` / `time_card_dtako` / `time_card_dtako_state` / `dtako_events` /
  `dtako_cars` / `dtako_ferry_rows` / `dtako_rows`) と `daily` (CakePHP `dailyJson` の
  読む `time_card_dstate` / `daily_report_other_detail` / `drivers` / `offices` /
  `time_card_non_legal_holiday`) の **11 テーブル**を `VERSION_SQL` (UNION ALL 1 本)
  で覆う。`src/kintai_version.rs` の unit test が全テーブルの在席を固定している
- マーカーは **COUNT + `SUM(CRC32(読む列))`** — `dtako_*` 系に modified 列が無いため
  `MAX(updated_at)` 方式は使えない。範囲・列はデータクエリの**上位集合** (広い分は
  無駄な再取得で済むが、狭いと「古い値」事故)
- **例外: `dtako_events` だけ COUNT + `MAX(id)` (index-only、範囲は前月初〜)** —
  428万行/2GB で CRC (行読み) は DB バッファ冷え時に 7〜24 秒かかった (2026-07-29
  本番実測)。binlog 検査で応答が読む 6 列への in-place UPDATE がゼロ件と確認済み
  という運用実態が前提 — 6 列を UPDATE する運用が始まったら CRC に戻すこと
  (`src/kintai_version.rs` モジュール docs の「例外」参照)
- **`KINTAI_OUTPUT_SHA` と `KosokuParams` (Debug 表現) も畳む** — 計算ロジックや
  TOML (丸め・閾値) の変更でもキャッシュを無効化するため。`KINTAI_OUTPUT_SHA` は
  `build.rs` が `src/kosoku*` / `src/kintai*` / `src/routes/kintai*` を glob して
  作る内容ハッシュで、**リポジトリ全体の `BUILD_SHA` ではない** (Refs #191) — 全体だと
  ETC・日報など kintai と無関係なデプロイでも relay の上流キャッシュが全月飛んでいた。
  対象ファイルが消えたら**ビルドが落ちる** (`KINTAI_OUTPUT_REQUIRED`)。接頭辞の外に
  出力ロジックを新設したら build.rs の glob を同じ PR で足すこと (取りこぼし =「古い値」)
- 覆えないもの: dailyJson の国民の祝日 (外部 API `holidays-jp.github.io`、DB に無い)
- **GRANT 前提**: `kintai_reader` に `daily_report_other_detail` / `drivers` /
  `offices` / `time_card_non_legal_holiday` の SELECT が要る。無ければ **502
  fail-closed** — 一部テーブル抜きの etag へは縮退しない (列挙漏れと同じ事故になる)
- 認可・パラメータは `kosoku-daily` に揃える (CF Access Service Token / `month` 必須)。
  `driver` は取らない — キャッシュ鍵は月単位
- テストは `src/kintai_version.rs` の unit test (畳み込み・SQL guard) と
  `tests/kintai_version_test.rs` (route の配線、mock で DB 無し) の 2 段

## 給与 (給与大臣) の読み出し — `/api/kyuyo/*`

一番星とは**別の SQL Server** (給与大臣、年度ごとに `KYDATA{会社}_{年度}` の DB が分かれる)。
pool も repo trait も分離してある (`src/kyuyo/repo.rs`)。認可は**ブラウザ JWT の in-service gate**
(auth-worker introspect + email allowlist) — 金額を返すので `/kintai/*` の Service Token とは扱いが違う。

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

## 燃料サーチャージ基礎データ (`/api/surcharge/base`、Refs #12)

調査 #12 で確定した「`運転日報明細` の単一行に完了条件の全項目が揃う」結論に基づく
基礎データ endpoint。請求のみ行 (`請求K`='1') を中心に、各行を
**得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定)** に展開して返す。

- query: `from` / `to` (売上年月 YYYY-MM、半開区間)、`kind` (`billing_only` default / `transport` / `all`)、`limit` (1..=10000、default 2000)
- 県正規化: `地域ﾏｽﾀ.地域N` の先頭を都道府県へ。`北海道` のみ 4 文字、他は最初の `県`/`府`/`都` まで。
  未マップ (`発地域C`='000000' 等) は `"?"`。ロジックは `routes/surcharge.rs::normalize_prefecture` (純粋関数)
- 運賃 = `金額 + 割増 + 実費` (#12 確定式。月計一致用の税抜カラムとは別物なので混同しない)
- **残課題は scope 外** (新規構築/外部取込が必要): 燃費 km/L マスタ / 県庁間距離マスタ (47×47) /
  週次全国平均軽油価格の取込 / サーチャージ対象得意先の識別。これらは本 endpoint では扱わない

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

- **実行先**: `ohishi-data.tailea945d.ts.net` (ubuntu / Ohishi55)
- **インストール先**: `/opt/ichibanboshi/`（ubuntu 所有、sudo 不要）
- **サービス**: `systemctl status ichibanboshi`
- **自動再起動**: `ichibanboshi-watcher.path` (systemd PathModified) がバイナリ変更を検知 → 自動 restart
- **ビルド**: musl スタティックリンク（GLIBC バージョン不一致回避）
- `deploy.sh` / `scripts/deploy-remote.sh` の流れ: `cargo build --release --target x86_64-unknown-linux-musl` → `scp /tmp` → `mv`（アトミック） → PathModified で自動 restart → `/health` 疎通確認

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
