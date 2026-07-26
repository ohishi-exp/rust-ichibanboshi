# 給与・タイムカード・拘束サマリの SQLite derived store 化

Refs #106 (epic) / Refs ohishi-exp/nuxt-dtako-admin#452

## 目的

nuxt-dtako-admin の拘束賃金画面 (`/restraint-wage`) の対話操作から、遅い源泉への
同期的アクセスを排除する。源泉は 3 つあり、遅さの質が異なる:

| 源泉 | 遅さ | 現状の読み経路 |
|---|---|---|
| 給与大臣 OHKEN (会社×年度 `KYDATA` DB) | 非力 PC + `KyuyoLimiter` semaphore=1 直列 + DB 立ち上げ待ちで数秒〜数十秒 | `/api/kyuyo/*` が毎回 OHKEN へ SELECT |
| タイムカード CakePHP (`127.0.0.1:120`) | 都度クエリ (中継のみ、store なし) | `/api/kintai/daily` が毎回 CakePHP へ |
| theearth (デジタコ、外部 SaaS) | scrape 前提 | relay が R2 にアーカイブ済み — ただし wage-report の読みが R2 GET 約300本で 4.9〜7.3 秒 |

解決手段は **#762 Phase 2 で確立済みのパターンの拡張**: 源泉 = source of truth、
SQLite = derived store (消して再構築できるキャッシュ)。読みは SQLite だけを触り、
源泉に触るのは sync の瞬間だけにする。

## 決定事項 (2026-07-26 設計議論)

- **sync は手動トリガーのみ**。夜間 cron (systemd timer) は別 issue
- SQLite は derived store — **バックアップしない・migration 管理しない**。
  `PRAGMA user_version` (schema_version) 不一致は drop → 再 sync で作り直す
- 読み応答には必ず `synced_at` を含め、UI が「いつ時点のデータか」を出せるようにする
- **認可の区分は変えない**: `/api/kyuyo/*` = in-service gate (introspect + email
  allowlist、金額あり)。`/api/kintai/*` = CF Access Service Token (金額なし)。
  sync endpoint も同じ区分に従う
- D1 側の拘束サマリ写し (nuxt-dtako-admin#454, migration 0016) は Phase 3 完了後に
  撤去する。それまで共存 (害なし)

## ストレージ方針

既存 `data.db` (`LocalStore`, uriage) には**同居させず**、ドメインごとに別ファイル:

```
/opt/ichibanboshi/kyuyo_local.sqlite    (Phase 1、金額あり — 0600)
/opt/ichibanboshi/kintai_local.sqlite   (Phase 2)
/opt/ichibanboshi/restraint_local.sqlite (Phase 3)
```

理由: 「消して再構築」の単位をドメインに揃える (uriage の recalc は高価なので
巻き込まない)。config は `[kyuyo] sqlite_path` 等で上書き可、テストは `:memory:`。
実装は `LocalStore` と同じ作法 (rusqlite + `Arc<Mutex<Connection>>` +
`spawn_blocking`)。

### 行の持ち方 — 応答 JSON の素通し保存

行は**応答型の serde JSON をそのまま TEXT で持つ** (`/api/kintai/daily` の
`serde_json::Value` 素通しと同じ哲学)。クエリキーだけ列に出す:

- 行の形が進化しても SQLite migration が不要 (schema_version bump → 再 sync)
- このファイルは分析用ではなく**配信キャッシュ** — SQL で金額を集計しない
- 源泉→応答の組み立てロジック (`build_payroll_rows` 等) は今のまま単一実装を維持

## Phase 1 — 給与 (`kyuyo_local.sqlite`)

### スキーマ

```sql
PRAGMA user_version = 1;
CREATE TABLE kyuyo_payroll (
  company TEXT NOT NULL,          -- '0100' 等
  month   TEXT NOT NULL,          -- 要求パラメータの 'YYYY-MM' (勤務月)
  employee_code TEXT NOT NULL,
  row_json TEXT NOT NULL,         -- PayrollRow の serde JSON verbatim
  PRIMARY KEY (company, month, employee_code)
);
CREATE TABLE kyuyo_employees (
  company TEXT NOT NULL,
  nendo   INTEGER NOT NULL,
  employee_code TEXT NOT NULL,
  row_json TEXT NOT NULL,         -- EmployeeRow の serde JSON verbatim
  PRIMARY KEY (company, nendo, employee_code)
);
CREATE TABLE kyuyo_sync_state (
  scope TEXT NOT NULL,            -- 'payroll:{company}:{month}' | 'employees:{company}:{nendo}'
  synced_at TEXT NOT NULL,        -- RFC3339
  row_count INTEGER NOT NULL,
  warnings_json TEXT NOT NULL,    -- 取得時 warning の保存 (応答で再現する)
  PRIMARY KEY (scope)
);
```

### 読み経路 (read-through + 明示 refresh)

- `GET /api/kyuyo/payroll?company=&month=`:
  1. `kyuyo_sync_state` にあれば SQLite から返す (`synced_at` / `source:"cache"` 付き)。
     **OHKEN には触らない** — 停止中でも返せる
  2. 無ければ従来どおり OHKEN から読み (`KyuyoLimiter` 直列)、**write-through で
     SQLite に保存してから** 返す (`source:"live"`)。初回アクセスが実質 sync になる
- `GET /api/kyuyo/employees?company=&nendo=` も同型
- `POST /api/kyuyo/sync?company=&month=`: キャッシュの有無に関わらず OHKEN から
  引き直して上書き (= 画面の「再取得」ボタン。給与の遡り修正後に使う)。応答は
  `{synced_at, row_count}`。認可は payroll と同じ in-service gate
- `/api/kyuyo/companies` / `/databases` はミリ秒応答 (sys.databases) なので
  **キャッシュ対象外** (現状維持)

### 不変条件

- write-through / sync の書き込みは 1 トランザクション (scope 単位で DELETE →
  INSERT → sync_state upsert)。途中失敗で半端な月を残さない
- 応答の行内容は「live 読みの結果」と byte 等価 (同じ `PayrollRow` を serialize
  するだけ)。**キャッシュ化で行の形を変えない** — 消費側 (dtako-admin 給与比較) は
  `source` / `synced_at` メタ以外の差分を見ない
- SQLite open 失敗・書き込み失敗は **live 読みへフォールバックして warn ログ**
  (読み機能を SQLite の健全性に依存させない)

## Phase 2 — タイムカード (`kintai_local.sqlite`)

```sql
PRAGMA user_version = 1;
CREATE TABLE kintai_daily (
  month TEXT NOT NULL,            -- 'YYYY-MM'
  seq   INTEGER NOT NULL,         -- 上流配列の順序保存
  row_json TEXT NOT NULL,         -- 上流行 (serde_json::Value) verbatim
  PRIMARY KEY (month, seq)
);
CREATE TABLE kintai_sync_state (
  month TEXT NOT NULL PRIMARY KEY,
  synced_at TEXT NOT NULL,
  row_count INTEGER NOT NULL
);
```

- `GET /api/kintai/daily?month=`: sync 済みなら SQLite、無ければ CakePHP live +
  write-through (Phase 1 と同型)。`?refresh=1` で強制引き直し
- 素通し方針は維持 — 保存するのは `serde_json::Value` の verbatim JSON。上流の
  項目追加に型追従不要のまま
- relay (dtako-scraper-relay) の勤怠取り込みは**無変更**で速くなる。当月の打刻は
  日々変わるので、relay 側の取り込みは `?refresh=1` を付ける (relay 1 行変更、
  Phase 2 に含める) — 過去月の再取り込みだけがキャッシュ命中で速くなる
- 認可: 既存 `/api/kintai/daily` と同じ CF Access Service Token (金額なし)

## Phase 3 — 拘束サマリ + wage-report 素材の一括配信 (概要のみ、実装 PR は別)

- `restraint_local.sqlite`: relay が theearth scrape / 勤怠取り込みの後に
  `PUT /api/restraint/summaries` (comp / month / source / summaries[] + noData[])
  で push した写しを保存。金額を含まない (分・日数) ので認可は Service Token
- `GET /api/restraint/wage-source?comp=&month=`: 当月+前月 × theearth+timecard の
  summaries を 1 応答で返す。relay の `handleWageReport` はこの 1 fetch +
  マスタ 3 種 (R2) + 社員マスタ (D1) で行を組み立てる — R2 GET 約300本が消える
- 完了後、nuxt-dtako-admin の D1 写し (migration 0016 + 二重書き込み) を撤去
- 過去 15 ヶ月のバックフィル: relay の resummarize (全月) を 1 回実行すれば
  push 経路経由で埋まる — 専用バックフィルは作らない

## 速度見込み (実測ベース、nuxt-dtako-admin#452 参照)

| 操作 | 現状 | 後 |
|---|---|---|
| 給与比較の会社・月切替 | OHKEN 直列待ち 数秒〜数十秒 | sync 済み月 0.3 秒前後 (tunnel 往復 + SQLite read) |
| wage-report 月切替 | 4.9〜7.3 秒 (R2 GET 300本) | 0.3〜1 秒 (Phase 3) |
| 初回ロード | #451 適用後 4.4〜6.2 秒 | コールド 2 秒弱 / ウォーム 1 秒前後 |

Phase 1 着手前に `/api/kyuyo/databases` (ミリ秒応答) の dtako-admin 経由実測で
tunnel 往復コストを分離測定し、設計値 (50〜200ms) を確定させる。

## テスト方針

- 行変換・scope キー・schema init は純粋関数 + `:memory:` rusqlite でテスト
  (`sqlite.rs` の既存作法)。新規ファイルは `coverage_100.toml` に登録
- 読み経路は MockRepo (OHKEN) / wiremock (CakePHP) + `:memory:` store の組で
  cache-hit / miss+write-through / refresh / 源泉停止時 (cache-hit は返る、
  miss は 502) / SQLite 故障時 (live フォールバック) を固定
- 実機確認 (各 Phase の PR に記録): sync → OHKEN/CakePHP を止めて読み →
  返ること。SQLite ファイル削除 → 再 sync で復元すること

## やらないこと

- 夜間 cron sync (別 issue)
- SQLite ファイルのバックアップ・migration 管理 (derived store のため)
- 金額データの D1 / R2 への配置 (nuxt-dtako-admin#367 の方針維持)
- `KOUMOKU` 項目マッピング等の組み立てロジック変更 (単一実装を維持し、キャッシュは
  応答をそのまま保存する)
