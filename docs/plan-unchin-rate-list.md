# 運賃リスト機能 実装計画（得意先・傭車先別、値上げ履歴管理）

Refs #57

## 背景・要件

得意先・傭車先別の運賃リストを作成する。

- 得意先ごと・傭車先ごとに運賃一覧を作る
- 値上げを考慮する（過去の運賃も追える形で保持する）
- 同じ品目・金額・取引先（もしくは傭車先）の場合は積地・卸地のペアをまとめて記憶する
- 運賃順に表示する
- 値上げのタイミングで一覧表示を切り替えられるようにする

対象: `ohishi-exp/rust-ichibanboshi`（データ抽出、read-only）+ `ohishi-exp/nuxt-ichibanboshi`（保存・表示）。

## 確定済みの設計判断

ユーザーへのヒアリング結果（#57 参照）:

| 項目 | 決定 |
|---|---|
| ストレージ | R2 のみ（nuxt-ichibanboshi Worker 側、既存 `URIAGE_R2` バケットを `unchin/` prefix で流用）。D1 は不採用（件数は多くない想定）。新規 KV namespace は CCoW 環境から CF 認証して provisioning できないため作成せず、index も R2 オブジェクトとして持つ（性能上必要になれば後から KV 化可能） |
| データ取得元 | rust-ichibanboshi の既存 SQL Server (`CAPE#01`) 連携経由 |
| 値上げ管理 | 履歴をバージョンとして保持し、日付/期間を選んで過去・現在の運賃表を切り替え表示する |
| 値上げタイミングの確定方法 | 自動検知ではなく、ユーザー（管理者）の操作で保存する。保存時に誰が登録したか（ユーザー情報）を記録する |
| グルーピング | 運賃レコード1件につき、積地・卸地ペアの配列を持たせる（品目・金額・取引先(傭車先)が同一なら同じレコードに積地/卸地ペアをまとめる） |

## アーキテクチャ（ラフ案、実装時に詳細化）

```
SQL Server (CAPE#01)
  ↓ GET /api/unchin/candidates?from=&to=&partner_type=customer|subcontractor (新規, rust-ichibanboshi, read-only)
    (得意先 or 傭車先 [C+H 複合キー] / 品名C・品名N / 運賃(金額+割増+実費) / 発地N・着地N /
     売上年月日 を運転日報明細から抽出。品名C IN ('9003','9998') は除外)
[nuxt-ichibanboshi Worker]
  ↓ 管理画面で候補を確認 → (得意先C+H or 傭車先C+H, 品名C, 金額) で集約
    (品名C が '0000'/'0002' 等の空品名コードの場合は 発地N+着地N も同一性キーに含める)
  ↓ 「このタイミングで確定 (値上げ登録)」操作
  ↓ R2 (URIAGE_R2, prefix `unchin/`): 取引先ごとの index オブジェクト (バージョン一覧、
    effective_from、登録者) + バージョンごとの確定運賃データ本体 (品目・金額・
    発地N/着地N ペア配列の JSON)
[ブラウザ UI]
  - header の「運賃リスト」ボタン → 一覧ページ (得意先別/傭車先別の売上・支払サマリ)
  - クリック → 明細ページ (運賃順リスト、バージョン切替プルダウン)
  - 一括表示 / PDF 印刷
```

- rust-ichibanboshi 側は **read-only**（新規 SQL 抽出 endpoint のみ追加、既存 `surcharge_base` / `uriage_rows` と同パターン）。`AppRepo` trait にメソッドを足し `TiberiusRepo` で実装、`MockRepo` でテスト可能にする（既存方針踏襲）
- バージョン登録（値上げ確定）の書き込みは **nuxt-ichibanboshi Worker 側**（KV/R2 binding を持つのは Worker のため）。登録者情報は `logi_auth_token`（auth-worker JWT、`email`/`name` claim）から取得して記録する
- 既存 uriage Phase 2 の「recalc → R2 sync」UI パターン (`app/pages/admin/recalc.vue`) を踏襲できる
- `運転日報明細` は直近 2.5 年程度のローリング窓しか保持しないため、定期抽出でスナップショットを
  KV/R2 に蓄積し続ける設計が必須（DB 側に過去の値上げ履歴は残らない）

## UI 設計

- header に運賃リストへの導線ボタンを追加し、リンク先のページで表示する
- 一覧ページ: 得意先ごと・傭車先ごとの売上（請求）金額・支払（傭車）金額を表示
- 一覧の行をクリック → その取引先の運賃（金額）リストを運賃順に表示
- 運賃リストは一括表示、および PDF 印刷に対応する

## 確定済みスキーマ・抽出ロジック（実機 DB 調査結果、#57 コメント参照）

`運転日報明細`（`運行年月日>=2024-01-01`、132,908 行、範囲 2024-01-31〜2026-07-25）と
`傭車先ﾏｽﾀ` を read-only クエリで実機検証済み。

### 取引先キー — `C` + `H` の複合キー必須

`傭車先H` は固定値ではなく変動する（マスタ側 `000`(3390件) のほか `010`〜`017`/`001` 多数。
明細側も `000`(108,446) 中心だが `013`/`017`/`015`/`011`/`016`/`014`/`012` に分散）。
`得意先H` も同様に分散する。**取引先の同一性は必ず `C`+`H` の複合キーで判定する**（`C` 単独は不可）。

- 傭車先マスタ: `傭車先ﾏｽﾀ`（複合キー `傭車先C`+`傭車先H`、名称 `傭車先N`、略号 `傭車先R`）。
  `得意先ﾏｽﾀ`（`得意先C`+`得意先H`）と同型
- 自社/傭車判定: `傭車先C = '000000'` で自社、それ以外は傭車（既存 `surcharge.rs` / `uriage.rs`
  と同じ判定式）

### 積地・卸地 — 生の `発地N`/`着地N` 文字列をそのままペアキーに使う

`運転日報明細.発地N` / `着地N` は自由入力。空文字率は発地N 28.3%・着地N 30.7%、distinct 値は
発地N 3,984 種・着地N 5,620 種（ほぼフリーテキスト）。粒度も市町村名（`釧路`）・県+市
（`福岡県北九州市`）・施設名（`SUMCO TECHXIV㈱　長崎工場`）が混在する。

→ `surcharge_base` 方式の県正規化（`地域ﾏｽﾀ` 経由）では情報が潰れるため、**本機能では生の
`発地N`/`着地N` 文字列をそのまま積卸ペアのキーにする**。空文字行は「不明」ペアとして残すか、
別途検討（実装時に決定）。

### 運賃額 — `金額` は既に税抜。`金額 + 割増 + 実費`（値引は無視可）

サンプルで `金額 == 税抜金額`（例: 46666==46666）、`消費税 = round(金額×0.10)` であり、
**`金額` 列自体が税抜額**。非ゼロ件数は `割増` 1,167 件・`実費` 9,970 件・`値引` はわずか 1 件のみ。

→ 既存 `surcharge_base`（#12 確定式）と同じ **`金額 + 割増 + 実費`** で運賃額とする。値引は
実質未使用のため無視してよい。月計一致用の `税抜金額+税抜割増+税抜実費-値引` は別目的（会計上の
確定額）なので混同しない。傭車側も対称列（`傭車金額`/`傭車割増`/`傭車実費`）で同式を適用する。

### 同一性キー — `(得意先C+H, 品名C, 金額)`。空品名コードは積卸地も含める

`(得意先C+H, 品名C, 金額)` ごとの distinct 積卸ペア数: 1 ペアのみ 18,271 グループ（73%）/
2-3 ペア 3,455 / 4-10 ペア 1,008 / 11+ ペア 163。大半（73%）は 1 ペアに自然収束する。

ただし `品名C='0000'`（7,438件、品名N空）や `'0002'`（30,749件、品名N空）のような**汎用・空品名
コード**では、無関係なルートが束ねられる過剰集約が発生する（例: 1 グループに 71 種の積卸ペアが
混入したケースを確認）。

→ 同一性キーは **`(得意先C+H, 品名C, 金額)`**。**`品名C` が `0000`/`0002` 等の空品名コードの
場合は `発地N`+`着地N` も同一性キーに含める**補正を行う。

### 調整行の除外 — `品名C IN ('9003','9998')` のみ除外

`9003`=消費税調整（8件、金額計 -11円）、`9998`=端数調整（12件、金額計 -9円）は微小マイナス
調整のため除外する。他の `9xxx`（`9608`=YBC 920.6万円、`9406`=肉、`9012`=馬(非課税)、
`9101`=ケーブルラック 等）は正規貨物で金額も大きく、**除外してはいけない**。

`9999`=`8306乗務`（35件、73.5万円）は乗務（人件）系の可能性があり要追加確認（実装着手前に
再確認する）。

### データ保持範囲 — `運転日報明細` は直近 2.5 年程度のローリング窓

`運転日報明細` は最古でも 2024-01-31 までしかデータが無い（過去全履歴は保持されない）。

→ 「値上げ履歴」を長期で追うには、**抽出時点のスナップショットを nuxt-ichibanboshi 側の
KV/R2 に蓄積していく前提が必須**（DB を遡って過去の運賃変遷を再構築することはできない）。
定期的な抽出（recalc 相当のジョブ）でスナップショットを取り続ける設計にする。

### 参考実装

旧 PHP `IchibanRowsController::jistuunsoPrint()`（`yhonda-ohishi/nginx`）が品名・得意先・
傭車先・発地N/着地N・積込/納入年月日を `運転日報明細` から直接 select しており、本機能の
抽出ロジックの元ネタに近い。

## 実装仕様（このまま実装可能な詳細）

実装はユーザー側で行う前提（Claude Code は本ドキュメントの整備まで）。

### A. バックエンド: rust-ichibanboshi 新規 endpoint

新規ファイル `src/routes/unchin.rs`（`src/routes/surcharge.rs` を参考に作成）:

```rust
// RawUnchinRow (repo 層): partner_code(=C+'-'+H), partner_name, item_code(品名C),
// item_name(品名N), fare(i64), origin(発地N), dest(着地N), sale_date(NaiveDateTime)
//
// UnchinCandidateRow (response 層): 上記を文字列化したもの (sale_date は "YYYY-MM-DD")

#[derive(Deserialize)]
pub struct UnchinQuery {
    pub from: String,          // YYYY-MM-DD (inclusive)
    pub to: String,            // YYYY-MM-DD (exclusive)
    pub partner_type: String,  // "customer" | "subcontractor"
    pub limit: Option<i32>,    // default 5000、1..=20000 にクランプ
}

// GET /api/unchin/candidates
pub async fn unchin_candidates(...) -> Result<Json<ApiResponse<Vec<UnchinCandidateRow>>>, StatusCode> {
    // partner_type が customer/subcontractor 以外なら 400
    // repo.unchin_candidates(from, to, partner_type, limit) を呼ぶだけの薄いハンドラ
}
```

`AppRepo` trait に追加するメソッド:

```rust
async fn unchin_candidates(
    &self,
    from: &str,
    to: &str,
    partner_type: &str, // "customer" | "subcontractor"
    limit: i32,
) -> Result<Vec<RawUnchinRow>, RepoError>;
```

`TiberiusRepo` 実装の SQL 案（`surcharge_base` と同じくスカラサブクエリでマスタ名を引く、
LEFT JOIN によるファンアウトは避ける）:

```sql
-- partner_type = "customer"
SELECT TOP {limit}
  CONCAT(t.[得意先C], '-', t.[得意先H]),
  ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c
          WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]), ''),
  ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''),
  ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0),
  ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''),
  t.[売上年月日]
FROM [運転日報明細] t
WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2
  AND t.[品名C] NOT IN ('9003', '9998')
ORDER BY t.[得意先C], t.[得意先H], t.[品名C], t.[金額]

-- partner_type = "subcontractor" は 傭車先C != '000000' を WHERE に追加し、
-- 得意先ﾏｽﾀ→傭車先ﾏｽﾀ（C+H 結合）、金額/割増/実費→傭車金額/傭車割増/傭車実費 に置き換える
```

- `品名C='9999'`（8306乗務）の除外要否は実装前に再確認すること（#57 参照）
- `server.rs` の `api_routes` に `.route("/unchin/candidates", get(routes::unchin::unchin_candidates))`
  を追加、`routes/mod.rs` に `pub mod unchin;` を追加
- テストは `tests/surcharge_test.rs` の `MockRepo` パターンを踏襲（`tests/unchin_test.rs` を新設）
- グルーピング（積卸ペアを1レコードにまとめる処理）は **rust 側ではやらない**。raw 行をそのまま
  返し、グルーピングは nuxt-ichibanboshi Worker 側で行う（同一性キー判定は SQL より TypeScript の
  方が調整しやすいため）

### B. フロントエンド: nuxt-ichibanboshi の R2 スキーマ・API・UI

#### R2 オブジェクトキー（`URIAGE_R2` バケットを `unchin/` prefix で流用、新規バケット作成不要）

```
unchin/index/{partner_type}/{partner_code}.json
  → [{ version_id, effective_from, registered_by, registered_at, item_count }, ...]
    (registered_by はメールアドレス、effective_from 降順で並べておく)

unchin/data/{partner_type}/{partner_code}/{version_id}.json
  → [{ item_code, item_name, fare, routes: [{ origin, dest }, ...] }, ...]
    (fare 降順 = 運賃順 で並べて保存しておくと一覧表示がそのまま使える)
```

`partner_code` は `得意先C-得意先H` または `傭車先C-傭車先H`（rust の `partner_code` をそのまま
key の一部に使う）。`version_id` は ULID や `effective_from` (YYYYMMDD) + 連番で一意にする。

#### server routes（新規）

- `GET /api/unchin/candidates?from=&to=&partner_type=` — rust `/api/unchin/candidates` を
  `salesApiFetch` 経由で呼び、`(partner_code, item_code, fare)` でグルーピングして
  `routes: [{origin, dest}]` 配列を組み立てて返す。`item_code` が `0000`/`0002` の場合は
  グルーピングキーに `origin`+`dest` も含める（過剰集約防止、#57 確定事項）
- `GET /api/unchin/versions?partner_type=&partner_code=` — R2 の index オブジェクトを読む
- `GET /api/unchin/versions/:version_id?partner_type=&partner_code=` — R2 の data オブジェクトを読む
- `POST /api/unchin/versions` — body `{ partner_type, partner_code, effective_from, items }`。
  認証ユーザーのメールアドレスを `registered_by` として記録し、R2 に data オブジェクトを書き、
  index オブジェクトに追記する。**認証ユーザーのメール取得方法は要確認**: `logi_auth_token` cookie
  の JWT を server 側で decode する必要があるが、本リポジトリの `server/middleware/auth.ts` は
  現状 tenant チェックのみで claims を取り出していない。`@ippoan/auth-client` (server 側 export)
  に decode helper が無いか確認し、無ければ `jsonwebtoken` 等で claims (`email`/`name`) を decode
  する処理を追加すること

#### UI

- header（`app/app.vue` または各ページ）に「運賃リスト」ボタンを追加 → `/unchin` へリンク
- `app/pages/unchin/index.vue` — 一覧ページ。得意先別・傭車先別の最新バージョン合計金額を表示。
  行クリックで詳細ページへ遷移
- `app/pages/unchin/[partnerType]/[partnerCode].vue` — 詳細ページ。バージョン切替プルダウン
  （`GET /api/unchin/versions` の一覧から選択）+ 運賃順テーブル + 「値上げとして登録」ボタン
  （`POST /api/unchin/versions` を叩く、effective_from は管理者が日付入力）
- PDF 印刷: サーバ側 PDF 生成ライブラリは導入せず、`@media print` CSS + `window.print()` の
  ブラウザ印刷機能で済ませる（Workers 環境にも依存せずシンプル）

## 参考

- `src/routes/surcharge.rs` — 運転日報明細から得意先/積地/卸地/車種/運賃を抽出する既存実装（本機能の最も近い前例）
- `src/routes/uriage.rs` — recalc → SQLite → raw NDJSON.gz → R2 sync のパイプライン実装（KV/R2 連携の前例）
- nuxt-ichibanboshi 側の R2 binding 例: `wrangler.toml` の `URIAGE_R2` / `server/utils/uriageR2Sync.ts`
- 親 issue: #57（実機 DB 調査結果は [issue コメント](https://github.com/ohishi-exp/rust-ichibanboshi/issues/57#issuecomment-4847905018) 参照）
