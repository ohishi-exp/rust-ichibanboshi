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
| ストレージ | KV + R2（nuxt-ichibanboshi Worker 側）。D1 は不採用（件数は多くない想定） |
| データ取得元 | rust-ichibanboshi の既存 SQL Server (`CAPE#01`) 連携経由 |
| 値上げ管理 | 履歴をバージョンとして保持し、日付/期間を選んで過去・現在の運賃表を切り替え表示する |
| 値上げタイミングの確定方法 | 自動検知ではなく、ユーザー（管理者）の操作で保存する。保存時に誰が登録したか（ユーザー情報）を記録する |
| グルーピング | 運賃レコード1件につき、積地・卸地ペアの配列を持たせる（品目・金額・取引先(傭車先)が同一なら同じレコードに積地/卸地ペアをまとめる） |

## アーキテクチャ（ラフ案、実装時に詳細化）

```
SQL Server (CAPE#01)
  ↓ GET /api/unchin/candidates?from=&to=&partner_type=customer|subcontractor (新規, rust-ichibanboshi, read-only)
    (得意先 or 傭車先 / 品目 / 運賃 / 積地N / 卸地N / 売上年月日 を運転日報明細から抽出)
[nuxt-ichibanboshi Worker]
  ↓ 管理画面で候補を確認 → 「このタイミングで確定 (値上げ登録)」操作
  ↓ KV: 取引先 (得意先 or 傭車先) ごとの index (バージョン一覧、effective_from、登録者)
  ↓ R2: バージョンごとの確定運賃データ本体 (品目・金額・積地/卸地ペア配列の JSON)
[ブラウザ UI]
  - header の「運賃リスト」ボタン → 一覧ページ (得意先別/傭車先別の売上・支払サマリ)
  - クリック → 明細ページ (運賃順リスト、バージョン切替プルダウン)
  - 一括表示 / PDF 印刷
```

- rust-ichibanboshi 側は **read-only**（新規 SQL 抽出 endpoint のみ追加、既存 `surcharge_base` / `uriage_rows` と同パターン）。`AppRepo` trait にメソッドを足し `TiberiusRepo` で実装、`MockRepo` でテスト可能にする（既存方針踏襲）
- バージョン登録（値上げ確定）の書き込みは **nuxt-ichibanboshi Worker 側**（KV/R2 binding を持つのは Worker のため）。登録者情報は `logi_auth_token`（auth-worker JWT、`email`/`name` claim）から取得して記録する
- 既存 uriage Phase 2 の「recalc → R2 sync」UI パターン (`app/pages/admin/recalc.vue`) を踏襲できる

## UI 設計

- header に運賃リストへの導線ボタンを追加し、リンク先のページで表示する
- 一覧ページ: 得意先ごと・傭車先ごとの売上（請求）金額・支払（傭車）金額を表示
- 一覧の行をクリック → その取引先の運賃（金額）リストを運賃順に表示
- 運賃リストは一括表示、および PDF 印刷に対応する

## 調査メモ（`yhonda-ohishi/nginx` 旧 PHP 実装からの手がかり、未検証）

CCoW からは CAPE#01 に直接到達できないため、以下は旧 PHP 実装
(`UriageJyuchuDisplayController` / `IchibanRowsController`) のコードを読んで拾った手がかり。
**実機検証はユーザー側で実施予定**（#57 参照）。

- 傭車先マスタ: `傭車先ﾏｽﾀ`（複合キー `傭車先C` + `傭車先H`、名称 `傭車先N`、略号 `傭車先R`）。
  `得意先ﾏｽﾀ`（`得意先C`+`得意先H`）と同型
- 品目: `運転日報明細.品名C` / `品名N`（例: `品名C=6301` `品名N=フレコン`）
- 積地・卸地: `運転日報明細.発地N` / `着地N`（生カラム）。既存 `surcharge_base` が使っている
  `地域ﾏｽﾀ` 経由の県正規化（`発地域C`/`着地域C` → `normalize_prefecture`）より細かい粒度の
  地名の可能性がある
- 自社/傭車判定: `傭車先C = '000000'` で自社、それ以外は傭車（既存 `surcharge.rs` / `uriage.rs`
  と同じ判定式）
- 運賃額: 既存 `surcharge_base` は `金額 + 割増 + 実費` を運賃として採用（#12 確定式）。本機能でも
  同じ式を踏襲するか、月計一致用の `税抜金額+税抜割増+税抜実費-値引` を使うかは要確認（前者は単発
  明細の請求額、後者は会計上の確定額で性質が異なる）
- 参考実装: 旧 PHP `IchibanRowsController::jistuunsoPrint()` が品名・得意先・傭車先・発地N/着地N・
  積込/納入年月日を `運転日報明細` から直接 select しており、本機能の元ネタに近い

## 未確定・要確認事項

- [ ] `傭車先H` は運用上常に固定値か、変動するか（`得意先H` 同様、複合キーとして必須か）
- [ ] `発地N` / `着地N` の値の粒度・空文字率・表記揺れ（同一地点が表記違いで複数パターンに分かれないか）
- [ ] 運賃額として使うべき式（単発明細ベース `金額+割増+実費` か、税抜消費税調整後か）
- [ ] 「品目・金額・取引先が同一」とみなす同一性判定の粒度（品名C基準かN基準か、得意先/傭車先はコード単位か）
- [ ] `品名N` の調整行（`※　請求一括調整明細　※` 等）や `品名C=9003/9998`（消費税調整/端数調整）の
      除外要否（既存 `uriage.rs` の `common_where` と同様のフィルタが必要か）

## 参考

- `src/routes/surcharge.rs` — 運転日報明細から得意先/積地/卸地/車種/運賃を抽出する既存実装（本機能の最も近い前例）
- `src/routes/uriage.rs` — recalc → SQLite → raw NDJSON.gz → R2 sync のパイプライン実装（KV/R2 連携の前例）
- nuxt-ichibanboshi 側の R2 binding 例: `wrangler.toml` の `URIAGE_R2` / `server/utils/uriageR2Sync.ts`
- 親 issue: #57
