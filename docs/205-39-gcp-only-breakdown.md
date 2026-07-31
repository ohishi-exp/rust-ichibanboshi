# #205-39 — 逆方向 401 件は「射程外 (A)」か「欠落 (B)」か

2026-07-31 の本番実測:

```
unko_diff_gcp_only_by_month: {"2026-04": 1, "2026-05": 47, "2026-06": 401, "2026-07": 29}
unko_diff_gcp_only_in_month: 401
```

6 月の alc 運行は 401 + 654(一致) = 1,055 件で、そのうち 401 件 (38%) が
突合の左辺に居ない。**この文書は「なぜ居ないのか」を分けるための材料**。

## 1. まず確定していること (コードの行で言えること)

### 左辺は「オンプレ全部」ではない — これは仕様として既に書いてある

`MONTH_OPERATIONS_SQL` ([src/kintai_push.rs](../src/kintai_push.rs)) は
`kintai.kintai_events` を `source = ANY(PUSHED_SOURCES)` で絞る。
`PUSHED_SOURCES = ["timecard", "dtako"]` で **`dtako_events` は決定 5 で
push していない**。`timecard` は `unko_no` が NULL なので自然に落ちる。

⇒ 左辺に載るのは `dtako` (= オンプレ MariaDB `time_card_dtako`) 由来だけ。

**これは新しく見つけた欠陥ではない。**
[`OnpremOperation`](../src/kintai_http_repo.rs) の docs に
「`dtako_events` にしか出てこない運行はここに 1 本も現れない」「名前に引かれて
『オンプレ全部と比べている』と読まないこと」と明記済みで、乗務員 1688 の
8/8 という実測もそこに書かれている。

### ★ 逆方向が大きいことは、勤怠が間違っている証拠にならない

**fold の入力は etags を 1 バイトも通らない。**

| 段 | 行 | 何が起きるか |
|---|---|---|
| 生イベント読み | `kintai_fold.rs` `fold_month` → `repo.fetch_all_events_between(&from, &to)` | alc の `GET /api/dtako/events` を日付範囲 + 乗務員ページングで直に引く |
| 3 表 UNION | 同 fn の docs「全乗務員版は 3 表を `UNION ALL` したもの」 | **`dtako_events` を含む**。打刻 2 表だけの読みは使わない (#234 の回帰) |
| 母集団 | `routes/kintai_recalc.rs` の `extra` = `all_units` の全 `driver_cd` | `RECALC_DRIVER_PAGE_SQL` の `pop` に `UNION` される |

`fetch_all` (`kintai_http_repo.rs`) は etags を参照しない。
⇒ **`dtako_events` にしか無い運行の乗務員も、イベントも、fold の入力に入っている。**

**「突合で見えない」と「勤怠が間違っている」は別。** 401 件が勤怠に効く経路は
「alc の全件読みの側で落ちている」場合だけで、それは `reading_date` の窓の話
(#205-38) であってこの 401 の話ではない。

## 2. まだ分かっていないこと — だから測る

401 件が

- **(A) 射程外** … `time_card_dtako` に最初から出ない車輌・乗務員のぶん
- **(B) 欠落** … 本来出るはずのものが落ちている

のどちらなのかは、**401 件の中身を乗務員別に割らないと決まらない**。
1688 のように「特定の乗務員が丸ごと」なのか「全乗務員に薄く散っている」のかで
意味がまるで違う。

### 足した観測 (このブランチ)

`run_kintai_recalc` の応答に 4 つ:

| キー | 中身 |
|---|---|
| `unko_diff_gcp_only_in_month_by_day` | 運行開始日別の件数 |
| `unko_diff_gcp_only_in_month_by_driver` | 乗務員別 `{driver_cd, gcp_only, onprem_in_month, onprem_ever}` |
| `unko_diff_gcp_only_in_month_unknown_driver` | 乗務員を引けなかった件数 |
| `unko_diff_gcp_only_driver_split` | 下の 3 桶 |

**判定には一切入らない素通し** (`unko_diff` 一族と同じ作法)。往復は増えない —
乗務員CD は既に引いている etags の `driver_cds` (`ippoan/rust-alc-api#587`) から、
`onprem_ever` は Postgres に 1 クエリ足すだけ。

### 3 桶の読み方 (`gcp_only_driver_split`)

| 桶 | 条件 | 読み |
|---|---|---|
| `never_onprem` | 押し込み済みに `unko_no` 付きの行を**一度も**持たない | **(A) 射程外** |
| `other_month_only` | 過去には持っていたのに**対象月だけ 0 件** | **(B) 欠落が最も濃い** |
| `also_in_month` | 対象月にも押し込み済みの運行が在る | **(B) 一部だけ欠けている** |

`onprem_ever` の「一度でも」は**押し込み済みのぶんだけ**で、オンプレ MariaDB の
全履歴ではない。push していない月のことはこの問いでは分からない。

### 車輌別は出せない

**両側とも車輌を持っていない。** etags の item は `unko_no` / `etag` /
`driver_cds` の 3 つだけ、押し込み済みの `kintai_events` も
`vehicle` は「常に `null` (単一乗務員版のみ)」(`kintai_pg_repo.rs` の表)。
車輌別が要るなら別経路 (管理画面の運行詳細) が要る — **この口では推測しない**。

## 3. 先に書いておく予想 (外れたら外れたと書く)

- **P1**: `never_onprem_ops` が 401 の過半 (>200) → (A) 寄り
- **P2**: `other_month_only_ops` は少数 (<50)
- **P3**: `by_day` は 6 月ぜんたいに散る (単日への集中が無い)
- **P4**: `unknown_driver` は 0 (alc は `driver_cds` を返している)

P4 が外れる (= 401 がまるごと `unknown_driver` に落ちる) なら、alc 側の
`driver_cds` が本番に出ていないということなので、そこから先に直す。

## 4. 測り方

```
POST /api/kintai/recalc  {"month": "2026-06", "apply": false}
```

`unko_diff_gcp_only_driver_split` と `unko_diff_gcp_only_in_month_by_driver` の
上位を読む。**`apply: false` で足りる** — 突合は月ゲートの中で毎回走る。

## 5. やらないこと

- **取り込みの取り直し (scrape の再実行)** — 2026-07-31 に 3 回試して 3 回とも
  無変化、しかも再アップロードが `has_kudgivt` を `DEFAULT FALSE` へ戻して運行を
  1 件壊した (復旧済み)。**選択肢に入れない。**
- **`reading_date` の窓と混ぜる** — 「6 月の勤怠が 142 行少ない」の真因は別で、
  既に確定している (#205-38 の担当)。
