# #205-39 — 逆方向 401 件は「射程外 (A)」か「欠落 (B)」か → **全件 (A)**

2026-07-31 の本番実測:

```
unko_diff_gcp_only_by_month: {"2026-04": 1, "2026-05": 47, "2026-06": 401, "2026-07": 29}
unko_diff_gcp_only_in_month: 401
```

6 月の alc 運行は 401 + 654(一致) = 1,055 件で、そのうち 401 件 (38%) が
突合の左辺に居ない。**結論から書くと 401 件はすべて (A) 射程外**で、
**勤怠の値には効かない**。以下は根拠。

## 1. コードだけで確定すること

### 左辺は「オンプレ全部」ではない — 仕様として既に書いてある

`MONTH_OPERATIONS_SQL` ([src/kintai_push.rs](../src/kintai_push.rs)) は
`kintai.kintai_events` を `source = ANY(PUSHED_SOURCES)` で絞る。
`PUSHED_SOURCES = ["timecard", "dtako"]` で **`dtako_events` は決定 5 で
push していない**。`timecard` は `unko_no` が NULL なので自然に落ちる。
⇒ 左辺に載るのは `dtako` (= オンプレ MariaDB `time_card_dtako`) 由来だけ。

**新しく見つけた欠陥ではない。**
[`OnpremOperation`](../src/kintai_http_repo.rs) の docs に
「`dtako_events` にしか出てこない運行はここに 1 本も現れない」「名前に引かれて
『オンプレ全部と比べている』と読まないこと」と明記済み。

### ★ 逆方向が大きいことは、勤怠が間違っている証拠にならない

**fold の入力は etags を 1 バイトも通らない。**

| 段 | 行 | 何が起きるか |
|---|---|---|
| 生イベント読み | `kintai_fold.rs` `fold_month` → `repo.fetch_all_events_between` | alc の `GET /api/dtako/events` を日付範囲 + 乗務員ページングで直に引く |
| 3 表 UNION | 同 fn の docs「全乗務員版は 3 表を `UNION ALL` したもの」 | **`dtako_events` を含む** |
| 母集団 | `routes/kintai_recalc.rs` の `extra` = `all_units` の全 `driver_cd` | `RECALC_DRIVER_PAGE_SQL` の `pop` に `UNION` される |

`fetch_all` (`kintai_http_repo.rs`) は etags を参照しない。
⇒ **`dtako_events` にしか無い運行の乗務員も、イベントも、fold の入力に入っている。**

## 2. 実測 — 3 桶と乗務員別 (2026-06、PR #264 deploy 後)

```
never_onprem_ops      : 387   (drivers 41)   ← 押し込み済みに一度も居ない
other_month_only_ops  :  14   (drivers  3)   ← 過去には居たのに当月 0 件
also_in_month_ops     :   0   (drivers  0)   ← 当月にも在るのに一部だけ欠け
unknown_driver        :   0
by_day                : 3〜27/日、全 30 日に分散 (単日集中なし)
```

**最も (B) らしい形 (`also_in_month`) はゼロ。** 最大は `driver_cd "0"` の 83 件
(= 乗務員が紐づいていない運行) で、これも `time_card_dtako` に出ないのが自然。

## 3. 残る 14 件 (`other_month_only`) も (A) だった

1718 / 1731 / 1372 の 3 名。**オンプレ MariaDB を直読みする
`GET /api/kintai/events`** (docs に「データ源は社内 MariaDB の直読み」と明記) で
月ごとの source 別行数を数えた:

| 乗務員 | 月 | 打刻 | `dtako` | `dtako_events` | 車輌 |
|---|---|---|---|---|---|
| 1718 | 2026-01 | 0 | 0 | 619 | 帯広100か7132 |
| 1718 | 2026-02 | 0 | **2** | 644 | 〃 (+ 十勝100か120) |
| 1718 | 2026-03..06 | 0 | 0 | 759 / 663 / 661 / 652 | 帯広100か7132 |
| 1731 | 2026-04..06 | 0 | 0 | 378 / 372 / 375 | 佐賀100か8339 |
| 1372 | 2026-03..06 | 0 | 0 | 393 / 539 / 431 / 416 | 佐賀100か7875 |
| **1078 (対照)** | 2026-06 | **6** | **59** | 470 | 長崎800か2302 |

**対照 (1078) は同じ口で打刻 6 行・`dtako` 59 行を返す**ので、0 は口の都合ではない。

- **3 名とも打刻ゼロ**。親の判定基準「打刻も無いなら (A)」にそのまま当たる
- 1718 の `onprem_ever = true` の正体は **2026-02-12 の `dtako` 2 行だけ**
  (`運行開始` / `運行終了`、運行NO `26021208325500000001201`)。その 1 日だけ
  **別の車輌 (十勝100か120)** に乗っており、他の 5 か月は常用の
  帯広100か7132 で `dtako_events` しか出ていない

⇒ **`onprem_ever` の粒度が粗すぎた。** 4 か月前の 1 運行で true になるので、
`other_month_only` 桶は (B) を過大に見せる。**「一度でも」ではなく「直前の月に
持っていたか」で見るべき**だった。

## 4. ★ 車輌CD は運行NO の中に埋まっている

**当初「両側とも車輌を持っていないので車輌別は出せない」と報告したが、これは誤り。**
運行NO の **13〜22 桁目が車輌CD**で、本番の生イベント 6 車輌すべてで
`vehicle` 名と 1:1 に一致する:

```
…0000007132… = 帯広100か7132    …0000002302… = 長崎800か2302
…0000000120… = 十勝100か120     …0000005355… = 帯広100か5355
…0000007875… = 佐賀100か7875    …0000008339… = 佐賀100か8339
```

**オンプレ 23 桁 / GCP 22 桁のどちらでも同じ位置**なので、両側とも
追加の列も往復も無しに車輌別へ割れる (`unko_no_vehicle_cd`)。

これで (A) の機構そのものを 401 件の規模で検証できる:
`unko_diff_gcp_only_in_month_by_vehicle` の `onprem_in_month` が 0 の車輌に
固まっていれば、**「`time_card_dtako` に出るかどうかは車輌で決まる」**の裏が取れる
(1718 の 2 月の 1 運行だけ別車輌だった、という単票の観察と同じ形)。

## 5. 応答から読める観測 (このブランチで足したもの)

| キー | 中身 |
|---|---|
| `unko_diff_gcp_only_in_month_by_day` | 運行開始日別 |
| `unko_diff_gcp_only_in_month_by_driver` | 乗務員別 `{gcp_only, onprem_in_month, onprem_ever}` |
| `unko_diff_gcp_only_in_month_unknown_driver` | 乗務員を引けなかった件数 |
| `unko_diff_gcp_only_driver_split` | 3 桶 |
| `unko_diff_gcp_only_in_month_by_vehicle` | **車輌別** `{gcp_only, onprem_in_month}` |
| `unko_diff_gcp_only_in_month_unknown_vehicle` | 車輌CD を読めなかった件数 |

**判定には一切入らない素通し** (`unko_diff` 一族と同じ作法)。往復は増えない。

## 6. 次に測る人へ

- **#205-38 (窓に運行日を足す) が入ると `gcp_only_in_month` は 401 → 416 前後へ
  増える見込み**。#205-38 自身が「新しく窓に入る 64 件のうち 35 件が
  `time_card_dtako` を持たない」と予告しており、**退行ではなく、この文書が
  言っている射程外の集団が可視化されるだけ**。増分も `driver_split` の
  `never_onprem` 側に乗るはずで、そうならなければその時点で読み直すこと
- `logic_version` は PR #264 の deploy で `c9df91727f7e40a0` に変わっている

## 7. やらないこと

- **取り込みの取り直し (scrape の再実行)** — 2026-07-31 に 3 回試して 3 回とも
  無変化、しかも再アップロードが `has_kudgivt` を `DEFAULT FALSE` へ戻して運行を
  1 件壊した (復旧済み)。**選択肢に入れない。**
- **`reading_date` の窓と混ぜる** — 「6 月の勤怠が 142 行少ない」の真因は別で、
  既に確定している (#205-38 の担当)。
