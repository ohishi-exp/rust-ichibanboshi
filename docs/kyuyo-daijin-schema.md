# 給与大臣 (OHKEN) KYDATA スキーマ調査

Refs #81

## 背景

nuxt-dtako-admin の給与比較 (#253/#268 系) は現在、給与大臣から手動エクスポートした XLS を
R2 に取り込んでいる。この XLS 手動エクスポートを廃止し、給与大臣の実体 DB
(SQL Server 2008 R2、インスタンス `OHKEN`。ホスト/ポートの実値は repo に書かず
実行時に受け取る) から直接読み取る API (#82) を作るための、前提スキーマ調査。

調査は ohishi-data 上で `kyuyo_reader` (db_datareader、読み取り専用) を使い、
給与大臣 PC へ TOP 付き SELECT のみで実施した (フルスキャン回避)。

## 接続方法

SQL Server 2008 R2 系は新しい OpenSSL の TLS ハンドシェイクに対応していないため、以下の
OpenSSL 設定で TLS1.0 / SECLEVEL=1 を許可してから `sqlcmd` を使う。

```bash
cat > /tmp/openssl-tls1.cnf <<'EOF'
openssl_conf = openssl_init

[openssl_init]
ssl_conf = ssl_sect

[ssl_sect]
system_default = system_default_sect

[system_default_sect]
MinProtocol = TLSv1
CipherString = DEFAULT@SECLEVEL=1
EOF

# KYUYO_SQL_SERVER は "<給与PC-HOST>,<PORT>" 形式。パスワードともども実行時に受け取る
OPENSSL_CONF=/tmp/openssl-tls1.cnf /opt/mssql-tools18/bin/sqlcmd \
  -S "$KYUYO_SQL_SERVER" -U kyuyo_reader -P "$KYUYO_READER_PASSWORD" -C \
  -d KYDATA0100_126C -Q "SELECT TOP 5 * FROM KYUYO"
```

## DB 一覧・命名規則

`KYDATA{会社コード4桁}_{年度3桁}C` で、年度は 112C=2012 〜 126C=2026 (最新)。

会社コードの正体 (`KYCOMSTD.SELDATA` の `KCODE`/`CONAME1` で確認):

| 会社コード | 会社名 | 現存する年度DB |
|---|---|---|
| 0100 | 有限会社 大石運輸 | 112C〜126C (継続中) |
| 0200 | 大石運輸倉庫株式会社 | 112C〜126C (継続中、114/115は欠番) |
| 0300 | 佐賀大石運輸株式会社 | 112C〜126C (継続中) |
| 0400 | 株式会社 北海大運 | 116C〜126C (継続中) |
| 0500 | 有限会社サトウ運輸 | 120C〜123C のみ (2023年で終了、以降DBなし) |
| 0900 | 有限会社 大石商事 | 116C のみ (2016年の1本だけ、以降なし) |

→ 0500/0900 はいずれも**廃業/データ移行終了済みの旧取引先**で、`kyuyo_reader` の
model 継承的にも新年度DBが作られていない。給与比較 API のスコープは実質
**0100/0200/0300/0400 の4社**でよい。

共通系DB:
- `KYCOMSTD` — 給与大臣ソフト自体の管理DB (`SELDATA`=会社×年度の登録一覧とDATAPATH、
  `CLIENT`は空、`BANKDIC`/`CITYDIC`/`KENDIC`/`YUBINDIC`などの辞書テーブル)
- `OHDIC` — 郵便番号・銀行・地域などの汎用辞書 (`YUBINDIC`, `BANKDIC`, `CITYDIC`, `SITENDIC`)
- `OHMN` — 未調査 (本Issueのスコープ外、給与本体データではなさそう)

## テーブルスキーマの会社間差異

`KYUYO`/`KOUMOKU`/`SHOZOKU`/`SHAIN1`/`SHUKEI1` の列構成 (列名+型のチェックサム) を
6 会社DBで比較した結果:

| テーブル | 0100/0200/0300/0400 (126C) | 0500 (123C) | 0900 (116C) |
|---|---|---|---|
| KOUMOKU | 21列・一致 | 一致 | 一致 |
| SHAIN1 | 23列・一致 | 一致 | 一致 |
| SHUKEI1 | 140列・一致 | 一致 | 一致 |
| SHOZOKU | 26列・一致 | 一致 | **25列** (列が1つ少ない) |
| KYUYO | 181列・一致 | **179列** (`HOKENITEM02`/`HOKENITEM12` が無い) | **164列** (後述、構造が異なる) |

現行4社 (0100/0200/0300/0400) は **完全に同一スキーマ**。API 実装はこの4社を前提にしてよい。

`KYDATA0900_116C` (2016年、大石商事) だけは古い世代のスキーマで、`KYUYO` に
`SHAIN`/`MONTH` の分離カラムが無く、代わりに複合カラムと思われる `SHAINMONTH` を持つほか、
`CHINGINKIKANST`/`CHINGINKIKANEN`(賃金期間)、`MASTER1`〜`MASTER5`、`KENHOKBN` などの
分類系カラムも存在しない。廃業済み会社の単年度DBのみなので、**給与比較APIの対象外として
明示的に除外**するのが妥当 (該当DBに当たったら「対応不要」として記録する運用でよい)。

## KYUYO (月次給与本体)

### 行の粒度

1行 = **社員 (`SHAIN`) × 支給回インデックス (`MONTH`)**。

```sql
SELECT TOP 20 SHAIN, MONTH, CALCEND, SHOZOKU, SHIKYUBI,
       CHINGINKIKANST, CHINGINKIKANEN
FROM KYUYO ORDER BY SHAIN, MONTH
```

| SHAIN | MONTH | SHOZOKU | SHIKYUBI | 賃金期間開始 | 賃金期間終了 |
|---|---|---|---|---|---|
| 4 | 0 | 14 | 2026-01-15 | 2025-12-01 | 2025-12-31 |
| 4 | 1 | 14 | 2026-02-13 | 2026-01-01 | 2026-01-31 |
| 4 | 5 | 14 | 2026-06-15 | 2026-05-01 | 2026-05-31 |
| 4 | 6 | 14 | 2026-07-15 | 2026-06-01 | 2026-06-30 |

- `SHAIN` = `SHAIN1.INCODE` (社員の内部コード、社員マスタと結合するキー)
- `MONTH` = **年度DB内の連番インデックス (0起点)**。カレンダー月と直結しない
  (支給日ベースでもなく、賃金計算期間ベース)。`KYDATA{code}_126C` では
  `MONTH=0` が「2025年12月分 (賃金期間) → 2026年1月15日支給」に対応しており、
  **年度の起点は12月分 (MONTH=0)**。つまり **12月分の給与は「翌年」の年度DB
  (`_126C`側) に入る** — 年度跨ぎで迷ったらこの起点を基準にする。
  2026-07-22 (調査時点) 時点で `_126C` に存在する最大の `MONTH` は 6
  (2026年6月分・7月15日支給) で、以降は月が進むごとに増えていく。
- `CALCEND` — 調査した範囲では**常に0**。再計算ラウンドなどの用途と推測されるが、
  0以外の実例は見つからなかった (非0ケースの意味は未確認)。
- `SHIKYUBI` (支給日) と `CHINGINKIKANST`/`CHINGINKIKANEN` (賃金計算期間の開始/終了)
  は別概念。「支給年月」を人間向けに表現するなら `CHINGINKIKANST` (対象月初) を使うのが
  自然、実際の入金日は `SHIKYUBI`。

### NULL慣習・型

- `KYUYO` の全カラムは `IS_NULLABLE = 'NO'` (Btrieve由来の古いスキーマのため、
  SQL的なNULLは使われない。未使用項目は `0` や空白パディングされた `char` で表現される)
- 金額カラム (`KINDATA*`, `MONEY*`, `KAZEI*`, `HOKEN*` 等) はすべて `int` (小数なし、円単位)
- `char` 型カラム (`BIKOU`, `FUTEK` 等) は固定長で末尾スペース埋め。文字コードは
  `sqlcmd` 側の自動コードページ判定で日本語 (氏名・所属名など) が正しく表示され、
  追加のコードページ指定は不要だった

## KOUMOKU (支給/控除項目マスタ) と KYUYO の対応

`KOUMOKU.TAIKEIKOUNO` (char(5)) は **「体系コード(2桁) + 項目番号(3桁)」** の合成キー。

- 体系コードは `SHOZOKU.TAIKEI` (smallint) の値と対応する。社員の所属部署
  (`KYUYO.SHOZOKU` → `SHOZOKU.INCODE`) が属する体系によって、使われる項目セットが変わる
  (例: `TAIKEI=1` は乗務員系、`TAIKEI=2` は事務・整備系、で手当構成が異なる)
- 項目番号 001〜017 (17項目) = 勤怠系 (出勤日数・残業時間 等) →
  `KYUYO.KINDATA0000, KINDATA0100, ..., KINDATA1600` (17列、100刻み、当月分) と
  `KINDATA0001, ..., KINDATA1601` (17列、別集計。累計等と推測、未確定)
- 項目番号 018〜097 (80項目) = 支給・控除項目 (基本給・各種手当・社会保険・雇用保険・
  所得税 等) → `KYUYO.MONEY00 〜 MONEY79` (80列)。**列番号 N は項目番号 `18+N` に対応**
- 項目番号 098〜147 は賞与 (`01098`=賞与) や保険改定関連で、`KYUYO` ではなく
  `SHOYO`/`KAITEISHUKEI*` 系のテーブル向け (本Issueのスコープ外)

マッピング式:

```
TAIKEIKOUNO = RIGHT('0' + CAST(SHOZOKU.TAIKEI AS varchar), 2)
            + RIGHT('000' + CAST(18 + N AS varchar), 3)   -- N = MONEY列の連番(0-79)
```

### 実データでの検証

`SHAIN=4` (所属コード14 `本社 乗務員`、`SHOZOKU.TAIKEI=1`)、`MONTH=5` (2026年6月分) の
`MONEY00/04/06/10` を確認し、`TAIKEIKOUNO=01018/01022/01024/01028` (体系1) の
`KOUMOKU.NAME` と突き合わせた:

| MONEY列 | 項目番号 | TAIKEIKOUNO | KOUMOKU.NAME | 金額 |
|---|---|---|---|---|
| MONEY00 | 018 | 01018 | 基本給 | 83,418 |
| MONEY04 | 022 | 01022 | 住宅手当 | 9,000 |
| MONEY06 | 024 | 01024 | 無事故手当 | 27,000 |
| MONEY10 | 028 | 01028 | 家畜運搬手当 | 52,000 |

マッピング式どおりに一致することを確認済み。

なお `KOUMOKU` には `KUBUN`(1〜5)・`KAZEI`(課税区分)・`GENGAKU` 等のフラグ列があるが、
`GENGAKU=1` は「時間修正控除」「遅早控除」「その他減額」等ごく一部にしか立っておらず、
支給/控除の符号をこれらのフラグから機械的に判定するのは信頼できない
(→ 支給合計/控除合計は次項の `SHUKEI1` の値を使う方が安全)。

## 支給合計・控除合計・差引支給 → `SHUKEI1`

社員ごとの集計値は `KYUYO` の生項目からではなく、**`SHUKEI1`** (社員×支給回インデックス
00〜21 の集計テーブル) に**既に計算済みの値として格納**されている。
インデックス番号は `KYUYO.MONTH` と1対1で一致する
(`SHUKEI1.SHIKYUBI{NN}` が `KYUYO` 同一社員同一MONTH行の `SHIKYUBI` と一致することで確認済み)。

| SHUKEI1 列 (NN=月インデックス) | 意味 |
|---|---|
| `SHIKYUBI{NN}` | 支給日 |
| `SOSHIKYU{NN}` | **総支給額 (支給合計)** |
| `KAZEI{NN}` | 課税支給合計 |
| `HOKEN{NN}` | 社会保険料 (控除) |
| `ZEI{NN}` | 税金 (所得税+住民税、控除) |
| `SHOKOUJO{NN}` | 諸控除 (貸付金等、控除) |

```
控除合計   = HOKEN{NN} + ZEI{NN} + SHOKOUJO{NN}
差引支給   = SOSHIKYU{NN} - 控除合計
```

`SHAIN=4`, `MONTH=5` (2026年6月分, 支給日2026-06-15) の実データ:

| SOSHIKYU05 (支給合計) | HOKEN05 | ZEI05 | SHOKOUJO05 | 控除合計 | 差引支給 |
|---|---|---|---|---|---|
| 404,045 | 56,398 | 7,830 | 30,500 | 94,728 | 309,317 |

**要フォローアップ**: 給与明細XLSとの金額突合 (Issueのタスク5) は、比較対象の
実XLSファイルをユーザーから提供いただき次第、上記 `SOSHIKYU05` (支給合計) の値と
照合する。現時点ではDB内で計算済み合計値の存在と算出式のみ確認済み。

## 社員マスタ (`SHAIN1`) と突合キー

```sql
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='SHAIN1'
```

| 列 | 意味 |
|---|---|
| `INCODE` | 社員の内部コード (`KYUYO.SHAIN` と結合するキー) |
| `CODE` (char(8)) | **社員番号** (例: `"1771    "`, `"0941    "` — 前ゼロ付き、末尾スペース埋め)。
  dtako側の社員コードと突合するには `LTRIM(RTRIM(CODE))` した上で先頭ゼロを除去する |
| `NAME` (char(60)) | 氏名 |
| `TAIKYU` | 在籍状態フラグ (`0`=在籍中, `1`=退職済み、と推測。実データで確認済み) |
| `SHOZOKU` | 所属コード (`SHOZOKU.INCODE` と結合) |
| `DEL` | 調査した範囲では全行 `1`。削除フラグというより「有効行」フラグの可能性 (要追加確認) |

`SHAINID` テーブルは `INCODE`/`JKID`/`KSID` 等を持つ別システム連携用の紐付けテーブルで、
本Issueの給与比較スコープでは使わなくてよさそう (未使用と判断)。

`SHAIN2`〜`SHAIN8` は未調査 (本Issueのスコープ外、`SHAIN1` で必要な情報は揃っている)。

## 所属マスタ (`SHOZOKU`)

| 列 | 意味 |
|---|---|
| `INCODE` | 所属コード (`KYUYO.SHOZOKU`, `SHAIN1.SHOZOKU` と結合) |
| `NAME1`/`NAME2` | 営業所名/職種名 (例: `本社` / `乗務員`) |
| `SNAME` | 表示用の結合名 (例: `本社　乗務員`) |
| `TAIKEI` | **KOUMOKU.TAIKEIKOUNO の先頭2桁と対応する体系コード** (前述) |

## 結論: API で発行すべき SELECT 文の草案

### (A) 社員×月の支給合計・控除合計・差引支給 (給与比較のメイン用途)

```sql
SELECT
    s1.CODE            AS 社員番号,   -- LTRIM/RTRIM + 前ゼロ除去はアプリ側で
    s1.NAME            AS 氏名,
    sz.SNAME           AS 所属,
    k.SHIKYUBI         AS 支給日,
    k.CHINGINKIKANST   AS 賃金期間開始,
    k.CHINGINKIKANEN   AS 賃金期間終了,
    sk.SOSHIKYU00      AS 支給合計,   -- ※実際は MONTH に応じた SOSHIKYU{NN} 列を
    sk.HOKEN00 + sk.ZEI00 + sk.SHOKOUJO00 AS 控除合計,
    sk.SOSHIKYU00 - (sk.HOKEN00 + sk.ZEI00 + sk.SHOKOUJO00) AS 差引支給
FROM KYUYO k
JOIN SHAIN1  s1 ON s1.INCODE = k.SHAIN
JOIN SHOZOKU sz ON sz.INCODE = k.SHOZOKU
JOIN SHUKEI1 sk ON sk.SHAIN  = k.SHAIN
WHERE k.MONTH = @month  -- 0..N、CHINGINKIKANST/SHIKYUBI から year/monthを特定して算出
```

`SOSHIKYU{NN}`/`HOKEN{NN}`/`ZEI{NN}`/`SHOKOUJO{NN}` は列名に `MONTH` の値が
埋め込まれているため、T-SQL側で動的列選択はできない。Rust実装側で
`MONTH` (0〜21程度) ごとに列名リストを持ち、対応する列を選ぶ設計にする
(生SQLで `CASE` 分岐するか、アプリ側でクエリ文字列を組み立てる)。

### (B) 項目別の内訳が必要な場合 (支給/控除の明細レベル)

`MONEY00`〜`MONEY79` は `UNPIVOT` で行展開できるが、対応する `KOUMOKU.NAME` の
特定に `TAIKEIKOUNO = 体系(SHOZOKU.TAIKEI) + (18+列番号)` という計算式が必要で、
T-SQL の `UNPIVOT` 構文だけでは列番号を式の入力に使えない。実装方針としては:

1. Rust側で `MONEY{N}` (N=0..79) → `TAIKEIKOUNO` 計算式を固定ロジックとして持つ
2. `KOUMOKU` を体系ごとに1回読み込んでキャッシュ (項目マスタは頻繁に変わらない)
3. `KYUYO` から生の `MONEY00..MONEY79` (+`KINDATA*`) を取得し、アプリ側で
   `TAIKEIKOUNO` 経由の項目名と突き合わせて明細化する

(生SQLで全項目をJOINしようとすると80列分のUNPIVOT+動的JOIN式が必要になり、
T-SQL側で完結させるのは非現実的。素直にアプリ側ロジックにするのが妥当)

### 対象範囲

- 対象会社: `0100`/`0200`/`0300`/`0400` の4社のみ (`0500`/`0900` は廃業済みDBのため対象外)
- 年度DB切り替え: 支給対象月から `年度3桁 = 対象年 - 1900 + (12月分なら+1)` を算出して
  `KYDATA{code}_{year}C` を組み立てる、または `CHINGINKIKANST` の月が12月なら
  年度を+1する、のいずれかの規則を実装で使う

## 未確認・要フォローアップ

- [ ] 給与明細XLSとの実額突合 (`SOSHIKYU{NN}` と手元XLSの「支給合計額」) — 比較対象の
      XLS提供待ち
- [ ] `CALCEND` が0以外になるケース (再計算ラウンド等) の意味
- [ ] `KINDATA0001`〜`KINDATA1601` (2つ目の勤怠系17列) の用途 (当月/累計の区別など)
- [ ] `SHAIN1.DEL` の正確な意味 (全行1だったため未確定)
- [ ] `OHMN` DB の役割 (未調査、給与本体には使わなそうという推測のみ)
