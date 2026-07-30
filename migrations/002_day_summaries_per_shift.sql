-- day_summaries を「勤務 1 本 = 1 行」にする
--
-- Refs ohishi-exp/rust-ichibanboshi#205 実装計画 04 (push)。
--
-- 001 の `kintai.day_summaries` は `PRIMARY KEY (tenant_id, driver_cd, date)` で、
-- 「1 暦日 = 1 勤務」を前提にしていた。ところが `src/kosoku.rs` の `daily_summary()`
-- は**同じ暦日に複数の勤務行**を返す。フェリー乗船が休息イベントなので勤務そのものが
-- 割れるためで、実データは 乗務員 1726 / 2026-03-14 がフェリー 2 本のせいで 4 勤務
-- (拘束 1 / 16 / 82 / 222 分) になる (src/kosoku.rs:1936-1942 に実測が残っている)。
--
-- このまま push すると 4 行のうち 1 行しか入らない。素の INSERT なら PK 違反で落ち、
-- ON CONFLICT DO UPDATE なら最後の 1 勤務で上書きされて残り 3 勤務が消える —
-- どちらも「その日の拘束が 321 分ではなく 222 分」という**静かに間違った数字**を作る。
-- 勤務を潰さずに持つため、勤務の同定子 `shift_start_at` を PK に足す。
--
-- ## 001 は書き換えない
--
-- 001 は本番 Supabase に適用済みで、ledger の checksum 照合が改変を loud fail させる
-- (`scripts/migrate_kintai.sh:176-187`)。修正は必ずこの 002 以降の新規ファイルで行う。
--
-- ## 非修飾名を使わない
--
-- 001 と同じく、テーブルは全て `kintai.` で修飾する。`search_path` に依存しないので、
-- 適用者のロール設定や Supabase 側の既定 search_path が何であっても同じ物ができる。

-- ── 勤務の同定子を足す ──────────────────────────────────────────────────
--
-- **DEFAULT を付けない。** 既存行が 1 行でもあれば
--   ERROR: column "shift_start_at" of relation "day_summaries" contains null values
-- で ALTER ごと (= migration ごと) 落ちる。これは事故ではなく**この migration の
-- 前提条件の表明**である: day_summaries への push はまだ 1 度も走っていない
-- (04 が未実装) ので、この ALTER が通ること自体が「表が空 = まだ誰も書いていない」
-- ことの証明になる。
--
-- 逆に `DEFAULT now()` や `DEFAULT '-infinity'` を付けて静かに通すと、既存行に
-- **嘘の始業時刻**が入る。その行は下の FK が指す勤務を持たず、下の CHECK が要求する
-- 「date = shift_start_at の JST 日付」も満たさないので、結局その場で落ちるか、
-- (制約を緩めれば) 二度と勤務へ辿れない孤児として残る。埋めるべき値が存在しない
-- のだから、静かに埋めずに loud に落ちるのが正しい。
--
-- 万一ここで落ちたら「backfill 付きの 002 に書き直す」のではなく、まず
-- **誰が day_summaries に書いたのか**を確かめること。前提の方が壊れている。
ALTER TABLE kintai.day_summaries
    ADD COLUMN shift_start_at TIMESTAMPTZ NOT NULL;

-- ── PK を勤務単位に張り直す ─────────────────────────────────────────────
--
-- 新しい PK は (tenant_id, driver_cd, date, shift_start_at)。
--
-- **shift_start_at は末尾に置き、date を前に残す。** 読み出しの主経路のひとつが
-- 「1 乗務員 1 か月」で、`WHERE tenant_id = ? AND driver_cd = ? AND date BETWEEN ? AND ?`
-- を PK の**前方一致** (tenant_id, driver_cd, date) で引いている。shift_start_at を
-- date より前に置くとこの前方一致が壊れ (間に等値でない列が挟まる)、同じ問い合わせが
-- 乗務員まるごとのスキャン + filter に落ちる。
--
-- shift_start_at が担うのは「同じ date の中の一意化」だけなので末尾で足りる。
-- date は shift_start_at から導ける (下の CHECK) ので、同じ date に並ぶ行数は
-- その日の勤務数 = 高々数本にしかならない。
ALTER TABLE kintai.day_summaries DROP CONSTRAINT day_summaries_pkey;

-- 名前は暗黙の既定 (day_summaries_pkey) と同じものを明示して付け直す。将来の 003 が
-- 名前で掴めるようにするため (暗黙の名前に依存すると、張り直すたびに名前が変わる罠がある)。
ALTER TABLE kintai.day_summaries
    ADD CONSTRAINT day_summaries_pkey
    PRIMARY KEY (tenant_id, driver_cd, date, shift_start_at);

-- ── どの勤務の行かを DB 側で保証する ────────────────────────────────────
--
-- 001 の day_parts と同じ形の FK。勤務を消したらその勤務のサマリも消える。
-- 再計算 (指紋が変わったときの入れ直し) は「shifts を消して入れ直す」経路になるので、
-- CASCADE が無いと**古い勤務のサマリだけが孤児として残り、月合計に二重に載る**。
--
-- CASCADE の逆引き専用の索引は足さない。参照側の列 (tenant_id, driver_cd,
-- shift_start_at) は新 PK の中で連続していない (間に date がある) ため完全な前方一致には
-- ならないが、(tenant_id, driver_cd) までは効き、残る候補は 1 乗務員ぶん
-- (= 年数 × 数百行) なので filter で足りる。上の読み出し計画を守る方を優先し、
-- ほぼ同じ列を並べた索引を 2 本持たない。
ALTER TABLE kintai.day_summaries
    ADD CONSTRAINT day_summaries_shift_fkey
    FOREIGN KEY (tenant_id, driver_cd, shift_start_at)
    REFERENCES kintai.shifts (tenant_id, driver_cd, start_at) ON DELETE CASCADE;

-- ── date は shift_start_at から導ける ───────────────────────────────────
--
-- date は「始業日」(001 の列コメント)。始業日は始業時刻の JST 日付なので、
-- date は shift_start_at から一意に決まる。writeback がここを取り違えても
-- DB 側で止まるようにする。
--
-- 具体的に止めたい取り違えは、**暦日ビュー (day_parts) の日付を day_summaries に
-- 書く**こと。day_parts は勤務を 0 時で切って配った行なので、日跨ぎ勤務では
-- 2 日目以降の日付を持つ (001 の day_parts は `date >= 始業日` しか要求していない)。
-- 同じ勤務から作った行なのに日付だけ違う、という最も気付きにくい間違いを、
-- ここで `=` に締めて弾く。
--
-- 生成列 (`GENERATED ALWAYS AS ... STORED`) にはしない。PostgreSQL には既存の
-- 通常列を生成列へ変える構文が無く、やるなら DROP COLUMN + 作り直しになる —
-- 適用済みの表に対する contract migration を、等価な CHECK で済む話のために
-- 持ち込まない。
--
-- `AT TIME ZONE 'Asia/Tokyo'` はゾーンを式で名指ししているためセッションの TimeZone に
-- 依存せず IMMUTABLE。001 の shifts.date_start (生成列) / day_parts の CHECK と同じ形。
ALTER TABLE kintai.day_summaries
    ADD CONSTRAINT day_summaries_date_is_shift_start_date
    CHECK (date = (shift_start_at AT TIME ZONE 'Asia/Tokyo')::date);

-- ── 既存の索引は 2 本ともそのまま (判断の記録) ──────────────────────────
--
-- day_summaries_month … (tenant_id, date) INCLUDE (driver_cd, 各分数)
--
--   触らない。この索引の主経路は「月次・全乗務員」の合算 (管理表 / wage-report) で、
--   参照する列は key の (tenant_id, date) と INCLUDE の driver_cd + 分数だけ。
--   1 暦日に複数行が並ぶようになっても、消費側は元々 GROUP BY で畳むので
--   index-only のまま効く。行数が「勤務数 / 暦日数」倍に増えるだけで、増えた分は
--   本来 1 行に潰れて消えていた勤務なので、この索引にとっては正しい増え方。
--   shift_start_at を INCLUDE に足す案は採らない — この経路が一度も射影しない
--   8 バイトを全 leaf ページに載せることになり、index-only の密度を落とすだけ。
--
-- day_summaries_over_24h … (tenant_id, date) WHERE restraint_minutes > 1440
--
--   触らない。むしろ**この 002 で初めて意図どおりに機能する**。改善基準告示の拘束は
--   勤務単位の値で (kosoku.rs:1615 付近、拘束 = 終業 − 始業 − 中の休息 を 1 勤務ぶん
--   出している)、1 行 = 1 勤務になった今、この部分索引が拾う行はそのまま
--   「拘束が 24 時間を超えた勤務」= 違反そのものを指す。
--
--   001 のままだと、1 暦日 1 行に押し込む過程で複数勤務が合算されるか上書きされるので、
--   (a) 24 時間を超えていない勤務の寄せ集めを違反として拾う 偽陽性 と
--   (b) 上書きで消えた勤務の違反を取りこぼす 偽陰性 の両方が起きていた。
--   閾値 1440 を DDL に焼かない (kosoku.rs の MAX_RESTRAINT_MINUTES が正) という
--   001 の方針もそのまま。基準が変わったら索引を作り直すだけでよい。
