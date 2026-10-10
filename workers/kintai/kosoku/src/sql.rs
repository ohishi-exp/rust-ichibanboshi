//! 社内 MariaDB を読む SQL 文 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版 (`kintai_repo` の MariaDB 実装・`kintai_version`) が使う文をここに置く。
//! 文は移す前と一字も変えていない。Worker 版も同じ文を読むために共有 crate に置く。

/// 打刻 (`time_card_dstate`) / 運行の確定イベント (`time_card_dtako`) /
/// デジタコ生イベント (`dtako_events`) を `UNION ALL` して時刻順に並べる。
///
/// - 日付は `DATE_FORMAT` で文字列にして取り出す — 応答がそのまま
///   `YYYY-MM-DD HH:MM:SS` になり、DB driver の時刻型と timezone 解釈を
///   経路に持ち込まない
/// - `dtako_events` だけ `end_datetime` を持つ (区間イベントのため)。
///   **区間長の判定はしない** — 何分から休憩と数えるかは規則側の話
/// - `dtako_events` は 2 ブランチに分ける。**期間内に始まる区間**に加えて、
///   **期間内に終わる区間 (開始は期間より前)** も拾う — `kosoku-daily` は
///   「休息の終了 = 始業」で勤務を切るので、月をまたぐ休息を落とすと月初の勤務が
///   組めない。2 つは `開始日時` の条件で排他なので重複しない
/// - **`COALESCE(終了日時, 開始日時) >= :from` で 1 本にまとめてはいけない。**
///   関数適用で索引が効かず `type=ALL` の全表走査 (427 万行) になる。実機で
///   0.2 秒が 4 分超になった (#121 → #122 で revert)。`開始日時` と `終了日時` は
///   それぞれ索引を持つので、条件を分けて両方に効かせる (各 0.2 秒)
pub const EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.id = :driver AND d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.driver_id = :driver AND t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`,
       c.`車輌名`
  FROM dtako_events e
  LEFT JOIN dtako_cars c ON c.`車輌CD` = e.`車輌CD`
 WHERE e.`対象乗務員CD` = :driver AND e.`開始日時` >= :from AND e.`開始日時` < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`,
       c.`車輌名`
  FROM dtako_events e
  LEFT JOIN dtako_cars c ON c.`車輌CD` = e.`車輌CD`
 WHERE e.`対象乗務員CD` = :driver
   AND e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
 ORDER BY datetime, source
"#;

/// 全乗務員ぶんを 1 リクエストで読む (Refs #125)。`EVENTS_SQL` から
/// **乗務員の絞り込みを外し、`運行NO` と `車輌名` を落とした**もの。
///
/// - **`運行NO` / `車輌名` と `dtako_cars` の JOIN を返さない。** 日別サマリ
///   ([`crate::kosoku::daily_summary`]) はどちらも使っていないので値は変わらないが、
///   実測ではここが支配的だった — 2026-06 の全乗務員で **1.20 秒 → 0.25 秒 (約 5 倍)**。
///   22,092 行それぞれで車輌マスタを引き当て、23 桁の `運行NO` を転送していた分。
///   「どの運行・どの車か」に降りるときは 1 名分の `/api/kintai/events` を叩く
/// - **`dtako_events` はイベント名で絞らない** (2026-07-29 に絞りを撤回、
///   Refs ohishi-exp/nuxt-dtako-admin#501)。かつて 休息/休憩/運行開始/運行終了 の
///   4 種に絞っていた (105,771 行 → 22,092 行) が、**単一乗務員経路と値が割れる**
///   事故を 2 度起こした — #167 (拾った運行の終わりが経路で変わる) と、ある乗務員
///   03-26 (終業打刻後の運転イベントが見えず `unpunched_ops_shift` が不発、
///   紙 979 に対し 864 で +115 の未説明差)。この SQL は in-process 消費
///   (`kosoku_daily_all`) で Tunnel を
///   通らないため、行数よりも**両経路の同値**を優先する
/// - **`/api/kintai/events` は絞らない。** あちらは数字がおかしいときに 1 名分の生時系列へ
///   降りるための口で、種別を絞ると調査ができなくなる (#116 の「解釈しない読み出し口」)
/// - 2 ブランチに分ける理由・`COALESCE` で 1 本にまとめてはいけない理由は
///   [`EVENTS_SQL`] と同じ
pub const ALL_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name)
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`
  FROM dtako_events e
 WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`
  FROM dtako_events e
 WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
 ORDER BY driver_id, datetime, source
"#;

/// 打刻 2 表だけを 1 乗務員ぶん読む (Refs #205 の 04b)。
///
/// [`EVENTS_SQL`] から **`dtako_events` の 2 ブランチと `dtako_cars` の JOIN を
/// 落とした**もの。列は `kintai_repo::EventRow` と同じ 7 列のままで、`vehicle` は常に NULL —
/// 車輌名は `dtako_events` 側にしか無く、打刻には最初から付いていない。
///
/// 落としてよい理由は `kintai_push::PUSHED_SOURCES` が
/// `timecard` / `dtako` の 2 つだけだから。`dtako_events` の行は読んでも
/// `kintai_push::parse_row` が `NotPushedSource` で捨てるので、
/// **捨てる行のために月ぶんの最大表を読んで Tunnel 越しに転送していた**ことになる。
/// `ALL_EVENTS_SQL` の実測 (1.20 秒 → 0.25 秒) が示すとおり、支配的なのは
/// `dtako_cars` の引き当てと 23 桁の `運行NO` の転送。
///
/// `unko_no` は残す — `time_card_dtako.unko_no` から取れるので、
/// 「どの運行のイベントか」は失われない。
///
/// **`休息` も読まない** (`kintai_push::NOT_CARRIED_STATES`)。開始 (20) と
/// 終了 (21) が同じ名前で来るため、`dtako_events` を運ばないこの経路では読み分けが
/// できない。畳むのに要る休息区間は GCP が alc から直接引く。
///
/// 落とすのは**解決後の名前**であって `state` の番号ではない。`event_name` は
/// 自由記述なので、番号で落とすと「state 20 だが別の名前」の行まで消える。
/// 名前で落とせば、知らない値が来たときは今までどおり `unknown_states` に出る。
pub const TIMECARD_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.id = :driver AND d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.driver_id = :driver AND t.datetime >= :from AND t.datetime < :to
   AND COALESCE(t.event_name, s.name) <> '休息'
 ORDER BY datetime, source
"#;

/// 期間ぶんの打刻を**全乗務員まとめて** 1 回で読む (Refs #205 の 04b)。
///
/// [`TIMECARD_EVENTS_SQL`] から乗務員の絞り込みを外しただけ。**列は同じ 7 列**なので、
/// 1 名ずつ読んだときと `raw` が一致する = 既に書いた日の署名が変わらない。
/// (`ALL_EVENTS_SQL` は速さのために `運行NO` を落としているので、あれは使えない。)
///
/// ## なぜ 1 名ずつ引かないのか
///
/// 2026-07-31 の実測: 署名の引き当てを乗務員ごとに 1 往復していたレグが
/// **33.6 秒 / 全体の 94%** を占めていた (94 名 × 約 358ms)。往復の回数が費用で、
/// 転送量ではない — 同じ月の全打刻は Tunnel 越しでも 1.3 秒で運べている。
///
/// `ORDER BY driver_id` は受け側が乗務員ごとに束ねるため。
pub const TIMECARD_WINDOW_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to AND t.driver_id > 0
   AND COALESCE(t.event_name, s.name) <> '休息'
 ORDER BY driver_id, datetime, source
"#;

/// 対象期間に打刻がある乗務員CD だけを昇順で返す (Refs #205 の 04b)。
///
/// **行を返さない。** 乗務員の洗い出しに `ALL_EVENTS_SQL` を使うと、CD の集合しか
/// 使わないのに月ぶんの全行を JSON にして捨てることになる。`UNION` (`UNION ALL`
/// ではない) が重複を潰すので、呼び出し側での dedup も要らない。
///
/// **`> 0` で絞る。** 乗務員CD を持たない行が 0 として出てきて、relay の
/// 1 ページぶんの枠を食っていた (2026-06 の dry-run で実測)。
pub const TIMECARD_DRIVERS_SQL: &str = r#"
SELECT d.id AS driver_id
  FROM time_card_dstate d
 WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0
UNION
SELECT t.driver_id
  FROM time_card_dtako t
 WHERE t.datetime >= :from AND t.datetime < :to AND t.driver_id > 0
 ORDER BY driver_id
"#;

/// 窓の中の**運行終了**と、その `unko_no` (Refs ohishi-exp/nuxt-dtako-admin#1123)。
/// 月初をまたぐ運行を見つける材料 ([`crate::window::month_head_anchors`] の運行の行)。
///
/// 運行開始日時は `unko_no` の先頭 12 桁から Rust 側で取る — `state = 10` の行と
/// 突き合わせない (運行開始の行が無い運行でも引けるように)。月初より前かの判定も
/// Rust 側。`datetime` の索引で窓を絞るのは [`ALL_EVENTS_SQL`] と同じ。
pub const HEAD_RUN_ENDS_SQL: &str = r#"
SELECT t.driver_id, t.unko_no
  FROM time_card_dtako t
 WHERE t.state = 11 AND t.datetime >= :from AND t.datetime < :to
   AND t.driver_id > 0 AND t.unko_no IS NOT NULL
"#;

/// 窓に打刻がある乗務員ごとの「月初時点の打刻の姿」(Refs
/// ohishi-exp/nuxt-dtako-admin#1123、[`crate::window::HeadPunch`])。
///
/// 相関サブクエリは 3 本とも `time_card_dstate` を乗務員 (`id`) と `datetime` で
/// 引く ([`EVENTS_SQL`] の単一乗務員ブランチと同じ経路)。`MAX(… < :from)` は
/// 月初から遡って最初に当たった 1 行で止まる。同時刻の始業・終業は始業を先に置く
/// (`state` 30 < 31)。
pub const HEAD_PUNCHES_SQL: &str = r#"
SELECT f.id AS driver_id,
       (SELECT IF(s.state = 30, '始業', '終業')
          FROM time_card_dstate s
         WHERE s.id = f.id AND s.state IN (30, 31)
           AND s.datetime >= :from AND s.datetime < :to
         ORDER BY s.datetime, s.state
         LIMIT 1) AS first_state,
       (SELECT DATE_FORMAT(MAX(b.datetime), '%Y-%m-%d %H:%i:%s')
          FROM time_card_dstate b
         WHERE b.id = f.id AND b.state = 30 AND b.datetime < :from) AS last_start,
       (SELECT DATE_FORMAT(MAX(e.datetime), '%Y-%m-%d %H:%i:%s')
          FROM time_card_dstate e
         WHERE e.id = f.id AND e.state = 31 AND e.datetime < :from) AS last_end
  FROM (SELECT DISTINCT d.id
          FROM time_card_dstate d
         WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0) f
"#;

/// 休息だけを `運行NO` 付きで両表から読む (Refs #205 の 41)。
///
/// [`EVENTS_SQL`] から **`timecard` のブランチと `dtako_cars` の JOIN を落とし、
/// `休息` に絞った**もの。列は 6 つで `vehicle` を持たない — 突合に要るのは
/// 「どの運行の休息が何時か」だけで、車輌名は使わない。
///
/// - **`ALL_EVENTS_SQL` と違い `運行NO` を返す。** これが鍵なので落とせない。
///   代わりに `dtako_cars` の引き当てを外し、`休息` で絞って行数を落とす
///   (`ALL_EVENTS_SQL` の実測で支配的だったのは JOIN と全イベントの転送)
/// - **絞りは解決後の名前で行う** (`COALESCE(t.event_name, s.name) = '休息'`)。
///   `state` の番号で絞ると「state 20 だが別の名前」の行を取り違える
///   (`kintai_push::NOT_CARRIED_STATES` と同じ理由)
/// - `dtako_events` を 2 ブランチに分ける理由・`COALESCE` で 1 本にまとめては
///   いけない理由は [`EVENTS_SQL`] と同じ
/// - `:driver` が NULL なら全乗務員 (`fetch_rest_events_between` の `driver: None`)
pub const REST_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       t.driver_id                                  AS driver_id,
       'dtako'                                      AS source,
       COALESCE(t.event_name, s.name)               AS state,
       t.unko_no                                    AS unko_no
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to
   AND (:driver IS NULL OR t.driver_id = :driver)
   AND COALESCE(t.event_name, s.name) = '休息'
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`
  FROM dtako_events e
 WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
   AND (:driver IS NULL OR e.`対象乗務員CD` = :driver)
   AND e.`イベント名` = '休息'
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`
  FROM dtako_events e
 WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
   AND (:driver IS NULL OR e.`対象乗務員CD` = :driver)
   AND e.`イベント名` = '休息'
 ORDER BY unko_no, datetime, source
"#;

/// 期間にかかる運行と、その**読取日** (Refs #205 の 42)。
///
/// `dtako_rows` は**1 運行 × 対象乗務員で 1 行**の表で、`読取日` / `運行日` /
/// `運行NO` / `対象乗務員CD` / `出庫日時` / `帰庫日時` を全部持っている
/// (`yhonda-ohishi/nginx` の `Model/Entity/DtakoRow.php` / `Model/Table/DtakoRowsTable.php`)。
/// JOIN も集約も要らない。
///
/// - **期間の条件は 3 つの OR。** 出庫・帰庫・運行日のどれかが窓に入れば拾う。
///   - `出庫日時` / `帰庫日時` の 2 本立ては [`FERRY_SQL`] と同じで、上流の
///     「当月に出庫**または**帰庫した運行」を写したもの
///   - `運行日` を足すのは #205 の 38 と同じ理由 — **日時だけだと月末の運行が落ちる**
///     (alc も `reading_date` 単独から `reading_date OR operation_date` へ直した)
///   - **`COALESCE` で 1 本にまとめないこと。** 関数適用で索引が効かなくなる
///     ([`EVENTS_SQL`] が 0.2 秒 → 4 分になった罠と同じ)。列ごとに条件を分ける
/// - **`読取日` で絞らない。** 読取日は運行終了の後に付く (実測: 運行日 06-24 →
///   読取日 07-06) ので、読取日で窓を切ると月末の運行が丸ごと落ちる
/// - `:driver` が NULL なら全乗務員
///
/// ## `kintai_reader` の GRANT は未確認 (Refs #205 の 42)
///
/// `dtako_rows` 自体は [`FERRY_SQL`] が既に読んでいる (`運行NO` / `対象乗務員CD` /
/// `帰庫日時` / `出庫日時`) が、**`読取日` / `運行日` が GRANT に入っているかは
/// 確かめられていない**。`dtako_ferry_rows` は料金列があるため列単位 GRANT で、
/// 列を足すときは GRANT の追加が要る (そちらの docs)。`dtako_rows` が表単位か
/// 列単位かは読み取れていない。
///
/// **外れても黙って 0 件にはならない** — `map_repo_err` が MariaDB のエラー文を
/// そのまま載せて 502 になる。落ちるのはこの口だけ。
pub const OPERATION_READING_DATES_SQL: &str = r#"
SELECT r.`対象乗務員CD`                                 AS driver_cd,
       r.`運行NO`                                       AS unko_no,
       DATE_FORMAT(r.`読取日`, '%Y-%m-%d')              AS reading_date,
       DATE_FORMAT(r.`運行日`, '%Y-%m-%d')              AS run_date,
       DATE_FORMAT(r.`出庫日時`, '%Y-%m-%d %H:%i:%s')   AS departure_at,
       DATE_FORMAT(r.`帰庫日時`, '%Y-%m-%d %H:%i:%s')   AS return_at
  FROM dtako_rows r
 WHERE (:driver IS NULL OR r.`対象乗務員CD` = :driver)
   AND (   (r.`出庫日時` >= :from AND r.`出庫日時` < :to)
        OR (r.`帰庫日時` >= :from AND r.`帰庫日時` < :to)
        OR (r.`運行日` >= DATE(:from) AND r.`運行日` < DATE(:to)) )
 ORDER BY r.`対象乗務員CD`, r.`運行NO`
"#;

/// フェリー区間 (Refs #146)。
///
/// - **`dtako_ferry_rows` は 3 列しか読めない** (`運行NO` / `開始日時` / `終了日時`)。
///   このテーブルは `標準料金` / `契約料金` を持つので、`kintai_reader` には列単位で
///   GRANT してある。列を足すときは GRANT の追加が要る
/// - 乗務員は `dtako_rows.対象乗務員CD` から取る。**`dtako_ferry_rows.乗務員CD1` は
///   使わない** — 2 名乗務では運行まるごとが別の乗務員のまま記録される (`EVENTS_SQL`
///   と同じ理由)
/// - 突合の鍵は `運行NO`。上流は `substr($dtako_row->運行NO, 0, 22) . "1"` で引いて
///   いるので、`CONCAT(LEFT(r.運行NO, 22), '1')` で同じものを作る
/// - 運行側も月で絞る。上流は当月に出庫 **または** 帰庫した運行だけを回しているので、
///   その条件も写す (ferry の月内条件だけだと、月をまたいだ運行のフェリーを拾って
///   紙と数字がずれる)
/// - `:driver` が NULL なら全乗務員 (`fetch_ferry_between` の `driver: None`)
pub const FERRY_SQL: &str = r#"
SELECT DATE_FORMAT(f.`開始日時`, '%Y-%m-%d %H:%i:%s') AS start_datetime,
       DATE_FORMAT(f.`終了日時`, '%Y-%m-%d %H:%i:%s') AS end_datetime,
       r.`対象乗務員CD`                               AS driver_id
  FROM dtako_ferry_rows f
  JOIN dtako_rows r ON f.`運行NO` = CONCAT(LEFT(r.`運行NO`, 22), '1')
 WHERE f.`開始日時` >= :from AND f.`開始日時` < :to
   AND (:driver IS NULL OR r.`対象乗務員CD` = :driver)
   AND (   (r.`帰庫日時` >= :from AND r.`帰庫日時` < :to)
        OR (r.`出庫日時` >= :from AND r.`出庫日時` < :to) )
 ORDER BY r.`対象乗務員CD`, f.`開始日時`
"#;

/// 全ソーステーブルのマーカーを 1 statement で取る。
///
/// - 範囲 (`:from`/`:to` = `month_range`、`:mfrom`/`:mto` = `exact_month_range`) と
///   列は、対応するデータクエリ (`EVENTS_SQL` / `ALL_EVENTS_SQL` / `FERRY_SQL` /
///   dailyJson) が読むものの**上位集合**に揃える。絞り (dailyJson の state 30/31 等)
///   は掛けない — 広い分は安全側
/// - `dtako_events` は CRC を使わない (モジュール docs の「例外」参照)。
///   `開始日時 >= :efrom` (前月初) の 1 ブランチで `EVENTS_SQL` の 2 ブランチ両方の
///   行集合を覆う — 第 4 ブランチの「前月開始・当月終了」行も `開始日時` は前月
///   範囲内にあるため。`COUNT(*)` + `MAX(e.id)` は `開始日時` インデックス
///   (id を含む) のオンリースキャンで、行本体を読まない (EXPLAIN: `Using index`)
/// - `dtako_ferry_rows` は `kintai_reader` に**列単位 GRANT** (`運行NO` / `開始日時` /
///   `終了日時` のみ)。`COUNT(*)` ではなく `COUNT(f.``開始日時``)` を使い、他の列
///   (`標準料金` 等) には一切触らない
/// - 日時は `DATE_FORMAT` で文字列化してから CRC に入れる — driver の時刻型と
///   timezone 解釈を fingerprint に持ち込まない (`EVENTS_SQL` と同じ理由)
pub const VERSION_SQL: &str = r#"
SELECT 'time_card_dstate' AS source,
       CAST(COUNT(*) AS CHAR) AS cnt,
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           d.id, DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s'), d.state,
           DATE_FORMAT(d.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR) AS fp
  FROM time_card_dstate d
 WHERE d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT 'time_card_dtako',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           t.driver_id, DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'), t.state,
           t.event_name, t.unko_no,
           DATE_FORMAT(t.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM time_card_dtako t
 WHERE t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT 'time_card_dtako_state',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', s.id, s.name))), 0) AS CHAR)
  FROM time_card_dtako_state s
UNION ALL
SELECT 'dtako_events',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(MAX(e.id), 0) AS CHAR)
  FROM dtako_events e
 WHERE e.`開始日時` >= :efrom AND e.`開始日時` < :to
UNION ALL
SELECT 'dtako_cars',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', c.`車輌CD`, c.`車輌名`))), 0) AS CHAR)
  FROM dtako_cars c
UNION ALL
SELECT 'dtako_ferry_rows',
       CAST(COUNT(f.`開始日時`) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           f.`運行NO`,
           DATE_FORMAT(f.`開始日時`, '%Y-%m-%d %H:%i:%s'),
           DATE_FORMAT(f.`終了日時`, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM dtako_ferry_rows f
 WHERE f.`開始日時` >= :mfrom AND f.`開始日時` < :mto
UNION ALL
SELECT 'dtako_rows',
       CAST(COUNT(r.`運行NO`) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           r.`運行NO`, r.`対象乗務員CD`,
           DATE_FORMAT(r.`出庫日時`, '%Y-%m-%d %H:%i:%s'),
           DATE_FORMAT(r.`帰庫日時`, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM dtako_rows r
 WHERE (r.`出庫日時` >= :mfrom AND r.`出庫日時` < :mto)
    OR (r.`帰庫日時` >= :mfrom AND r.`帰庫日時` < :mto)
UNION ALL
SELECT 'daily_report_other_detail',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           o.driver_id, o.act_date, o.detail,
           DATE_FORMAT(o.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM daily_report_other_detail o
 WHERE o.report_type = 'kyuka' AND o.act_date >= :mfrom AND o.act_date < :mto
UNION ALL
SELECT 'drivers',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', v.id, v.name, v.bumon))), 0) AS CHAR)
  FROM drivers v
UNION ALL
SELECT 'offices',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', ofc.id, ofc.name, ofc.bumon_code_id))), 0) AS CHAR)
  FROM offices ofc
UNION ALL
SELECT 'time_card_non_legal_holiday',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CAST(h.p_date AS CHAR))), 0) AS CHAR)
  FROM time_card_non_legal_holiday h
 WHERE h.p_date >= :mfrom AND h.p_date < :mto
"#;
