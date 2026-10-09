//! 一番星 (CAPE#01) に流す 6 本の SQL 文 (Refs ohishi-exp/rust-ichibanboshi#322)。オンプレ版 (repo ルートの
//! `src/repo.rs`、bb8 + tiberius) と Worker (`workers/ichiban/worker`、1 リクエスト 1 接続) の両方が
//! この文字列をそのまま流す — 並走期間に応答を比べるので、SQL 文はここ 1 か所に置く。
//!
//! **列の並びは行の詰め直し (オンプレ `src/repo.rs` の `rows_to_*` と Worker) と 1 対 1。変えるときは両方直す。**
//! 列番号は手で対応させているので、列を足す・並べ替えると型不一致で静かに空文字 / 0 に化ける。
//!
//! 売上集計の式 (月計一致ルール、CLAUDE.md) は一字も変えない: 自車 `税抜金額+税抜割増+税抜実費-値引`、
//! 傭車 `税抜傭車金額+税抜傭車割増+税抜傭車実費-傭車値引`。`金額` 列は使わない。
//! 自車/傭車の判定は `傭車先C = '000000'` (6 桁ゼロ) で、ロジック層 (`build_vehicle_daily_rows`) が行う。

/// `/health` の生死確認。
pub const HEALTH_SQL: &str = "SELECT 1";

/// `/api/sales/departments`。列: 部門C, 部門N。
pub const DEPARTMENTS_SQL: &str =
    "SELECT [部門C], ISNULL([部門N], '') FROM [部門ﾏｽﾀ] ORDER BY [部門C]";

/// `/api/vehicles`。列: 車種C, 車種N。
pub const VEHICLES_SQL: &str =
    "SELECT [車種C], ISNULL([車種N], '') FROM [車種ﾏｽﾀ] ORDER BY [車種C]";

/// `/api/employees`。列: 社員C, 社員N, 社員R。
///
/// 社員C は数値型の可能性があるため CONVERT で varchar に寄せる。
/// 同一 社員C の複数行は GROUP BY で 1 行に潰す (MAX は NULL を無視するので
/// 外側の ISNULL で空文字に落とす)。
pub const EMPLOYEES_SQL: &str = "SELECT CONVERT(varchar(20), [社員C]) AS [社員C], \
                 ISNULL(MAX([社員N]), '') AS [社員N], \
                 ISNULL(MAX([社員R]), '') AS [社員R] \
                 FROM [社員ﾏｽﾀ] GROUP BY [社員C] ORDER BY [社員C]";

/// `/api/sales/vehicle-daily` の `SELECT TOP n` より後ろ。全体は [`vehicle_daily_sql`] で組む。
///
/// 積地・卸地は 2 系統返す (#12 実機調査): 発地域C/着地域C → 地域ﾏｽﾀ.地域N は
/// 市区町村レベルまで届くマスタ由来値 (surcharge_base と違い県へ丸めない)。
/// 発地N/着地N は自由入力・粒度不揃い (docs/plan-unchin-rate-list.md #57 実機
/// 調査) だが施設名等の補助信号として残す (unchin.rs と同型)。得意先名の解決は
/// surcharge_base と同じくスカラサブクエリ (TOP 1、得意先C 単独)。
/// 自車/傭車の金額は両方取得し、ロジック層 (build_vehicle_daily_rows) が
/// 傭車先C で選択する (月計一致ルール、CLAUDE.md)。
/// 品名(品名C/品名N)・数量・単価・単位は nuxt-dtako-admin#330 実データ検証で追加
/// 要望が出た項目 (同一日でも複数明細で単価が異なりうるため)。schema確認済み
/// (数量/単価は decimal NOT NULL、単位は varchar nullable)。
///
/// vehicle/customer/origin/dest は全て任意 (#79、nuxt-dtako-admin#330 PR5
/// 「類似運行検索」が車輌を横断して積地・卸地/得意先で絞る必要があるため)。
/// `(@Pn IS NULL OR ...)` 形で毎回 7 パラメータ固定のバインドにし、動的な
/// クエリ文字列組み立て (injection リスク) を避ける。呼び出し側 (handler) が
/// 絞り込みの最低 1 つ必須をバリデーションする (全件スキャン防止)。
/// origin/dest は 地域ﾏｽﾀ由来 (市区町村レベル) と自由入力のどちらかに部分一致
/// すれば hit とする (NFKC 正規化等の高度な突合は消費側の責務)。
///
/// バインドの順: @P1 from, @P2 to, @P3 vehicle, @P4 customer, @P5 `%origin%`,
/// @P6 `%dest%`, @P7 driver (@P7 は後から足したので番号と WHERE の並びがずれている)。
pub const VEHICLE_DAILY_SQL_BODY: &str = "t.[売上年月日], \
             ISNULL(t.[車輌C], ''), \
             ISNULL(t.[得意先C], ''), \
             ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = t.[得意先C]), ''), \
             ISNULL((SELECT TOP 1 o.[地域N] FROM [地域ﾏｽﾀ] o WHERE o.[地域C] = t.[発地域C]), ''), \
             ISNULL((SELECT TOP 1 d.[地域N] FROM [地域ﾏｽﾀ] d WHERE d.[地域C] = t.[着地域C]), ''), \
             ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
             ISNULL(t.[傭車先C], ''), \
             ISNULL(t.[税抜金額],0)+ISNULL(t.[税抜割増],0)+ISNULL(t.[税抜実費],0)-ISNULL(t.[値引],0), \
             ISNULL(t.[税抜傭車金額],0)+ISNULL(t.[税抜傭車割増],0)+ISNULL(t.[税抜傭車実費],0)-ISNULL(t.[傭車値引],0), \
             ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
             ISNULL(t.[数量], 0), ISNULL(t.[単価], 0), ISNULL(t.[単位], ''), \
             CONCAT(CONVERT(varchar(8), t.[管理年月日], 112), '-', t.[管理C]), \
             ISNULL(t.[車輌H], ''), ISNULL(t.[運転手C], ''), \
             ISNULL((SELECT TOP 1 s.[社員N] FROM [社員ﾏｽﾀ] s WHERE s.[社員C] = t.[運転手C]), ''), \
             ISNULL(t.[請求K], '') \
             FROM [運転日報明細] t \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND (@P3 IS NULL OR t.[車輌C] = @P3) \
               AND (@P7 IS NULL OR t.[運転手C] = @P7) \
               AND (@P4 IS NULL OR t.[得意先C] = @P4) \
               AND (@P5 IS NULL \
                 OR ISNULL((SELECT TOP 1 om.[地域N] FROM [地域ﾏｽﾀ] om WHERE om.[地域C] = t.[発地域C]), '') LIKE @P5 \
                 OR ISNULL(t.[発地N], '') LIKE @P5) \
               AND (@P6 IS NULL \
                 OR ISNULL((SELECT TOP 1 dm.[地域N] FROM [地域ﾏｽﾀ] dm WHERE dm.[地域C] = t.[着地域C]), '') LIKE @P6 \
                 OR ISNULL(t.[着地N], '') LIKE @P6) \
             ORDER BY t.[売上年月日], t.[管理C]";

/// `/api/costs/vehicle-daily` の `SELECT TOP n` より後ろ。全体は [`costs_daily_sql`] で組む。
///
/// 経費名 (経費ﾏｽﾀ.経費N) と経費種別名 (経費種別ﾏｽﾀ.経費種別N) は
/// vehicle_daily と同じくスカラサブクエリ (TOP 1) で引く。LEFT JOIN にすると
/// マスタ側が同一コードで複数行を持つとき明細が N 重に返る。
/// **経費ﾏｽﾀ は 経費種別C + 経費C の複合キーで引く** (これがマスタの主キー。
/// 例 01+0621=軽油費、02+0631=車検整備費)。実測 (2026-08-22) では 経費C は
/// マスタ 51 行で一意なので単独でも当たるが、それだと将来 経費C が別種別で
/// 再利用されたとき **amount は正しいまま cost_name だけ静かにすり替わる**。
/// 複合にしても引けなくなる行は 0 件 (経費明細 100 行で確認済み) なので、
/// 得るものだけがある。経費種別ﾏｽﾀ は 経費種別C 単独キーなのでそのまま。
/// 金額は 税抜金額 (金額 は実費の税処理で消費税の含み方が違う。CLAUDE.md)。
/// 軽油引取税は 税抜金額 に含まれない別立てなので独立に返す。
/// 未払先名 (未払先ﾏｽﾀ.未払先N) も同じく TOP 1 のスカラサブクエリ (#760-11)。
/// 引き当ては 未払先C + 未払先H の複合 (車輌C/車輌H と同じ形。これが
/// マスタの主キーかどうかは INFORMATION_SCHEMA で要確認だが、TOP 1 なので
/// 複合キーでなくても明細が N 重に返ることは構造的に無い)。
/// vehicle/driver/kind は全て任意だが、`(@Pn IS NULL OR ...)` 形で毎回 5
/// パラメータ固定のバインドにし、動的なクエリ文字列組み立て (injection リスク)
/// を避ける。呼び出し側 (handler) が「最低 1 つ必須」を検証する (全件スキャン防止)。
///
/// バインドの順: @P1 from, @P2 to, @P3 vehicle, @P4 driver, @P5 kind。
pub const COSTS_DAILY_SQL_BODY: &str = "t.[運行年月日], \
             ISNULL(t.[車輌C], ''), ISNULL(t.[車輌H], ''), ISNULL(t.[運転手C], ''), \
             ISNULL(t.[経費C], ''), \
             ISNULL((SELECT TOP 1 m.[経費N] FROM [経費ﾏｽﾀ] m \
               WHERE m.[経費種別C] = t.[経費種別C] AND m.[経費C] = t.[経費C]), ''), \
             ISNULL(t.[経費種別C], ''), \
             ISNULL((SELECT TOP 1 k.[経費種別N] FROM [経費種別ﾏｽﾀ] k WHERE k.[経費種別C] = t.[経費種別C]), ''), \
             ISNULL(t.[数量], 0), ISNULL(t.[単価], 0), \
             ISNULL(t.[税抜金額], 0), ISNULL(t.[軽油引取税], 0), ISNULL(t.[KM], 0), \
             ISNULL(t.[固定経費K], ''), \
             CONCAT(CONVERT(varchar(8), t.[管理年月日], 112), '-', t.[管理C]), \
             ISNULL(t.[備考], ''), ISNULL(t.[未払先C], ''), ISNULL(t.[未払先H], ''), \
             ISNULL((SELECT TOP 1 v.[未払先N] FROM [未払先ﾏｽﾀ] v \
               WHERE v.[未払先C] = t.[未払先C] AND v.[未払先H] = t.[未払先H]), ''), \
             t.[入力年月日] \
             FROM [経費明細] t \
             WHERE t.[運行年月日] >= @P1 AND t.[運行年月日] < @P2 \
               AND (@P3 IS NULL OR t.[車輌C] = @P3) \
               AND (@P4 IS NULL OR t.[運転手C] = @P4) \
               AND (@P5 IS NULL OR t.[経費種別C] = @P5) \
             ORDER BY t.[運行年月日], t.[管理C]";

/// `/api/sales/vehicle-daily` の SQL 全体。`limit` は 1..=5000 に丸める。
pub fn vehicle_daily_sql(limit: i32) -> String {
    let n = limit.clamp(1, 5000);
    format!("SELECT TOP {n} {VEHICLE_DAILY_SQL_BODY}")
}

/// `/api/costs/vehicle-daily` の SQL 全体。`limit` は 1..=5000 に丸める。
pub fn costs_daily_sql(limit: i32) -> String {
    let n = limit.clamp(1, 5000);
    format!("SELECT TOP {n} {COSTS_DAILY_SQL_BODY}")
}

/// vehicle-daily の積地・卸地 (@P5 / @P6) に渡す部分一致の LIKE パターン。
pub fn like_pattern(s: Option<&str>) -> Option<String> {
    s.map(|s| format!("%{s}%"))
}
