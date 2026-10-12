//! 一番星 SQL Server (CAPE#01) の `[運転日報明細]` を読む SQL と、結果の列 → [`UriageRow`]
//! (root の `src/repo.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! **SQL の文字列・英数字の whitelist・NULL の埋め方・列の並びは 1 文字も変えない** (`tests/golden.rs` が
//! 代表的な入力で生成した文字列の sha256 を固定している)。接続と行の取り出し (tiberius / VPC の TDS) は呼び手が持つ。

use crate::compute::UriageRow;

/// `[運転日報明細]` から `compute_person_sum` の入力行を取る SQL を組み立てる。
///
/// PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を 1:1 で再現する
/// (傭車 / 営業所傭車 / 傭車傭車 / sql_from_other_with_bumon / sql_from_other)。`bumon_codes` は
/// 受注/稼動部門の IN 条件、`persons_id_list` は `UriageJyuchuDisplayPersons_id` 相当。
/// 引数は `@P1` = 運行年月日の下限、`@P2` = 上限 (どちらも `YYYY-MM-DD`、含む)。
///
/// 英数字 (と `_`) だけに絞った後の部門が 1 つも無ければ `None` (= 0 行)。
/// **呼び手は `bumon_codes` が空かどうかを接続の前に、`None` を接続の後に見る** (オンプレ版の順序。
/// pool が死んでいて部門が全部英数字以外のとき、503 を返す挙動を保つため)。
pub fn uriage_rows_sql(bumon_codes: &[String], persons_id_list: &[i32]) -> Option<String> {
    // 受注部門 IN (...) を動的に組む。bumon_codes は呼び出し側で whitelist 済み
    // (営業所マスタから引いた `'010'`,`'011'` 等の固定形式) のため SQL injection
    // 上は安全だが、念のため英数字のみに絞ってから組み立てる。
    let safe_bumon: Vec<String> = bumon_codes
        .iter()
        .filter(|c| !c.is_empty() && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_'))
        .cloned()
        .collect();
    if safe_bumon.is_empty() {
        return None;
    }
    let bumon_in = safe_bumon
        .iter()
        .map(|c| format!("'{}'", c))
        .collect::<Vec<_>>()
        .join(",");

    // 入力担当C IN (...) — persons_id_list は数値なのでそのまま組み立てる
    // (型は i32、untrusted 入力ではないが念のため format で固定)。
    // 空リストでも SQL は valid であるべき: IN () は SQL Server で構文エラーに
    // なるので NULL (= 全件 false) で埋める fallback を入れる。
    let persons_in = if persons_id_list.is_empty() {
        "NULL".to_string()
    } else {
        persons_id_list
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(",")
    };

    // PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を再現。
    //
    // PHP L1769-1791:
    //   foreach ($UriageJyuchuDisplayPersons_id as $ddp) {
    //       $this->make_array(['入力担当C in' => $ddp]);  // sql_options の 3 ケース
    //   }
    //   sql_from_other          → 受注∉ AND 稼動∈ AND 傭車先≠000000
    //   sql_from_other_with_bumon → 受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
    //
    // sql_options (PHP L1827-1850) は `make_yosha_sql` を base に 3 ケース:
    //   - 傭車:       稼動∈ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=0
    //   - 営業所傭車: 稼動∉ AND 配車K=0 AND 入力担当C ∈ persons    → 横横=1
    //   - 傭車傭車:   稼動∉ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=1
    //
    // つまり 5 つの subquery を UNION ALL する (PHP は array_push なので重複なし
    // のはずだが、PHP の row レベルでも 5 case 互いに排他 = 配車K=0/1 + 稼動部門
    // ∈/∉ で組み分けされている)。
    //
    // 香月 NG 行 (#762、user 2026-06-30) の原因:
    //   営業所 10、入力担当C=1180 (∈ persons)、傭車先=000000、配車K=9 (未配車)、
    //   受注∈ AND 稼動∉。PHP 5 ケース全部 hit せず → $sum 0 円。
    //   旧 Rust SQL は `(受注∈ AND (稼動∉ OR 配車K=1))` で拾ってしまっていた。
    //   配車K=9 は sql_options の 3 ケース (配車K=0/1) からも漏れ、
    //   sql_from_other_with_bumon は 入力担当C ∉ persons が必要で hit せず。
    //
    // 細部:
    // - 品名N の調整行除外は全角空白 U+3000 (PHP L1708-1709 と同形)
    // - `日報K != 3` (PHP L1710)
    // - `請求K=2 → 備考2='表示' or LIKE '売上%'` (PHP L1712/1738/1760 OR superset)
    // - `NOT (請求K='1' AND 備考2='請求のみ')` (PHP L1713)
    // - 社員R は **TOP 1 スカラサブクエリ** で引く (社員ﾏｽﾀ 複数行/社員C による
    //   JOIN ファンアウト防止)
    // - 順序は print テンプレ表示順 (運行年月日 ASC, 管理C ASC, LC ASC)
    //   同点は出力列で割って全順序にする (fingerprint を揺らさないため)
    // - 請求K / 入力担当C は varchar の可能性あり `TRY_CAST(... AS INT)` で int 化
    // - 共通 WHERE 句を repeat する代わりに subquery を inline で書き、外側で
    //   ORDER BY する形 (UNION ALL は ORDER BY を最外殻に置く制約があるため)
    let select_cols = "\
                 t.[横横] AS [横横], \
                 ISNULL(TRY_CAST(t.[請求K] AS INT), 0) AS [請求K], \
                 ISNULL(t.[備考2], '') AS [備考2], \
                 ISNULL(TRY_CAST(t.[入力担当C] AS INT), 0) AS [入力担当C], \
                 ISNULL(t.[稼動部門], '') AS [稼動部門], \
                 ISNULL(t.[金額], 0) AS [金額], \
                 ISNULL(t.[値引], 0) AS [値引], \
                 ISNULL(t.[割増], 0) AS [割増], \
                 ISNULL(t.[実費], 0) AS [実費], \
                 ISNULL(t.[傭車金額], 0) AS [傭車金額], \
                 ISNULL(t.[傭車値引], 0) AS [傭車値引], \
                 ISNULL(t.[傭車割増], 0) AS [傭車割増], \
                 ISNULL(t.[傭車実費], 0) AS [傭車実費], \
                 ISNULL((SELECT TOP 1 e.[社員R] FROM [社員ﾏｽﾀ] e WHERE e.[社員C] = t.[入力担当C]), '') AS [社員R], \
                 ISNULL(t.[傭車先C], '000000') AS [傭車先C], \
                 CONVERT(varchar(10), t.[運行年月日], 23) AS [運行年月日], \
                 CONVERT(varchar(10), t.[売上年月日], 23) AS [売上年月日], \
                 CONCAT(ISNULL(t.[得意先C], ''), '-', ISNULL(t.[得意先H], '')) AS [得意先複合キー], \
                 ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c \
                   WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]), '') AS [得意先N], \
                 CONCAT(ISNULL(t.[傭車先C], ''), '-', ISNULL(t.[傭車先H], '')) AS [傭車先複合キー], \
                 ISNULL((SELECT TOP 1 y.[傭車先N] FROM [傭車先ﾏｽﾀ] y \
                   WHERE y.[傭車先C] = t.[傭車先C] AND y.[傭車先H] = t.[傭車先H]), '') AS [傭車先N], \
                 t.[管理C] AS [管理C], t.[LC] AS [LC]";

    // 最外殻の ORDER BY。表示順の 3 列の後ろに SELECT の別名を select_cols の並びで全部足す
    let order_by = "\
                 ORDER BY t.[運行年月日] ASC, t.[管理C] ASC, t.[LC] ASC, \
                 [横横] ASC, [請求K] ASC, [備考2] ASC, [入力担当C] ASC, [稼動部門] ASC, \
                 [金額] ASC, [値引] ASC, [割増] ASC, [実費] ASC, \
                 [傭車金額] ASC, [傭車値引] ASC, [傭車割増] ASC, [傭車実費] ASC, \
                 [社員R] ASC, [傭車先C] ASC, [売上年月日] ASC, \
                 [得意先複合キー] ASC, [得意先N] ASC, [傭車先複合キー] ASC, [傭車先N] ASC";

    // 各 case 共通の WHERE 述語 (日付・品名・日報K・請求K・備考2 系)
    let common_where = "\
                 [品名N] NOT IN ('※\u{3000}請求一括調整明細\u{3000}※', '※\u{3000}傭車一括調整明細\u{3000}※') \
                 AND ISNULL([日報K], 0) != 3 \
                 AND NOT ([請求K] = '1' AND [備考2] = '請求のみ') \
                 AND ([請求K] != '2' OR [備考2] = '表示' OR [備考2] LIKE '売上%') \
                 AND [運行年月日] >= @P1 AND [運行年月日] <= @P2";

    // Case 1: 傭車 (横横=0)
    //   make_yosha_sql + sql_options('傭車'):
    //   受注∈ AND 稼動∈ AND 配車K='1' AND 入力担当C ∈ persons
    let case_yosha = format!(
        "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
    );
    // Case 2: 営業所傭車 (横横=1)
    //   受注∈ AND 稼動∉ AND 配車K='0' AND 入力担当C ∈ persons
    let case_eigyosho_yosha = format!(
        "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '0' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
    );
    // Case 3: 傭車傭車 (横横=1)
    //   受注∈ AND 稼動∉ AND 配車K='1' AND 入力担当C ∈ persons
    let case_yosha_yosha = format!(
        "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
    );
    // Case 4: sql_from_other_with_bumon (横横=1、PHP L1747-1767)
    //   受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
    //   ※ 配車K 条件は無い (= 配車K=9 等もここで拾われる、ただし 入力担当C ∉ persons の人だけ)
    let case_with_bumon = format!(
        "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') = '000000' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) NOT IN ({persons_in}) \
               AND {common_where}"
    );
    // Case 5: sql_from_other (横横=0、PHP L1725-1745)
    //   受注∉ AND 稼動∈ AND 傭車先≠000000
    let case_from_other = format!(
        "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] NOT IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') != '000000' \
               AND {common_where}"
    );

    let query = format!(
        "SELECT {select_cols} FROM ( \
                 {case_yosha} \
                 UNION ALL {case_eigyosho_yosha} \
                 UNION ALL {case_yosha_yosha} \
                 UNION ALL {case_with_bumon} \
                 UNION ALL {case_from_other} \
             ) AS t \
             {order_by}"
    );
    Some(query)
}

// 結果の列番号 (`uriage_rows_sql` の SELECT の並び。最後の 管理C・LC は ORDER BY 用で読まない)
pub const COL_YOKOYOKO: usize = 0;
pub const COL_SEIKYU_K: usize = 1;
pub const COL_BIKO2: usize = 2;
pub const COL_NYURYOKU_TANTO_C: usize = 3;
pub const COL_KADO_BUMON: usize = 4;
pub const COL_KINGAKU: usize = 5;
pub const COL_NEBIKI: usize = 6;
pub const COL_WARIMASHI: usize = 7;
pub const COL_JIPPI: usize = 8;
pub const COL_YOSHA_KINGAKU: usize = 9;
pub const COL_YOSHA_NEBIKI: usize = 10;
pub const COL_YOSHA_WARIMASHI: usize = 11;
pub const COL_YOSHA_JIPPI: usize = 12;
pub const COL_SHAIN_R: usize = 13;
pub const COL_YOSHASAKI_C: usize = 14;
pub const COL_UNKO_DATE: usize = 15;
pub const COL_URIAGE_DATE: usize = 16;
pub const COL_TOKUISAKI_KEY: usize = 17;
pub const COL_TOKUISAKI_N: usize = 18;
pub const COL_YOSHASAKI_KEY: usize = 19;
pub const COL_YOSHASAKI_N: usize = 20;

/// 結果の 1 行から値を取り出す口。読み方 (cp932 の文字列の trim・数値型の揺れ・NULL → 0 / 空文字) は
/// 実装側 (オンプレ版は tiberius の Row、Worker は TDS の行) が持つ。
pub trait Cells {
    /// 文字列の列 (NULL・型違いは空文字、前後の空白は落とす)。
    fn text(&self, idx: usize) -> String;
    /// 整数の列 (NULL・型違いは 0)。
    fn int(&self, idx: usize) -> i32;
    /// 金額の列 (float / numeric / int のどれでも整数へ。NULL・型違いは 0)。
    fn money(&self, idx: usize) -> i64;
}

/// 結果の 1 行 → [`UriageRow`]。
pub fn uriage_row(r: &impl Cells) -> UriageRow {
    UriageRow {
        yokoyoko: r.int(COL_YOKOYOKO),
        seikyu_k: r.int(COL_SEIKYU_K),
        biko2: r.text(COL_BIKO2),
        nyuryoku_tanto_c: r.int(COL_NYURYOKU_TANTO_C),
        kado_bumon: r.text(COL_KADO_BUMON),
        kingaku: r.money(COL_KINGAKU),
        nebiki: r.money(COL_NEBIKI),
        warimashi: r.money(COL_WARIMASHI),
        jippi: r.money(COL_JIPPI),
        yosha_kingaku: r.money(COL_YOSHA_KINGAKU),
        yosha_nebiki: r.money(COL_YOSHA_NEBIKI),
        yosha_warimashi: r.money(COL_YOSHA_WARIMASHI),
        yosha_jippi: r.money(COL_YOSHA_JIPPI),
        shain_r: r.text(COL_SHAIN_R),
        yoshasaki_c: r.text(COL_YOSHASAKI_C),
        // CONVERT(varchar(10), …, 23) で 'YYYY-MM-DD' 文字列が返る (locale 非依存)
        unko_date: r.text(COL_UNKO_DATE),
        uriage_date: r.text(COL_URIAGE_DATE),
        tokuisaki_key: r.text(COL_TOKUISAKI_KEY),
        tokuisaki_n: r.text(COL_TOKUISAKI_N),
        yoshasaki_key: r.text(COL_YOSHASAKI_KEY),
        yoshasaki_n: r.text(COL_YOSHASAKI_N),
    }
}
