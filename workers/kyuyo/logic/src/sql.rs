//! 給与大臣 (OHKEN) に流す SQL 文 (Refs #82 / #322)。オンプレ版 (repo ルートの `src/kyuyo/repo.rs`、bb8 + tiberius) と
//! Worker (`workers/kyuyo/worker/src/repo.rs`、DO から都度接続) の両方がこの文字列をそのまま流す —
//! 並走期間に応答を比べるので、SQL 文と DB 名の検証はここ 1 か所に置く。
//!
//! 純粋関数: 文字列を組み立てて返すだけで、接続も行の読み取りもしない。列の並びは各呼び出し側の
//! 行の詰め方 (`payroll::Raw*Row`) と 1 対 1 なので、列を足す・並べ替えるときは両方の repo を直す。
//!
//! **CAST / CONVERT(…, 120) を落とさない。** KCODE / KAZEI / MEISAI / GENGAKU / INCODE / TAIKEI は実列型が
//! int や文字列とは限らず、素のまま `try_get` すると型不一致で Err → 空文字 / 0 に化ける (#86 #95)。
//! 日付は `CONVERT(varchar(10), _, 120)` で "YYYY-MM-DD" に寄せて datetime / smalldatetime の差を吸収する
//! (SQL Server 2008 互換)。

use crate::payroll::{KINDATA_COLUMNS, MAX_MONTH_INDEX, MONEY_COLUMNS};

/// `sys.databases` から KYDATA DB の (名前, HAS_DBACCESS)。HAS_DBACCESS: 1=可 / 0=不可 / NULL=DB名不正等。
/// restore で作られた DB の権限抜け (model 継承が効かない) をここで検知する。
/// AUTO_CLOSE の全 DB を開いて回るため**遅い** (〜10 秒)。
pub const DATABASES_WITH_ACCESS_SQL: &str = "SELECT name, HAS_DBACCESS(name) FROM sys.databases \
                 WHERE name LIKE 'KYDATA%' ORDER BY name";

/// `sys.databases` から KYDATA DB 名だけ。どの DB も開かずメタデータだけ読む (ミリ秒)。
pub const DATABASE_NAMES_SQL: &str =
    "SELECT name FROM sys.databases WHERE name LIKE 'KYDATA%' ORDER BY name";

/// `KYCOMSTD.SELDATA` の (会社コード, 会社名)。KCODE の実列型によらず文字列で取れるよう CAST する
/// (素の KCODE は型不一致で空文字化し、全社が "" キーに衝突して名前が消える、#86)。
pub const COMPANY_NAMES_SQL: &str =
    "SELECT CAST(KCODE AS varchar(10)), CONAME1 FROM [KYCOMSTD].dbo.SELDATA";

/// [`payroll_month_sql`] の本体。`{money}` / `{kindata}` / `{db}` を差し込む。パラメータは
/// `@P1` = 賃金期間開始の下限 (含む)、`@P2` = 上限 (含まない)。月の特定は固定式でなく
/// CHINGINKIKANST の範囲照合 (#83: 月内複数支給・欠月に強い)。
const PAYROLL_MONTH_SQL: &str = "SELECT CAST(k.SHAIN AS int), CAST(k.[MONTH] AS int), \
             CONVERT(varchar(10), k.SHIKYUBI, 120), \
             CONVERT(varchar(10), k.CHINGINKIKANST, 120), \
             CONVERT(varchar(10), k.CHINGINKIKANEN, 120), \
             s1.CODE, s1.NAME, CAST(ISNULL(s1.TAIKYU, 0) AS int), \
             ISNULL(sz.SNAME, ''), CAST(ISNULL(sz.TAIKEI, 0) AS int), \
             {money}, {kindata} \
             FROM [{db}].dbo.KYUYO k \
             JOIN [{db}].dbo.SHAIN1 s1 ON s1.INCODE = k.SHAIN \
             LEFT JOIN [{db}].dbo.SHOZOKU sz ON sz.INCODE = k.SHOZOKU \
             WHERE k.CHINGINKIKANST >= @P1 AND k.CHINGINKIKANST < @P2 \
             ORDER BY k.SHAIN, k.[MONTH]";

/// 社員マスタ直読み (給与明細 `KYUYO` は経由しない。所属は SHAIN1.SHOZOKU が持つ。金額列には触れない)。
/// INCODE/TAIKEI は実列型が int とは限らないので CAST する (#95)。
///
/// 給与区分 (KKUBUN) は **SHAIN3** にあり、SHOZOKU.TAIKEI とは独立した軸 (#101)。同じ TAIKEI=1 (乗務員) でも
/// 月給/日給/時給が混在し、TAIKEI=2 (事務員) にも時給者がいるので、TAIKEI から給与区分を推定してはいけない。
///
/// 入社日/退社日は **SHAIN2** (DAYNYU/DAYTAI、どちらも datetime)。SHAIN1 ではない — docs/kyuyo-daijin-schema.md が
/// SHAIN1 の 23 列中 6 列しか書いておらず SHAIN2〜8 を「未調査」としているため、給与大臣に無いと誤読しやすい
/// (2026-07-26 に実データ 15 件で確定)。
const EMPLOYEES_SQL: &str = "SELECT s1.CODE, s1.NAME, CAST(ISNULL(s1.TAIKYU, 0) AS int), \
             ISNULL(sz.SNAME, ''), CAST(ISNULL(sz.TAIKEI, 0) AS int), \
             CAST(ISNULL(sz.INCODE, 0) AS int), ISNULL(sz.NAME1, ''), ISNULL(sz.NAME2, ''), \
             CAST(ISNULL(s3.KKUBUN, 0) AS int), \
             ISNULL(CONVERT(varchar(10), s2.DAYNYU, 120), ''), \
             ISNULL(CONVERT(varchar(10), s2.DAYTAI, 120), ''), \
             CAST(ISNULL(s2.TAIKBN, 0) AS int) \
             FROM [{db}].dbo.SHAIN1 s1 \
             LEFT JOIN [{db}].dbo.SHOZOKU sz ON sz.INCODE = s1.SHOZOKU \
             LEFT JOIN [{db}].dbo.SHAIN3 s3 ON s3.INCODE = s1.INCODE \
             LEFT JOIN [{db}].dbo.SHAIN2 s2 ON s2.INCODE = s1.INCODE \
             ORDER BY s1.CODE";

/// 項目マスタ。KAZEI/MEISAI/GENGAKU は実列型が int ではない (smallint 等) ので CAST する — 素で try_get::<i32> すると
/// 型不一致で Err → 0 に化け、全項目が kazei=0 (控除) / meisai≠1 (単価除外が効かない) になる。本番では payments が
/// 全件空・単価が deductions に混入していた (#95、KCODE の #86 と同じ罠)。
const KOUMOKU_SQL: &str = "SELECT TAIKEIKOUNO, NAME, CAST(ISNULL(KAZEI, 0) AS int), \
             CAST(ISNULL(MEISAI, 0) AS int), CAST(ISNULL(GENGAKU, 0) AS int) \
             FROM [{db}].dbo.KOUMOKU";

/// 支給回ごとの計算済み集計。SHUKEI1 は支給回インデックスが列名に埋まっている (SOSHIKYU00..21 等) ので
/// 検証済みの index を `{nn}` に差し込む。
const SHUKEI_TOTALS_SQL: &str = "SELECT CAST(SHAIN AS int), \
             CAST(ISNULL(SOSHIKYU{nn}, 0) AS int), CAST(ISNULL(KAZEI{nn}, 0) AS int), \
             CAST(ISNULL(HOKEN{nn}, 0) AS int), CAST(ISNULL(ZEI{nn}, 0) AS int), \
             CAST(ISNULL(SHOKOUJO{nn}, 0) AS int) \
             FROM [{db}].dbo.SHUKEI1";

/// DB 名の検証 (defense in depth)。`KYDATA0100_126C` / `KYCOMSTD` 形式 = 英数字と `_` だけ、1〜64 文字。
/// DB 名は SQL 文に `[…]` で埋め込むので、これを通らない名前では SQL を組み立てない。
/// `Err` の文言はオンプレ版の `KyuyoRepoError::QueryError` の中身と同じ。
pub fn validate_db_name(db: &str) -> Result<(), String> {
    let ok = !db.is_empty()
        && db.len() <= 64
        && db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(format!("invalid database name: {db}"))
    }
}

/// 指定 DB の `KYUYO` (× `SHAIN1` × `SHOZOKU`) を賃金期間開始の半開区間 [@P1, @P2) で読む SQL。
/// 列は `RawKyuyoRow` の順: SHAIN, MONTH, 支給日, 期間開始, 期間終了, CODE, NAME, TAIKYU, SNAME, TAIKEI,
/// MONEY00..79 ([`MONEY_COLUMNS`] 列), KINDATA0000..1600 ([`KINDATA_COLUMNS`] 列)。
pub fn payroll_month_sql(db: &str) -> Result<String, String> {
    validate_db_name(db)?;
    // T-SQL に動的列は無いので MONEY / KINDATA の列名は code 側で作る (全列 NOT NULL だが念のため ISNULL)。
    // KINDATA は 100 刻みの列名で、項目番号 001〜017 (MONEY の 018〜097 とは項目帯が違う、#103)
    let money: Vec<String> = (0..MONEY_COLUMNS)
        .map(|n| format!("CAST(ISNULL(k.MONEY{n:02}, 0) AS int)"))
        .collect();
    let kindata: Vec<String> = (0..KINDATA_COLUMNS)
        .map(|n| format!("CAST(ISNULL(k.KINDATA{n:02}00, 0) AS int)"))
        .collect();
    Ok(PAYROLL_MONTH_SQL
        .replace("{money}", &money.join(", "))
        .replace("{kindata}", &kindata.join(", "))
        .replace("{db}", db))
}

/// 指定 DB の社員マスタ (`SHAIN1` × `SHOZOKU` × `SHAIN3` × `SHAIN2`) を読む SQL。列は `RawEmployeeRow` の順:
/// CODE, NAME, TAIKYU, SNAME, TAIKEI, 所属 INCODE, NAME1, NAME2, KKUBUN, 入社日, 退社日, TAIKBN。
pub fn employees_sql(db: &str) -> Result<String, String> {
    validate_db_name(db)?;
    Ok(EMPLOYEES_SQL.replace("{db}", db))
}

/// 指定 DB の `KOUMOKU` を読む SQL。列は `RawKoumokuRow` の順: TAIKEIKOUNO, NAME, KAZEI, MEISAI, GENGAKU。
pub fn koumoku_sql(db: &str) -> Result<String, String> {
    validate_db_name(db)?;
    Ok(KOUMOKU_SQL.replace("{db}", db))
}

/// 指定 DB の `SHUKEI1` から支給回 `month_index` の集計を読む SQL。`month_index` は 0..=[`MAX_MONTH_INDEX`]。
/// 列は SHAIN, SOSHIKYU, KAZEI, HOKEN, ZEI, SHOKOUJO。`Err` の文言はオンプレ版と同じ。
pub fn shukei_totals_sql(db: &str, month_index: i32) -> Result<String, String> {
    validate_db_name(db)?;
    if !(0..=MAX_MONTH_INDEX).contains(&month_index) {
        return Err(format!("invalid month index: {month_index}"));
    }
    let nn = format!("{month_index:02}");
    Ok(SHUKEI_TOTALS_SQL.replace("{nn}", &nn).replace("{db}", db))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_name_boundaries() {
        assert_eq!(validate_db_name("KYDATA0100_126C"), Ok(()));
        assert_eq!(validate_db_name("KYCOMSTD"), Ok(()));
        assert_eq!(validate_db_name("a"), Ok(()));
        assert_eq!(validate_db_name(&"A".repeat(64)), Ok(()));
        assert_eq!(
            validate_db_name(&"A".repeat(65)),
            Err(format!("invalid database name: {}", "A".repeat(65)))
        );
        assert_eq!(
            validate_db_name(""),
            Err("invalid database name: ".to_string())
        );
        for bad in [
            "KYDATA]; DROP",
            "a b",
            "a-b",
            "a.b",
            "a]",
            "[a",
            "a'",
            "ａ",
            "日本",
        ] {
            assert_eq!(
                validate_db_name(bad),
                Err(format!("invalid database name: {bad}")),
                "{bad}"
            );
        }
    }

    #[test]
    fn fixed_statements() {
        assert_eq!(
            DATABASES_WITH_ACCESS_SQL,
            "SELECT name, HAS_DBACCESS(name) FROM sys.databases WHERE name LIKE 'KYDATA%' ORDER BY name"
        );
        assert_eq!(
            DATABASE_NAMES_SQL,
            "SELECT name FROM sys.databases WHERE name LIKE 'KYDATA%' ORDER BY name"
        );
        assert_eq!(
            COMPANY_NAMES_SQL,
            "SELECT CAST(KCODE AS varchar(10)), CONAME1 FROM [KYCOMSTD].dbo.SELDATA"
        );
    }

    #[test]
    fn payroll_month() {
        let sql = payroll_month_sql("KYDATA0100_126C").unwrap();
        let head = concat!(
            "SELECT CAST(k.SHAIN AS int), CAST(k.[MONTH] AS int), ",
            "CONVERT(varchar(10), k.SHIKYUBI, 120), ",
            "CONVERT(varchar(10), k.CHINGINKIKANST, 120), ",
            "CONVERT(varchar(10), k.CHINGINKIKANEN, 120), ",
            "s1.CODE, s1.NAME, CAST(ISNULL(s1.TAIKYU, 0) AS int), ",
            "ISNULL(sz.SNAME, ''), CAST(ISNULL(sz.TAIKEI, 0) AS int), ",
            "CAST(ISNULL(k.MONEY00, 0) AS int), CAST(ISNULL(k.MONEY01, 0) AS int), "
        );
        assert!(sql.starts_with(head), "{sql}");
        let tail = concat!(
            "CAST(ISNULL(k.MONEY79, 0) AS int), CAST(ISNULL(k.KINDATA0000, 0) AS int), ",
            "CAST(ISNULL(k.KINDATA0100, 0) AS int), "
        );
        assert!(sql.contains(tail), "{sql}");
        let end = concat!(
            "CAST(ISNULL(k.KINDATA1600, 0) AS int) ",
            "FROM [KYDATA0100_126C].dbo.KYUYO k ",
            "JOIN [KYDATA0100_126C].dbo.SHAIN1 s1 ON s1.INCODE = k.SHAIN ",
            "LEFT JOIN [KYDATA0100_126C].dbo.SHOZOKU sz ON sz.INCODE = k.SHOZOKU ",
            "WHERE k.CHINGINKIKANST >= @P1 AND k.CHINGINKIKANST < @P2 ",
            "ORDER BY k.SHAIN, k.[MONTH]"
        );
        assert!(sql.ends_with(end), "{sql}");
        // CAST は SHAIN / MONTH / TAIKYU / TAIKEI の 4 列 + MONEY 80 列 + KINDATA 17 列
        assert_eq!(sql.matches("MONEY").count(), MONEY_COLUMNS);
        assert_eq!(sql.matches("KINDATA").count(), KINDATA_COLUMNS);
        assert_eq!(
            sql.matches("CAST(").count(),
            4 + MONEY_COLUMNS + KINDATA_COLUMNS
        );
        assert_eq!(sql.matches("CONVERT(varchar(10), ").count(), 3);
        assert!(!sql.contains('{') && !sql.contains("  "), "{sql}");
        assert!(payroll_month_sql("x]").is_err());
    }

    #[test]
    fn employees() {
        let want = concat!(
            "SELECT s1.CODE, s1.NAME, CAST(ISNULL(s1.TAIKYU, 0) AS int), ",
            "ISNULL(sz.SNAME, ''), CAST(ISNULL(sz.TAIKEI, 0) AS int), ",
            "CAST(ISNULL(sz.INCODE, 0) AS int), ISNULL(sz.NAME1, ''), ISNULL(sz.NAME2, ''), ",
            "CAST(ISNULL(s3.KKUBUN, 0) AS int), ",
            "ISNULL(CONVERT(varchar(10), s2.DAYNYU, 120), ''), ",
            "ISNULL(CONVERT(varchar(10), s2.DAYTAI, 120), ''), ",
            "CAST(ISNULL(s2.TAIKBN, 0) AS int) ",
            "FROM [KYDATA0200_126C].dbo.SHAIN1 s1 ",
            "LEFT JOIN [KYDATA0200_126C].dbo.SHOZOKU sz ON sz.INCODE = s1.SHOZOKU ",
            "LEFT JOIN [KYDATA0200_126C].dbo.SHAIN3 s3 ON s3.INCODE = s1.INCODE ",
            "LEFT JOIN [KYDATA0200_126C].dbo.SHAIN2 s2 ON s2.INCODE = s1.INCODE ",
            "ORDER BY s1.CODE"
        );
        assert_eq!(employees_sql("KYDATA0200_126C").unwrap(), want);
        assert_eq!(
            employees_sql("a b"),
            Err("invalid database name: a b".to_string())
        );
    }

    #[test]
    fn koumoku() {
        let want = concat!(
            "SELECT TAIKEIKOUNO, NAME, CAST(ISNULL(KAZEI, 0) AS int), ",
            "CAST(ISNULL(MEISAI, 0) AS int), CAST(ISNULL(GENGAKU, 0) AS int) ",
            "FROM [KYDATA0100_126C].dbo.KOUMOKU"
        );
        assert_eq!(koumoku_sql("KYDATA0100_126C").unwrap(), want);
        assert!(koumoku_sql("").is_err());
    }

    #[test]
    fn shukei_totals() {
        let want = concat!(
            "SELECT CAST(SHAIN AS int), ",
            "CAST(ISNULL(SOSHIKYU07, 0) AS int), CAST(ISNULL(KAZEI07, 0) AS int), ",
            "CAST(ISNULL(HOKEN07, 0) AS int), CAST(ISNULL(ZEI07, 0) AS int), ",
            "CAST(ISNULL(SHOKOUJO07, 0) AS int) ",
            "FROM [KYDATA0100_126C].dbo.SHUKEI1"
        );
        assert_eq!(shukei_totals_sql("KYDATA0100_126C", 7).unwrap(), want);
        assert!(shukei_totals_sql("KYDATA0100_126C", 0)
            .unwrap()
            .contains("SOSHIKYU00"));
        assert!(shukei_totals_sql("KYDATA0100_126C", MAX_MONTH_INDEX)
            .unwrap()
            .contains("SHOKOUJO21"));
        assert_eq!(
            shukei_totals_sql("KYDATA0100_126C", -1),
            Err("invalid month index: -1".to_string())
        );
        assert_eq!(
            shukei_totals_sql("KYDATA0100_126C", 22),
            Err("invalid month index: 22".to_string())
        );
        // DB 名の検証が先 (オンプレ版と同じ順)
        assert_eq!(
            shukei_totals_sql("a b", 99),
            Err("invalid database name: a b".to_string())
        );
    }
}
