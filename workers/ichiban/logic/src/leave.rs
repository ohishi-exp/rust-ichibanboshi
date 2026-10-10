//! `/api/leave/days`・`/api/leave/employees` の SQL・Raw 型・応答型・Query・組み立て (Refs ohishi-exp/rust-leave-worker#1)。
//!
//! 休暇申請の突き合わせ (rust-leave-worker) が呼ぶ。休暇入力は `運転日報明細` の 得意先C = `'000002'` の行で、
//! 品名N に 有休・労災・特別休暇・欠勤・午前休… が入る。列名と条件は社内の勤怠画面 (TimeCardController) の
//! SELECT に合わせたが、**部門C の絞りは入れない** (会社の区別は呼ぶ側が 部門C の集合で行う)。
//! 品名N は trim だけで正規化しない (正規化も呼ぶ側)。
//!
//! 認可はこの Worker に無い。返す列を絞ることが担保なので、社員の口は 社員C・社員N・部門C・入社年月日・
//! 退職年月日 だけを SELECT する (住所・電話・携帯・生年月日・免許証番号・性別・血液型などは入れない)。

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

/// `/api/leave/days` の `source_table`。
pub const LEAVE_DAYS_SOURCE: &str = "運転日報明細";
/// 休暇入力の行を表す 得意先C (6 桁の文字列)。SQL には埋めず @P1 にバインドして渡す。
pub const LEAVE_CUSTOMER_CODE: &str = "000002";
/// `from`〜`to` に許す日数 (両端を含む)。これを超えると 400。
pub const MAX_DAYS: i64 = 92;

// ══════════════════════════════════════════════════════════════
// SQL
// ══════════════════════════════════════════════════════════════

/// 休暇入力の行 (@P1 得意先C, @P2 開始日 (以上), @P3 終了日の翌日 (未満))。
/// 列: 0 運転手C (varchar に寄せる), 1 運行年月日, 2 品名N。並びは 運行年月日, 運転手C。
pub const LEAVE_DAYS_SQL: &str =
    "SELECT CONVERT(varchar(20), [運転手C]), [運行年月日], ISNULL([品名N], '') \
             FROM [運転日報明細] \
             WHERE [得意先C] = @P1 AND [運行年月日] >= @P2 AND [運行年月日] < @P3 \
             ORDER BY [運行年月日], [運転手C]";

/// 社員 1 人 1 行。列: 0 社員C, 1 社員N, 2 部門C, 3 入社年月日, 4 退職年月日。
///
/// 社員C が複数行ある (`EMPLOYEES_SQL` と同じ) ので `GROUP BY [社員C]` で 1 行に潰し、他の列は `MAX()` で取る。
/// 名前・部門C は文字列の最大値 (NULL は無視。名前だけ外側の ISNULL で空文字)。入社年月日・退職年月日は
/// 日付の最大値 = **複数行のうち最も新しい日付**で、1 行でも日付が入っていれば NULL にはならない。
pub const LEAVE_EMPLOYEES_SQL: &str = "SELECT CONVERT(varchar(20), [社員C]) AS [社員C], \
             ISNULL(MAX([社員N]), '') AS [社員N], \
             ISNULL(MAX([部門C]), '') AS [部門C], \
             MAX([入社年月日]) AS [入社年月日], \
             MAX([退職年月日]) AS [退職年月日] \
             FROM [社員ﾏｽﾀ] GROUP BY [社員C] ORDER BY [社員C]";

// ══════════════════════════════════════════════════════════════
// Query
// ══════════════════════════════════════════════════════════════

/// `/api/leave/days` のクエリ。欠落は `None` で受け、[`LeaveDaysQuery::plan`] が 400 (= `None`) にする。
#[derive(Deserialize, Debug, Default)]
pub struct LeaveDaysQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// 検証を通ったクエリ。SQL の @P2・@P3 にそのまま渡す。
#[derive(Debug, PartialEq, Eq)]
pub struct LeaveDaysPlan {
    /// `YYYY-MM-DD` (以上)。
    pub from: String,
    /// `to` の翌日 `YYYY-MM-DD` (未満)。
    pub to_exclusive: String,
}

/// `YYYY-MM-DD` (ゼロ埋め必須) の実在する日付だけ受ける。
fn parse_date(s: &str) -> Option<NaiveDate> {
    let d = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    (d.format("%Y-%m-%d").to_string() == s).then_some(d)
}

impl LeaveDaysQuery {
    /// 欠落・日付として不正・`from` > `to`・日数が [`MAX_DAYS`] 超 (両端を含む) は `None` (= 400)。
    pub fn plan(&self) -> Option<LeaveDaysPlan> {
        let from = parse_date(self.from.as_deref()?)?;
        let to = parse_date(self.to.as_deref()?)?;
        if from > to || (to - from).num_days() + 1 > MAX_DAYS {
            return None;
        }
        let next = to + Duration::days(1);
        Some(LeaveDaysPlan {
            from: from.format("%Y-%m-%d").to_string(),
            to_exclusive: next.format("%Y-%m-%d").to_string(),
        })
    }
}

// ══════════════════════════════════════════════════════════════
// Raw 中間構造体 (DB 層 → ロジック層 の橋渡し)
// ══════════════════════════════════════════════════════════════

/// `LEAVE_DAYS_SQL` の 1 行。
#[derive(Debug, Clone)]
pub struct RawLeaveDayRow {
    pub employee_code: String,
    pub date: NaiveDateTime,
    pub item_name: String,
}

/// `LEAVE_EMPLOYEES_SQL` の 1 行。日付の NULL は `None`。
#[derive(Debug, Clone)]
pub struct RawLeaveEmployeeRow {
    pub employee_code: String,
    pub employee_name: String,
    pub dept_code: String,
    pub hire_date: Option<NaiveDateTime>,
    pub retire_date: Option<NaiveDateTime>,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct LeaveDay {
    pub employee_code: String,
    /// `YYYY-MM-DD`。
    pub date: String,
    /// 品名N (trim のみ。正規化は呼ぶ側)。
    pub item_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct LeaveEmployee {
    pub employee_code: String,
    pub employee_name: String,
    pub dept_code: String,
    /// `YYYY-MM-DD` (時刻部は捨てる)。NULL は `null`。
    pub hire_date: Option<String>,
    /// `YYYY-MM-DD` (時刻部は捨てる)。NULL は `null`。
    pub retire_date: Option<String>,
}

fn ymd(d: &NaiveDateTime) -> String {
    d.format("%Y-%m-%d").to_string()
}

// ══════════════════════════════════════════════════════════════
// 組み立て
// ══════════════════════════════════════════════════════════════

/// Raw 行を応答行に変換 (並びは SQL のまま。コード・品名は trim、日付は `YYYY-MM-DD`)。
pub fn build_leave_days(raw: &[RawLeaveDayRow]) -> Vec<LeaveDay> {
    raw.iter()
        .map(|r| LeaveDay {
            employee_code: r.employee_code.trim().to_string(),
            date: ymd(&r.date),
            item_name: r.item_name.trim().to_string(),
        })
        .collect()
}

/// Raw 行を応答行に変換 (並びは SQL のまま)。
pub fn build_leave_employees(raw: &[RawLeaveEmployeeRow]) -> Vec<LeaveEmployee> {
    raw.iter()
        .map(|r| LeaveEmployee {
            employee_code: r.employee_code.trim().to_string(),
            employee_name: r.employee_name.trim().to_string(),
            dept_code: r.dept_code.trim().to_string(),
            hire_date: r.hire_date.as_ref().map(ymd),
            retire_date: r.retire_date.as_ref().map(ymd),
        })
        .collect()
}
