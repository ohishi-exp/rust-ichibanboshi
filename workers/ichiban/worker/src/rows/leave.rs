//! `/api/leave/days`・`/api/leave/employees` の `tiberius::Row` → `ichiban_logic::leave` の Raw 型。
//! 列番号は `LEAVE_DAYS_SQL`・`LEAVE_EMPLOYEES_SQL` の並びと 1 対 1。日付は panic しない `try_get`。

use chrono::NaiveDateTime;
use ichiban_logic::leave::{RawLeaveDayRow, RawLeaveEmployeeRow};
use tiberius::Row;

use super::{decode_cp932, get_datetime};

/// days の列: 0 運転手C, 1 運行年月日, 2 品名N。
pub(crate) fn days(rows: &[Row]) -> Vec<RawLeaveDayRow> {
    rows.iter()
        .map(|r| RawLeaveDayRow {
            employee_code: decode_cp932(r, 0),
            date: get_datetime(r, 1),
            item_name: decode_cp932(r, 2),
        })
        .collect()
}

/// employees の列: 0 社員C, 1 社員N, 2 部門C, 3 入社年月日, 4 退職年月日 (NULL・型不一致は None)。
pub(crate) fn employees(rows: &[Row]) -> Vec<RawLeaveEmployeeRow> {
    rows.iter()
        .map(|r| RawLeaveEmployeeRow {
            employee_code: decode_cp932(r, 0),
            employee_name: decode_cp932(r, 1),
            dept_code: decode_cp932(r, 2),
            hire_date: r.try_get::<NaiveDateTime, _>(3).ok().flatten(),
            retire_date: r.try_get::<NaiveDateTime, _>(4).ok().flatten(),
        })
        .collect()
}
