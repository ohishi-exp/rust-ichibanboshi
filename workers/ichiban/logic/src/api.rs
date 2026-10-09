//! 6 本の応答の型と `source_table` の値。
//!
//! オンプレ版の 6 本は包みに `routes::sales::ApiResponse`、部門に `routes::sales::DepartmentOption` を
//! 使い続ける (6 本以外の口も使うため、ここへは移さない)。Worker 用に**同じ JSON の形**の
//! [`ListResponse`]・[`Department`] をここに別に持つ — serde の出力 (フィールド名・順序) を変えないこと。

use serde::Serialize;

/// `/api/employees` の `source_table`。
pub const EMPLOYEES_SOURCE: &str = "社員ﾏｽﾀ";
/// `/api/vehicles` の `source_table`。
pub const VEHICLES_SOURCE: &str = "車種ﾏｽﾀ";
/// `/api/sales/departments` の `source_table`。
pub const DEPARTMENTS_SOURCE: &str = "部門ﾏｽﾀ";
/// `/api/sales/vehicle-daily` の `source_table`。
pub const VEHICLE_DAILY_SOURCE: &str = "運転日報明細 + 得意先ﾏｽﾀ + 地域ﾏｽﾀ + 社員ﾏｽﾀ";
/// `/api/costs/vehicle-daily` の `source_table`。
pub const COSTS_DAILY_SOURCE: &str = "経費明細 + 経費ﾏｽﾀ + 経費種別ﾏｽﾀ";

/// 一覧の包み。オンプレ版の `routes::sales::ApiResponse` と同じ JSON の形 (Worker 用)。
#[derive(Serialize, Debug, PartialEq)]
pub struct ListResponse<T: Serialize> {
    pub source_table: String,
    pub data: T,
}

/// 部門 1 件。オンプレ版の `routes::sales::DepartmentOption` と同じ JSON の形 (Worker 用)。
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Department {
    pub department_code: String,
    pub department_name: String,
}

/// 社員ﾏｽﾀ 1 件。
#[derive(Serialize, Debug, PartialEq)]
pub struct EmployeeRow {
    /// 社員C (コード)。数値型でも varchar に寄せた文字列で返す。
    pub employee_code: String,
    /// 社員N (氏名)。
    pub employee_name: String,
    /// 社員R (表示名)。nuxt-trouble 側の担当者名はこれを使う。
    pub employee_r: String,
}

/// 車種ﾏｽﾀ 1 件 (燃費マスタの車種ドロップダウン用)。
#[derive(Serialize, Debug, PartialEq)]
pub struct VehicleOption {
    pub vehicle_code: String,
    pub vehicle_name: String,
}
