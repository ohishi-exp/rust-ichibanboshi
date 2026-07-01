#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::routing::{get, post};
use axum::{Extension, Router};
use chrono::{NaiveDate, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use rust_ichibanboshi::auth::{AppClaims, JwtSecret};
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::config::RawConfig;
use rust_ichibanboshi::repo::{AppRepo, DynRepo, RepoError};
use rust_ichibanboshi::routes;
use rust_ichibanboshi::routes::sales::*;
use rust_ichibanboshi::routes::schema::{ColumnInfo, SampleRow, TableInfo};
use rust_ichibanboshi::routes::surcharge::RawSurchargeRow;
use rust_ichibanboshi::routes::unchin::{RawUnchinRow, RawUnchinSummaryRow};
use rust_ichibanboshi::routes::uriage::UriageRow;
use rust_ichibanboshi::sqlite::{DynLocalStore, LocalStore};
use uuid::Uuid;

pub const TEST_JWT_SECRET: &str = "test-jwt-secret-ichibanboshi";

// ── MockRepo: テスト用 ──

pub struct MockRepo;

#[async_trait]
impl AppRepo for MockRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> {
        Ok(vec![TableInfo {
            schema_name: "dbo".into(),
            table_name: "種別別月計".into(),
        }])
    }

    async fn list_columns(&self, _table: &str) -> Result<Vec<ColumnInfo>, RepoError> {
        Ok(vec![ColumnInfo {
            column_name: "年月度".into(),
            data_type: "datetime".into(),
            is_nullable: "NO".into(),
            max_length: None,
        }])
    }

    async fn sample_data(&self, _table: &str, _limit: i32) -> Result<SampleRow, RepoError> {
        Ok(SampleRow {
            columns: vec!["col1".into(), "col2".into()],
            rows: vec![vec![Some("val1".into()), Some("val2".into())]],
        })
    }

    async fn monthly(
        &self,
        _from: &str,
        _to: &str,
        _prev_from: &str,
        _prev_to: &str,
        _exclude: Option<&str>,
        _include: Option<&str>,
    ) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> {
        Ok((
            "種別別月計 (種別C=99)".into(),
            vec![RawMonthlyRow {
                year_month: dt(2025, 4, 1),
                own_sales: 1_000_000,
                charter_sales: 500_000,
                transport_count: 50,
            }],
            vec![RawMonthlyRow {
                year_month: dt(2024, 4, 1),
                own_sales: 900_000,
                charter_sales: 400_000,
                transport_count: 0,
            }],
        ))
    }

    async fn by_department(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<RawDepartmentRow>, RepoError> {
        Ok(vec![RawDepartmentRow {
            department_code: "01".into(),
            department_name: "本社".into(),
            own_sales: 500,
            charter_sales: 200,
            transport_count: 10,
        }])
    }

    async fn by_customer(
        &self,
        _from: &str,
        _to: &str,
        _limit: i32,
    ) -> Result<Vec<RawCustomerRow>, RepoError> {
        Ok(vec![RawCustomerRow {
            customer_code: "001".into(),
            customer_name: "得意先A".into(),
            own_sales: 1000,
            charter_sales: 500,
            transport_count: 20,
        }])
    }

    async fn customer_yoy_data(
        &self,
        _from: &str,
        _to: &str,
        _prev_from: &str,
        _prev_to: &str,
    ) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> {
        let mut cur = std::collections::HashMap::new();
        cur.insert("A".into(), ("顧客A".into(), 1_200_000i64));
        let mut prev = std::collections::HashMap::new();
        prev.insert("A".into(), ("顧客A".into(), 1_000_000i64));
        Ok((cur, prev))
    }

    async fn yoy_data(
        &self,
        _year: i32,
    ) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> {
        Ok((
            vec![RawMonthTotalRow {
                month: 1,
                total: 1_000_000,
            }],
            vec![RawMonthTotalRow {
                month: 1,
                total: 900_000,
            }],
        ))
    }

    async fn daily(
        &self,
        _from: &str,
        _to: &str,
        _prev_from: &str,
        _prev_to: &str,
        _bf: &str,
        _df: &str,
        _ep: &str,
    ) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> {
        Ok((
            vec![RawDailyRow {
                date: dt(2025, 4, 1),
                own_sales: 100,
                charter_sales: 50,
                own_sales_raw: 110,
                charter_sales_raw: 55,
                transport_count: 10,
            }],
            vec![],
        ))
    }

    async fn customer_trend_data(
        &self,
        _from: &str,
        _to: &str,
        _limit: i32,
    ) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> {
        Ok((
            vec![("A".into(), "顧客A".into())],
            vec![RawCustomerMonthlyRow {
                customer_code: "A".into(),
                year_month: dt(2025, 4, 1),
                total: 1000,
            }],
        ))
    }

    async fn customer_detail_data(
        &self,
        _code: &str,
    ) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> {
        Ok((
            "得意先A".into(),
            vec![RawCustomerDetailRow {
                year_month: dt(2025, 4, 1),
                own_sales: 100,
                charter_sales: 50,
                transport_count: 10,
            }],
        ))
    }

    async fn customer_yoy_by_dept_data(
        &self,
        _from: &str,
        _to: &str,
        _prev_from: &str,
        _prev_to: &str,
        _dept: Option<&str>,
    ) -> Result<(Vec<RawCustomerDeptRow>, Vec<RawCustomerDeptRow>), RepoError> {
        Ok((
            vec![RawCustomerDeptRow {
                department_code: "01".into(),
                department_name: "本社".into(),
                customer_code: "A".into(),
                customer_name: "顧客A".into(),
                total: 1_200_000,
            }],
            vec![RawCustomerDeptRow {
                department_code: "01".into(),
                department_name: "本社".into(),
                customer_code: "A".into(),
                customer_name: "顧客A".into(),
                total: 1_000_000,
            }],
        ))
    }

    async fn list_departments(&self) -> Result<Vec<(String, String)>, RepoError> {
        Ok(vec![
            ("01".into(), "本社".into()),
            ("02".into(), "大阪".into()),
        ])
    }

    async fn vehicles(&self) -> Result<Vec<(String, String)>, RepoError> {
        Ok(vec![
            ("04".into(), "大型幌".into()),
            ("07".into(), "ﾄﾚｰﾗｰ".into()),
            ("00".into(), "".into()), // 未設定 (車種N 空)
        ])
    }

    async fn surcharge_base(
        &self,
        _from: &str,
        _to: &str,
        _kind_filter: &str,
        _limit: i32,
    ) -> Result<Vec<RawSurchargeRow>, RepoError> {
        Ok(vec![
            RawSurchargeRow {
                request_kind: "1".into(),
                customer_code: "000001".into(),
                customer_name: "㈱田浦畜産".into(),
                origin_area_name: "長崎県".into(),
                dest_area_name: "福岡県".into(),
                vehicle_code: "04".into(),
                vehicle_name: "大型幌".into(),
                sale_date: dt(2026, 6, 21),
                fare: 65_000,
                billing_date: Some(dt(2026, 7, 31)),
                subcontractor_code: "000000".into(),
                item_code: "".into(),
                item_name: "".into(),
                vehicle_number: "8504".into(),
                fuel_surcharge: 4_020,
                row_id: "20260621-1001".into(),
                input_staff_code: "0012".into(),
                input_staff_name: "西田　和恵".into(),
            },
            // エッジ: 未マップ地域 (000000)・車種未設定 (00)・入金予定日 NULL
            RawSurchargeRow {
                request_kind: "1".into(),
                customer_code: "000002".into(),
                customer_name: "㈱谷川商事".into(),
                origin_area_name: "".into(),
                dest_area_name: "".into(),
                vehicle_code: "00".into(),
                vehicle_name: "".into(),
                sale_date: dt(2026, 6, 20),
                fare: 840_000,
                billing_date: None,
                subcontractor_code: "001234".into(),
                item_code: "".into(),
                item_name: "".into(),
                vehicle_number: "8504".into(),
                fuel_surcharge: 0,
                row_id: "20260620-1002".into(),
                input_staff_code: "".into(),
                input_staff_name: "".into(),
            },
        ])
    }

    async fn uriage_rows(
        &self,
        _from: &str,
        _to: &str,
        _bumon_codes: &[String],
        _persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        // 担当者振替で B5 経路 (入力担当C=1499 → "青井") に当たる 1 行と
        // B6 経路 (マスタ外、表示のみ) に当たる 1 行を返す。
        // 横横=0 で 傭車金額は独立。
        Ok(vec![
            UriageRow {
                yokoyoko: 0,
                seikyu_k: 0,
                biko2: String::new(),
                nyuryoku_tanto_c: 1499,
                kado_bumon: "010".into(),
                kingaku: 50_000,
                nebiki: 0,
                warimashi: 1_000,
                jippi: 500,
                yosha_kingaku: 30_000,
                yosha_nebiki: 0,
                yosha_warimashi: 600,
                yosha_jippi: 200,
                shain_r: "青井".into(),
                yoshasaki_c: "000000".into(),
                unko_date: "2026-06-15".into(),
                uriage_date: "2026-06-15".into(),
                tokuisaki_key: "TESTCUST-0".into(),
                tokuisaki_n: "テスト得意先".into(),
                yoshasaki_key: "000000-0".into(),
                yoshasaki_n: "".into(),
            },
            // 入力担当 9999 (マスタ外) → B6 で表示のみ、$sum に積まない
            UriageRow {
                yokoyoko: 0,
                seikyu_k: 0,
                biko2: String::new(),
                nyuryoku_tanto_c: 9999,
                kado_bumon: "010".into(),
                kingaku: 8_000,
                nebiki: 0,
                warimashi: 0,
                jippi: 0,
                yosha_kingaku: 6_000,
                yosha_nebiki: 0,
                yosha_warimashi: 0,
                yosha_jippi: 0,
                shain_r: "無関係".into(),
                yoshasaki_c: "021970".into(),
                unko_date: "2026-06-16".into(),
                uriage_date: "2026-06-16".into(),
                tokuisaki_key: "TESTCUST2-0".into(),
                tokuisaki_n: "テスト得意先2".into(),
                yoshasaki_key: "021970-0".into(),
                yoshasaki_n: "テスト傭車先".into(),
            },
        ])
    }

    async fn unchin_candidates(
        &self,
        _from: &str,
        _to: &str,
        partner_type: &str,
        _kind_filter: &str,
    ) -> Result<Vec<RawUnchinRow>, RepoError> {
        if partner_type == "subcontractor" {
            return Ok(vec![RawUnchinRow {
                partner_code: "021970-000".into(),
                partner_name: "㈱九州運輸".into(),
                item_code: "6301".into(),
                item_name: "フレコン".into(),
                fare: 28_000,
                origin: "鳥栖".into(),
                dest: "大石運輸  本社".into(),
                sale_date: dt(2026, 6, 18),
                bumon_code: "012".into(),
                bumon_name: "佐賀".into(),
            }]);
        }
        Ok(vec![
            RawUnchinRow {
                partner_code: "034760-015".into(),
                partner_name: "全農物流㈱　九州支店".into(),
                item_code: "6301".into(),
                item_name: "フレコン".into(),
                fare: 30_000,
                origin: "釧路".into(),
                dest: "八代".into(),
                sale_date: dt(2026, 6, 20),
                bumon_code: "010".into(),
                bumon_name: "本社".into(),
            },
            // 空品名コード (汎用コード、過剰集約の補正対象。#57 確定事項)
            RawUnchinRow {
                partner_code: "034760-015".into(),
                partner_name: "全農物流㈱　九州支店".into(),
                item_code: "0000".into(),
                item_name: "".into(),
                fare: 140_000,
                origin: "".into(),
                dest: "福岡県北九州市".into(),
                sale_date: dt(2026, 6, 19),
                bumon_code: "010".into(),
                bumon_name: "本社".into(),
            },
        ])
    }

    async fn unchin_summary(
        &self,
        _from: &str,
        _to: &str,
        partner_type: &str,
        _kind_filter: &str,
    ) -> Result<Vec<RawUnchinSummaryRow>, RepoError> {
        if partner_type == "subcontractor" {
            return Ok(vec![RawUnchinSummaryRow {
                partner_code: "021970-000".into(),
                partner_name: "㈱九州運輸".into(),
                total: 28_000,
                bumon_code: "012".into(),
                bumon_name: "佐賀".into(),
            }]);
        }
        Ok(vec![RawUnchinSummaryRow {
            partner_code: "034760-015".into(),
            partner_name: "全農物流㈱　九州支店".into(),
            total: 170_000,
            bumon_code: "010".into(),
            bumon_name: "本社".into(),
        }])
    }
}

// ── ErrorRepo: 全メソッドがエラーを返す ──

pub struct ErrorRepo;

#[async_trait]
impl AppRepo for ErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn list_columns(&self, _: &str) -> Result<Vec<ColumnInfo>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn sample_data(&self, _: &str, _: i32) -> Result<SampleRow, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn monthly(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn by_department(&self, _: &str, _: &str) -> Result<Vec<RawDepartmentRow>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn by_customer(
        &self,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<Vec<RawCustomerRow>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn customer_yoy_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn yoy_data(
        &self,
        _: i32,
    ) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn daily(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn customer_trend_data(
        &self,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn customer_detail_data(
        &self,
        _: &str,
    ) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn customer_yoy_by_dept_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<(Vec<RawCustomerDeptRow>, Vec<RawCustomerDeptRow>), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn list_departments(&self) -> Result<Vec<(String, String)>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn vehicles(&self) -> Result<Vec<(String, String)>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn surcharge_base(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<Vec<RawSurchargeRow>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn uriage_rows(
        &self,
        _: &str,
        _: &str,
        _: &[String],
        _: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn unchin_candidates(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<Vec<RawUnchinRow>, RepoError> {
        Err(RepoError::PoolError)
    }
    async fn unchin_summary(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<Vec<RawUnchinSummaryRow>, RepoError> {
        Err(RepoError::PoolError)
    }
}

// ── QueryErrorRepo: QueryError を返す ──

pub struct QueryErrorRepo;

#[async_trait]
impl AppRepo for QueryErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Err(RepoError::QueryError("test query error".into()))
    }
    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn list_columns(&self, _: &str) -> Result<Vec<ColumnInfo>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn sample_data(&self, _: &str, _: i32) -> Result<SampleRow, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn monthly(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
        _: Option<&str>,
    ) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn by_department(&self, _: &str, _: &str) -> Result<Vec<RawDepartmentRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn by_customer(
        &self,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<Vec<RawCustomerRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn customer_yoy_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn yoy_data(
        &self,
        _: i32,
    ) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn daily(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn customer_trend_data(
        &self,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn customer_detail_data(
        &self,
        _: &str,
    ) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn customer_yoy_by_dept_data(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: Option<&str>,
    ) -> Result<(Vec<RawCustomerDeptRow>, Vec<RawCustomerDeptRow>), RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn list_departments(&self) -> Result<Vec<(String, String)>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn vehicles(&self) -> Result<Vec<(String, String)>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn surcharge_base(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: i32,
    ) -> Result<Vec<RawSurchargeRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn uriage_rows(
        &self,
        _: &str,
        _: &str,
        _: &[String],
        _: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn unchin_candidates(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<Vec<RawUnchinRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
    async fn unchin_summary(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
    ) -> Result<Vec<RawUnchinSummaryRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
}

// ── ヘルパー ──

pub fn dt(y: i32, m: u32, d: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

pub fn local_store() -> DynLocalStore {
    Arc::new(LocalStore::open(":memory:").expect("in-memory sqlite"))
}

/// テスト用 raw dir。**test 毎にユニーク**な path を作って衝突を避ける (各 test が
/// 同 PID + 同時刻 nanos でぶつかる可能性を考慮し、UUID もまぶす)。
pub fn temp_raw_dir() -> Arc<RawConfig> {
    let dir = std::env::temp_dir().join(format!(
        "ichibanboshi-test-raw-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        Uuid::new_v4()
    ));
    Arc::new(RawConfig {
        dir: dir.to_string_lossy().into_owned(),
    })
}

/// CakePHP 未配線 (base_url 空) の client。/recalc が 503 を返す経路用。
pub fn disabled_cakephp() -> Arc<CakephpClient> {
    Arc::new(CakephpClient::new(String::new(), 30).expect("cakephp client build"))
}

pub fn build_app(repo: DynRepo) -> Router {
    build_app_full(repo, local_store(), disabled_cakephp(), temp_raw_dir())
}

pub fn build_app_with_store(repo: DynRepo, store: DynLocalStore) -> Router {
    build_app_full(repo, store, disabled_cakephp(), temp_raw_dir())
}

pub fn build_app_full(
    repo: DynRepo,
    store: DynLocalStore,
    cakephp: Arc<CakephpClient>,
    raw_cfg: Arc<RawConfig>,
) -> Router {
    let jwt_secret = JwtSecret(TEST_JWT_SECRET.to_string());
    let api_routes = Router::new()
        .route("/sales/monthly", get(routes::sales::monthly))
        .route("/sales/by-department", get(routes::sales::by_department))
        .route("/sales/by-customer", get(routes::sales::by_customer))
        .route("/sales/yoy", get(routes::sales::yoy))
        .route("/sales/daily", get(routes::sales::daily))
        .route("/sales/customer-trend", get(routes::sales::customer_trend))
        .route("/sales/customer-yoy", get(routes::sales::customer_yoy))
        .route(
            "/sales/customer-yoy-by-dept",
            get(routes::sales::customer_yoy_by_dept),
        )
        .route(
            "/sales/departments",
            get(routes::sales::list_departments_handler),
        )
        .route(
            "/sales/customer-detail",
            get(routes::sales::customer_detail),
        )
        .route("/surcharge/base", get(routes::surcharge::surcharge_base))
        .route("/vehicles", get(routes::surcharge::vehicles))
        .route("/unchin/candidates", get(routes::unchin::unchin_candidates))
        .route("/unchin/summary", get(routes::unchin::unchin_summary))
        .route("/uriage/by-person", post(routes::uriage::by_person))
        .route("/uriage/recalc", post(routes::uriage::recalc))
        .route("/uriage/daily", get(routes::uriage::daily))
        .route(
            "/uriage/person-monthly-totals",
            get(routes::uriage::person_monthly_totals),
        )
        .route(
            "/uriage/person-partner-totals",
            get(routes::uriage::person_partner_totals),
        )
        .route("/uriage/r2/pending", get(routes::uriage::r2_pending))
        .route(
            "/uriage/raw/{month}/{eigyosho_id}",
            get(routes::uriage::raw_get),
        )
        .route(
            "/uriage/raw/{month}/{eigyosho_id}/ack",
            post(routes::uriage::raw_ack),
        )
        .route("/uriage/admin/delete", post(routes::uriage::admin_delete))
        .route("/uriage/admin/rebuild", post(routes::uriage::admin_rebuild))
        .route("/uriage/verify", get(routes::uriage::verify))
        .route(
            "/uriage/verify-history",
            get(routes::uriage::verify_history),
        )
        .route("/uriage/recalc-jobs", get(routes::uriage::list_recalc_jobs));
    let schema_routes = Router::new()
        .route("/schema/tables", get(routes::schema::list_tables))
        .route("/schema/columns", get(routes::schema::list_columns))
        .route("/schema/sample", get(routes::schema::sample_data));
    Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .nest("/api", schema_routes)
        .layer(Extension(repo))
        .layer(Extension(store))
        .layer(Extension(cakephp))
        .layer(Extension(raw_cfg))
        .layer(Extension(jwt_secret))
}

pub fn mock_repo() -> DynRepo {
    Arc::new(MockRepo)
}
pub fn error_repo() -> DynRepo {
    Arc::new(ErrorRepo)
}
pub fn query_error_repo() -> DynRepo {
    Arc::new(QueryErrorRepo)
}

pub fn create_test_jwt(tenant_id: Uuid, role: &str) -> String {
    let claims = AppClaims {
        sub: Uuid::new_v4(),
        email: "test@example.com".into(),
        name: "Test User".into(),
        tenant_id,
        role: role.into(),
        org_slug: None,
        env: None,
        iat: Utc::now().timestamp(),
        exp: Utc::now().timestamp() + 3600,
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes()),
    )
    .unwrap()
}
