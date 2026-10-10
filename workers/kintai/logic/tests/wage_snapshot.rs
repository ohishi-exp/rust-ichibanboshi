//! `kintai_logic::wage_snapshot` の単体テスト。root の `src/wage_snapshot.rs` の `mod tests` (39 本) の写し
//! (中身は変えていない。`use super::*` を crate の path に替えただけ)。

use chrono::NaiveDate;
use kintai_logic::wage_snapshot::*;

fn ym(y: i32, m: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, 1).unwrap()
}

fn row(driver_cd: i64) -> WageSnapshotRow {
    WageSnapshotRow {
        driver_cd,
        driver_name: "山田".to_string(),
        company: Some("0200".to_string()),
        branch_name: Some("本社".to_string()),
        branch_code: Some(210),
        job_name: Some("乗務員".to_string()),
        pay_kubun: Some(1),
        hourly_rate: Some(1420),
        calc_base: Some(200_000),
        calc_overtime: Some(80_000),
        calc_total: Some(280_000),
        paid_base: Some(198_000),
        paid_overtime: Some(78_000),
        working_minutes: Some(11_820),
        restraint_missing: false,
    }
}

fn bucket(rows: Vec<WageSnapshotRow>) -> MonthBucket {
    MonthBucket {
        rows,
        masters: MonthMasters {
            salary_item_sha: Some("item-1".to_string()),
            payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
        },
        timecard_kosoku: Some("no".to_string()),
        wage_logic_version: Some("wage-1".to_string()),
        computed_at: Some("2026-08-05T01:20:00Z".to_string()),
    }
}

fn req() -> SnapshotRequest {
    SnapshotRequest {
        comp_id: "comp".to_string(),
        month: "2026-01".to_string(),
        restraint_source: "gcp".to_string(),
        timecard_kosoku: None,
        wage_logic_version: "wage-1".to_string(),
        masters: MonthMasters::default(),
        rows: vec![row(1035)],
    }
}

#[test]
fn month_start_rejects_bad_shapes() {
    assert_eq!(month_start("2026-06"), Some(ym(2026, 6)));
    for bad in ["2026-6", "", "2026-13", "2026/06", "2026-00", "2026-0x"] {
        assert_eq!(month_start(bad), None, "{bad:?}");
    }
}

#[test]
fn add_months_rolls_across_years() {
    assert_eq!(add_months(ym(2026, 12), 1), ym(2027, 1));
    assert_eq!(add_months(ym(2026, 1), -1), ym(2025, 12));
    assert_eq!(ym_label(ym(2026, 3)), "2026-03");
}

#[test]
fn validate_accepts_a_well_formed_request() {
    let v = validate_snapshot(req()).unwrap();
    assert_eq!(v.ym, ym(2026, 1));
    assert_eq!(v.rows.len(), 1);
    assert_eq!(v.comp_id, "comp");
    assert_eq!(v.restraint_source, "gcp");
    assert_eq!(v.wage_logic_version, "wage-1");
    assert_eq!(v.masters, MonthMasters::default());
    assert_eq!(v.timecard_kosoku, None);
}

#[test]
fn validate_rejects_empty_comp_id() {
    let bad = SnapshotRequest {
        comp_id: "  ".to_string(),
        ..req()
    };
    assert!(validate_snapshot(bad).unwrap_err().contains("comp_id"));
}

#[test]
fn validate_rejects_bad_month() {
    let bad = SnapshotRequest {
        month: "2026-13".to_string(),
        ..req()
    };
    assert!(validate_snapshot(bad).unwrap_err().contains("month"));
}

/// DDL の CHECK と同じ 2 値。ここで弾かないと DB エラーが 502 になって出る。
#[test]
fn validate_rejects_unknown_restraint_source() {
    let bad = SnapshotRequest {
        restraint_source: "supabase".to_string(),
        ..req()
    };
    assert!(validate_snapshot(bad)
        .unwrap_err()
        .contains("restraint_source"));
}

/// **後方互換の要**: `timecard_kosoku` を送ってこない既存クライアントの payload は
/// `None` (= 見ていない) になる。`"yes"` (揃っていた) に化けてはいけない —
/// 化けると「拘束が取れていないのに健全に見える保存物」がまた増える
/// (Refs ohishi-exp/nuxt-dtako-admin#986 / #980)。
#[test]
fn timecard_kosoku_defaults_to_none_when_omitted() {
    let body = r#"{"comp_id":"comp","month":"2026-01","restraint_source":"gcp",
                   "wage_logic_version":"wage-1","rows":[]}"#;
    let parsed: SnapshotRequest = serde_json::from_str(body).expect("既存の payload は通る");
    assert_eq!(parsed.timecard_kosoku, None);
    let v = validate_snapshot(parsed).expect("省略は検証も通る");
    assert_eq!(v.timecard_kosoku, None);
}

/// 3 値はそのまま通る。**`no` と `unreadable` を畳まない** (処方が逆)。
#[test]
fn validate_accepts_every_timecard_kosoku_state() {
    for state in TIMECARD_KOSOKU_STATES {
        let req = SnapshotRequest {
            timecard_kosoku: Some(state.to_string()),
            ..req()
        };
        let v = validate_snapshot(req).unwrap_or_else(|e| panic!("{state}: {e}"));
        assert_eq!(v.timecard_kosoku.as_deref(), Some(state));
    }
    assert_eq!(TIMECARD_KOSOKU_STATES.len(), 3);
    assert!(TIMECARD_KOSOKU_STATES.contains(&"no"));
    assert!(TIMECARD_KOSOKU_STATES.contains(&"unreadable"));
}

/// 知らない値は**黙って `None` に倒さず**弾く。`None` は「見ていない」という
/// 別の事実なので、倒すと保存物が嘘をつく (`payroll_synced_at` と同じ判断)。
#[test]
fn validate_rejects_unknown_timecard_kosoku() {
    for bad in ["", "YES", "missing", "null"] {
        let req = SnapshotRequest {
            timecard_kosoku: Some(bad.to_string()),
            ..req()
        };
        let err = validate_snapshot(req).unwrap_err();
        assert!(err.contains("timecard_kosoku"), "{bad:?}: {err}");
    }
}

/// 明示された `null` も省略と同じく `None` (画面は `null` を送ってくる)。
#[test]
fn timecard_kosoku_accepts_explicit_null() {
    let body = r#"{"comp_id":"comp","month":"2026-01","restraint_source":"gcp",
                   "timecard_kosoku":null,"wage_logic_version":"wage-1","rows":[]}"#;
    let parsed: SnapshotRequest = serde_json::from_str(body).expect("null は通る");
    assert_eq!(parsed.timecard_kosoku, None);
}

/// 保存に付いていた値が月別カバレッジに乗る (読み出しで返す)。
/// 未保存の月は**列ごと出さない** — 「見ていない」と「揃っていた」を混ぜないため。
#[test]
fn aggregate_returns_timecard_kosoku_per_month() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let mut b = bucket(vec![row(1035)]);
    b.timecard_kosoku = Some("unreadable".to_string());
    let agg = aggregate_range(&months, &[Some(b), None], &CurrentVersions::default());

    assert_eq!(agg.months[0].timecard_kosoku.as_deref(), Some("unreadable"));
    assert_eq!(agg.months[1].timecard_kosoku, None);
    let json = serde_json::to_value(&agg.months[1]).unwrap();
    assert!(
        json.get("timecard_kosoku").is_none(),
        "未保存の月は出さない"
    );
}

/// 給与未取込で集計から外れた月でも、土台の取得可否は返す
/// (「なぜこの月が外れたか」と「拘束が取れていたか」は別の話)。
#[test]
fn excluded_month_still_reports_timecard_kosoku() {
    let mut r = row(1035);
    r.paid_base = None;
    let mut b = bucket(vec![r]);
    b.timecard_kosoku = Some("no".to_string());
    let agg = aggregate_range(&[ym(2026, 1)], &[Some(b)], &CurrentVersions::default());

    assert_eq!(agg.months[0].excluded.as_deref(), Some("payroll_missing"));
    assert_eq!(agg.months[0].timecard_kosoku.as_deref(), Some("no"));
}

#[test]
fn validate_rejects_empty_logic_version() {
    let bad = SnapshotRequest {
        wage_logic_version: " ".to_string(),
        ..req()
    };
    assert!(validate_snapshot(bad)
        .unwrap_err()
        .contains("wage_logic_version"));
}

#[test]
fn validate_rejects_too_many_rows() {
    let bad = SnapshotRequest {
        rows: (0..=MAX_SNAPSHOT_ROWS as i64).map(row).collect(),
        ..req()
    };
    assert!(validate_snapshot(bad).unwrap_err().contains("rows"));
}

/// 重複した乗務員CD は主キー衝突になる前に弾く (どちらが正か決められない)。
#[test]
fn validate_rejects_duplicate_driver_cd() {
    let bad = SnapshotRequest {
        rows: vec![row(1035), row(1035)],
        ..req()
    };
    assert!(validate_snapshot(bad).unwrap_err().contains("1035"));
}

#[test]
fn validate_normalizes_the_payroll_sync_time() {
    let req = SnapshotRequest {
        masters: MonthMasters {
            payroll_synced_at: Some("2026-02-03T18:12:00+09:00".to_string()),
            ..Default::default()
        },
        ..req()
    };
    let v = validate_snapshot(req).unwrap();
    assert_eq!(
        v.masters.payroll_synced_at.as_deref(),
        Some("2026-02-03T09:12:00+00:00")
    );
}

/// 形が違う時刻を黙って NULL にしない — NULL は「給与未取込」の意味になり、
/// その月が期間集計から丸ごと消える。
#[test]
fn validate_rejects_a_malformed_payroll_sync_time() {
    let req = SnapshotRequest {
        masters: MonthMasters {
            payroll_synced_at: Some("2026/02/03".to_string()),
            ..Default::default()
        },
        ..req()
    };
    assert!(validate_snapshot(req)
        .unwrap_err()
        .contains("payroll_synced_at"));
}

#[test]
fn normalize_ts_folds_offsets_to_utc() {
    assert_eq!(
        normalize_ts("2026-02-03T09:12:00Z").as_deref(),
        Some("2026-02-03T09:12:00+00:00")
    );
    assert_eq!(normalize_ts("nope"), None);
}

/// 送る順 (表示順) と DB の順 (乗務員CD順) が違っても、内容が同じなら同じと見る。
#[test]
fn rows_equal_ignores_order_but_not_content() {
    let a = vec![row(1035), row(2042)];
    let b = vec![row(2042), row(1035)];
    assert!(rows_equal(&a, &b));
    assert!(!rows_equal(&a, &[row(1035)]));
    let changed = vec![
        row(1035),
        WageSnapshotRow {
            paid_base: Some(1),
            ..row(2042)
        },
    ];
    assert!(!rows_equal(&a, &changed));
}

#[test]
fn resolve_months_lists_both_ends() {
    let months = resolve_months("2026-01", "2026-03").unwrap();
    assert_eq!(months, vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)]);
}

#[test]
fn resolve_months_rejects_bad_shapes_and_order_and_span() {
    assert!(resolve_months("2026-1", "2026-03")
        .unwrap_err()
        .contains("from"));
    assert!(resolve_months("2026-01", "").unwrap_err().contains("to"));
    assert!(resolve_months("2026-03", "2026-01")
        .unwrap_err()
        .contains("以前"));
    assert!(resolve_months("2024-01", "2026-03")
        .unwrap_err()
        .contains("上限"));
    // 上限ちょうどは通る
    assert_eq!(resolve_months("2025-01", "2026-12").unwrap().len(), 24);
}

#[test]
fn stale_reasons_lists_only_what_moved() {
    let saved = bucket(vec![]).masters;
    let current = CurrentVersions {
        salary_item_sha: Some("item-2".to_string()),
        wage_logic_version: Some("wage-1".to_string()),
        payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
    };
    assert_eq!(
        stale_reasons(&saved, Some("wage-1"), &current),
        vec!["salary_item".to_string()]
    );
}

#[test]
fn stale_reasons_catches_payroll_and_logic_version() {
    let saved = bucket(vec![]).masters;
    let current = CurrentVersions {
        payroll_synced_at: Some("2026-03-01T00:00:00Z".to_string()),
        wage_logic_version: Some("wage-2".to_string()),
        ..Default::default()
    };
    assert_eq!(
        stale_reasons(&saved, Some("wage-1"), &current),
        vec!["payroll".to_string(), "wage_logic_version".to_string()]
    );
}

/// 渡されていない項目は「変わっていない」ではなく**判定しない**。
#[test]
fn stale_reasons_ignores_versions_the_caller_did_not_send() {
    let saved = bucket(vec![]).masters;
    assert!(stale_reasons(&saved, Some("wage-1"), &CurrentVersions::default()).is_empty());
    assert!(CurrentVersions::default().is_empty());
}

/// 保存側に版が無い (古い保存) 場合も、今の版が来ていれば動いたと見る。
#[test]
fn stale_reasons_treats_missing_saved_version_as_moved() {
    let current = CurrentVersions {
        salary_item_sha: Some("item-1".to_string()),
        wage_logic_version: Some("wage-1".to_string()),
        ..Default::default()
    };
    let reasons = stale_reasons(&MonthMasters::default(), None, &current);
    assert_eq!(
        reasons,
        vec!["salary_item".to_string(), "wage_logic_version".to_string()]
    );
}

#[test]
fn payroll_missing_only_when_every_row_lacks_pay() {
    let b = bucket(vec![row(1)]);
    assert!(!month_payroll_missing(&b));

    let mut b = bucket(vec![WageSnapshotRow {
        paid_base: None,
        ..row(1)
    }]);
    assert!(month_payroll_missing(&b));
    // 1 人でも金額が入っていれば取り込み済み
    b.rows.push(row(2));
    assert!(!month_payroll_missing(&b));
}

/// **鮮度メタの欠落を「データが無い」と読まない** (2026-08-05 に本番で全月が
/// 集計から消えた)。同期時刻が取れなくても金額が入っていれば集計する。
#[test]
fn payroll_present_even_without_sync_time() {
    let mut b = bucket(vec![row(1)]);
    b.masters.payroll_synced_at = None;
    assert!(!month_payroll_missing(&b));
}

#[test]
fn row_counts_rejects_missing_restraint_rate_or_payroll() {
    assert!(row_counts(&row(1)));
    assert!(!row_counts(&WageSnapshotRow {
        restraint_missing: true,
        ..row(1)
    }));
    assert!(!row_counts(&WageSnapshotRow {
        calc_total: None,
        ..row(1)
    }));
    assert!(!row_counts(&WageSnapshotRow {
        paid_base: None,
        ..row(1)
    }));
}

#[test]
fn aggregate_sums_saved_months_and_keeps_month_cells() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let buckets = vec![Some(bucket(vec![row(1035)])), Some(bucket(vec![row(1035)]))];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

    assert_eq!(agg.rows.len(), 1);
    let d = &agg.rows[0];
    assert_eq!(d.months_counted, 2);
    assert_eq!(d.calc_total, 560_000);
    assert_eq!(d.paid_base, 396_000);
    assert_eq!(d.by_month.len(), 2);
    assert_eq!(d.by_month["2026-01"].hourly_rate, Some(1420));
    // 実働は行合計とは別に月ごとでも返す (画面が月セルの内訳を出せるように)
    assert_eq!(d.by_month["2026-01"].working_minutes, Some(11_820));
    assert!(d.months_missing.is_empty());
    assert!(agg.months.iter().all(|m| m.saved && m.excluded.is_none()));
    // 版を渡していないので鮮度は判定しない
    assert!(agg.months.iter().all(|m| m.stale.is_none()));
}

/// 保存が無い月は `saved: false` で必ず並ぶ (「応答に無い = 0」を作らない)。
#[test]
fn aggregate_lists_unsaved_months_without_dropping_them() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let buckets = vec![Some(bucket(vec![row(1035)])), None];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

    assert_eq!(agg.months.len(), 2);
    assert!(!agg.months[1].saved);
    assert_eq!(agg.months[1].ym, "2026-02");
    assert_eq!(agg.rows[0].months_counted, 1);
    // 月ごと外れた月は months_missing に入れない (カバレッジで分かる)
    assert!(agg.rows[0].months_missing.is_empty());
}

/// 給与未取込の月は**そもそも集計に出さない** (ユーザー決定 2026-08-05)。
///
/// 「未取込」の判定は**金額の有無だけ** — 全行の `paid_base` が NULL の月。
/// 同期時刻の欠落では外さない ([`month_payroll_missing`] の docs 参照)。
#[test]
fn aggregate_excludes_months_without_payroll() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let no_payroll = bucket(vec![WageSnapshotRow {
        paid_base: None,
        paid_overtime: None,
        ..row(1035)
    }]);
    let buckets = vec![Some(bucket(vec![row(1035)])), Some(no_payroll)];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

    assert_eq!(agg.months[1].excluded.as_deref(), Some("payroll_missing"));
    assert_eq!(agg.months[1].drivers, 0);
    assert_eq!(agg.rows[0].months_counted, 1);
    assert_eq!(agg.rows[0].calc_total, 280_000);
}

/// **同期時刻が取れなくても、金額が入っていれば集計する** (2026-08-05 の本番事故)。
/// 混同していたせいで、給与が入っている月まで全部「給与未取込」で消えていた。
#[test]
fn aggregate_keeps_months_whose_sync_time_is_unknown() {
    let months = vec![ym(2026, 1)];
    let mut no_sync = bucket(vec![row(1035)]);
    no_sync.masters.payroll_synced_at = None;
    let agg = aggregate_range(&months, &[Some(no_sync)], &CurrentVersions::default());

    assert!(agg.months[0].excluded.is_none());
    assert_eq!(agg.months[0].drivers, 1);
    assert_eq!(agg.rows[0].months_counted, 1);
}

/// 欠測・単価未設定・その人だけ給与に無い月は、その人の集計だけから外れる。
#[test]
fn aggregate_skips_rows_that_cannot_be_counted() {
    let months = vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)];
    let buckets = vec![
        Some(bucket(vec![
            row(1035),
            WageSnapshotRow {
                restraint_missing: true,
                ..row(2042)
            },
        ])),
        Some(bucket(vec![
            row(1035),
            WageSnapshotRow {
                calc_total: None,
                ..row(2042)
            },
        ])),
        Some(bucket(vec![
            row(1035),
            WageSnapshotRow {
                paid_base: None,
                ..row(2042)
            },
        ])),
    ];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

    // 2042 は 3 か月とも数えられないので行ごと出ない
    assert_eq!(agg.rows.len(), 1);
    assert_eq!(agg.rows[0].driver_cd, 1035);
    assert_eq!(agg.rows[0].months_counted, 3);
    // 月の drivers は「合計に寄与した人数」
    assert!(agg.months.iter().all(|m| m.drivers == 1));
}

/// 期間の途中で欠けた人は行に残り、集計月数だけが減る (退職者の扱い)。
#[test]
fn aggregate_keeps_partial_drivers_with_missing_months() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let buckets = vec![
        Some(bucket(vec![row(1035), row(2042)])),
        Some(bucket(vec![
            row(1035),
            WageSnapshotRow {
                restraint_missing: true,
                ..row(2042)
            },
        ])),
    ];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

    let d = agg.rows.iter().find(|d| d.driver_cd == 2042).unwrap();
    assert_eq!(d.months_counted, 1);
    assert_eq!(d.months_missing, vec!["2026-02".to_string()]);
    assert_eq!(d.by_month.len(), 1);
}

/// 属性は期間内で最後に見た月のものを採る (退職者は最後の所属で並ぶ)。
#[test]
fn aggregate_takes_attributes_from_the_latest_month() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let buckets = vec![
        Some(bucket(vec![row(1035)])),
        Some(bucket(vec![WageSnapshotRow {
            branch_name: Some("大阪".to_string()),
            branch_code: Some(310),
            ..row(1035)
        }])),
    ];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
    assert_eq!(agg.rows[0].branch_name.as_deref(), Some("大阪"));
    assert_eq!(agg.rows[0].branch_code, Some(310));
}

/// **月ごとの差の合計 == 期間合計から出した差** (画面が横に並べる値と右端の値が合う)。
#[test]
fn monthly_diffs_sum_to_the_range_diff() {
    let months = vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)];
    let buckets = vec![
        Some(bucket(vec![row(1035)])),
        Some(bucket(vec![WageSnapshotRow {
            paid_base: Some(210_000),
            paid_overtime: Some(90_000),
            ..row(1035)
        }])),
        Some(bucket(vec![WageSnapshotRow {
            calc_base: Some(190_000),
            calc_total: Some(265_000),
            ..row(1035)
        }])),
    ];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
    let d = &agg.rows[0];

    let monthly_diff: i64 = d
        .by_month
        .values()
        .map(|m| {
            i64::from(m.paid_base.unwrap_or(0) + m.paid_overtime.unwrap_or(0))
                - i64::from(m.calc_total.unwrap_or(0))
        })
        .sum();
    let range_diff = d.paid_base + d.paid_overtime - d.calc_total;
    assert_eq!(monthly_diff, range_diff);
}

/// 3 段の縦計 (基本給 + 残業代 = 合計) は期間合計でも成り立つ
/// (ohishi-exp/nuxt-dtako-admin#673 と同じ不変則)。
#[test]
fn base_plus_overtime_equals_total_in_range_sum() {
    let months = vec![ym(2026, 1), ym(2026, 2)];
    let buckets = vec![Some(bucket(vec![row(1035)])), Some(bucket(vec![row(1035)]))];
    let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
    let d = &agg.rows[0];
    assert_eq!(d.calc_base + d.calc_overtime, d.calc_total);
}

#[test]
fn aggregate_marks_stale_months_when_versions_move() {
    let months = vec![ym(2026, 1)];
    let buckets = vec![Some(bucket(vec![row(1035)]))];
    let current = CurrentVersions {
        salary_item_sha: Some("item-2".to_string()),
        ..Default::default()
    };
    let agg = aggregate_range(&months, &buckets, &current);
    assert_eq!(agg.months[0].stale, Some(true));
    assert_eq!(agg.months[0].stale_reason, vec!["salary_item".to_string()]);
    assert_eq!(
        agg.months[0].computed_at.as_deref(),
        Some("2026-08-05T01:20:00Z")
    );
}

#[test]
fn aggregate_marks_fresh_months_when_versions_match() {
    let months = vec![ym(2026, 1)];
    let buckets = vec![Some(bucket(vec![row(1035)]))];
    let current = CurrentVersions {
        salary_item_sha: Some("item-1".to_string()),
        wage_logic_version: Some("wage-1".to_string()),
        payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
    };
    let agg = aggregate_range(&months, &buckets, &current);
    assert_eq!(agg.months[0].stale, Some(false));
    assert!(agg.months[0].stale_reason.is_empty());
}
