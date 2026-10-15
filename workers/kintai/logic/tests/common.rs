//! 共通部分 (`kintai_logic::common`) とテナントの担保の単体テスト。
//!
//! テナントの解決・月の境界・設定欠落の 503 は、元 (root の src/) では口ごとの写しにそれぞれテストがあった
//! (`read_tenant_of` / `tenant_of` / `month_date_bounds` / `month_bounds` / `store`)。写しを 1 つに畳んだので
//! テストもここに畳む。
//!
//! **テナントの担保**: 接続ロールは BYPASSRLS なので、`WHERE tenant_id = $1` と「`$1` が UUID のテナント pin」が
//! 他テナントを見せない唯一の担保。5 本の SQL 定数 (change-log は 2 本) すべてと、各口の `Binds::params()` の
//! 第 1 引数を確かめる。

use bytes::BytesMut;
use chrono::{NaiveDate, TimeZone, Utc};
use kintai_logic::common::{
    bad_request, db_fail, is_valid_month, jst_midnight, month_bounds, no_db,
    other_tenant_forbidden, parse_driver, parse_query, preflight, resolve_tenant, tenant_gate,
    tenant_not_allowed, tenant_of, Fail, Param, Reach, Tenancy, TenantInput, HYPERDRIVE_BINDING,
    TENANT_HEADER, TENANT_IDS_VAR, TENANT_VAR,
};
use kintai_logic::{change_log, day_summaries, shift_days, shift_overlaps, wage_range};
use postgres_types::{IsNull, Type};
use serde::Deserialize;
use uuid::Uuid;

fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

// ── 月の境界 (元: day-summaries の month_date_bounds 2 本・shift-overlaps の month_bounds 2 本) ──

#[test]
fn month_bounds_within_year() {
    assert_eq!(
        month_bounds("2026-06"),
        Some((ymd(2026, 6, 1), ymd(2026, 7, 1)))
    );
}

#[test]
fn month_bounds_rolls_over_year() {
    assert_eq!(
        month_bounds("2026-12"),
        Some((ymd(2026, 12, 1), ymd(2027, 1, 1)))
    );
}

/// `TIMESTAMPTZ` の境界は JST の 00:00 (= 前日 15:00 UTC)。元の `jst_day_bounds(first).0` と同じ値。
#[test]
fn timestamptz_bounds_are_jst_midnight() {
    let (first, next) = month_bounds("2026-12").unwrap();
    assert_eq!(
        jst_midnight(first).with_timezone(&Utc),
        Utc.with_ymd_and_hms(2026, 11, 30, 15, 0, 0).unwrap()
    );
    assert_eq!(
        jst_midnight(next).with_timezone(&Utc),
        Utc.with_ymd_and_hms(2026, 12, 31, 15, 0, 0).unwrap()
    );
    assert_eq!(
        jst_midnight(first).to_rfc3339(),
        "2026-12-01T00:00:00+09:00"
    );
}

#[test]
fn month_bounds_rejects_what_is_valid_month_rejects() {
    assert_eq!(month_bounds("abcd-06"), None);
    assert_eq!(month_bounds("2026-xx"), None);
    assert_eq!(month_bounds("2026-13"), None);
}

// ── 月・乗務員CD の検査 (元: routes/kintai.rs の is_valid_month / parse_driver) ──

#[test]
fn valid_months() {
    for ok in ["2026-01", "2026-12", "0001-06"] {
        assert!(is_valid_month(ok), "{ok}");
    }
    for bad in [
        "", "2026-6", "2026-13", "2026-00", "2026/06", "20x6-06", "2026-0x", "2026-061",
    ] {
        assert!(!is_valid_month(bad), "{bad:?}");
    }
}

#[test]
fn driver_is_digits_only() {
    assert_eq!(parse_driver("1051"), Some(1051));
    assert_eq!(parse_driver("0"), Some(0));
    for bad in ["", "-1", "1a", " 1", "99999999999999999999"] {
        assert_eq!(parse_driver(bad), None, "{bad:?}");
    }
}

// ── Query の読み方 (axum 0.8 の Query と同じ拒否文言) ──

#[derive(Debug, Deserialize)]
struct N {
    #[allow(dead_code)]
    n: i32,
}

/// axum 0.8 の `correct_rejection_status_code` のテストと同じ入力・同じ本文。
#[test]
fn parse_query_rejects_like_axum() {
    assert_eq!(
        parse_query::<N>("n=hi").unwrap_err(),
        Fail::new(
            400,
            "Failed to deserialize query string: n: invalid digit found in string"
        )
    );
    assert!(parse_query::<N>("n=1").is_ok());
}

// ── 失敗の形 ──

#[test]
fn failure_shapes() {
    assert_eq!(bad_request("x"), Fail::new(400, "x"));
    assert_eq!(
        db_fail("kintai.shifts read", "42P01"),
        Fail::new(502, "kintai.shifts read failed: 42P01")
    );
}

/// 元の `store` (「[kintai_push] が無効です」の 503) に当たるもの。binding の名前で言う。
#[test]
fn missing_hyperdrive_is_service_unavailable() {
    let f = no_db();
    assert_eq!(f.status, 503);
    assert!(f.body.contains(HYPERDRIVE_BINDING), "{}", f.body);
    assert_eq!(HYPERDRIVE_BINDING, "KINTAI_HYPERDRIVE");
}

// ── テナントの解決 (元: read_tenant_of ×3 + tenant_of の 4 つの写しのテスト) ──

/// 設定 pin が UUID ならそれを使う (元: read_tenant_wins_over_the_write_pin)。
#[test]
fn the_pin_is_the_tenant() {
    let t = uuid(1);
    assert_eq!(tenant_of(Some(&t.to_string())), Ok(t));
}

/// **どれも無ければ 503。** nil で引いて 0 件を返すと「設定が無い」と「その月の勤務が無い」が区別できない
/// (元: no_tenant_at_all_is_service_unavailable と、nil を「無い」と扱う without_a_read_tenant_…)。
#[test]
fn no_usable_pin_is_service_unavailable() {
    for raw in [
        None,
        Some(""),
        Some("not-a-uuid"),
        Some("00000000-0000-0000-0000-000000000000"),
    ] {
        let f = tenant_of(raw).expect_err("must fail without a usable pin");
        assert_eq!(f.status, 503, "{raw:?}");
        assert_eq!(
            f.body,
            "読み先のテナントが決まりません (KINTAI_TENANT_ID を設定してください)"
        );
        assert!(f.body.contains(TENANT_VAR));
    }
}

/// DB に繋ぐ前の検査の順は元と同じ: binding → テナント。どちらも欠けたら binding の 503 が先。
/// worker は `preflight` が `Ok` を返したときだけ connect する (`KINTAI_TENANT_ID` が空の初期状態では
/// connect せずに 503 — 接続の 502 が設定欠落を隠さない)。connect を呼ばないこと自体は wasm 側の分岐。
#[test]
fn preflight_decides_before_connecting_in_the_original_order() {
    let t = uuid(9);
    let raw = t.to_string();
    let pinned = |raw| TenantInput::pinned(raw);
    assert_eq!(preflight(true, &pinned(Some(&raw))), Ok(default_tenancy(t)));
    assert_eq!(preflight(false, &pinned(Some(&raw))), Err(no_db()));
    assert_eq!(
        preflight(false, &pinned(None)),
        Err(no_db()),
        "両方欠けたら binding が先"
    );
    for raw in [None, Some(""), Some("00000000-0000-0000-0000-000000000000")] {
        let f =
            preflight(true, &pinned(raw)).expect_err("テナントが決まらなければ connect に進まない");
        assert_eq!(f, tenant_of(raw).unwrap_err(), "{raw:?}");
        assert_eq!(f.status, 503);
    }
    // ヘッダの 403 も binding の 503 より後
    let bad = TenantInput {
        header: Some("not-a-uuid"),
        ..pinned(Some(&raw))
    };
    assert_eq!(preflight(false, &bad), Err(no_db()));
    assert_eq!(preflight(true, &bad), Err(tenant_not_allowed()));
}

// ── リクエストごとのテナント (X-Kintai-Tenant-Id) ──

fn default_tenancy(tenant: Uuid) -> Tenancy {
    Tenancy {
        tenant,
        is_default: true,
    }
}

#[test]
fn tenant_names_are_fixed() {
    assert_eq!(TENANT_VAR, "KINTAI_TENANT_ID");
    assert_eq!(TENANT_IDS_VAR, "KINTAI_TENANT_IDS");
    assert_eq!(TENANT_HEADER, "X-Kintai-Tenant-Id");
}

/// ヘッダ無し = 今までの pin と同じ (KINTAI_TENANT_IDS は見ない。不正な既定は 503 のまま)。
#[test]
fn without_the_header_the_default_pin_decides() {
    let d = uuid(1).to_string();
    let other = uuid(2).to_string();
    let input = TenantInput {
        default: Some(&d),
        allowed: Some(&other),
        header: None,
    };
    assert_eq!(resolve_tenant(&input), Ok(default_tenancy(uuid(1))));
    for raw in [
        None,
        Some(""),
        Some("x"),
        Some("00000000-0000-0000-0000-000000000000"),
    ] {
        let input = TenantInput {
            default: raw,
            allowed: Some(&other),
            header: None,
        };
        assert_eq!(resolve_tenant(&input), Err(tenant_of(raw).unwrap_err()));
    }
}

/// ヘッダが既定と同じなら既定扱い (大文字でも UUID として同じなら同じ)。KINTAI_TENANT_IDS は空でよい。
#[test]
fn the_header_naming_the_default_is_the_default() {
    let d = uuid(1).to_string();
    let upper = d.to_uppercase();
    for allowed in [None, Some("")] {
        let input = TenantInput {
            default: Some(&d),
            allowed,
            header: Some(&upper),
        };
        assert_eq!(resolve_tenant(&input), Ok(default_tenancy(uuid(1))));
    }
}

/// KINTAI_TENANT_IDS に入っていれば既定以外として採用 (空白を挟んでも・読めない要素が混ざっても)。
#[test]
fn a_listed_header_is_another_tenant() {
    let d = uuid(1).to_string();
    let listed = format!("junk, {} ,{}", uuid(2), uuid(3));
    for n in [2, 3] {
        let h = uuid(n).to_string();
        let input = TenantInput {
            default: Some(&d),
            allowed: Some(&listed),
            header: Some(&h),
        };
        let t = resolve_tenant(&input).expect("listed");
        assert_eq!(
            t,
            Tenancy {
                tenant: uuid(n),
                is_default: false
            }
        );
        assert_eq!(t.alc_tenant_id(), Some(h.clone()));
    }
    // 既定が空でも、許可リストに入っていれば選べる
    let h = uuid(2).to_string();
    let input = TenantInput {
        default: None,
        allowed: Some(&listed),
        header: Some(&h),
    };
    assert!(!resolve_tenant(&input).unwrap().is_default);
}

/// 既定の Tenancy は alc に tenantId を渡さない。
#[test]
fn the_default_tenancy_sends_no_tenant_id_to_alc() {
    assert_eq!(default_tenancy(uuid(1)).alc_tenant_id(), None);
}

/// 許可されていない・読めない・nil・空のヘッダは 403 (KINTAI_TENANT_IDS が空なら既定以外は全部 403)。
#[test]
fn an_unlisted_or_broken_header_is_forbidden() {
    let d = uuid(1).to_string();
    let listed = uuid(2).to_string();
    let unlisted = uuid(4).to_string();
    let nil = Uuid::nil().to_string();
    for (allowed, header) in [
        (Some(listed.as_str()), unlisted.as_str()),
        (None, listed.as_str()),
        (Some(""), listed.as_str()),
        (Some(listed.as_str()), "not-a-uuid"),
        (Some(listed.as_str()), ""),
        (Some(nil.as_str()), nil.as_str()),
    ] {
        let input = TenantInput {
            default: Some(&d),
            allowed,
            header: Some(header),
        };
        let f = resolve_tenant(&input).expect_err("forbidden");
        assert_eq!(f, tenant_not_allowed(), "{allowed:?} {header:?}");
        assert_eq!(f.status, 403);
    }
}

/// 口の関門: ヘッダ無しは何も見ない (既定が空でも通す = 今と同じく口ごとの順で決まる)。
/// 既定以外のテナントは Supabase と alc だけの口 (AnyAllowed) なら通り、それ以外 (DefaultOnly) は 403。
#[test]
fn the_gate_lets_other_tenants_reach_only_supabase_and_alc() {
    for reach in [Reach::AnyAllowed, Reach::DefaultOnly] {
        assert_eq!(tenant_gate(&TenantInput::default(), reach), Ok(()));
    }
    let d = uuid(1).to_string();
    let other = uuid(2).to_string();
    let with = |header| TenantInput {
        default: Some(&d),
        allowed: Some(&other),
        header,
    };
    // 既定を名指し → どの口も通る
    for reach in [Reach::AnyAllowed, Reach::DefaultOnly] {
        assert_eq!(tenant_gate(&with(Some(&d)), reach), Ok(()));
    }
    // 既定以外
    assert_eq!(tenant_gate(&with(Some(&other)), Reach::AnyAllowed), Ok(()));
    let f = tenant_gate(&with(Some(&other)), Reach::DefaultOnly).unwrap_err();
    assert_eq!(f, other_tenant_forbidden());
    assert_eq!(f.status, 403);
    assert!(f.body.contains("既定テナント"), "{}", f.body);
    // 許可されていないヘッダはどの口でも 403
    for reach in [Reach::AnyAllowed, Reach::DefaultOnly] {
        assert_eq!(
            tenant_gate(&with(Some("x")), reach),
            Err(tenant_not_allowed())
        );
    }
}

// ── テナントの担保 (BYPASSRLS の下で他テナントを見せない唯一の担保) ──

/// 5 本の SQL 定数 (change-log は 2 本) すべてが `$1` のテナントで絞っている。
#[test]
fn every_sql_filters_by_the_tenant_in_dollar_one() {
    let sqls = [
        ("day-summaries", day_summaries::SELECT_SQL, "tenant_id = $1"),
        (
            "shift-overlaps",
            shift_overlaps::SELECT_SQL,
            "a.tenant_id = $1",
        ),
        ("shift-days", shift_days::SELECT_SQL, "s.tenant_id = $1"),
        ("change-log", change_log::SELECT_SQL, "tenant_id = $1"),
        ("change-log since", change_log::SINCE_SQL, "tenant_id = $1"),
        ("wage-range", wage_range::SELECT_RANGE_SQL, "tenant_id = $1"),
    ];
    for (name, sql, want) in sqls {
        assert!(sql.contains(want), "{name}: {want} が無い");
        // WHERE 句の中にある (SELECT 列や JOIN の ON に紛れていない)
        let where_at = sql
            .find("WHERE")
            .unwrap_or_else(|| panic!("{name}: WHERE が無い"));
        assert!(sql[where_at..].contains(want), "{name}: WHERE の中に無い");
    }
    // shift-overlaps の b 側は a と同じテナントに結ぶ (自己結合で他テナントへ広がらない)
    assert!(shift_overlaps::SELECT_SQL.contains("b.tenant_id = a.tenant_id"));
    // shift-days の JOIN 先も同じテナント
    assert!(shift_days::SELECT_SQL.contains("d.tenant_id = s.tenant_id"));
    assert!(shift_days::SELECT_SQL.contains("p.tenant_id = s.tenant_id"));
}

/// 第 1 引数が `Type::UUID` で、値が pin そのもの (送る bytes が pin の 16 bytes)。
fn assert_first_is_the_pin(name: &str, params: &[Param<'_>], pin: Uuid) {
    let (value, ty) = params
        .first()
        .unwrap_or_else(|| panic!("{name}: 引数が無い"));
    assert_eq!(*ty, Type::UUID, "{name}: $1 が UUID でない");
    let mut buf = BytesMut::new();
    let is_null = value
        .to_sql_checked(ty, &mut buf)
        .expect("UUID として書ける");
    assert!(matches!(is_null, IsNull::No), "{name}: $1 が NULL");
    assert_eq!(&buf[..], pin.as_bytes(), "{name}: $1 が pin でない");
}

#[test]
fn every_binds_puts_the_pin_first_as_uuid() {
    let pin = uuid(0x1234_5678_9abc_def0_1122_3344_5566_7788);

    let req = day_summaries::parse("month=2026-06").unwrap();
    let b = day_summaries::Binds::new(pin, &req);
    assert_first_is_the_pin("day-summaries", &b.params(), pin);

    let req = shift_overlaps::parse("month=2026-06").unwrap();
    let b = shift_overlaps::Binds::new(pin, &req);
    assert_first_is_the_pin("shift-overlaps", &b.params(), pin);

    let req = shift_days::parse("month=2026-06&driver=9001").unwrap();
    let b = shift_days::Binds::new(pin, &req);
    assert_first_is_the_pin("shift-days", &b.params(), pin);

    let req = change_log::parse("from=2026-02-01&to=2026-02-28").unwrap();
    let b = change_log::Binds::new(pin, &req);
    assert_first_is_the_pin("change-log", &b.params(), pin);
    assert_first_is_the_pin("change-log since", &b.since_params(), pin);
    assert_eq!(b.since_params().len(), 1);

    let req = wage_range::parse("comp=c&from=2026-01&to=2026-03").unwrap();
    let b = wage_range::Binds::new(pin, &req);
    assert_first_is_the_pin("wage-range", &b.params(), pin);
}

/// `$n` の数と SQL の中の最大の `$n` が合う (足りない・余るは DB で落ちる前にここで分かる)。
#[test]
fn param_counts_match_the_sql() {
    fn max_placeholder(sql: &str) -> usize {
        (1..=9)
            .filter(|n| sql.contains(&format!("${n}")))
            .max()
            .unwrap_or(0)
    }
    let pin = uuid(7);
    let ds = day_summaries::Binds::new(pin, &day_summaries::parse("month=2026-06").unwrap());
    assert_eq!(
        ds.params().len(),
        max_placeholder(day_summaries::SELECT_SQL)
    );
    let so = shift_overlaps::Binds::new(pin, &shift_overlaps::parse("month=2026-06").unwrap());
    assert_eq!(
        so.params().len(),
        max_placeholder(shift_overlaps::SELECT_SQL)
    );
    let sd = shift_days::Binds::new(pin, &shift_days::parse("month=2026-06&driver=1").unwrap());
    assert_eq!(sd.params().len(), max_placeholder(shift_days::SELECT_SQL));
    let cl = change_log::Binds::new(
        pin,
        &change_log::parse("from=2026-02-01&to=2026-02-02").unwrap(),
    );
    assert_eq!(cl.params().len(), max_placeholder(change_log::SELECT_SQL));
    assert_eq!(
        cl.since_params().len(),
        max_placeholder(change_log::SINCE_SQL)
    );
    let wr = wage_range::Binds::new(
        pin,
        &wage_range::parse("comp=c&from=2026-01&to=2026-01").unwrap(),
    );
    assert_eq!(
        wr.params().len(),
        max_placeholder(wage_range::SELECT_RANGE_SQL)
    );
}
