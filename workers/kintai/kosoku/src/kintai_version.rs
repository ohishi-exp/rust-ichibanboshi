//! `GET /api/kintai/version` の純粋部分 — `VERSION_SQL` の範囲と etag の畳み方 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `kintai_version.rs` から I/O (MariaDB の往復) を除いた部分を移したもの。ソーステーブルの列挙・
//! マーカーの形・範囲の決め方の理由はオンプレ版のモジュール docs を参照。
//!
//! **版 (`build`) は引数で受ける** — オンプレ版は root の `build.rs` が焼く `KINTAI_OUTPUT_SHA`、勤怠 Worker は
//! 自分の build で焼く版を渡す (出どころが違うので、ここでは `env!` しない)。

use sha2::{Digest, Sha256};

use crate::kosoku::KosokuParams;
use crate::window::{exact_month_range, lookback_from, month_range};

/// ソーステーブル 1 つぶんの鮮度マーカー。値は SQL 側で `CAST(... AS CHAR)` 済み —
/// 数値型の推測で駆動側が黙って落ちる事故 (tiberius #86/#95 と同族) を避ける。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMarker {
    /// テーブル名 (etag の折り込みキー)
    pub source: String,
    /// 対象範囲の行数
    pub count: String,
    /// 対象範囲の内容指紋。ほとんどのテーブルは対象列の CRC32 の和、
    /// `dtako_events` のみ `MAX(id)` (オンプレ版のモジュール docs の「例外」参照)。0 行なら "0"
    pub fingerprint: String,
}

/// 前月初 (`YYYY-MM-01 00:00:00`) — `dtako_events` マーカーの `:efrom`。
/// 月 M の応答に含まれる「前月開始・M 月終了」行 (`EVENTS_SQL` 第 4 ブランチ) を
/// 覆うため、範囲を前月へ 1 ヶ月広げる。
fn prev_month_start(month: &str) -> Option<String> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let prev = if mm == 1 {
        chrono::NaiveDate::from_ymd_opt(year - 1, 12, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm - 1, 1)?
    };
    Some(format!("{prev} 00:00:00"))
}

/// `VERSION_SQL` に渡す範囲 (Refs ohishi-exp/nuxt-dtako-admin#1123)。名前は SQL の引数と同じ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRanges {
    /// 打刻 2 表の始端 — 読み窓の始端 (遡り起点の最小、無ければ月初)
    pub from: String,
    pub to: String,
    pub mfrom: String,
    pub mto: String,
    /// `dtako_events` の始端 — `from` の属する月の前月初 (起点が無ければ今までどおり
    /// 対象月の前月初)。窓の前に始まって窓の中で終わる休息 (`EVENTS_SQL` 第 4
    /// ブランチ) の余裕を、遡った始端に対しても同じだけ取る
    pub efrom: String,
}

/// 月と遡り起点から `VERSION_SQL` の範囲を決める。起点が無ければ今までと同じ範囲。
///
/// イベント系はデータクエリと同じ [月初, 翌月+1日)、フェリー系・daily 系はその月ちょうど
/// [月初, 翌月初) — 範囲がデータクエリとズレると「データは変わったのに etag が変わらない」を作り込む。
pub fn version_ranges(
    month: &str,
    anchors: &std::collections::BTreeMap<u64, String>,
) -> Option<VersionRanges> {
    let (month_start, to) = month_range(month)?;
    let (mfrom, mto) = exact_month_range(month)?;
    let from = lookback_from(&month_start, anchors);
    // 休息は窓の前に始まって窓の中で終わる区間も読まれる — 始端の属する月の前月初から
    let efrom = prev_month_start(from.get(..7)?)?;
    Some(VersionRanges {
        from,
        to,
        mfrom,
        mto,
        efrom,
    })
}

/// マーカー列を不透明な etag へ畳む。
///
/// - **マーカーの並び順に依存しない** — source 名で整列してから畳む。
///   `UNION ALL` の行順は保証されないため、順序を意味に含めない
/// - `month` / `build` (応答を形づくるコードの版) / `params` (`KosokuParams` の Debug 表現)
///   も材料に入れる — テーブルが 1 行も変わらなくても、計算ロジックや TOML
///   (丸め方・閾値) が変われば応答は変わるため
/// - 返り値は HTTP の quoted ETag そのもの (`"…"` 込み)。JSON の `etag` と
///   `ETag` ヘッダに**同じ文字列**を使い、relay は文字列比較だけで済ませる
pub fn fold_etag(month: &str, build: &str, params: &str, markers: &[SourceMarker]) -> String {
    let mut lines: Vec<String> = markers
        .iter()
        .map(|m| format!("{}|{}|{}", m.source, m.count, m.fingerprint))
        .collect();
    lines.sort();
    let mut hasher = Sha256::new();
    hasher.update(month.as_bytes());
    hasher.update(b"\n");
    hasher.update(build.as_bytes());
    hasher.update(b"\n");
    hasher.update(params.as_bytes());
    hasher.update(b"\n");
    for line in &lines {
        hasher.update(line.as_bytes());
        hasher.update(b"\n");
    }
    format!("\"{:x}\"", hasher.finalize())
}

/// [`fold_etag`] に `KosokuParams` を Debug 表現で畳む (再ビルド無しの TOML 変更でも応答が変わるため)。
/// オンプレ版と勤怠 Worker が同じ表現を使うように、書式はここにだけ置く。
pub fn version_etag(
    month: &str,
    build: &str,
    params: &KosokuParams,
    markers: &[SourceMarker],
) -> String {
    fold_etag(month, build, &format!("{params:?}"), markers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::VERSION_SQL;

    fn marker(source: &str, count: &str, fp: &str) -> SourceMarker {
        SourceMarker {
            source: source.to_string(),
            count: count.to_string(),
            fingerprint: fp.to_string(),
        }
    }

    /// `kosoku-daily` / `daily` の全ソーステーブルが SQL に居ることを固定する。
    /// **列挙漏れが唯一の危険点** (#184) — うっかり削らないための guard。
    #[test]
    fn version_sql_covers_every_source_table() {
        for table in [
            "time_card_dstate",
            "time_card_dtako",
            "time_card_dtako_state",
            "dtako_events",
            "dtako_cars",
            "dtako_ferry_rows",
            "dtako_rows",
            "daily_report_other_detail",
            "drivers",
            "offices",
            "time_card_non_legal_holiday",
        ] {
            // 後ろに空白 (alias) を要求する — `time_card_dtako` の検査が
            // `time_card_dtako_state` の行に前方一致で通ってしまわないように
            assert!(
                VERSION_SQL.contains(&format!("FROM {table} ")),
                "VERSION_SQL misses source table: {table}"
            );
        }
    }

    /// `dtako_events` は index-only の COUNT + MAX(id) 1 ブランチ (オンプレ版のモジュール docs
    /// の「例外」)。行本体を読む CRC を復活させると冷えた DB で 7〜24 秒に戻り、
    /// `COALESCE` で `終了日時` を混ぜると全表走査 (#121 → #122 の実害)。
    #[test]
    fn version_sql_keeps_index_only_dtako_events() {
        assert_eq!(VERSION_SQL.matches("FROM dtako_events").count(), 1);
        // 範囲は前月初 (:efrom) から — EVENTS_SQL 第 4 ブランチの
        // 「前月開始・当月終了」行を覆う
        assert!(VERSION_SQL.contains("`開始日時` >= :efrom"));
        assert!(VERSION_SQL.contains("MAX(e.id)"));
        assert!(!VERSION_SQL.contains("COALESCE(`終了日時`"));
        // events ブランチに CRC が無いこと (CRC32 は他テーブル用に残る)
        let events_branch = VERSION_SQL
            .split("'dtako_events'")
            .nth(1)
            .unwrap()
            .split("UNION ALL")
            .next()
            .unwrap();
        assert!(!events_branch.contains("CRC32"));
    }

    /// `dtako_ferry_rows` は列単位 GRANT (`運行NO`/`開始日時`/`終了日時` のみ)。
    /// `COUNT(*)` や他の列 (`標準料金` 等) に触ると権限エラーで endpoint ごと落ちる。
    #[test]
    fn version_sql_stays_within_ferry_column_grants() {
        assert!(VERSION_SQL.contains("COUNT(f.`開始日時`)"));
        assert!(!VERSION_SQL.contains("標準料金"));
        assert!(!VERSION_SQL.contains("契約料金"));
    }

    /// 遡り起点が無い月は今までと同じ範囲 (Refs ohishi-exp/nuxt-dtako-admin#1123)。
    #[test]
    fn version_ranges_without_anchors_keep_the_month_window() {
        let r = version_ranges("2026-04", &Default::default()).unwrap();
        assert_eq!(r.from, "2026-04-01 00:00:00");
        assert_eq!(r.to, "2026-05-02 00:00:00");
        assert_eq!(r.mfrom, "2026-04-01 00:00:00");
        assert_eq!(r.mto, "2026-05-01 00:00:00");
        assert_eq!(r.efrom, "2026-03-01 00:00:00");
        assert!(version_ranges("2026-13", &Default::default()).is_none());
    }

    /// 起点があれば打刻 2 表の始端がそこまで下がり、`dtako_events` は始端の属する
    /// 月の前月初から。フェリー・daily 系 (`mfrom`) は動かない。
    #[test]
    fn version_ranges_follow_the_earliest_anchor() {
        // 前月内の起点 (1194 型) — 始端は 3 月なので休息は 2 月初から
        let anchors = [
            (1194, "2026-03-31 21:36:28".to_string()),
            (1300, "2026-03-30 08:00:00".to_string()),
        ]
        .into_iter()
        .collect();
        let r = version_ranges("2026-04", &anchors).unwrap();
        assert_eq!(r.from, "2026-03-30 08:00:00");
        assert_eq!(r.mfrom, "2026-04-01 00:00:00");
        assert_eq!(r.efrom, "2026-02-01 00:00:00");

        // 前々月の起点 (1731 型の閉じ忘れ運行) — 2/20 20:00 に始まり起点 2/21 05:00 の
        // 後に終わる休息も画面は読むので、etag も 1 月初から数える
        let fossil = [(1731, "2026-02-21 05:00:00".to_string())]
            .into_iter()
            .collect();
        let r = version_ranges("2026-04", &fossil).unwrap();
        assert_eq!(r.from, "2026-02-21 05:00:00");
        assert_eq!(r.efrom, "2026-01-01 00:00:00");
        assert!(r.efrom.as_str() <= "2026-02-20 20:00:00");

        // 起点が読めない形 (7 文字に満たない) なら範囲を作らない
        let broken = [(1, "2026".to_string())].into_iter().collect();
        assert!(version_ranges("2026-04", &broken).is_none());
    }

    #[test]
    fn prev_month_start_handles_year_boundary() {
        assert_eq!(
            prev_month_start("2026-07").as_deref(),
            Some("2026-06-01 00:00:00")
        );
        assert_eq!(
            prev_month_start("2026-01").as_deref(),
            Some("2025-12-01 00:00:00")
        );
        assert_eq!(prev_month_start("bad"), None);
    }

    #[test]
    fn fold_is_deterministic_and_order_insensitive() {
        let a = marker("time_card_dstate", "10", "12345");
        let b = marker("dtako_events", "20", "67890");
        let e1 = fold_etag("2026-07", "abc", "params", &[a.clone(), b.clone()]);
        let e2 = fold_etag("2026-07", "abc", "params", &[b, a]);
        assert_eq!(e1, e2);
        // HTTP の quoted ETag そのもの
        assert!(e1.starts_with('"') && e1.ends_with('"'));
    }

    #[test]
    fn fold_changes_when_any_ingredient_changes() {
        let base = vec![
            marker("time_card_dstate", "10", "12345"),
            marker("dtako_events", "20", "67890"),
        ];
        let e = fold_etag("2026-07", "abc", "params", &base);

        // 行数だけ変わる (INSERT + 同値 DELETE でも count が動く)
        let mut m = base.clone();
        m[0].count = "11".into();
        assert_ne!(e, fold_etag("2026-07", "abc", "params", &m));

        // fingerprint だけ変わる (在数同じ UPDATE)
        let mut m = base.clone();
        m[1].fingerprint = "67891".into();
        assert_ne!(e, fold_etag("2026-07", "abc", "params", &m));

        // 月・build (デプロイ)・params (TOML) でも変わる
        assert_ne!(e, fold_etag("2026-08", "abc", "params", &base));
        assert_ne!(e, fold_etag("2026-07", "abd", "params", &base));
        assert_ne!(e, fold_etag("2026-07", "abc", "params2", &base));

        // テーブルが 1 つ増減しても変わる (列挙漏れの検知はできないが、SQL 側の
        // 行が欠けたまま etag が同じになることはない)
        let m = base[..1].to_vec();
        assert_ne!(e, fold_etag("2026-07", "abc", "params", &m));
    }

    #[test]
    fn fold_separates_fields_unambiguously() {
        // 隣接フィールドの再分割で同じ材料にならないこと ("1|23" vs "12|3")
        let e1 = fold_etag("2026-07", "abc", "p", &[marker("t", "1", "23")]);
        let e2 = fold_etag("2026-07", "abc", "p", &[marker("t", "12", "3")]);
        assert_ne!(e1, e2);
    }

    /// `KosokuParams` は Debug 表現で畳む (オンプレ版がこれまで畳んでいたのと同じ材料)。
    #[test]
    fn version_etag_folds_the_params_debug_form() {
        let p = KosokuParams::default();
        let m = [marker("t", "1", "2")];
        let want = fold_etag("2026-07", "abc", &format!("{p:?}"), &m);
        assert_eq!(version_etag("2026-07", "abc", &p, &m), want);
    }
}
