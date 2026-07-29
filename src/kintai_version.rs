//! 勤怠の月別バージョン (ETag) 読み取り (Refs #184)。
//!
//! nuxt-dtako-admin の relay (dtako-scraper-relay) が上流応答キャッシュの
//! **条件付き再検証**に使う。relay はキャッシュを返す前に毎回ここを叩き、
//! etag が変わっていなければ MB 級の `daily` / `kosoku-daily` の引き直しを省く。
//! 鮮度要件は「古い値は一切返さない」なので、**唯一の危険点はソーステーブルの
//! 列挙漏れ** — 上流が変わったのに etag が変わらないと relay が古い値を返し続ける。
//!
//! ## ソーステーブルの列挙 (完全性が正義、安さは二の次)
//!
//! `/api/kintai/kosoku-daily` (MariaDB 直読み、`kintai_repo.rs`) と
//! `/api/kintai/daily` (CakePHP `TimeCardController::dailyJson` 中継) の**両方**の
//! 読むテーブルを覆う:
//!
//! | テーブル | 消費者 | 範囲 | 根拠 |
//! |---|---|---|---|
//! | `time_card_dstate` | 両方 | 月 (`month_range`) | `EVENTS_SQL` / dailyJson の打刻 30/31 |
//! | `time_card_dtako` | kosoku-daily | 月 (`month_range`) | `EVENTS_SQL` 2 本目 |
//! | `time_card_dtako_state` | kosoku-daily | 全体 (マスタ) | `EVENTS_SQL` の JOIN |
//! | `dtako_events` | kosoku-daily | 月 (2 ブランチ) | `EVENTS_SQL` 3/4 本目 |
//! | `dtako_cars` | kosoku-daily | 全体 (マスタ) | `EVENTS_SQL` の JOIN (`車輌名`) |
//! | `dtako_ferry_rows` | kosoku-daily | 月 (`exact_month_range`) | `FERRY_SQL` |
//! | `dtako_rows` | kosoku-daily | 月 (出庫 or 帰庫) | `FERRY_SQL` の JOIN |
//! | `daily_report_other_detail` | daily | 月 (`act_date`, kyuka) | dailyJson の leaves |
//! | `drivers` | daily | 全体 (マスタ) | dailyJson の name / office 引き当て |
//! | `offices` | daily | 全体 (マスタ) | dailyJson の office 名 (`bumon_code_id` 結合) |
//! | `time_card_non_legal_holiday` | daily | 月 (`p_date`) | `HolidaysTrait::getNonLegalHoliday` |
//!
//! **覆えないもの**: dailyJson の国民の祝日は外部 API
//! (`holidays-jp.github.io`) で DB に無い — etag には畳めない (変化は年 1 回程度、
//! 祝日は `holiday` 区分の表示にしか効かない)。計算ロジック側の変化は
//! `BUILD_SHA` (デプロイごとに変わる) と `KosokuParams` (TOML 追随) を
//! 畳んで覆う — route 側 (`routes/kintai_version.rs`) の担当。
//!
//! ## マーカーの形 — COUNT + CRC32 の SUM (updated_at に頼らない)
//!
//! `dtako_*` 系は modified 列を持たないため `MAX(updated_at)` 方式が使えない。
//! 代わりに**データクエリが読む列そのもの**の `SUM(CRC32(CONCAT_WS(...)))` +
//! `COUNT(*)` を月範囲で取る — 応答に影響し得る変更 (INSERT / UPDATE / DELETE の
//! いずれも) が必ずどちらかを動かす。範囲・列はデータクエリの**上位集合**に
//! 揃える (広すぎる分は「無駄な再取得」で済むが、狭いと「古い値」になる)。
//! コストはデータクエリ自身と同程度の index range scan で、転送が無い分軽い。
//!
//! ## 追加 GRANT が要る (デプロイ前提条件)
//!
//! `kintai_reader` は SELECT のみの専用アカウントで、テーブル (一部は列) 単位の
//! GRANT 運用。本モジュールが新たに読む `daily_report_other_detail` / `drivers` /
//! `offices` / `time_card_non_legal_holiday` に SELECT が無いと endpoint は
//! **502 fail-closed** になる (黙って一部テーブルを外した etag は返さない —
//! それは列挙漏れと同じ「古い値」事故になるため)。

use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{params, Pool};
use sha2::{Digest, Sha256};

use crate::config::MariadbConfig;
use crate::kintai_repo::{exact_month_range, month_range, KintaiRepoError};

/// ソーステーブル 1 つぶんの鮮度マーカー。値は SQL 側で `CAST(... AS CHAR)` 済み —
/// 数値型の推測で駆動側が黙って落ちる事故 (tiberius #86/#95 と同族) を避ける。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMarker {
    /// テーブル名 (etag の折り込みキー)
    pub source: String,
    /// 対象範囲の行数
    pub count: String,
    /// 対象範囲・対象列の CRC32 の和 (0 行なら "0")
    pub fingerprint: String,
}

/// バージョンマーカーの読み出し口。DB 実装と mock を差し替えるための trait
/// (`KintaiEventsApi` と同じ形 — route のテストを DB 無しで回すため)。
#[async_trait]
pub trait KintaiVersionApi: Send + Sync {
    /// 対象月 (`YYYY-MM`) の全ソーステーブルのマーカーを返す。
    async fn fetch_markers(&self, month: &str) -> Result<Vec<SourceMarker>, KintaiRepoError>;
}

pub type DynKintaiVersionRepo = Arc<dyn KintaiVersionApi>;

/// `[mariadb]` 未設定時の実装 — 常に `NotConfigured` (= 503)。
pub struct DisabledKintaiVersionRepo;

#[async_trait]
impl KintaiVersionApi for DisabledKintaiVersionRepo {
    async fn fetch_markers(&self, _month: &str) -> Result<Vec<SourceMarker>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }
}

/// 全ソーステーブルのマーカーを 1 statement で取る。
///
/// - 範囲 (`:from`/`:to` = `month_range`、`:mfrom`/`:mto` = `exact_month_range`) と
///   列は、対応するデータクエリ (`EVENTS_SQL` / `ALL_EVENTS_SQL` / `FERRY_SQL` /
///   dailyJson) が読むものの**上位集合**に揃える。絞り (dailyJson の state 30/31 等)
///   は掛けない — 広い分は安全側
/// - `dtako_events` は `EVENTS_SQL` と同じ **2 ブランチ** (期間内に始まる区間 +
///   期間内に終わる区間)。`COALESCE` で 1 本にまとめると索引が効かず全表走査に
///   なる (#121 → #122 の実害) のも同じ
/// - `dtako_ferry_rows` は `kintai_reader` に**列単位 GRANT** (`運行NO` / `開始日時` /
///   `終了日時` のみ)。`COUNT(*)` ではなく `COUNT(f.``開始日時``)` を使い、他の列
///   (`標準料金` 等) には一切触らない
/// - 日時は `DATE_FORMAT` で文字列化してから CRC に入れる — driver の時刻型と
///   timezone 解釈を fingerprint に持ち込まない (`EVENTS_SQL` と同じ理由)
const VERSION_SQL: &str = r#"
SELECT 'time_card_dstate' AS source,
       CAST(COUNT(*) AS CHAR) AS cnt,
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           d.id, DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s'), d.state,
           DATE_FORMAT(d.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR) AS fp
  FROM time_card_dstate d
 WHERE d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT 'time_card_dtako',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           t.driver_id, DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'), t.state,
           t.event_name, t.unko_no,
           DATE_FORMAT(t.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM time_card_dtako t
 WHERE t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT 'time_card_dtako_state',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', s.id, s.name))), 0) AS CHAR)
  FROM time_card_dtako_state s
UNION ALL
SELECT 'dtako_events',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(ev.fp), 0) AS CHAR)
  FROM (
    SELECT CRC32(CONCAT_WS('|',
               e.`対象乗務員CD`,
               DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
               DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
               e.`イベント名`, e.`運行NO`, e.`車輌CD`)) AS fp
      FROM dtako_events e
     WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
    UNION ALL
    SELECT CRC32(CONCAT_WS('|',
               e.`対象乗務員CD`,
               DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
               DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
               e.`イベント名`, e.`運行NO`, e.`車輌CD`))
      FROM dtako_events e
     WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
       AND e.`開始日時` < :from
  ) ev
UNION ALL
SELECT 'dtako_cars',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', c.`車輌CD`, c.`車輌名`))), 0) AS CHAR)
  FROM dtako_cars c
UNION ALL
SELECT 'dtako_ferry_rows',
       CAST(COUNT(f.`開始日時`) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           f.`運行NO`,
           DATE_FORMAT(f.`開始日時`, '%Y-%m-%d %H:%i:%s'),
           DATE_FORMAT(f.`終了日時`, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM dtako_ferry_rows f
 WHERE f.`開始日時` >= :mfrom AND f.`開始日時` < :mto
UNION ALL
SELECT 'dtako_rows',
       CAST(COUNT(r.`運行NO`) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           r.`運行NO`, r.`対象乗務員CD`,
           DATE_FORMAT(r.`出庫日時`, '%Y-%m-%d %H:%i:%s'),
           DATE_FORMAT(r.`帰庫日時`, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM dtako_rows r
 WHERE (r.`出庫日時` >= :mfrom AND r.`出庫日時` < :mto)
    OR (r.`帰庫日時` >= :mfrom AND r.`帰庫日時` < :mto)
UNION ALL
SELECT 'daily_report_other_detail',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|',
           o.driver_id, o.act_date, o.detail,
           DATE_FORMAT(o.modified, '%Y-%m-%d %H:%i:%s')))), 0) AS CHAR)
  FROM daily_report_other_detail o
 WHERE o.report_type = 'kyuka' AND o.act_date >= :mfrom AND o.act_date < :mto
UNION ALL
SELECT 'drivers',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', v.id, v.name, v.bumon))), 0) AS CHAR)
  FROM drivers v
UNION ALL
SELECT 'offices',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CONCAT_WS('|', ofc.id, ofc.name, ofc.bumon_code_id))), 0) AS CHAR)
  FROM offices ofc
UNION ALL
SELECT 'time_card_non_legal_holiday',
       CAST(COUNT(*) AS CHAR),
       CAST(IFNULL(SUM(CRC32(CAST(h.p_date AS CHAR))), 0) AS CHAR)
  FROM time_card_non_legal_holiday h
 WHERE h.p_date >= :mfrom AND h.p_date < :mto
"#;

/// `VERSION_SQL` の 1 行 (source, cnt, fp — 全列 CHAR)。
type MarkerRow = (String, String, String);

/// MariaDB 実装。`MariadbKintaiEventsRepo` と同じく pool は lazy —
/// DB 停止中でも起動は失敗せず、実際に読むときに 502。
pub struct MariadbKintaiVersionRepo {
    pool: Pool,
}

impl MariadbKintaiVersionRepo {
    pub fn new(cfg: &MariadbConfig) -> Self {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(cfg.host.clone())
            .tcp_port(cfg.port)
            .user(Some(cfg.user.clone()))
            .pass(Some(cfg.password.clone()))
            .db_name(Some(cfg.database.clone()));
        Self {
            pool: Pool::new(opts),
        }
    }
}

#[async_trait]
impl KintaiVersionApi for MariadbKintaiVersionRepo {
    async fn fetch_markers(&self, month: &str) -> Result<Vec<SourceMarker>, KintaiRepoError> {
        // イベント系はデータクエリと同じ [月初, 翌月+1日)、フェリー系・daily 系は
        // その月ちょうど [月初, 翌月初) — 範囲がデータクエリとズレると
        // 「データは変わったのに etag が変わらない」を作り込む
        let (from, to) = month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        let (mfrom, mto) = exact_month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<MarkerRow> = conn
            .exec(
                VERSION_SQL,
                params! {
                    "from" => &from,
                    "to" => &to,
                    "mfrom" => &mfrom,
                    "mto" => &mto,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|(source, count, fingerprint)| SourceMarker {
                source,
                count,
                fingerprint,
            })
            .collect())
    }
}

/// マーカー列を不透明な etag へ畳む (純粋関数)。
///
/// - **マーカーの並び順に依存しない** — source 名で整列してから畳む。
///   `UNION ALL` の行順は保証されないため、順序を意味に含めない
/// - `month` / `build` (BUILD_SHA) / `params` (`KosokuParams` の Debug 表現) も
///   材料に入れる — テーブルが 1 行も変わらなくても、デプロイ (計算ロジック) や
///   TOML (丸め方・閾値) が変われば応答は変わるため
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// `dtako_events` は `EVENTS_SQL` と同じ 2 ブランチ。`COALESCE` で 1 本に
    /// まとめると全表走査 (#121 → #122 の実害)。
    #[test]
    fn version_sql_keeps_two_branch_dtako_events() {
        assert_eq!(VERSION_SQL.matches("FROM dtako_events").count(), 2);
        assert!(VERSION_SQL.contains("`開始日時` < :from"));
        assert!(!VERSION_SQL.contains("COALESCE(`終了日時`"));
    }

    /// `dtako_ferry_rows` は列単位 GRANT (`運行NO`/`開始日時`/`終了日時` のみ)。
    /// `COUNT(*)` や他の列 (`標準料金` 等) に触ると権限エラーで endpoint ごと落ちる。
    #[test]
    fn version_sql_stays_within_ferry_column_grants() {
        assert!(VERSION_SQL.contains("COUNT(f.`開始日時`)"));
        assert!(!VERSION_SQL.contains("標準料金"));
        assert!(!VERSION_SQL.contains("契約料金"));
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

    #[tokio::test]
    async fn disabled_repo_is_not_configured() {
        let err = DisabledKintaiVersionRepo
            .fetch_markers("2026-07")
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
    }
}
