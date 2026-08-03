//! ③ (勤務時間再登録、`resetby-unko-no`) の材料件数を PHP `_setbyUnkoNo` と
//! **完全一致**する絞り込みで数える (Refs #633 の 5、issue #281 の再発防止)。
//!
//! ## 何のガードか
//!
//! [`crate::routes::dtako_autoload`] の `?reset_timecard=true` は、③ (`time_card_dtako`
//! を全削除してから `dtako_events` を材料に作り直す) を打つ前に「材料が本当に
//! あるか」を数える。**材料 0 件のまま③を打つと削除だけが走り、`time_card_dtako`
//! が消えて戻らない** (issue #281 の実害)。
//!
//! ## なぜ `休息` だけでは足りなかったか (2026-08-04、issue #281 の再発)
//!
//! PHP `_setbyUnkoNo` は `dtako_events` を
//! `運行NO IN (先頭22桁+"1", 先頭22桁+"2") AND イベント名 IN ('休息','運行開始','運行終了')`
//! で読む。旧実装は [`crate::kintai_repo::KintaiEventsApi::fetch_rest_events_between`]
//! (`休息` 固定) しか無かったため `休息` だけを数えており、**休息が 0 件でも
//! 運行開始/運行終了だけが材料として残るケースを見逃していた**。実測 (運行
//! `26060608220000000041571`、乗務員 1740): `dtako_events` 23 行のうち `休息` は
//! 0 件、`運行開始`/`運行終了` は各 1 件 — 旧実装は `reset_skip_reason:
//! "no_dtako_events"` で③を誤ってスキップしていた。
//!
//! ## なぜ `kintai_repo.rs` に SQL を足さないか (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/` 配下をファイル名の接頭辞
//! (`kosoku` / `kintai`、`src/routes/` は `kintai` のみ) で拾い、拾われたファイルの
//! 内容ハッシュが `logic_version` (`/api/kintai/version` の etag) に畳まれる。この
//! materialカウントは勤怠の計算 (`kosoku-daily` / `daily`) に一切関与しない
//! (①②③の導線専用) ので、`kintai_repo.rs` を触ると無関係な deploy まで全乗務員
//! stale にしてしまう ([`crate::routes::dtako_day`] / [`crate::routes::unko_gaps`]
//! と同じ理由)。**この理由から `fetch_rest_events_between` は変更せず、独立した
//! trait・SQL・MariaDB pool をこのファイルに持つ** — [`crate::kintai_version`]
//! (`events` と同じ MariaDB を読むが trait / pool を分ける) と同じ形。
//!
//! `MARIADB_SESSION_SETUP` (convoy 防止の session 変数) だけは `kintai_repo.rs` から
//! `pub(crate)` のまま import する — セッション設定は重複させると片方だけ古くなる
//! 実害クラスの罠なので、[`crate::kintai_version::MariadbKintaiVersionRepo`] と同じく
//! 再利用する ([`crate::routes::unko_gaps`] のモジュール doc の「既に pub 公開済みの
//! 部品を呼ぶだけ」と同じ考え方)。
//!
//! ## 窓とフェイルセーフは変えない
//!
//! `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) が読めない入力は材料無し (`Ok(0)`) に倒す
//! (数えられない ＝ 安全と確認できない、既存の方針のまま)。窓
//! (開始日の前日 0 時から 3 日ぶん) も旧実装のまま — 日をまたぐ運行を取りこぼさない
//! ための余白で、絞り込みの種類 (イベント名) を増やしただけなので変える理由が無い。

use std::sync::Arc;

use async_trait::async_trait;
use chrono::NaiveDateTime;
use mysql_async::prelude::Queryable;
use mysql_async::{params, Pool};

use crate::config::MariadbConfig;
use crate::kintai_repo::KintaiRepoError;

/// 材料件数の読み出し口。DB 実装と mock を差し替えるための trait
/// ([`crate::kintai_repo::KintaiEventsApi`] と同じ形 — route のテストを DB 無しで
/// 回すため)。
#[async_trait]
pub trait ResetMaterialApi: Send + Sync {
    /// `dtako_events` を `運行NO IN (variant1, variant2)` (対象CD 1/2 両方) かつ
    /// `イベント名 IN ('休息','運行開始','運行終了')` で絞った件数を、窓
    /// `[from, to)` 内で返す。
    async fn count_material(
        &self,
        from: &str,
        to: &str,
        variant1: &str,
        variant2: &str,
    ) -> Result<i64, KintaiRepoError>;
}

pub type DynResetMaterialRepo = Arc<dyn ResetMaterialApi>;

/// `[mariadb]` 未設定時の実装 — 常に `NotConfigured` (= フェイルクローズ)。
pub struct DisabledResetMaterialRepo;

#[async_trait]
impl ResetMaterialApi for DisabledResetMaterialRepo {
    async fn count_material(
        &self,
        _from: &str,
        _to: &str,
        _variant1: &str,
        _variant2: &str,
    ) -> Result<i64, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }
}

/// 材料件数を 1 statement で数える。PHP `_setbyUnkoNo` の絞り込みと完全一致させる
/// (モジュール doc 参照)。
///
/// - **`dtako_events` だけを見る。** `time_card_dtako` は PHP 側が読まない
///   (`_setbyUnkoNo` は `dtako_events` からしか INSERT し直さない) ので混ぜない
/// - 2 ブランチに分ける理由 ([`crate::kintai_repo::REST_EVENTS_SQL`] と同じ):
///   **期間内に始まる区間**と**期間内に終わる区間 (開始は期間より前)** の両方を
///   拾わないと、日をまたぐ運行の材料を取りこぼす
/// - `運行NO IN (:v1, :v2)` と `イベント名 IN (...)` は両ブランチに掛ける —
///   窓は取りこぼし防止の余白であって、絞り込みそのものを緩める理由にはならない
const RESET_MATERIAL_SQL: &str = r#"
SELECT CAST(COUNT(*) AS SIGNED) AS n FROM (
  SELECT e.`運行NO` AS unko_no
    FROM dtako_events e
   WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
     AND e.`運行NO` IN (:v1, :v2)
     AND e.`イベント名` IN ('休息', '運行開始', '運行終了')
  UNION ALL
  SELECT e.`運行NO`
    FROM dtako_events e
   WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
     AND e.`開始日時` < :from
     AND e.`運行NO` IN (:v1, :v2)
     AND e.`イベント名` IN ('休息', '運行開始', '運行終了')
) counted
"#;

/// MariaDB 実装。pool は lazy — DB 停止中でも起動は失敗せず、実際に読むときに 502。
pub struct MariadbResetMaterialRepo {
    pool: Pool,
}

impl MariadbResetMaterialRepo {
    pub fn new(cfg: &MariadbConfig) -> Self {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(cfg.host.clone())
            .tcp_port(cfg.port)
            .user(Some(cfg.user.clone()))
            .pass(Some(cfg.password.clone()))
            .db_name(Some(cfg.database.clone()))
            // 60 秒超のステートメントを MariaDB 側で自動 abort (convoy 防止、
            // kintai_repo.rs の MARIADB_SESSION_SETUP 参照、モジュール doc も参照)
            .setup(vec![crate::kintai_repo::MARIADB_SESSION_SETUP.to_string()]);
        Self {
            pool: Pool::new(opts),
        }
    }
}

#[async_trait]
impl ResetMaterialApi for MariadbResetMaterialRepo {
    async fn count_material(
        &self,
        from: &str,
        to: &str,
        variant1: &str,
        variant2: &str,
    ) -> Result<i64, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let n: Option<i64> = conn
            .exec_first(
                RESET_MATERIAL_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                    "v1" => variant1,
                    "v2" => variant2,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(n.unwrap_or(0))
    }
}

/// `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) を運行開始日時として読む。
/// [`crate::routes::dtako_autoload`] / [`crate::routes::dtako_day`] と同じロジックを
/// 独立して持つ (モジュール doc の「なぜ `kintai_repo.rs` に足さないか」参照)。
fn unko_no_start_datetime(unko_no: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(unko_no.get(..12)?, "%y%m%d%H%M%S").ok()
}

/// 材料を数える窓。運行は日をまたぐ (実測: 開始 16:50 → 終了翌日 01:23) ので、
/// 開始日の前日 0 時から 3 日ぶんという広めの余白を取る (旧実装のまま)。
fn material_window(start_dt: NaiveDateTime) -> (String, String) {
    let from = start_dt.date() - chrono::Duration::days(1);
    let to = from + chrono::Duration::days(4);
    (format!("{from} 00:00:00"), format!("{to} 00:00:00"))
}

/// PHP `_setbyUnkoNo` が材料として見る**運行NO の 2 パターン** (対象CD 1/2 両方) を
/// 組む。`substr($id, 0, 22)` に "1"/"2" を付けるだけの PHP 実装をそのまま写す
/// (旧実装のまま)。呼び出し側が渡した末尾 1 桁は使わない — PHP 自身が無視して
/// 両方を見るため。
fn reset_material_unko_no_variants(unko_no: &str) -> (String, String) {
    let prefix: String = unko_no.chars().take(22).collect();
    (format!("{prefix}1"), format!("{prefix}2"))
}

/// ③ の材料件数を数える ([`crate::routes::dtako_autoload::autoload`] の
/// `reset_timecard=true` から呼ばれる)。`unko_no` の先頭 12 桁が読めない (壊れた
/// 入力) 場合は材料無しとして `Ok(0)` (fail-safe — 数えられないなら実行しない側に
/// 倒す)。
pub async fn count_reset_material(
    repo: &DynResetMaterialRepo,
    unko_no: &str,
) -> Result<i64, KintaiRepoError> {
    let Some(start_dt) = unko_no_start_datetime(unko_no) else {
        return Ok(0);
    };
    let (from, to) = material_window(start_dt);
    let (variant1, variant2) = reset_material_unko_no_variants(unko_no);
    repo.count_material(&from, &to, &variant1, &variant2).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 呼ばれた引数を記録し、仕込んだ件数を返す mock。
    struct MockRepo {
        count: i64,
    }

    #[async_trait]
    impl ResetMaterialApi for MockRepo {
        async fn count_material(
            &self,
            _from: &str,
            _to: &str,
            _variant1: &str,
            _variant2: &str,
        ) -> Result<i64, KintaiRepoError> {
            Ok(self.count)
        }
    }

    /// 呼ばれた引数を検証する mock (窓・variant の組み立てを固定する)。
    struct AssertingRepo {
        expected_from: &'static str,
        expected_to: &'static str,
        expected_v1: &'static str,
        expected_v2: &'static str,
        count: i64,
    }

    #[async_trait]
    impl ResetMaterialApi for AssertingRepo {
        async fn count_material(
            &self,
            from: &str,
            to: &str,
            variant1: &str,
            variant2: &str,
        ) -> Result<i64, KintaiRepoError> {
            assert_eq!(from, self.expected_from);
            assert_eq!(to, self.expected_to);
            assert_eq!(variant1, self.expected_v1);
            assert_eq!(variant2, self.expected_v2);
            Ok(self.count)
        }
    }

    /// 呼ばれたら panic する repo (fail-safe が本当にクエリを投げないことの検証用)。
    struct PanicRepo;

    #[async_trait]
    impl ResetMaterialApi for PanicRepo {
        async fn count_material(
            &self,
            _from: &str,
            _to: &str,
            _variant1: &str,
            _variant2: &str,
        ) -> Result<i64, KintaiRepoError> {
            panic!("材料を数えてはいけない場面で呼ばれた")
        }
    }

    struct FailingRepo;

    #[async_trait]
    impl ResetMaterialApi for FailingRepo {
        async fn count_material(
            &self,
            _from: &str,
            _to: &str,
            _variant1: &str,
            _variant2: &str,
        ) -> Result<i64, KintaiRepoError> {
            Err(KintaiRepoError::QueryFailed("boom".to_string()))
        }
    }

    #[test]
    fn unko_no_start_datetime_reads_the_leading_12_digits() {
        let dt = unko_no_start_datetime("26060608220000000041571").unwrap();
        assert_eq!(dt.to_string(), "2026-06-06 08:22:00");
        assert_eq!(unko_no_start_datetime("U1"), None, "12桁に満たない");
    }

    #[test]
    fn material_window_spans_a_day_before_to_three_days_after_the_start_date() {
        let start = unko_no_start_datetime("26060608220000000041571").unwrap();
        let (from, to) = material_window(start);
        assert_eq!(
            from, "2026-06-05 00:00:00",
            "日をまたぐ運行を取りこぼさない余白"
        );
        assert_eq!(to, "2026-06-09 00:00:00");
    }

    #[test]
    fn reset_material_unko_no_variants_builds_both_crew_suffixes_from_the_leading_22_digits() {
        assert_eq!(
            reset_material_unko_no_variants("26060608220000000041571"),
            (
                "26060608220000000041571".to_string(),
                "26060608220000000041572".to_string()
            ),
            "呼び出し側の末尾1桁は無視し、両クルーを組む (PHP _setbyUnkoNo と同じ)"
        );
        assert_eq!(
            reset_material_unko_no_variants("2606060822000000004157"),
            (
                "26060608220000000041571".to_string(),
                "26060608220000000041572".to_string()
            ),
            "22桁ちょうどの入力でも動く"
        );
    }

    #[tokio::test]
    async fn count_reset_material_passes_the_window_and_both_crew_variants_through() {
        let repo: DynResetMaterialRepo = Arc::new(AssertingRepo {
            expected_from: "2026-06-05 00:00:00",
            expected_to: "2026-06-09 00:00:00",
            expected_v1: "26060608220000000041571",
            expected_v2: "26060608220000000041572",
            count: 2,
        });
        let n = count_reset_material(&repo, "26060608220000000041571")
            .await
            .unwrap();
        assert_eq!(n, 2);
    }

    /// ★実測 (issue #281 の再発、2026-08-04): 運行 26060608220000000041571 は
    /// `休息` 0 件・`運行開始` 1 件・`運行終了` 1 件で、旧実装 (休息だけ数える)
    /// では 0 と誤判定していた。新実装は SQL 側で 3 種を数えるので、mock は
    /// その集計結果 (2) をそのまま返せばよい — 1 以上なら③がスキップされない
    /// ことを固定する。
    #[tokio::test]
    async fn count_reset_material_counts_rest_start_and_end_events_not_just_rest() {
        let repo: DynResetMaterialRepo = Arc::new(MockRepo { count: 2 });
        let n = count_reset_material(&repo, "26060608220000000041571")
            .await
            .unwrap();
        assert_eq!(n, 2, "休息0件でも運行開始/運行終了があれば1以上を返す");
    }

    /// 回帰: issue #281 の実害ケース (`dtako_events` が丸ごと 0 件) では
    /// 従来どおり 0 を返し、③を止める歯止めを壊さない。
    #[tokio::test]
    async fn count_reset_material_returns_zero_when_dtako_events_is_completely_empty() {
        let repo: DynResetMaterialRepo = Arc::new(MockRepo { count: 0 });
        let n = count_reset_material(&repo, "26060608220000000041571")
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn count_reset_material_is_zero_and_never_queries_when_unko_no_is_too_short_to_parse() {
        // fail-safe: 開始日時が読めないなら「材料無し」に倒す。クエリも投げない
        // (PanicRepo が呼ばれたら panic するので、投げていないことも同時に確認する)
        let repo: DynResetMaterialRepo = Arc::new(PanicRepo);
        let n = count_reset_material(&repo, "1234").await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn count_reset_material_surfaces_repo_errors() {
        let repo: DynResetMaterialRepo = Arc::new(FailingRepo);
        let err = count_reset_material(&repo, "26060608220000000041571")
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::QueryFailed(_)));
    }

    #[tokio::test]
    async fn disabled_repo_is_not_configured() {
        let err = DisabledResetMaterialRepo
            .count_material("f", "t", "v1", "v2")
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
    }

    #[test]
    fn reset_material_sql_covers_rest_start_and_end_events_on_dtako_events_only() {
        assert!(RESET_MATERIAL_SQL.contains("'休息'"));
        assert!(RESET_MATERIAL_SQL.contains("'運行開始'"));
        assert!(RESET_MATERIAL_SQL.contains("'運行終了'"));
        assert!(RESET_MATERIAL_SQL.contains("FROM dtako_events"));
        assert!(
            !RESET_MATERIAL_SQL.contains("time_card_dtako"),
            "PHP _setbyUnkoNo は dtako_events からしか作り直さない"
        );
    }
}
