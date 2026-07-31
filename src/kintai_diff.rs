//! 打刻の差分を**返す**側 (Refs #205 の 04b、オンプレ)。
//!
//! GCP 側の `rust-ichibanboshi` は社内 MariaDB に到達できないので打刻を読めない。
//! オンプレが読み、relay が運ぶ。受け側は [`crate::routes::kintai_timecard`]。
//!
//! ## オンプレは外へ出ない
//!
//! **relay が起動する側なので、オンプレから折り返さない。** 1 往復は relay が持つ:
//!
//! 1. relay → オンプレ `GET /api/kintai/timecard/drivers` — 対象月の乗務員を 1 ページ
//! 2. relay → GCP `GET /api/kintai/timecard/signatures` — その乗務員ぶんの署名
//! 3. relay → オンプレ `POST /api/kintai/timecard/diff` — 署名を渡し、**差分を受け取る**
//! 4. relay → GCP `POST /api/kintai/timecard` — 差分を渡す
//!
//! この形だと**オンプレは request / response だけ**になり、CF Tunnel の内側から外へ
//! 出る経路も、相手の資格情報も持たない。
//!
//! ## 署名の計算を relay 側に持たない
//!
//! relay が渡すのは GCP から引いた署名そのもので、**突き合わせるのはここ**
//! ([`crate::kintai_push::plan_batch`])。relay (TypeScript) 側で署名を計算すると
//! `day_signature` が 2 実装になり、式が少しでもずれると「中身は同じなのに毎回全日が
//! 違う」と判定して静かに全件を書き直し続ける。#205 の決定 3 で `kosoku.rs` を
//! 写さないと決めているのと同じ理由。
//!
//! ## 1 回の呼び出しを乗務員数で区切る
//!
//! この経路は **Cloudflare Tunnel (30 秒上限) を通って叩かれる**。全乗務員を
//! 1 リクエストで回すと必ず 502 になる (#199 で踏んだ壁そのもの)。
//!
//! そこで `drivers` が **`max_drivers` で区切り、続きの位置を `next_after_driver_cd`
//! で返す**。relay は `null` が返るまで呼び直す。上流の `GET /api/dtako/events` が
//! `after_driver_cd` で同じことをしているのと同じ形。
//!
//! ## 冪等なので呼び直しは安全
//!
//! どちらの口も**読むだけで、何も書かない**。途中で落ちても relay がやり直せば同じ
//! 状態に収束する。

use std::collections::{BTreeMap, BTreeSet};

use chrono::NaiveDate;

use crate::kintai_push::{
    dedup_events, group_by_date, parse_rows, plan_batch, read_driver_events, TimecardBatch,
};
use crate::kintai_repo::{exact_month_range, DynKintaiEventsRepo, KintaiRepoError};

/// 1 回の呼び出しで返す乗務員数の既定。
///
/// 経緯 — かつての根拠「1 乗務員あたり 0.2 秒」は本番で成立していなかった。
/// 2026-07-30 の初回 dry-run では 10 人ぶんの `POST /api/kintai/timecard/diff` が
/// Cloudflare の 524 (100 秒) を超え、1 人なら通った。原因は乗務員ごとの読み出しが
/// `dtako_events` と `dtako_cars` まで引いていたこと (#225) — 押し出さない行だった。
///
/// 打刻 2 表に絞ったあとの実測 (2026-07-31、2026-06 = 94 名):
///
/// | 1 回の人数 | 結果 |
/// |---|---|
/// | 10 | 通る (524 が消えた) |
/// | 50 | 通る |
///
/// **50 は実測済みなので既定に上げる。** 94 名なら 2 回で終わる。
pub const DEFAULT_MAX_DRIVERS: usize = 50;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて Tunnel を殺すのを防ぐ。
///
/// **100 は未実測。** 50 が通ったこと・上限に当たっても 524 で落ちるだけで
/// **1 件も書かれない** (この経路は読むだけ、呼び直せば同じ状態に収束する) ことから、
/// 現在の頭数 (94 名) が 1 回で終わる値まで開ける。踏んだら下げればよい。
pub const MAX_MAX_DRIVERS: usize = 100;

/// 差分の取り出しに失敗した。
///
/// **相手側の失敗という区分を持たない** — この経路はもう外へ出ない。
#[derive(Debug)]
pub enum KintaiDiffError {
    /// 呼び出し側の指定が不正 (月の形など)。
    BadRequest(String),
    Read(KintaiRepoError),
}

impl std::fmt::Display for KintaiDiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRequest(m) => write!(f, "kintai diff bad request: {m}"),
            Self::Read(e) => write!(f, "kintai events read failed: {e}"),
        }
    }
}

impl std::error::Error for KintaiDiffError {}

impl From<KintaiRepoError> for KintaiDiffError {
    fn from(e: KintaiRepoError) -> Self {
        Self::Read(e)
    }
}

impl From<crate::kintai_push::KintaiPushError> for KintaiDiffError {
    /// `read_driver_events` は 04 と共有なので push 側の型で返る。**読み出しの失敗**
    /// にしか使わない (ここは 1 行も書かない)。
    fn from(e: crate::kintai_push::KintaiPushError) -> Self {
        match e {
            crate::kintai_push::KintaiPushError::Read(r) => Self::Read(r),
            other => Self::BadRequest(other.to_string()),
        }
    }
}

/// `GET /api/kintai/timecard/drivers` の応答。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct DriversPage {
    pub drivers: Vec<u64>,
    /// 続きの位置。`None` なら回りきった。
    pub next_after_driver_cd: Option<u64>,
}

/// `POST /api/kintai/timecard/diff` の応答。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct DiffReport {
    /// 相手へそのまま渡せる batch。**空の batch は載せない**。
    pub batches: Vec<TimecardBatch>,
    pub drivers: usize,
    pub days_changed: usize,
    pub days_deleted: usize,
    pub events: usize,
    /// DDL の CHECK に無かった `state` の実値。**空でないと上流に知らない値が来ている**。
    pub unknown_states: BTreeSet<String>,
}

impl DiffReport {
    pub fn has_unexpected(&self) -> bool {
        !self.unknown_states.is_empty()
    }
}

/// 対象月に**打刻がある**乗務員を昇順で洗い出し、`after` の次から `max` 人だけ返す。
///
/// **全イベントを読んでから CD を拾わない。** 使うのは CD の集合だけなので、
/// `dtako_events` まで JSON にして捨てるのは丸ごと無駄
/// ([`fetch_timecard_driver_cds_between`])。ページごとに毎回払う費用なので、
/// ページングしても減らなかった。
///
/// [`fetch_timecard_driver_cds_between`]:
///     crate::kintai_repo::KintaiEventsApi::fetch_timecard_driver_cds_between
pub async fn drivers_page(
    repo: &DynKintaiEventsRepo,
    month: &str,
    after: Option<u64>,
    max: usize,
) -> Result<DriversPage, KintaiDiffError> {
    let (from, to) = exact_month_range(month)
        .ok_or_else(|| KintaiDiffError::BadRequest(format!("bad month: {month}")))?;
    let max = max.clamp(1, MAX_MAX_DRIVERS);
    let all = repo.fetch_timecard_driver_cds_between(&from, &to).await?;
    let rest: Vec<u64> = match after {
        Some(a) => all.into_iter().filter(|d| *d > a).collect(),
        None => all,
    };
    let next = rest.get(max).map(|_| rest[max - 1]);
    Ok(DriversPage {
        drivers: rest.into_iter().take(max).collect(),
        next_after_driver_cd: next,
    })
}

/// 相手が持っている署名と突き合わせ、**渡すべき差分だけ**を返す。
///
/// `remote` のキーが対象の乗務員 — relay が [`drivers_page`] で引いた 1 ページぶんに
/// ついて GCP から署名を集めたもの。**ここには何も書かない。**
pub async fn diff_month(
    repo: &DynKintaiEventsRepo,
    month: &str,
    remote: &BTreeMap<u64, BTreeMap<NaiveDate, String>>,
) -> Result<DiffReport, KintaiDiffError> {
    let (from, to) = exact_month_range(month)
        .ok_or_else(|| KintaiDiffError::BadRequest(format!("bad month: {month}")))?;
    if remote.len() > MAX_MAX_DRIVERS {
        // Tunnel の 30 秒に収める。drivers が同じ上限で区切っているので、素直に
        // 使っていればここには来ない
        return Err(KintaiDiffError::BadRequest(format!(
            "too many drivers: {} (max {MAX_MAX_DRIVERS})",
            remote.len()
        )));
    }

    let mut report = DiffReport::default();
    for (driver, remote_sigs) in remote {
        let rows = read_driver_events(repo, *driver, &from, &to).await?;
        let parsed = parse_rows(&rows);
        for s in &parsed.unknown_states {
            report.unknown_states.insert(s.clone());
        }
        let local = group_by_date(&dedup_events(parsed.events));
        let batch = plan_batch(month, *driver as i64, &local, remote_sigs);

        report.drivers += 1;
        // 変化なしの乗務員で応答を膨らませない (1 か月・全乗務員はほとんどが不変)
        if batch.is_empty() {
            continue;
        }
        report.days_changed += batch.days.len();
        report.days_deleted += batch.delete_dates.len();
        report.events += batch.days.values().map(Vec::len).sum::<usize>();
        report.batches.push(batch);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::kintai_repo::{KintaiEventsApi, KintaiRepoError};
    use async_trait::async_trait;
    use serde_json::json;

    struct StubRepo {
        rows: Vec<serde_json::Value>,
    }

    #[async_trait]
    impl KintaiEventsApi for StubRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            driver: u64,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(self
                .rows
                .iter()
                .filter(|r| r["driver_id"].as_u64() == Some(driver))
                .cloned()
                .collect())
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(self.rows.clone())
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(Vec::new())
        }
    }

    fn punch(driver: u64, at: &str, state: &str) -> serde_json::Value {
        json!({
            "datetime": at,
            "end_datetime": null,
            "driver_id": driver,
            "source": "timecard",
            "state": state,
            "unko_no": null,
        })
    }

    fn repo_of(rows: Vec<serde_json::Value>) -> DynKintaiEventsRepo {
        std::sync::Arc::new(StubRepo { rows })
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    /// デジタコ生イベントしか無い乗務員は**運ぶ相手ではない**ので出てこない。
    ///
    /// 乗務員CD 0 も落ちる — 2026-06 の dry-run では、これが 1 ページ目を丸ごと
    /// 食って `batchesSent: 0` になっていた。
    #[tokio::test]
    async fn drivers_without_a_punch_are_not_listed() {
        let dtako_only = json!({
            "datetime": "2026-07-01 08:00:00",
            "driver_id": 9999,
            "source": "dtako_events",
            "state": "運行開始",
        });
        let mut no_driver = punch(0, "2026-07-01 08:00:00", "始業");
        no_driver["driver_id"] = json!(null);
        let repo = repo_of(vec![
            punch(0, "2026-07-01 08:00:00", "始業"),
            no_driver,
            dtako_only,
            punch(1130, "2026-07-01 08:00:00", "始業"),
        ]);
        let page = drivers_page(&repo, "2026-07", None, 10).await.unwrap();
        assert_eq!(page.drivers, vec![1130]);
        assert_eq!(page.next_after_driver_cd, None);
    }

    /// 乗務員はページで返り、続きが `next_after_driver_cd` に出る。
    #[tokio::test]
    async fn drivers_come_back_in_pages() {
        let repo = repo_of(vec![
            punch(1130, "2026-07-01 08:00:00", "始業"),
            punch(1200, "2026-07-01 08:00:00", "始業"),
            punch(1300, "2026-07-01 08:00:00", "始業"),
        ]);
        let first = drivers_page(&repo, "2026-07", None, 2).await.unwrap();
        assert_eq!(first.drivers, vec![1130, 1200]);
        assert_eq!(first.next_after_driver_cd, Some(1200));

        let second = drivers_page(&repo, "2026-07", Some(1200), 2).await.unwrap();
        assert_eq!(second.drivers, vec![1300]);
        assert_eq!(second.next_after_driver_cd, None);
    }

    /// **上限で頭打ちにする** — Tunnel を殺す大きな値を受け取らない。
    #[tokio::test]
    async fn the_page_size_is_clamped_and_bad_months_are_refused() {
        let repo = repo_of(vec![punch(1130, "2026-07-01 08:00:00", "始業")]);
        // 0 でも 1 人は返る (clamp の下限)
        assert_eq!(
            drivers_page(&repo, "2026-07", None, 0)
                .await
                .unwrap()
                .drivers,
            vec![1130]
        );
        // 上限超えは頭打ち (エラーにはしない)
        assert_eq!(
            drivers_page(&repo, "2026-07", None, 9_999)
                .await
                .unwrap()
                .drivers,
            vec![1130]
        );
        assert!(matches!(
            drivers_page(&repo, "nope", None, 10).await,
            Err(KintaiDiffError::BadRequest(_))
        ));
    }

    /// 署名が違う日だけが batch に載る。**一致していれば 1 件も返らない。**
    #[tokio::test]
    async fn only_the_differing_days_come_back() {
        let repo = repo_of(vec![
            punch(1130, "2026-07-01 08:00:00", "始業"),
            punch(1130, "2026-07-01 18:00:00", "終業"),
        ]);
        // 相手が何も持っていない → その日は「変わった」
        let mut remote = BTreeMap::new();
        remote.insert(1130u64, BTreeMap::new());
        let report = diff_month(&repo, "2026-07", &remote).await.unwrap();
        assert_eq!(report.drivers, 1);
        assert_eq!(report.batches.len(), 1);
        assert_eq!(report.days_changed, 1);
        assert_eq!(report.events, 2);

        // その署名をそのまま返す = 一致 → 何も出ない
        let sig = crate::kintai_push::day_signature(&crate::kintai_push::dedup_events(
            crate::kintai_push::parse_rows(&[
                punch(1130, "2026-07-01 08:00:00", "始業"),
                punch(1130, "2026-07-01 18:00:00", "終業"),
            ])
            .events,
        ));
        let mut same = BTreeMap::new();
        same.insert(1130u64, BTreeMap::from([(d(2026, 7, 1), sig)]));
        let report = diff_month(&repo, "2026-07", &same).await.unwrap();
        assert_eq!(report.drivers, 1);
        assert!(report.batches.is_empty(), "{:?}", report.batches);
        assert_eq!(report.days_changed, 0);
    }

    /// 手元に無い日を相手が持っていたら**消す指示**が出る。
    #[tokio::test]
    async fn days_the_source_no_longer_has_are_marked_for_deletion() {
        let repo = repo_of(vec![punch(1130, "2026-07-01 08:00:00", "始業")]);
        let mut remote = BTreeMap::new();
        remote.insert(
            1130u64,
            BTreeMap::from([(d(2026, 7, 20), "whatever".to_string())]),
        );
        let report = diff_month(&repo, "2026-07", &remote).await.unwrap();
        assert_eq!(report.days_deleted, 1);
        assert_eq!(report.batches[0].delete_dates, vec![d(2026, 7, 20)]);
    }

    /// DDL の CHECK に無い `state` は報告する (**捨てて黙らない**)。
    #[tokio::test]
    async fn unknown_states_are_reported() {
        let repo = repo_of(vec![punch(1130, "2026-07-01 08:00:00", "🤷 知らない状態")]);
        let mut remote = BTreeMap::new();
        remote.insert(1130u64, BTreeMap::new());
        let report = diff_month(&repo, "2026-07", &remote).await.unwrap();
        assert!(report.has_unexpected());
        assert!(report.unknown_states.contains("🤷 知らない状態"));
    }

    /// 月が壊れていれば読みに行く前に落とす。乗務員が多すぎる呼び方も断る。
    #[tokio::test]
    async fn bad_requests_are_refused_before_reading() {
        let repo = repo_of(Vec::new());
        assert!(matches!(
            diff_month(&repo, "2026-7", &BTreeMap::new()).await,
            Err(KintaiDiffError::BadRequest(_))
        ));

        let too_many: BTreeMap<u64, BTreeMap<NaiveDate, String>> = (0..=MAX_MAX_DRIVERS as u64)
            .map(|d| (d, BTreeMap::new()))
            .collect();
        assert!(matches!(
            diff_month(&repo, "2026-07", &too_many).await,
            Err(KintaiDiffError::BadRequest(_))
        ));
    }

    #[test]
    fn errors_say_which_side_failed() {
        let bad = KintaiDiffError::BadRequest("nope".to_string()).to_string();
        assert!(bad.contains("bad request"), "{bad}");
        let read = KintaiDiffError::from(KintaiRepoError::NotConfigured).to_string();
        assert!(read.contains("read failed"), "{read}");
    }
}
