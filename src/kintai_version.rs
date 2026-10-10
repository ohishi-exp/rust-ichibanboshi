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
//! | `time_card_dstate` | 両方 | 読み窓 (`month_range` の始端を遡り起点まで下げたもの、下記) | `EVENTS_SQL` / dailyJson の打刻 30/31 |
//! | `time_card_dtako` | kosoku-daily | 読み窓 (同上) | `EVENTS_SQL` 2 本目 |
//! | `time_card_dtako_state` | kosoku-daily | 全体 (マスタ) | `EVENTS_SQL` の JOIN |
//! | `dtako_events` | kosoku-daily | 読み窓の始端が属する月の前月初〜 (下記) | `EVENTS_SQL` 3/4 本目 |
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
//! `KINTAI_OUTPUT_SHA` (応答を形づくるコードの内容ハッシュ、`build.rs`) と
//! `KosokuParams` (TOML 追随) を畳んで覆う — route 側
//! (`routes/kintai_version.rs`) の担当。
//!
//! **リポジトリ全体の `BUILD_SHA` は使わない** (Refs #191): 全体を畳むと ETC・日報など
//! kintai と無関係なデプロイでも etag が動き、relay (nuxt-dtako-admin#543) の上流
//! キャッシュが全月まとめて無効になって 1.7MB 級の取り直しが起きていた。データ側は
//! 11 テーブルを範囲まで詰めて列挙しているのに、コード側だけ雑な代理指標だった、という
//! 非対称が実害の正体。対象の決め方と「取りこぼしたら古い値」の警戒点は `build.rs` 参照。
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
//! ## 例外: `dtako_events` は COUNT + MAX(id) (index-only)
//!
//! `dtako_events` だけは 428 万行 / 約 2GB で、CRC 方式 (行本体のランダム読み) は
//! DB のバッファプール (128MB、変更不可) が冷えていると **7〜24 秒**かかることを
//! 本番で実測した (2026-07-29)。そこでこのテーブルのみ、`開始日時` インデックスの
//! オンリースキャンで済む `COUNT(*)` + `MAX(id)` に置き換える (冷えた月でも実測
//! 0.64 秒)。
//!
//! **前提となる運用実態 (binlog 検証 2026-07-29)**: 直近 7 日の binlog 全書き込み
//! 約 30 万件を検査した結果、UPDATE 296,824 件のうち `EVENTS_SQL` が読む 6 列
//! (`開始日時`/`終了日時`/`対象乗務員CD`/`イベント名`/`運行NO`/`車輌CD`) を SET
//! するものは **0 件** (SET されるのは `得意先`・`kosoku_o15_time`・`旅費id`・
//! `非表示` 等、応答が読まない列だけ)。DELETE 4,321 件は COUNT が、INSERT は
//! COUNT/MAX(id) が検知する。REPLACE は 0 件。つまり 6 列は「挿入時確定・以後
//! 不変」— **もし将来 `イベント名` 等を in-place UPDATE する運用が始まると
//! 検知漏れ (古い値) になる**ので、その場合はこのブランチを CRC に戻すこと。
//!
//! 範囲は `[前月初, 翌月+1日)` に広げる — 月 M の応答は「前月に開始して M 月に
//! 終わる区間」(`EVENTS_SQL` 第 4 ブランチ) を含むため、前月分の増減も月 M の
//! マーカーを動かす必要がある (上位集合原則)。
//!
//! ## 読み窓は月初をまたぐ運行・勤務の開始まで遡る (Refs ohishi-exp/nuxt-dtako-admin#1123)
//!
//! `kosoku-daily` は乗務員ごとに窓を遡り起点 (`kintai_repo::month_head_anchors`) まで
//! 広げて読む。打刻 2 表の範囲は全乗務員の起点の最小 (`lookback_from`) から取り、
//! `dtako_events` はその始端が属する月の前月初から取る (起点の前に始まって窓の中で
//! 終わる休息も読まれるため、月初からの読みと同じ 1 か月の余裕を持たせる)。月初のままだと、前月末の打刻が
//! 後から直っても etag が動かず relay が古い値を返し続ける。起点は
//! `mariadb_month_head_anchors` を**同じ関数のまま**呼んで求める (決め方を 2 つにしない)。
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

use crate::config::MariadbConfig;
use crate::kintai_repo::{month_range, KintaiRepoError};
use kintai_kosoku::kintai_version::version_ranges;
use kintai_kosoku::sql::VERSION_SQL;

// マーカーの型・範囲の決め方・etag の畳み方は共有 crate (勤怠 Worker と同じもの、Refs #322)。
// ここは MariaDB の往復だけ。
pub use kintai_kosoku::kintai_version::{version_etag, SourceMarker};

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
            .db_name(Some(cfg.database.clone()))
            // 60 秒超のステートメントを MariaDB 側で自動 abort (convoy 防止、
            // kintai_repo.rs の MARIADB_SESSION_SETUP 参照)
            .setup(vec![crate::kintai_repo::MARIADB_SESSION_SETUP.to_string()]);
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
        // 読み窓の始端は月初をまたぐ運行・勤務の開始まで遡る (モジュール docs)
        let bad_month = || KintaiRepoError::QueryFailed(format!("bad month: {month}"));
        let (month_start, month_to) = month_range(month).ok_or_else(bad_month)?;
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let anchors =
            crate::kintai_repo::mariadb_month_head_anchors(&mut conn, &month_start, &month_to)
                .await?;
        let r = version_ranges(month, &anchors).ok_or_else(bad_month)?;
        let rows: Vec<MarkerRow> = conn
            .exec(
                VERSION_SQL,
                params! {
                    "from" => &r.from,
                    "to" => &r.to,
                    "mfrom" => &r.mfrom,
                    "mto" => &r.mto,
                    "efrom" => &r.efrom,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `build.rs` が焼き込む出力コード版が「取れている」ことを固定する (Refs #191)。
    ///
    /// 空文字や `unknown` に化けると **全デプロイで etag が同じ**になり、計算ロジックを
    /// 変えても relay が古い本文を返し続ける (この仕組みで最悪の壊れ方)。build.rs 側は
    /// 対象ファイルの欠落でビルドを落とすので、ここは「値が届いているか」を見る。
    #[test]
    fn kintai_output_sha_is_baked_in() {
        let sha = env!("KINTAI_OUTPUT_SHA");
        assert_eq!(sha.len(), 16, "KINTAI_OUTPUT_SHA={sha}");
        assert!(
            sha.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c)),
            "KINTAI_OUTPUT_SHA={sha} (小文字 hex のはず)"
        );
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
