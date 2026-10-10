//! `POST /api/dtako/autoload` (csvdata.zip を社内 CakePHP の取り込み口へ 1 件ずつ中継する) の純粋部分
//! (Refs ohishi-exp/rust-ichibanboshi#322)。クエリの検査・400 の文言・① ② ③ の段取り・応答の組み立て。
//! root の `src/routes/dtako_autoload.rs` から移した (設計の経緯はそちらのモジュール doc)。送受信は [`AutoloadIo`]
//! として呼び手 (オンプレ版は reqwest と mysql_async、勤怠 Worker は Workers VPC) が持つ。
//!
//! ## 段取り (`reset_timecard=true` のとき)
//!
//! **② autoload → (② が 2xx のとき) ① 材料を数える → (1 件以上なら) ③ reset。** ② の前に数えるのは
//! `preview` のときだけ (② の取り込みで `dtako_events` が増え得るので、実行時は ② の後に数える)。
//!
//! - **② が非 2xx なら ③ をやらない** (`reset_skip_reason: "step2_failed"`)。② の送信自体が失敗したら
//!   (接続・timeout) [`run`] は `Err` を返し、③ には到達しない
//! - **材料 0 件なら ③ をやらない** (`no_dtako_events`)。③ は `time_card_dtako` を削除してから材料で作り直す
//!   ので、材料が無いと消えて戻らない (#281・#290 の実害)
//! - **数えられなければ ③ をやらない** (`count_failed`、fail-closed)
//! - **`http_status` で成否を判断しない。** 307 でも取り込みは走っている (redirect 判定より前に取り込む)。
//!   3xx は `location` を返す。③ の `reset_http_status` も成功の証明にならない ([`RESET_TIMECARD_STATUS_NOTE`])

use chrono::NaiveDateTime;
use serde::Deserialize;

use crate::cakephp::{
    is_success, reset_timecard_path, CakephpError, DtakoAutoloadResponse, ResetTimecardResponse,
    AUTOLOAD_PATH,
};

/// ③ (`resetby-unko-no`) の応答に添える注意書き。**空 200 は成功の証明ではない** (`yhonda-ohishi/nginx#796`)。
pub const RESET_TIMECARD_STATUS_NOTE: &str = "reset_http_status は空 200 でも失敗でも同じ値になり得ます。成否は Flash (session) にしか出ないため呼び出し側からは判別できません (yhonda-ohishi/nginx#796)";

/// 本文の上限 (20 MiB)。1 件 (1 unko_no) ぶんの csvdata.zip は通常ごく小さい。
pub const MAX_ZIP_BYTES: usize = 20 * 1024 * 1024;

/// `unko_no` が無い・不正のときの 400 の本文。
pub const UNKO_NO_INVALID: &str =
    "unko_no は対象を1件、数字だけで指定してください (一括取り込みは不可)";

/// 本文が空のときの 400 の本文。
pub const BODY_EMPTY: &str = "body が空です。csvdata.zip の中身を送ってください";

/// 既定のファイル名 (`file_name` が無い・空白だけ)。
pub const DEFAULT_FILE_NAME: &str = "csvdata.zip";

/// `?unko_no=&file_name=&preview=&reset_timecard=`
#[derive(Debug, Deserialize)]
pub struct AutoloadQuery {
    pub unko_no: Option<String>,
    pub file_name: Option<String>,
    #[serde(default)]
    pub preview: bool,
    /// ③ (勤務時間再登録) まで続けるか。**既定 `false`** (破壊的操作を既定で増やさない)。
    #[serde(default)]
    pub reset_timecard: bool,
}

/// 検査済みの要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoloadRequest {
    pub unko_no: String,
    pub file_name: String,
    pub size_bytes: usize,
    pub preview: bool,
    pub reset_timecard: bool,
}

/// `unko_no` の受け入れ判定。**空・非数字は拒否** — 「対象を名指しで受け取る」(月まるごと等の一括指定を弾く)
/// 歯止め。桁数は固定しない — オンプレ 23 桁 / GCP・theearth 側 22 桁で実物の桁が揺れるため、
/// 「全部数字で最低限それらしい長さ」だけを見る (12 = 先頭の開始日時 `YYMMDDHHMMSS` の桁数)。
pub fn parse_unko_no(raw: &str) -> Option<&str> {
    if raw.len() < 12 || !raw.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(raw)
}

/// クエリと本文の長さを検査する (順は `unko_no` → 本文)。`Err` は 400 の本文。
pub fn parse(query: AutoloadQuery, body_len: usize) -> Result<AutoloadRequest, &'static str> {
    let Some(unko_no) = query.unko_no.as_deref().and_then(parse_unko_no) else {
        return Err(UNKO_NO_INVALID);
    };
    if body_len == 0 {
        return Err(BODY_EMPTY);
    }
    let file_name = query
        .file_name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_FILE_NAME.to_string());
    Ok(AutoloadRequest {
        unko_no: unko_no.to_string(),
        file_name,
        size_bytes: body_len,
        preview: query.preview,
        reset_timecard: query.reset_timecard,
    })
}

/// CakePHP のエラーを status と本文 (平文) へ写す。`not_configured` は binding・設定が無いときの 503 の本文
/// (オンプレ版は [`NOT_CONFIGURED_ONPREM`])。
pub fn map_err(e: CakephpError, not_configured: &str) -> (u16, String) {
    match e {
        CakephpError::NotConfigured => (503, not_configured.to_string()),
        CakephpError::RequestFailed(m) => (502, format!("nginx への接続に失敗: {m}")),
        // autoload は非 2xx も Ok で返すので実際には作らないが、enum は他の fetch と共有なので網羅する
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => (502, format!("CakePHP returned {status}: {body_excerpt}")),
        CakephpError::JsonError(m) => (502, format!("CakePHP response parse failed: {m}")),
    }
}

/// オンプレ版の 503 の本文。
pub const NOT_CONFIGURED_ONPREM: &str = "CakePHP base_url が未設定 (CAKEPHP_BASE_URL)";

/// 送受信。`count_material` の `Err` は応答に出す文言 (`dtako_events_count_error` / `reset_error`)。
#[allow(async_fn_in_trait)]
pub trait AutoloadIo {
    /// CakePHP の送り先があるか (preview の `configured`)。
    fn configured(&self) -> bool;
    /// ② `POST /dtako-events/autoload`。非 2xx も `Ok`。
    async fn autoload(&self, file_name: &str) -> Result<DtakoAutoloadResponse, CakephpError>;
    /// ① 材料の件数 ([`MaterialQuery`] の SQL)。
    async fn count_material(&self, q: &MaterialQuery) -> Result<i64, String>;
    /// ③ `POST /time-card-dtako/resetby-unko-no/<unko_no>`。
    async fn reset(&self, unko_no: &str) -> Result<ResetTimecardResponse, CakephpError>;
}

/// ① 材料を数える。`unko_no` の先頭 12 桁が読めない (壊れた入力) なら数えずに 0 (fail-safe)。
async fn count<I: AutoloadIo>(io: &I, unko_no: &str) -> Result<i64, String> {
    match MaterialQuery::new(unko_no) {
        Some(q) => io.count_material(&q).await,
        None => Ok(0),
    }
}

/// 段取りを回して応答の JSON を返す。`Err` は ② の送信自体の失敗 ([`map_err`] で 502 / 503 にする)。
pub async fn run<I: AutoloadIo>(
    io: &I,
    req: &AutoloadRequest,
) -> Result<serde_json::Value, CakephpError> {
    if req.preview {
        // preview でも③の材料件数は計算する — 打つ前に危険が見える。reset_timecard=false なら DB を叩かない
        let (count_value, count_error) = if req.reset_timecard {
            match count(io, &req.unko_no).await {
                Ok(n) => (Some(n), None),
                Err(e) => (None, Some(e)),
            }
        } else {
            (None, None)
        };
        return Ok(serde_json::json!({
            "preview": true,
            "unko_no": req.unko_no,
            "file_name": req.file_name,
            "size_bytes": req.size_bytes,
            "target_path": AUTOLOAD_PATH,
            "configured": io.configured(),
            // ③ は preview でも実行しない — 予定だけを返す
            "reset_timecard": req.reset_timecard,
            "reset_target_path": req.reset_timecard.then(|| reset_timecard_path(&req.unko_no)),
            "dtako_events_count": count_value,
            "dtako_events_count_error": count_error,
            "note": "preview=true のため実際には送信していません",
        }));
    }

    let res = io.autoload(&req.file_name).await?;
    let http_ok = is_success(res.status);
    let mut reset = ResetOutcome::default();
    if req.reset_timecard {
        if http_ok {
            // ★③の直前 (②のあと) に数える — ②の取り込みで増えた分を取りこぼさない
            match count(io, &req.unko_no).await {
                Ok(0) => {
                    reset.skip_reason = Some("no_dtako_events");
                    reset.count = Some(0);
                }
                Ok(n) => {
                    reset.count = Some(n);
                    reset.attempted = true;
                    match io.reset(&req.unko_no).await {
                        Ok(r) => {
                            reset.http_status = Some(r.status);
                            reset.location = r.location;
                        }
                        Err(e) => reset.error = Some(e.to_string()),
                    }
                }
                Err(e) => {
                    reset.skip_reason = Some("count_failed");
                    reset.error = Some(e);
                }
            }
        } else {
            reset.skip_reason = Some("step2_failed");
        }
    }

    Ok(serde_json::json!({
        "preview": false,
        "unko_no": req.unko_no,
        "file_name": req.file_name,
        "size_bytes": req.size_bytes,
        "target_path": AUTOLOAD_PATH,
        "http_status": res.status,
        "http_ok": http_ok,
        // 3xx でも取り込みは走っている — http_status では成否を判断できないので redirect 先だけでも渡す
        "location": res.location,
        "response_excerpt": res.body_excerpt,
        // ③ の結果は reset_ prefix で分ける (②の http_status/location とは混ぜない)
        "reset_timecard": req.reset_timecard,
        "reset_attempted": reset.attempted,
        "reset_skip_reason": reset.skip_reason,
        "dtako_events_count": reset.count,
        "reset_http_status": reset.http_status,
        "reset_location": reset.location,
        "reset_error": reset.error,
        "reset_note": reset.attempted.then_some(RESET_TIMECARD_STATUS_NOTE),
    }))
}

/// ③ の結果 (応答の `reset_*` と `dtako_events_count`)。
#[derive(Debug, Default)]
struct ResetOutcome {
    attempted: bool,
    http_status: Option<u16>,
    location: Option<String>,
    error: Option<String>,
    skip_reason: Option<&'static str>,
    count: Option<i64>,
}

/// ③ の材料件数を PHP `_setbyUnkoNo` と**完全一致**する絞り込みで数える SQL (Refs #633 の 5、#281 の再発防止)。
/// root の `src/dtako_reset_material.rs` から移した。
///
/// - **`dtako_events` だけを見る。** `time_card_dtako` は PHP 側が読まない (`_setbyUnkoNo` は `dtako_events`
///   からしか INSERT し直さない) ので混ぜない
/// - 2 ブランチに分ける理由 (`REST_EVENTS_SQL` と同じ): **期間内に始まる区間**と**期間内に終わる区間
///   (開始は期間より前)** の両方を拾わないと、日をまたぐ運行の材料を取りこぼす
/// - `運行NO IN (:v1, :v2)` と `イベント名 IN (...)` は両ブランチに掛ける — 窓は取りこぼし防止の余白であって、
///   絞り込みそのものを緩める理由にはならない
/// - **`休憩` は材料ではない** (`休息` とは別のイベント)。`休息` だけを数えていた旧実装は運行開始/運行終了しか
///   無い運行を誤ってスキップした (#290)
///
/// 社内 MariaDB の SQL だが `kintai-kosoku` の `sql.rs` には置かない (kosoku は勤怠の版の glob の中)。
pub const RESET_MATERIAL_SQL: &str = r#"
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

/// [`RESET_MATERIAL_SQL`] の引数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterialQuery {
    /// 窓 `[from, to)`: 開始日の前日 0 時から 4 日後の 0 時 (日をまたぐ運行を取りこぼさない余白)
    pub from: NaiveDateTime,
    pub to: NaiveDateTime,
    /// PHP `_setbyUnkoNo` が材料として見る運行NO の 2 パターン (先頭 22 桁 + `1` / `2`)。
    /// 呼び出し側が渡した末尾 1 桁は使わない — PHP 自身が無視して両方を見るため。どちらも数字だけ
    pub v1: String,
    pub v2: String,
}

impl MaterialQuery {
    /// `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) が開始日時として読めなければ `None` (= 数えずに 0)。
    pub fn new(unko_no: &str) -> Option<Self> {
        let start = NaiveDateTime::parse_from_str(unko_no.get(..12)?, "%y%m%d%H%M%S").ok()?;
        let from = (start.date() - chrono::Duration::days(1)).and_hms_opt(0, 0, 0)?;
        let to = from + chrono::Duration::days(4);
        let prefix: String = unko_no.chars().take(22).collect();
        Some(Self {
            from,
            to,
            v1: format!("{prefix}1"),
            v2: format!("{prefix}2"),
        })
    }

    /// 窓の両端の文字列 (`YYYY-MM-DD HH:MM:SS`。オンプレ版はこれを bind する)。
    pub fn window_strings(&self) -> (String, String) {
        let f = |t: &NaiveDateTime| t.format("%Y-%m-%d %H:%M:%S").to_string();
        (f(&self.from), f(&self.to))
    }
}
