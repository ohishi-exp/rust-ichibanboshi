//! 社内 CakePHP (`yhonda-ohishi/nginx`) を勤怠の口から叩くときの純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! URL・クエリの組み立て、multipart の本文の組み立て、応答の型と読み取り、エラーの種別。HTTP の送受信は持たない
//! (オンプレ版は root の `src/cakephp.rs` が reqwest で、勤怠 Worker は Workers VPC の fetch で送る)。
//! root の `src/cakephp.rs` から移した。**オンプレ版が CakePHP へ送るリクエストは移す前と 1 バイトも変えない**
//! (root の `cakephp::tests::wire_snapshot_matches_the_baseline` が基点の実物で縛る)。

use serde::{Deserialize, Serialize};

/// CakePHP fetch エラー。
#[derive(Debug)]
pub enum CakephpError {
    /// `base_url` 未設定 (= CakePHP fetch 機能が無効化されている)
    NotConfigured,
    /// HTTP request 失敗 (DNS / 接続 / timeout 等)
    RequestFailed(String),
    /// HTTP non-2xx
    StatusError { status: u16, body_excerpt: String },
    /// レスポンス JSON parse 失敗
    JsonError(String),
}

impl std::fmt::Display for CakephpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "CakePHP base_url is not configured"),
            Self::RequestFailed(m) => write!(f, "CakePHP request failed: {m}"),
            Self::StatusError {
                status,
                body_excerpt,
            } => write!(
                f,
                "CakePHP returned status {status}, body excerpt: {body_excerpt}"
            ),
            Self::JsonError(m) => write!(f, "CakePHP response parse failed: {m}"),
        }
    }
}

impl std::error::Error for CakephpError {}

/// `/time-card/daily-json?month=YYYY-MM` のレスポンス (Refs
/// ohishi-exp/nuxt-dtako-admin#424 / yhonda-ohishi/nginx#773, #776)。
///
/// **行は `serde_json::Value` のまま持つ** — このサービスは中継であって解釈者では
/// ないので、上流が項目を足しても型を触らずに素通しできるようにする。同じ理由で
/// `deny_unknown_fields` は付けず、トップレベルの未知フィールドも `extra` に拾って
/// 再シリアライズ時に復元する。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimecardDailyResponse {
    pub rows: Vec<serde_json::Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// `POST /dtako-events/autoload` の応答 (Refs #274 / #205 の 58 / #205 の 61)。
///
/// HTTP status と PHP が返した本文 (先頭 2000 文字) を**そのまま**保持する。
/// CakePHP 側は MIME 判定に失敗しても展開もエラー応答も出さず 200 を返す
/// (親が実物で確認済み) ため、ここで「成功/失敗」に丸めない — 呼び出し側
/// (route) が status と本文の両方を見て判断できるようにする。
///
/// `location` は 3xx 応答の `Location` ヘッダをそのまま持つ (無ければ
/// `None`)。**通常は空になるはず** — 送信時に `api` フィールドを常に真値で
/// 付けているため (Refs #205 の 61)。それでも 3xx が返ってきた場合に body が
/// 空だと何も分からないので、保険として残す。
#[derive(Debug, Clone, Serialize)]
pub struct DtakoAutoloadResponse {
    pub status: u16,
    pub body_excerpt: String,
    pub location: Option<String>,
}

/// `POST /time-card-dtako/resetby-unko-no/<unko_no>` の応答 (③、Refs #205 の 63 /
/// yhonda-ohishi/nginx#795, #796)。
///
/// **応答は空 200 で、成否は PHP 側の Flash (session) にしか出ない**
/// (yhonda-ohishi/nginx#796 に起票済み)。ここに持つ `status` は「HTTP レベルで
/// 届いたか」の記録でしかなく、**「勤務時間が実際に再登録されたか」の証明では
/// ない** — 呼び出し側 (route / 人) は `status` を成功の証拠に使ってはいけない。
#[derive(Debug, Clone, Serialize)]
pub struct ResetTimecardResponse {
    pub status: u16,
    pub location: Option<String>,
}

/// GET の非 2xx の本文を何文字まで持つか。
pub const GET_EXCERPT_CHARS: usize = 500;

/// autoload の応答本文を何文字まで持つか。
pub const AUTOLOAD_EXCERPT_CHARS: usize = 2000;

/// PHP (`DtakoEventsController::autoload`) が zip として受け付ける唯一の
/// Content-Type。**`$file->getClientMediaType() === "application/x-zip-compressed"`
/// でしか判定しない** — OS/ブラウザの一般的な既定 MIME である
/// `application/zip` で送ると、展開もエラー応答も無く黙って無視される
/// (親が実物で確認済み、Refs #274)。送り手 (reqwest / fetch) は拡張子や中身から
/// MIME を推測しないので、ここで固定しないと必ず踏む。
pub const DTAKO_AUTOLOAD_MIME: &str = "application/x-zip-compressed";

/// autoload 専用の timeout (秒)。取り込みは応答より前に走り、応答が遅いことがある
/// (kintai-ops §4.7)。「取り込みは進んでいるのにこちらが先に諦めて失敗と誤判定する」方を避ける —
/// 待ちすぎるコストより、進行中の書き込みを失敗と誤報するコストの方が高い。
pub const DTAKO_AUTOLOAD_TIMEOUT_SECS: u64 = 120;

/// 取り込み口の相対パス。**host は含めない** (内部アドレスを commit / PR / docs に書かない)。
pub const AUTOLOAD_PATH: &str = "/dtako-events/autoload";

/// `base_url` (末尾の `/` は落とす) に相対パスを足す。
pub fn join(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.trim_end_matches('/'))
}

/// `/time-card/daily-json?month=YYYY-MM` (勤怠の日別データ。1 社員 × 1 日 = 1 行)。
pub fn daily_json_path(month: &str) -> String {
    format!("/time-card/daily-json?month={}", urlencode(month))
}

/// `/time-card/pdf-json?month=YYYY-MM[&driver_id=1021]&recalc=0` (Refs #143、yhonda-ohishi/nginx#782)。
///
/// タイムカード表 **PDF (`TimeCardController::createPdf`) が出す数字**の JSON 版。
/// **`driver_id` 省略で全乗務員。**
///
/// ## `recalc=0` を必ず付ける (yhonda-ohishi/nginx#786)
///
/// 上流は既定 (`recalc=1`) だと拘束時間を**再計算し、値が変われば
/// `time_card_kosoku` を DELETE + INSERT する**。この口は突合のための
/// **読み取り口**なので、叩くたびに本番データが書き換わってよいはずがない。
/// パラメータで選ばせず、ここで固定する — 呼び出し側 (relay / MCP) が付け忘れる
/// 余地を残さないため。読むのは保存済みの値になるが、突合の相手は
/// **紙のタイムカード表 = 保存済みの値**なのでこちらが正しい。
pub fn pdf_json_path(month: &str, driver: Option<u64>) -> String {
    let month = urlencode(month);
    match driver {
        Some(d) => format!("/time-card/pdf-json?month={month}&driver_id={d}&recalc=0"),
        None => format!("/time-card/pdf-json?month={month}&recalc=0"),
    }
}

/// ③ の相対パス。URL は CakePHP の `postLink` が生成するものと同じ形。**DashedRoute なので
/// action は `resetby-unko-no`** (controller は `time-card-dtako`)。`unko_no` は呼び出し前に
/// 数字のみと確定済みなので percent-encode は不要。
pub fn reset_timecard_path(unko_no: &str) -> String {
    format!("/time-card-dtako/resetby-unko-no/{unko_no}")
}

/// 先頭 `max` 文字 (バイトではない)。
pub fn excerpt(body: &str, max: usize) -> String {
    body.chars().take(max).collect()
}

/// 2xx か (reqwest の `StatusCode::is_success` と同じ)。
pub fn is_success(status: u16) -> bool {
    (200..300).contains(&status)
}

/// GET の非 2xx を `StatusError` にする (本文は先頭 500 文字)。
pub fn status_error(status: u16, body: &str) -> CakephpError {
    CakephpError::StatusError {
        status,
        body_excerpt: excerpt(body, GET_EXCERPT_CHARS),
    }
}

/// GET の 2xx の本文を型に読む (勤怠 Worker 用。オンプレ版は reqwest の `json` で読む)。
pub fn parse_json<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, CakephpError> {
    serde_json::from_slice(body).map_err(|e| CakephpError::JsonError(e.to_string()))
}

/// autoload の応答 (status・本文の先頭 2000 文字・`Location`) を組む。
pub fn autoload_response(
    status: u16,
    body: &str,
    location: Option<String>,
) -> DtakoAutoloadResponse {
    DtakoAutoloadResponse {
        status,
        body_excerpt: excerpt(body, AUTOLOAD_EXCERPT_CHARS),
        location,
    }
}

/// 組み立て済みの multipart/form-data。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multipart {
    /// リクエストの `Content-Type` ヘッダーの値 (`multipart/form-data; boundary=…`)
    pub content_type: String,
    pub body: Vec<u8>,
}

/// 境界の文字列。reqwest と同じ形 (16 桁の hex × 4 を `-` で繋ぐ。67 文字) にする —
/// 長さが同じなら本文の長さ (`Content-Length`) も移す前と同じになる。乱数は呼び手が渡す。
pub fn boundary(words: [u64; 4]) -> String {
    let [a, b, c, d] = words;
    format!("{a:016x}-{b:016x}-{c:016x}-{d:016x}")
}

/// 1 つの part の頭 (境界の行・Content-Disposition・あれば Content-Type・空行)。reqwest 0.12 の
/// `PercentEncoding::encode_headers` と同じ書式。名前 (`api`・`file[]`) は percent-encode の要らない定数だけ。
fn part_head(
    out: &mut Vec<u8>,
    boundary: &str,
    name: &str,
    file_name: Option<&str>,
    mime: Option<&str>,
) {
    out.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    out.extend_from_slice(format!("Content-Disposition: form-data; name=\"{name}\"").as_bytes());
    if let Some(f) = file_name {
        // reqwest と同じ escape (RFC7578 4.2 で filename*= は使わない)
        let legal = f
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\r', "\\\r")
            .replace('\n', "\\\n");
        out.extend_from_slice(format!("; filename=\"{legal}\"").as_bytes());
    }
    if let Some(m) = mime {
        out.extend_from_slice(format!("\r\nContent-Type: {m}").as_bytes());
    }
    out.extend_from_slice(b"\r\n\r\n");
}

/// `api=1` の part (本文の改行まで)。
///
/// ## `api` フィールドが必須な理由 (実物で確定済み、Refs #205 の 61 / yhonda-ohishi/nginx#795)
///
/// `DtakoEventsController::autoload()` は末尾で、POST データに `api` (真値) が無いと
/// **307 で `/` へ redirect する**。取り込み自体はこの分岐より前に走るので、`api` を付け忘れても
/// 取り込みは走るが、応答が 307 になり `location` 以外の情報が失われる。
/// `resetby-unko-no` は `api` が無いと既定の redirect 先 (`TimeCardDtako::index()` → `_recheck()`)
/// で**最大 100 運行ぶんの書き込みが走る** — redirect を追う HTTP client ならそこへ丸ごと巻き込まれる。
/// 中身は真値であれば何でもよい (`getData('api')` は真偽判定のみ)。
fn api_part(out: &mut Vec<u8>, boundary: &str) {
    part_head(out, boundary, "api", None, None);
    out.extend_from_slice(b"1\r\n");
}

fn finish(mut body: Vec<u8>, boundary: &str) -> Multipart {
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    Multipart {
        content_type: format!("multipart/form-data; boundary={boundary}"),
        body,
    }
}

/// `POST /dtako-events/autoload` の本文: `api=1` と、zip を `file[]` (MIME は
/// [`DTAKO_AUTOLOAD_MIME`] 固定) で 1 つ。
pub fn autoload_multipart(boundary: &str, file_name: &str, zip: &[u8]) -> Multipart {
    let mut body = Vec::with_capacity(zip.len() + 512);
    api_part(&mut body, boundary);
    part_head(
        &mut body,
        boundary,
        "file[]",
        Some(file_name),
        Some(DTAKO_AUTOLOAD_MIME),
    );
    body.extend_from_slice(zip);
    body.extend_from_slice(b"\r\n");
    finish(body, boundary)
}

/// `POST /time-card-dtako/resetby-unko-no/<unko_no>` の本文: `api=1` だけ。
pub fn reset_multipart(boundary: &str) -> Multipart {
    let mut body = Vec::with_capacity(256);
    api_part(&mut body, boundary);
    finish(body, boundary)
}

/// 最小限の URL encode (date 文字列が `:` `+` 等を含むことは無い想定だが念のため `%` 関連だけ吸収)
pub fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "%20".to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}
