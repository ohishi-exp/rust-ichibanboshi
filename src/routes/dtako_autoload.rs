//! csvdata.zip を受け取り、社内 nginx (CakePHP) の取り込み口
//! (`POST /dtako-events/autoload`) へ 1 件ずつ中継する (Refs #205 の 58 / #274)。
//!
//! ## 全体の経路のうち、ここは 2 段目
//!
//! ```text
//! kyuyo-mcp ─(SCRAPER_RELAY)→ relay ─→ theearth      ① zip を取る (別タスク #205-59)
//! kyuyo-mcp ─(fetchIchibanJson)→ オンプレ rust ─→ nginx  ② 取り込む   ★ここ
//! ```
//!
//! **オンプレだけが社内 nginx (内部アドレス) に届く。** Cloudflare の worker からは
//! 届かない (Refs #205、kintai-ops skill §4.5)。
//!
//! ## ★ edge 経由で叩くには別 repo (`ippoan/auth-worker`) 側の allowlist 登録が要る
//!
//! `kyuyo-mcp` からオンプレを呼ぶ経路 (`NUXT_ICHIBAN_API_URL` 経由) は
//! `auth-worker` の `/ichibanboshi-proxy` を通り、そこは **path + method の完全一致
//! allowlist**。この endpoint (`POST /api/dtako/autoload`) は**登録するまで外から
//! 届かない** (このタスクの範囲外・親が別途手配、Refs #274)。オンプレ内 / 作業 PC
//! からの直接 POST はこの登録が無くても届く。
//!

//! ## ファイル名がなぜ `dtako_autoload.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/routes/` 配下を**ファイル名の接頭辞**
//! (`kintai`) で拾い、拾われたファイルの内容ハッシュが `logic_version`
//! (`/api/kintai/version` の etag) に畳まれる。この endpoint は勤怠の計算に一切
//! 関与しない (取り込みの中継だけ) ので、`kintai` で始まる名前を付けると無関係な
//! deploy まで全乗務員 stale にしてしまう ([`crate::routes::dtako_day`] と同じ理由)。
//!
//! ## 一括取り込みを作らない (受け入れ条件3)
//!
//! `dtako_events` の取り込みは既存行を書き換える破壊的操作なので、1 回の POST は
//! **必ず 1 件の `unko_no`** に紐付ける。月まるごと等の一括指定は受け付けない —
//! `unko_no` を必須にし、数字以外や空を 400 で弾くのがその歯止め。
//!
//! ## preview (受け入れ条件4)
//!
//! `?preview=true` は実際には nginx へ送らず、`unko_no` / 送信サイズ / 投げ先
//! (相対パスのみ、host は含めない — 受け入れ条件6) / CakePHP 設定済みかを返す。
//! 実行するかどうかを事前に確認できるようにするための口で、zip の受信自体は
//! preview でも行う (でなければサイズを計算できない)。
//!
//! ## 応答 (受け入れ条件5)
//!
//! 「成功シグナルだけ」には丸めない。CakePHP は MIME 判定に失敗しても展開も
//! エラーも出さず 200 を返す (親が実物で確認済み) ため、HTTP status と PHP の
//! 応答本文の抜粋をそのまま返し、「実際に何が起きたか」の判断材料を呼び出し側
//! (kyuyo-mcp / 人) に渡す。
//!
//! ## ★★ `http_status` で成否を判断してはいけない (実物で確定済み、#205 の 61)
//!
//! `POST /dtako-events/autoload` は multipart body に `api` (真値) が無いと
//! 307 で `/` へ redirect する (`DtakoEventsController::autoload()` 末尾の分岐)。
//! **その 307 は「取り込みが失敗した」を意味しない** — zip の受信・展開・取り込み
//! は redirect 判定より**前**のコードで実行済みだからだ。親が実測で確認している:
//!
//! ```text
//! 取り込み前: 1021 / 2026-06-05 11:58:59  state=休息
//! 取り込み後: 1021 / 2026-06-05 11:58:59  state=積み   ← 307 が返っても変わっていた
//! ```
//!
//! 親も子も一度「307 だから失敗した」と誤診した。この endpoint は `api` を常に
//! 真値で送るので通常は 3xx を見ないはずだが、それでも 3xx が返ってきた場合は
//! `location` (CakePHP の `Location` ヘッダ) を応答に含める — `response_excerpt`
//! だけでは redirect 先が body に出ないため空同然になる (受け入れ条件2)。
//! **判断材料は `response_excerpt` と実データの突合であって `http_status` の
//! 2xx/3xx 分類ではない。**
//!
//! ## `?redirect=` は作らない (受け入れ条件7、判断)
//!
//! PHP 側の redirect 先は `getQuery('redirect', '/')` で `?redirect=` クエリから
//! 決まるが、それは **`api` が無いときの分岐**でしか通らない。この route は
//! `api` を常に真値で送るので、その分岐そのものに入らない — `?redirect=` を
//! ここに足しても中継先の挙動には影響しない。むしろ「効かないパラメータが
//! ある」方が呼び出し側を混乱させるので、対応するパラメータは作らない。
//!
//! ## `reset_timecard` (③、Refs #205 の 63、issue #277)
//!
//! 値ずれを直すには 3 段そろう必要があり、②(このファイル) だけでは
//! `time_card_dtako` の古い行が残る (実証: 乗務員 1021 / 運行 2026-06-05 —
//! ②のあと③ (`resetby-unko-no`) を押して初めて勤務が 2 本 (277+273 分) →
//! 1 本 (802 分) になり GCP と完全一致した)。`?reset_timecard=true` を付けると
//! ②に続けて③ (`CakephpClient::post_reset_timecard`) まで実行する。
//!
//! - **既定は `false`。** 破壊的操作 (`time_card_dtako` への書き戻し) を既定で
//!   増やさない (受け入れ条件1)
//! - **②が失敗 (非 2xx) したら③はやらない。** `reset_skip_reason` に理由を返す
//!   (受け入れ条件3)。②の接続自体が失敗した場合はこの関数が早期リターンする
//!   ので、そもそも③まで到達しない
//! - **`preview=true` のときは③も実行しない。** `reset_target_path` (相対パスの
//!   み) で「何を叩く予定か」だけを返す (受け入れ条件6)
//! - **③の結果は `reset_*` prefix で分けて返す。** ②の `http_status` /
//!   `location` とは別の JSON key (`reset_http_status` / `reset_location`) を
//!   使い、混ぜない (受け入れ条件4)
//! - **★★ `reset_http_status` は成功の証明にならない。** ③の応答は空 200 で、
//!   成否は PHP 側の Flash (session) にしか出ない
//!   (`yhonda-ohishi/nginx#796` に起票済み、`CakephpClient::post_reset_timecard`
//!   の doc 参照)。応答には注意書き (`reset_note`) を添えるが、`http_status`
//!   だけを見て「成功した」と判断してはいけない (受け入れ条件5、親も子も一度
//!   `http_status` で誤診している)
//!
//! ## ★★ ③は「削除してから作り直す」— 材料が無ければ消えるだけ (issue #281)
//!
//! `resetby-unko-no` (PHP `TimeCardDtakoController::resetbyUnkoNo`) は
//! **`time_card_dtako` を `unko_no` で全削除してから、`dtako_events` を材料に
//! INSERT し直す**(`deleteAll` → `_setbyUnkoNo`、実機のソースで確認)。
//! **材料が 0 件なら削除だけが実行され、`time_card_dtako` が消えて戻らない。**
//! 実害 (2026-08-01): `dtako_events` 0 件の運行に③を打ち、`time_card_dtako` の
//! 2 行 (運行開始/運行終了) が消えた。`#279` は「②が失敗したら③をやらない」は
//! 入れたが「③の材料が無いなら③をやらない」が抜けていた (親の設計不備)。
//!
//! **材料の定義 (PHP `_setbyUnkoNo` を実機で確認、2026-08-01):**
//! `dtako_events` を `運行NO IN (先頭22桁+"1", 先頭22桁+"2")` (対象CD 1/2 の
//! 両方を見る。呼び出し側が渡した末尾 1 桁は無視される) かつ
//! `イベント名 IN ('休息','運行開始','運行終了')` で読む。**「dtako_events が
//! 何行あるか」ではなく「この絞り込みで何行拾えるか」が本当の材料件数。**
//!
//! **この実装は材料のうち `休息` だけを数える** ([`count_reset_material`])。
//! `driver` (乗務員CD) を渡さずに `運行NO` 付きで `dtako_events` を読める既存の
//! 口が [`crate::kintai_repo::KintaiEventsApi::fetch_rest_events_between`]
//! (`休息` 固定) しか無いため — `運行開始`/`運行終了` まで含めるには
//! `kintai_repo.rs` (build.rs の `KINTAI_OUTPUT_GLOBS` 対象) に新しい SQL を
//! 足す必要があり、それは `logic_version` を動かす (受け入れ条件7 と衝突)。
//! **⇒ 休息が 0 件でも運行開始/運行終了だけが材料として残るケースはこの歯止めを
//! すり抜け得る** (安全側の誤り — 材料が無いのに実行、ではなく材料があるのに
//! スキップと誤判定する側)。実害の事例 (issue #281) は `dtako_events` が丸ごと
//! 0 件だったので休息も 0 件になり、この絞りでも検知できる。完全一致させる
//! フォローアップは親と協議 (2026-08-01、[質問] で報告済み)。
//!
//! - **③の直前 (②のあと) に数える。** ②の取り込みで `dtako_events` が増え得る
//!   ので、②の前に数えると取りこぼす (受け入れ条件・数え方)
//! - **0 件なら③を実行しない。** `reset_skip_reason: "no_dtako_events"` を返し、
//!   黙って飛ばさない (受け入れ条件1/2)
//! - **件数は `dtako_events_count` で返す。** `preview=true` でも
//!   `reset_timecard=true` なら計算して返す — 打つ前に危険が見える
//!   (受け入れ条件3/4)
//! - **件数の取得自体が失敗したら fail-closed で③をやらない。**
//!   `reset_skip_reason: "count_failed"` (`reset_error` に理由)。「数えられない
//!   ＝安全と確認できない」なので既存の `step2_failed` と同じ扱いで止める
//!   (受け入れ条件6 と両立)

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::Deserialize;

use crate::cakephp::{CakephpClient, CakephpError};
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiRepoError};

/// ③ (`CakephpClient::post_reset_timecard`) の応答に添える注意書き。
/// **空 200 は成功の証明ではない** (`yhonda-ohishi/nginx#796` に起票済み)。
/// 受け入れ条件5: `reset_http_status` を成功の証明に使わせない。
const RESET_TIMECARD_STATUS_NOTE: &str = "reset_http_status は空 200 でも失敗でも同じ値になり得ます。成否は Flash (session) にしか出ないため呼び出し側からは判別できません (yhonda-ohishi/nginx#796)";

/// nginx 側の相対パス。**host は含めない** — 受け入れ条件6 (内部アドレスを
/// commit / PR / docs に書かない)。実際の到達先は `CakephpClient` の
/// `base_url` (env `CAKEPHP_BASE_URL`) が持つ。
const AUTOLOAD_PATH: &str = "/dtako-events/autoload";

/// この route 専用の body size 上限 (20 MiB)。axum の `Bytes` extractor は
/// 既定 2 MiB までしか受けない — 1 件 (1 unko_no) ぶんの csvdata.zip は
/// 数 CSV の集合で通常はごく小さいはずだが、上限自体は
/// `DefaultBodyLimit::max` で個別に緩めておく (他の route の既定 2 MiB には
/// 影響しない、server.rs でこの route にだけ layer する)。
pub const MAX_ZIP_BYTES: usize = 20 * 1024 * 1024;

/// `?unko_no=&file_name=&preview=&reset_timecard=`
#[derive(Debug, Deserialize)]
pub struct AutoloadQuery {
    pub unko_no: Option<String>,
    pub file_name: Option<String>,
    #[serde(default)]
    pub preview: bool,
    /// ③ (勤務時間再登録) まで続けるか。**既定 `false`** (受け入れ条件1、
    /// モジュール doc 「`reset_timecard`」節参照)。
    #[serde(default)]
    pub reset_timecard: bool,
}

/// ③ の相対パス。**host は含めない** — 受け入れ条件6 と同じ理由
/// (`AUTOLOAD_PATH` 参照)。`unko_no` は呼び出し前に `parse_unko_no` で
/// 数字のみと確定済みなので percent-encode は不要。
fn reset_timecard_path(unko_no: &str) -> String {
    format!("/time-card-dtako/resetby-unko-no/{unko_no}")
}

/// `unko_no` の受け入れ判定。**空・非数字は拒否** — 「対象を名指しで受け取る」
/// (月まるごと等の一括指定を弾く) 歯止め。桁数は固定しない — オンプレ23桁 /
/// GCP・theearth 側22桁で実物の桁が揺れる (Refs #205 の 57 実機確認、
/// `dtako_day.rs` のモジュール doc 参照) ため、「全部数字で最低限それらしい
/// 長さ」だけを見る (12 = `unko_no` 先頭の開始日時 `YYMMDDHHMMSS` の桁数)。
fn parse_unko_no(raw: &str) -> Option<&str> {
    if raw.len() < 12 || !raw.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(raw)
}

/// `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) を運行開始日時として読む。
/// `dtako_day.rs` と同じロジックを独立して持つ (モジュール doc の「ファイル名が
/// なぜ」参照 — `kintai_repo.rs`/`dtako_day.rs` 経由にすると余計な依存が増える)。
fn unko_no_start_datetime(unko_no: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(unko_no.get(..12)?, "%y%m%d%H%M%S").ok()
}

/// ③ (PHP `_setbyUnkoNo`) が材料として見る**運行NO の 2 パターン** (対象CD 1/2
/// 両方) を組む。`substr($id, 0, 22)` に "1"/"2" を付けるだけの PHP 実装をそのまま
/// 写す (モジュール doc 「③は削除してから作り直す」参照)。呼び出し側が渡した
/// 末尾 1 桁は使わない — PHP 自身が無視して両方を見るため。
fn reset_material_unko_no_variants(unko_no: &str) -> (String, String) {
    let prefix: String = unko_no.chars().take(22).collect();
    (format!("{prefix}1"), format!("{prefix}2"))
}

/// 材料を数える窓。運行は日をまたぐ (実測: 開始 16:50 → 終了翌日 01:23) ので、
/// 開始日の前日 0 時から 3 日ぶんという広めの余白を取る。`fetch_rest_events_between`
/// は `開始日時`/`終了日時` それぞれに索引が効く範囲検索なので、広めでも安い。
fn material_window(start_dt: NaiveDateTime) -> (String, String) {
    let from = start_dt.date() - chrono::Duration::days(1);
    let to = from + chrono::Duration::days(4);
    (format!("{from} 00:00:00"), format!("{to} 00:00:00"))
}

/// ③ の材料のうち `休息` ぶんを数える (モジュール doc 「③は削除してから作り直す」
/// 参照)。`運行開始`/`運行終了` は数えない — 理由と限界はモジュール doc に明記。
///
/// `unko_no` の先頭 12 桁が読めない (壊れた入力) 場合は材料無しとして `Ok(0)`
/// (fail-safe — 数えられないなら実行しない側に倒す)。
async fn count_reset_material(
    repo: &DynKintaiEventsRepo,
    unko_no: &str,
) -> Result<i64, KintaiRepoError> {
    let Some(start_dt) = unko_no_start_datetime(unko_no) else {
        return Ok(0);
    };
    let (from, to) = material_window(start_dt);
    let (variant1, variant2) = reset_material_unko_no_variants(unko_no);
    let rows = repo.fetch_rest_events_between(&from, &to, None).await?;
    let count = rows
        .iter()
        .filter(|r| r.get("source").and_then(|v| v.as_str()) == Some("dtako_events"))
        .filter(|r| {
            let u = r.get("unko_no").and_then(|v| v.as_str());
            u == Some(variant1.as_str()) || u == Some(variant2.as_str())
        })
        .count();
    Ok(count as i64)
}

/// CakePHP client のエラーを HTTP ステータスへ写す。`routes/kintai.rs` に同じ形の
/// `map_cakephp_err` があるが、あちらは `build.rs` の glob 対象 (`logic_version` が
/// 動く) なので import せず独立して持つ (`dtako_day.rs` と同じ方針)。
fn map_cakephp_err(e: CakephpError) -> (StatusCode, String) {
    match e {
        CakephpError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url が未設定 (CAKEPHP_BASE_URL)".to_string(),
        ),
        CakephpError::RequestFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("nginx への接続に失敗: {m}"),
        ),
        // post_dtako_autoload は非2xxも Ok で返すので実際には作らないが、
        // CakephpError は他の fetch_* と共有の enum なので網羅のために残す
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP returned {status}: {body_excerpt}"),
        ),
        CakephpError::JsonError(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP response parse failed: {m}"),
        ),
    }
}

/// POST /api/dtako/autoload?unko_no=&file_name=&preview= — csvdata.zip (body) を
/// 1 件だけ社内 nginx の取り込み口へ中継する (Refs #205 の 58 / #274)。
pub async fn autoload(
    Query(params): Query<AutoloadQuery>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Extension(kintai_events): Extension<DynKintaiEventsRepo>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let unko_no = match params.unko_no.as_deref().and_then(parse_unko_no) {
        Some(u) => u.to_string(),
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "unko_no は対象を1件、数字だけで指定してください (一括取り込みは不可)".to_string(),
            ))
        }
    };
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "body が空です。csvdata.zip の中身を送ってください".to_string(),
        ));
    }
    let file_name = params
        .file_name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "csvdata.zip".to_string());
    let size_bytes = body.len();

    if params.preview {
        tracing::info!(unko_no, size_bytes, "dtako autoload: preview");
        // preview でも③の材料件数は計算する (受け入れ条件4) — 打つ前に危険が見える。
        // reset_timecard=false なら計算しない (preview は今までどおり DB を叩かない)。
        let (dtako_events_count, count_error) = if params.reset_timecard {
            match count_reset_material(&kintai_events, &unko_no).await {
                Ok(n) => (Some(n), None),
                Err(e) => {
                    tracing::warn!(unko_no, error = %e, "dtako reset material count failed (preview)");
                    (None, Some(e.to_string()))
                }
            }
        } else {
            (None, None)
        };
        return Ok(Json(serde_json::json!({
            "preview": true,
            "unko_no": unko_no,
            "file_name": file_name,
            "size_bytes": size_bytes,
            "target_path": AUTOLOAD_PATH,
            "configured": cakephp.is_enabled(),
            // ③ は preview でも実行しない (受け入れ条件6) — 予定だけを返す
            "reset_timecard": params.reset_timecard,
            "reset_target_path": params.reset_timecard.then(|| reset_timecard_path(&unko_no)),
            // ③ の材料件数 (受け入れ条件3/4)。数えられなければ null + エラー文言
            "dtako_events_count": dtako_events_count,
            "dtako_events_count_error": count_error,
            "note": "preview=true のため実際には送信していません",
        })));
    }

    let res = cakephp
        .post_dtako_autoload(&file_name, body.to_vec())
        .await
        .map_err(map_cakephp_err)?;
    let http_ok = (200..300).contains(&res.status);
    let status = res.status;
    tracing::info!(unko_no, size_bytes, status, "dtako autoload sent");

    // ③ (reset_timecard)。②が非2xxなら実行しない (受け入れ条件3)。②の接続自体が
    // 失敗した場合は上の `?` で既に早期リターンしているのでここには来ない。
    let mut reset_attempted = false;
    let mut reset_http_status: Option<u16> = None;
    let mut reset_location: Option<String> = None;
    let mut reset_error: Option<String> = None;
    let mut reset_skip_reason: Option<&str> = None;
    let mut dtako_events_count: Option<i64> = None;
    if params.reset_timecard {
        if http_ok {
            // ★③の直前 (②のあと) に数える — ②の取り込みで増えた分を取りこぼさない
            // (モジュール doc 「③は削除してから作り直す」参照)。
            match count_reset_material(&kintai_events, &unko_no).await {
                Ok(0) => {
                    tracing::info!(unko_no, "dtako reset_timecard skipped: no dtako_events");
                    reset_skip_reason = Some("no_dtako_events");
                    dtako_events_count = Some(0);
                }
                Ok(n) => {
                    dtako_events_count = Some(n);
                    reset_attempted = true;
                    match cakephp.post_reset_timecard(&unko_no).await {
                        Ok(r) => {
                            let s = r.status;
                            tracing::info!(unko_no, status = s, "dtako reset_timecard sent");
                            reset_http_status = Some(r.status);
                            reset_location = r.location;
                        }
                        Err(e) => {
                            tracing::warn!(unko_no, error = %e, "dtako reset_timecard failed");
                            reset_error = Some(e.to_string());
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(unko_no, error = %e, "dtako reset_timecard skipped: count failed");
                    reset_skip_reason = Some("count_failed");
                    reset_error = Some(e.to_string());
                }
            }
        } else {
            tracing::info!(unko_no, "dtako reset_timecard skipped: step2 not http_ok");
            reset_skip_reason = Some("step2_failed");
        }
    }

    Ok(Json(serde_json::json!({
        "preview": false,
        "unko_no": unko_no,
        "file_name": file_name,
        "size_bytes": size_bytes,
        "target_path": AUTOLOAD_PATH,
        "http_status": res.status,
        "http_ok": http_ok,
        // 3xx でも取り込みは走っている (モジュール doc 参照) — http_status では
        // 成否を判断できないので、redirect 先だけでも渡しておく (受け入れ条件2)。
        "location": res.location,
        "response_excerpt": res.body_excerpt,
        // ③ の結果は reset_ prefix で分ける (②の http_status/location とは混ぜない、受け入れ条件4)
        "reset_timecard": params.reset_timecard,
        "reset_attempted": reset_attempted,
        "reset_skip_reason": reset_skip_reason,
        // ③ の材料件数 (受け入れ条件3)。0 件なら reset_skip_reason=no_dtako_events で
        // reset_attempted=false のまま (モジュール doc 「③は削除してから作り直す」参照)
        "dtako_events_count": dtako_events_count,
        "reset_http_status": reset_http_status,
        "reset_location": reset_location,
        "reset_error": reset_error,
        "reset_note": reset_attempted.then_some(RESET_TIMECARD_STATUS_NOTE),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::routing::post;
    use axum::Router;
    use serde_json::Value;
    use tower::ServiceExt;

    /// `KintaiEventsApi` の mock。`fetch_rest_events_between` (③の材料数え、
    /// [`count_reset_material`] 参照) に仕込んだ行を返すだけ — 他のメソッドは
    /// この route の経路では使わないので panic する
    /// (`dtako_day.rs` の `MockRepo` と同じ形)。
    struct MockRepo {
        rows: Vec<Value>,
    }

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for MockRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("dtako_autoload はドライバ指定の events を読まない")
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("dtako_autoload は全乗務員 events を読まない")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("dtako_autoload はフェリーを読まない")
        }

        async fn fetch_rest_events_between(
            &self,
            _from: &str,
            _to: &str,
            driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            assert_eq!(
                driver, None,
                "材料数えは driver を指定しない (unko_no だけで絞る)"
            );
            Ok(self.rows.clone())
        }
    }

    /// 呼ばれたら panic する repo。「②失敗 / reset_timecard=false のときは
    /// 材料を数えにいかない」ことをテストで強制するのに使う。
    struct PanicRepo;

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for PanicRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_rest_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("材料を数えてはいけない場面で呼ばれた")
        }
    }

    /// 材料の件数クエリ自体が失敗する repo (`count_failed` の歯止め用)。
    struct FailingRestRepo;

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for FailingRestRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_rest_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            Err(crate::kintai_repo::KintaiRepoError::QueryFailed(
                "boom".to_string(),
            ))
        }
    }

    fn empty_repo() -> DynKintaiEventsRepo {
        Arc::new(MockRepo { rows: Vec::new() })
    }

    fn material_row(unko_no: &str) -> Value {
        serde_json::json!({
            "source": "dtako_events",
            "state": "休息",
            "unko_no": unko_no,
        })
    }

    fn app(cakephp: Arc<CakephpClient>, repo: DynKintaiEventsRepo) -> Router {
        Router::new()
            .route("/dtako/autoload", post(autoload))
            .layer(Extension(cakephp))
            .layer(Extension(repo))
    }

    async fn call(router: Router, uri: &str, body: Vec<u8>) -> (StatusCode, Value) {
        let res = router
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        (status, body)
    }

    fn unconfigured_client() -> Arc<CakephpClient> {
        Arc::new(CakephpClient::new(String::new(), 30).unwrap())
    }

    #[test]
    fn parse_unko_no_requires_all_digits_and_at_least_12_chars() {
        assert_eq!(
            parse_unko_no("26060507533000000042861"),
            Some("26060507533000000042861")
        );
        assert_eq!(
            parse_unko_no("2602241025060000000272"),
            Some("2602241025060000000272")
        );
        assert_eq!(parse_unko_no(""), None, "空は拒否");
        assert_eq!(
            parse_unko_no("260605075330"),
            Some("260605075330"),
            "12桁ちょうどは受ける"
        );
        assert_eq!(parse_unko_no("26060507533"), None, "11桁は拒否");
        assert_eq!(
            parse_unko_no("2026-06"),
            None,
            "月まるごと等の一括指定っぽい文字列は拒否"
        );
        assert_eq!(
            parse_unko_no("2606050753300000004286a"),
            None,
            "非数字混じりは拒否"
        );
    }

    #[test]
    fn map_cakephp_err_covers_all_variants() {
        assert_eq!(
            map_cakephp_err(CakephpError::NotConfigured).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            map_cakephp_err(CakephpError::RequestFailed("dns".into())).0,
            StatusCode::BAD_GATEWAY
        );
        let (status, msg) = map_cakephp_err(CakephpError::StatusError {
            status: 404,
            body_excerpt: "not found".into(),
        });
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(msg.contains("404"));
        assert_eq!(
            map_cakephp_err(CakephpError::JsonError("bad".into())).0,
            StatusCode::BAD_GATEWAY
        );
    }

    #[tokio::test]
    async fn autoload_rejects_missing_or_invalid_unko_no() {
        let router = app(unconfigured_client(), empty_repo());
        let (status, _) = call(router, "/dtako/autoload?preview=true", vec![1, 2, 3]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let router2 = app(unconfigured_client(), empty_repo());
        let (status2, _) = call(
            router2,
            "/dtako/autoload?unko_no=2026-06&preview=true",
            vec![1, 2, 3],
        )
        .await;
        assert_eq!(status2, StatusCode::BAD_REQUEST, "一括っぽい指定は拒否");
    }

    #[tokio::test]
    async fn autoload_rejects_empty_body() {
        let router = app(unconfigured_client(), empty_repo());
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true",
            vec![],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn autoload_preview_never_calls_cakephp_and_never_leaks_the_host() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // preview では 1 回も叩かれないことを expect(0) で保証する
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["preview"], serde_json::json!(true));
        assert_eq!(
            body["unko_no"],
            serde_json::json!("26060507533000000042861")
        );
        assert_eq!(body["file_name"], serde_json::json!("csvdata.zip"));
        assert_eq!(body["size_bytes"], serde_json::json!(12));
        assert_eq!(
            body["target_path"],
            serde_json::json!("/dtako-events/autoload")
        );
        assert_eq!(body["configured"], serde_json::json!(true));
        assert!(
            !body.to_string().contains(&server.uri()),
            "応答に nginx の host が出てはいけない"
        );
    }

    #[tokio::test]
    async fn autoload_preview_reports_configured_false_when_base_url_is_empty() {
        let router = app(unconfigured_client(), empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true&file_name=x.zip",
            vec![1, 2, 3],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["configured"], serde_json::json!(false));
        assert_eq!(body["file_name"], serde_json::json!("x.zip"));
    }

    #[tokio::test]
    async fn autoload_executes_and_returns_the_raw_status_and_body() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            // ★ #205 の 61: api を送っていなければ PHP 側が 307 を返す想定なので、
            // ここで送信していることを直接検証する
            .and(body_string_contains("name=\"api\""))
            .respond_with(ResponseTemplate::new(200).set_body_string("import queued"))
            .expect(1)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["preview"], serde_json::json!(false));
        assert_eq!(body["http_status"], serde_json::json!(200));
        assert_eq!(body["http_ok"], serde_json::json!(true));
        assert_eq!(body["response_excerpt"], serde_json::json!("import queued"));
        assert_eq!(body["location"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn autoload_surfaces_the_location_header_when_php_returns_a_3xx() {
        // #205 の 61: api を送っていても PHP 側の挙動が変わる等で 3xx が返って
        // きた場合、body だけでは redirect 先が分からない (受け入れ条件2)。
        // ★ 307 でも取り込み自体は先に実行済みなので http_ok=false は
        // 「失敗した」ではなく「redirect された」としてのみ読む (受け入れ条件3)。
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(307).insert_header("location", "/"))
            .expect(1)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_status"], serde_json::json!(307));
        assert_eq!(body["http_ok"], serde_json::json!(false));
        assert_eq!(body["location"], serde_json::json!("/"));
    }

    #[tokio::test]
    async fn autoload_surfaces_a_non_2xx_php_response_instead_of_hiding_it() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        // HTTP レベルでは中継自体は成功しているので 200 のまま、
        // 中身 (http_ok=false) で PHP 側の失敗を読ませる (条件5)
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_status"], serde_json::json!(500));
        assert_eq!(body["http_ok"], serde_json::json!(false));
        assert_eq!(body["response_excerpt"], serde_json::json!("boom"));
    }

    #[tokio::test]
    async fn autoload_returns_503_when_cakephp_is_not_configured() {
        let router = app(unconfigured_client(), empty_repo());
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn autoload_returns_502_when_nginx_is_unreachable() {
        let cakephp = Arc::new(CakephpClient::new("http://127.0.0.1:0".to_string(), 1).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn autoload_default_never_calls_step3_reset_timecard() {
        // 受け入れ条件1: reset_timecard の既定は false。②のみ呼ばれ③は 1 回も叩かれないことを保証する
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        // reset_timecard=false のときは材料を数えにもいかない — 呼ばれたら panic
        let router = app(cakephp, Arc::new(PanicRepo));
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["reset_timecard"], serde_json::json!(false));
        assert_eq!(body["reset_attempted"], serde_json::json!(false));
        assert_eq!(body["reset_http_status"], serde_json::Value::Null);
        assert_eq!(body["dtako_events_count"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn autoload_preview_with_reset_timecard_reports_the_plan_without_calling_anything() {
        // 受け入れ条件6: preview=true のときは③も実行しない。何をするかだけ返す
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        // preview でも材料件数は計算する (受け入れ条件4) — 1件仕込んで確認する
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo {
            rows: vec![material_row("26060507533000000042861")],
        });
        let router = app(cakephp, repo);
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true&reset_timecard=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["preview"], serde_json::json!(true));
        assert_eq!(body["reset_timecard"], serde_json::json!(true));
        assert_eq!(
            body["reset_target_path"],
            serde_json::json!("/time-card-dtako/resetby-unko-no/26060507533000000042861")
        );
        assert_eq!(
            body["dtako_events_count"],
            serde_json::json!(1),
            "preview でも件数が見える (受け入れ条件4)"
        );
    }

    #[tokio::test]
    async fn autoload_reset_timecard_runs_step3_after_step2_succeeds() {
        // 受け入れ条件2/4: api=1 を送り、③の結果を reset_ prefix で②と分けて返す
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(200).set_body_string("import queued"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            .and(body_string_contains("name=\"api\""))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        // 材料 (dtako_events の休息、対象CD 1/2 両方) を2件仕込む
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo {
            rows: vec![
                material_row("26060507533000000042861"),
                material_row("26060507533000000042862"),
            ],
        });
        let router = app(cakephp, repo);
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&reset_timecard=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_status"], serde_json::json!(200));
        assert_eq!(body["reset_timecard"], serde_json::json!(true));
        assert_eq!(body["reset_attempted"], serde_json::json!(true));
        assert_eq!(body["reset_skip_reason"], serde_json::Value::Null);
        assert_eq!(body["dtako_events_count"], serde_json::json!(2));
        assert_eq!(body["reset_http_status"], serde_json::json!(200));
        assert_eq!(body["reset_location"], serde_json::Value::Null);
        assert_eq!(body["reset_error"], serde_json::Value::Null);
        assert!(
            body["reset_note"].as_str().unwrap().contains("Flash"),
            "reset_note は http_status が成功の証明にならないことを書く (受け入れ条件5)"
        );
    }

    #[tokio::test]
    async fn autoload_reset_timecard_is_skipped_when_no_dtako_events_material() {
        // ★実害 (issue #281): dtako_events が 0 件の運行に③を打つと
        // time_card_dtako が削除だけされて消える。0 件なら③を実行しない
        // (受け入れ条件1/2/3)。
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, empty_repo());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&reset_timecard=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_ok"], serde_json::json!(true), "②自体は成功");
        assert_eq!(body["reset_timecard"], serde_json::json!(true));
        assert_eq!(
            body["reset_attempted"],
            serde_json::json!(false),
            "材料が無いので③は呼ばない"
        );
        assert_eq!(
            body["reset_skip_reason"],
            serde_json::json!("no_dtako_events"),
            "黙って飛ばさない (受け入れ条件2)"
        );
        assert_eq!(body["dtako_events_count"], serde_json::json!(0));
        assert_eq!(body["reset_http_status"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn autoload_reset_timecard_is_skipped_when_material_count_query_fails() {
        // 件数を数えられないなら fail-closed で③をやらない (受け入れ条件6と両立)
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp, Arc::new(FailingRestRepo));
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&reset_timecard=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["reset_attempted"], serde_json::json!(false));
        assert_eq!(body["reset_skip_reason"], serde_json::json!("count_failed"));
        assert_eq!(body["dtako_events_count"], serde_json::Value::Null);
        assert!(body["reset_error"].as_str().unwrap().contains("boom"));
    }

    #[tokio::test]
    async fn autoload_reset_timecard_is_skipped_when_step2_is_not_http_ok() {
        // 受け入れ条件3: ②が失敗 (非2xx) したら③をやらない
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        // ②が失敗したら材料も数えにいかない — 呼ばれたら panic
        let router = app(cakephp, Arc::new(PanicRepo));
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&reset_timecard=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_ok"], serde_json::json!(false));
        assert_eq!(body["reset_timecard"], serde_json::json!(true));
        assert_eq!(body["reset_attempted"], serde_json::json!(false));
        assert_eq!(body["reset_skip_reason"], serde_json::json!("step2_failed"));
        assert_eq!(body["reset_http_status"], serde_json::Value::Null);
        assert_eq!(body["dtako_events_count"], serde_json::Value::Null);
    }

    #[test]
    fn reset_material_unko_no_variants_builds_both_crew_suffixes_from_the_leading_22_digits() {
        assert_eq!(
            reset_material_unko_no_variants("26060507533000000042861"),
            (
                "26060507533000000042861".to_string(),
                "26060507533000000042862".to_string()
            ),
            "呼び出し側の末尾1桁は無視し、両クルーを組む (PHP _setbyUnkoNo と同じ)"
        );
        assert_eq!(
            reset_material_unko_no_variants("2606050753300000004286"),
            (
                "26060507533000000042861".to_string(),
                "26060507533000000042862".to_string()
            ),
            "22桁ちょうどの入力でも動く"
        );
    }

    #[test]
    fn unko_no_start_datetime_reads_the_leading_12_digits() {
        let dt = unko_no_start_datetime("26060507533000000042861").unwrap();
        assert_eq!(dt.to_string(), "2026-06-05 07:53:30");
        assert_eq!(unko_no_start_datetime("U1"), None, "12桁に満たない");
    }

    #[test]
    fn material_window_spans_a_day_before_to_three_days_after_the_start_date() {
        let start = unko_no_start_datetime("26060507533000000042861").unwrap();
        let (from, to) = material_window(start);
        assert_eq!(
            from, "2026-06-04 00:00:00",
            "日をまたぐ運行を取りこぼさない余白"
        );
        assert_eq!(to, "2026-06-08 00:00:00");
    }

    #[tokio::test]
    async fn count_reset_material_counts_only_dtako_events_rows_matching_either_crew_suffix() {
        let rows = vec![
            material_row("26060507533000000042861"), // 対象、対象CD=1
            material_row("26060507533000000042862"), // 対象、対象CD=2 (別クルー)
            material_row("26060507533000000042869"), // 別運行なので対象外
            serde_json::json!({"source": "dtako", "unko_no": "26060507533000000042861"}), // dtako_events以外は対象外
        ];
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows });
        let n = count_reset_material(&repo, "26060507533000000042861")
            .await
            .unwrap();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn count_reset_material_is_zero_and_never_queries_when_unko_no_is_too_short_to_parse() {
        // fail-safe: 開始日時が読めないなら「材料無し」に倒す。クエリも投げない
        // (PanicRepo が呼ばれたら panic するので、投げていないことも同時に確認する)
        let repo: DynKintaiEventsRepo = Arc::new(PanicRepo);
        let n = count_reset_material(&repo, "1234").await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn count_reset_material_surfaces_repo_errors() {
        let repo: DynKintaiEventsRepo = Arc::new(FailingRestRepo);
        let err = count_reset_material(&repo, "26060507533000000042861")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::kintai_repo::KintaiRepoError::QueryFailed(_)
        ));
    }
}
