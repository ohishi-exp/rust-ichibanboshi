//! `POST /probe`: 給与大臣 SQL Server にログインして `SELECT 1` を 1 本流す。DO `KyuyoState` のロックの中からだけ呼ぶ。
//!
//! 1 回の流れ: 資格情報 (Secrets Store `KYUYO_SQL`) を読む → `KYUYO_VPC` から TCP を開く →
//! tiberius でログイン (社内 LAN 区間は平文 TDS、`EncryptionLevel::NotSupported`) → `SELECT 1`。
//! 成功は 200 `{"ok":true}`、失敗は 502 `{"ok":false,"stage":"secret|connect|login|query"}`。
//! 応答にもログにもエラーの生文言・ホスト・ポート・ユーザー名を出さない (ログは stage・失敗の種類 `ErrKind`・所要ミリ秒だけ)。

use std::time::Duration;

use kyuyo_logic::{log_line, reply_for_probe, ErrKind, Reply, Stage};
use worker::{console_error, console_log, Date, Env};

use crate::repo::{connect, kind_of, timeout};

/// `SELECT 1` の上限。
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// probe を 1 回走らせ、結果をログに 1 行出して応答を返す。
pub(crate) async fn run(env: &Env) -> Reply {
    let started = Date::now().as_millis();
    let outcome = probe(env).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    match outcome {
        Ok(()) => console_log!("kyuyo probe: ok ({ms} ms)"),
        Err((stage, ref kind)) => console_error!("{}", log_line(stage, kind, ms)),
    }
    reply_for_probe(outcome.map_err(|(stage, _)| stage))
}

/// ログインして `SELECT 1` を 1 本流す。エラーの中身は捨てて stage と失敗の種類だけ返す。
async fn probe(env: &Env) -> Result<(), (Stage, ErrKind)> {
    // 資格情報・TCP・ログイン (上限 20 秒) は 5 口と同じ `repo::connect`
    let mut client = connect(env).await.map_err(|e| (e.stage, e.kind))?;

    let one: Option<Result<Option<i32>, ErrKind>> = timeout(QUERY_TIMEOUT, async {
        let row = client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| kind_of(&e))?
            .into_row()
            .await
            .map_err(|e| kind_of(&e))?;
        Ok(row.and_then(|r| r.get::<i32, _>(0)))
    })
    .await;
    let _ = client.close().await;
    match one {
        None => Err((Stage::Query, ErrKind::Timeout)),
        Some(Err(kind)) => Err((Stage::Query, kind)),
        Some(Ok(Some(1))) => Ok(()),
        Some(Ok(_)) => Err((Stage::Query, ErrKind::Other)),
    }
}
