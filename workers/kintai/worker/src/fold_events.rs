//! fold の読み 3 本 (`FoldEventsIo`) の往復: alc は [`crate::alc::EnvAlc`]、社内 MariaDB は [`EnvMariadb`] (Refs #322)。
//! 読み方 (順番・合流・ページング・非 2xx の扱い) は kintai-logic の `fold_events::WorkerFoldEvents`。
//! 使うのは fold の口 ([`crate::fold`] の recalc・timecard/window)。

use kintai_logic::common::{mariadb_unconfigured, Fail};
use kintai_logic::fold_events::{MariadbRun, WorkerFoldEvents};
use kintai_logic::mariadb_reads::jst_today;
use kintai_mysql::response::Row;
use worker::{Date, Env};

use crate::alc::EnvAlc;
use crate::{db_fail, load_creds, open, query};

/// 社内 MariaDB に 1 接続を開いて SQL を並びのまま流す (遡り起点は 2 本、全乗務員の行は 1 本)。資格情報が無ければ 503、
/// 接続・クエリの失敗は 502 (他の MariaDB の口と同じ)。
pub(crate) struct EnvMariadb<'a>(pub &'a Env);

impl MariadbRun for EnvMariadb<'_> {
    async fn run(&self, sqls: &[String]) -> Result<Vec<Vec<Row>>, Fail> {
        let creds = load_creds(self.0).await.ok_or_else(mariadb_unconfigured)?;
        let mut session = open(self.0, &creds).await.map_err(db_fail)?;
        let mut out = Vec::with_capacity(sqls.len());
        for sql in sqls {
            match query(&mut session, sql).await {
                Ok(set) => out.push(set.rows),
                Err(f) => {
                    session.quit().await;
                    return Err(db_fail(f));
                }
            }
        }
        session.quit().await;
        Ok(out)
    }
}

/// fold の読み。`today` (月ゲートの末尾検知の切り下げ) は Worker の時計から JST の日付を作る。
pub(crate) fn fold_events<'a>(
    alc: &'a EnvAlc<'a>,
    mariadb: &'a EnvMariadb<'a>,
) -> WorkerFoldEvents<'a, EnvAlc<'a>, EnvMariadb<'a>> {
    WorkerFoldEvents {
        alc,
        mariadb,
        today: jst_today(Date::now().as_millis()),
    }
}
