//! 全量再計算の口 (Refs #205 の 06 の HTTP 版)。**GCP 側の instance が使う**。
//!
//! 窓の受け口 ([`crate::routes::kintai_timecard::receive_window`]) が畳み直すのは
//! **その窓で打刻が変わった乗務員だけ**で、定常時はほぼ 0 人。畳んだ結果が古くなる
//! 経路はもう 1 つあり、そちらはこの口が受け持つ:
//!
//! | 古くなる原因 | 誰が直すか |
//! |---|---|
//! | 打刻が増えた / 直った | 窓の受け口 (変わった乗務員だけ) |
//! | **`LOGIC_BASE` を上げて deploy した** | **この口** (全乗務員が stale) |
//! | **TOML の閾値・丸め方を変えた** | **この口** (同上) |
//!
//! 後者は `logic_version` が変わるので**全単位が一斉に stale になる**。窓 1 往復で
//! 畳み直そうとすると必ず時間切れになるため、口を分けて `after_driver_cd` で
//! ページングする。stale かどうかは窓の応答の `stale.drivers` で分かる。
//!
//! ## 1 ページの費用は「全量読み × ページ数」であって乗務員数ではない
//!
//! `GET /api/kintai/timecard/drivers` の 50 ([`crate::kintai_diff::DEFAULT_MAX_DRIVERS`])
//! とは費用の形が違う。あちらは乗務員CD を数える 1 クエリで 1 ページぶんが
//! 返るのに対し、こちらは**乗務員数によらず月ぶんの生イベントを 1 回丸ごと読む**
//! ([`crate::kintai_fold::fold_month`] — alc / R2 への往復)。母集団の決定 ([`crate::kintai_fold::recalc_driver_page`])
//! もこの 1 回読みを使い回すので、1 乗務員だけ畳んでも 100 人畳んでも読み出し側の
//! 費用は同じだけ払う。ページを増やす意味は「1 回のリクエストで書く乗務員数」に
//! しかない。
//!
//! 実測 (2026-07-31、2026-06 = 94 名、[`DEFAULT_MAX_FOLD_DRIVERS`] = 20 名/ページ時):
//!
//! | 内訳 | 時間 |
//! |---|---|
//! | 全量読み (alc / R2) | 25〜55 秒 (ページ数・乗務員数に非依存) |
//! | fold (20 名ぶん書く) | 3〜4.6 秒 |
//!
//! **費用がページ数 × 全量読みなので、1 ページの乗務員数を上げるほど総費用が減る。**
//! [`DEFAULT_MAX_FOLD_DRIVERS`] を 100 に上げると 94 名が 1 回で終わり、全量読みを
//! 2 回払わずに済む。100 名分の fold は実測の線形外挿で 15〜23 秒、合計しても
//! auth-worker proxy の 100 秒上限に収まる (40〜78 秒)。内訳は応答の
//! `elapsed_ms` (全体) と `fold.elapsed_ms` (fold だけ) の差で読める
//! — 見積もりで上限を決めない、が [`crate::kintai_diff`] と同じ方針。
//!
//! ## 月ゲート (実装計画 13) — 変化が無ければ全量読みそのものを省く
//!
//! 上の実測は「毎回 R2 から読み直す」前提。前回 fold 時から dtako 側 (alc の
//! etag) も打刻側 (Pg) も変わっていなければ、`month_gate_report` が刺さり、
//! `fold_month` を 1 回も呼ばずに「変化無し」を返す (25〜55 秒 → 索引参照だけの
//! 数十ミリ秒)。定常時 (当日以外) はほぼこちら。当月は毎日データが増えるので、
//! 日 1 回程度は上の実測どおり全量読みになる。
//!
//! **この口は「そのページで月まるごとを完結させたときだけ」gate を書く** —
//! `apply` かつ `after_driver_cd` 無し (1 ページ目から) かつ `next_after_driver_cd`
//! が無い (回りきった) かつ `stale_only` でない (母集団を狭めていない) かつ
//! **封を止める warning が無い** (欠けた入力を「最新」と刻まない。上流の応答の
//! 横流しだけでなく、指紋を取るときに自分で見つけた入力の欠けも含む —
//! Refs #205 の 21。**ただし tail gap は診断専用で対象外** — 運行開始日という
//! 稼働の代理指標では「月の途中で稼働が止まった」と「入力が欠けた」を区別できず、
//! 対象から外さないと恒常的に鳴って封が一生開かない。応答の `warnings` からは
//! 消えない (降格であって削除ではない) — Refs #205-51) の
//! 5 条件が揃ったときだけ。母集団 (実測 132 名) は `DEFAULT_MAX_FOLD_DRIVERS`
//! (100) より上の `max_drivers` を渡せば 1 ページで収まる。書く digest は
//! ページ処理の**冒頭**で判定した値のまま (fold の後に作り直さない —
//! [`crate::kintai_fold::MonthGate::Miss`] の docs)。それ以外のページ (2 ページ目
//! 以降・stale_only・dry-run 等) は読むだけで書かない — 1 ページ目だけ処理した
//! 時点で「この月は最新」と刻むと、未処理ページの乗務員が古い fingerprint の
//! まま取り残される (#225 / #234 と同型の事故) ため。
//!
//! ### 運用: `LOGIC_BASE` を上げた deploy 直後の gate 書き込みは**2 段構え**
//!
//! 共有 crate の `LOGIC_BASE` (計算結果を変えたときに手で上げる版) を上げて deploy すると
//! `logic_version` が変わり全乗務員が stale になる (コメント・移設だけの deploy では変わらない)
//! (モジュール冒頭の表)。この直後に**いきなり 1 ページで全員 apply しようとしない
//! こと** — #205-15 の本番実測 (2026-06、137 名) では、実際に書き込みが乗ると
//! 1 ページ 100 名で応答 109 秒 (fold だけで 82.6 秒)。137 名を 1 ページに収めて
//! 全員書かせようとすると 150 秒超が見込まれ、auth-worker proxy の 100 秒上限に
//! 触りうる。
//!
//! 収束済ませてから封をする、の 2 回に分ける:
//!
//! 1. **収束させる**: `apply=true` を最後のページ (`next_after_driver_cd` が `null`
//!    になる) まで回す。実際の書き込みはここで終わる。
//!    **`max_drivers` は 50 に落とすこと** — 下の実測を参照
//! 2. **封をする**: 続けて `max_drivers=150` (母集団を 1 ページに収める) +
//!    `apply=true` を**もう 1 回**。1 で全員書き終えた直後なので全員 unchanged —
//!    fold だけで済み (実測 fold 23.5 秒、応答 50 秒程度)、上の 5 条件
//!    (1 ページ目・回りきる・stale_only でない・封を止める warning 無し) を満たして
//!    **その回が gate の初回書き込みを兼ねる**
//!
//! #### ①のページサイズは 50。既定の 100 は Cloudflare の上限を超える
//!
//! 月ゲートは **gate miss の呼び出しに etags の約 25 秒を上乗せする**。
//! `logic_version` を変えた直後は全乗務員が stale なので、①は「etags 約 25 秒 +
//! 全量読み + 全員ぶんの書き込み」になる。ゲート導入前は 1 ページ 100 名で
//! 109 秒とぎりぎり通っていたところに 25 秒が乗ったため、**既定の 100 名では
//! Cloudflare の 100 秒上限を超えて 524 (gateway timeout)** になる
//! (#205-16 の本番実測、2026-07-31)。**`max_drivers: 50` に落として 3 ページに
//! 割ると通る** (137 名で 50 + 50 + 37)。②の封は全員 unchanged で fold だけなので、
//! 150 名 1 ページでも収まる。
//!
//! | 局面 | 実測 |
//! |---|---|
//! | ① 1 ページ 100 名 | **524** (Cloudflare gateway timeout) |
//! | ① 50 名 × 3 ページ | 成功 |
//! | ② 封をする回 | `fold.elapsed_ms` 31,153ms |
//! | ③ 以後のゼロ読み | `fold.elapsed_ms` **0ms** (呼び全体は etags のぶん 22〜27 秒残る) |
//!
//! 以後は月ゲートが刺さり続け、次に `kosoku.rs` や TOML を変えるまで実質ゼロ読み
//! になる。**5 条件は「そのページで実際に何か書いたか」を問わない** — 2 の呼び出しが
//! 全員 unchanged でも、月まるごと 1 ページで回りきっていれば gate は書かれる。
//!
//! ## 応答から読める観測 — `unsplit` / `unko_diff` (Refs #205 の 32 / 37)
//!
//! どちらも**判定に一切入らない素通し**で、材料は月ゲートが毎回引いている
//! `GET /api/dtako/events/etags` の応答:
//!
//! | キー | 中身 |
//! |---|---|
//! | `unsplit` / `unsplit_total` | alc 側で `has_kudgivt = FALSE` の運行 (#205 の 32) |
//! | `unko_diff` / `unko_diff_total` | **押し込み済み (`kintai.kintai_events`) に在って GCP に無い運行** (#205 の 37) |
//! | `unko_diff_gcp_only` | 逆方向 (GCP に在ってオンプレに無い) の件数。窓ぜんたい |
//! | `unko_diff_gcp_only_in_month` / `..._by_month` | **対象月に始まった運行だけ**の逆方向と、月別の内訳 |
//! | `unko_diff_gcp_only_in_month_by_day` | 対象月の逆方向を**運行開始日**で割った件数 (#205 の 39) |
//! | `unko_diff_gcp_only_in_month_by_driver` | 同じく**乗務員別** ([`crate::kintai_http_repo::UnkoDiffDriver`])。alc が `driver_cds` を返さない環境では空 |
//! | `unko_diff_gcp_only_in_month_unknown_driver` | そのうち**乗務員を引けなかった**件数。内訳が空なのが「居ない」か「引けていない」かの区別 |
//! | `unko_diff_gcp_only_driver_split` | 「射程外」と「欠落」の 3 桶 ([`crate::kintai_http_repo::UnkoDiffDriverSplit`]) |
//! | `unko_diff_gcp_only_sample` / `unko_diff_onprem_sample` | **突合キーの実物**を両側 10 件ずつ |
//!
//! **逆方向が大きいことは、それ自体では勤怠が間違っている証拠にならない**
//! (#205 の 39)。fold の入力は etags を一切通らない — [`crate::kintai_fold::fold_month`]
//! が `fetch_all_events_between` で alc の生イベントを直に読み、その乗務員は
//! `extra` として母集団に足される (下の `all_units` / `extra`)。**「突合で見えない」
//! と「勤怠が間違っている」を混同しないこと。**
//!
//! `unko_diff` の突合は「押し込み済みの `kintai.kintai_events` の
//! `(乗務員CD, unko_no)`」対「etags の `unko_no`」。**左辺は「オンプレ全部」では
//! ない** — `dtako_events` にしか出てこない運行は push されないので射程外
//! ([`crate::kintai_http_repo::OnpremOperation`] の docs)。**空でなければ `warnings` に**
//! **封を止める行が 1 本立つ**ので、上の 5 条件の「封を止める warning が無い」が
//! 外れて月ゲートは封をしない — 運行が欠けたままの月を「最新」と刻まないための
//! 意図した挙動 (欠けが埋まれば静かになる)。tail gap と違い診断への降格対象では
//! ない (Refs #205-51 の在庫表)。
//!
//! **標本を両側で返すのは、本番の初回実測 (2026-06) で `unko_diff_gcp_only` が
//! etags の item 総数 (1,130) と一致した = 2 集合の重なりがゼロだったため。**
//! データの欠落ではなく突合キーが一度も一致していない形なので、正規化を推測で
//! 入れる前に生の文字列と長さを並べて見る ([`crate::kintai_http_repo::UnkoSample`])。
//!
//! ## GET は絶対に書かない
//!
//! `GET` は `apply` を持たない。「読むだけのつもりが全乗務員を書き直していた」を
//! 作れないようにするため、書ける口は `POST` だけにしてある。かつて auth-worker の
//! `/ichibanboshi-proxy` が **path + method の完全一致 allowlist** で通していた
//! 経路は撤去済みで、口を足しても proxy 側の登録は要らない。

use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::Extension;
use axum::Json;

use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{assert_same_tenant, map_push_err, store_for};
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

// 入力の型・ページの既定と上限・段取り (月ゲート → 畳み → 封 → 応答) は共有 crate (勤怠 Worker と同じもの、Refs #322)。
// ここは HTTP の殻 (月の 400・書き先とテナントの解決・エラーの status) だけを持つ
pub use kintai_kosoku::route_flow::{
    RecalcQuery, RecalcRequest, DEFAULT_MAX_FOLD_DRIVERS, MAX_MAX_FOLD_DRIVERS,
};

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// GET /api/kintai/recalc — **1 行も書かずに**、いま畳み直すと何が変わるかを返す。
pub async fn preview(
    headers: HeaderMap,
    Query(q): Query<RecalcQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    run(headers, pg, repo, params, read_tenant, q.into_request()).await
}

/// POST /api/kintai/recalc — 1 ページぶんを畳み直す。`apply` が無ければ書かない。
pub async fn recalc(
    headers: HeaderMap,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(req): Json<RecalcRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    run(headers, pg, repo, params, read_tenant, req).await
}

/// 殻: 月の検査 → 書き先 → 読みと書きのテナントの一致 → 共有 crate の段取り (`route_flow::recalc_page`)。
async fn run(
    headers: HeaderMap,
    pg: DynKintaiPgStore,
    repo: DynKintaiEventsRepo,
    params: std::sync::Arc<crate::kosoku::KosokuParams>,
    read_tenant: ReadTenant,
    req: RecalcRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&req.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let st = store_for(&pg, &headers)?;
    // 読みと書きが別テナントのまま畳まない (窓の受け口と同じ検査)
    assert_same_tenant(read_tenant, st.tenant_id())?;
    let ctx = crate::kintai_fold::ctx(&params);
    let body = kintai_kosoku::route_flow::recalc_page(&*repo, &st, &ctx, &req)
        .await
        .map_err(map_push_err)?;
    Ok(Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr() -> HeaderMap {
        HeaderMap::new()
    }

    fn repo() -> DynKintaiEventsRepo {
        std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo)
    }

    fn params() -> std::sync::Arc<crate::kosoku::KosokuParams> {
        std::sync::Arc::new(crate::kosoku::KosokuParams::default())
    }

    /// 壊れた月は**書き先を見に行く前に** 400 で落とす。
    #[tokio::test]
    async fn the_month_is_validated_before_the_store() {
        for m in ["", "2026-7", "nope"] {
            let (code, _) = recalc(
                hdr(),
                Extension(None),
                Extension(repo()),
                Extension(params()),
                Extension(ReadTenant(None)),
                Json(RecalcRequest {
                    month: m.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }
    }

    /// **書き先が無い instance では 503。** オンプレの既定がこれ。
    #[tokio::test]
    async fn it_fails_closed_without_a_store() {
        let (code, msg) = recalc(
            hdr(),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
            Json(RecalcRequest {
                month: "2026-07".to_string(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }

    /// **GET は `apply` を持たない。** 読むだけの口から書けないようにしてある。
    #[tokio::test]
    async fn the_get_route_cannot_apply() {
        let (code, _) = preview(
            hdr(),
            Query(RecalcQuery {
                month: Some("2026-07".to_string()),
                ..Default::default()
            }),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
        )
        .await
        .unwrap_err();
        // 書き先が無いので 503 まで進む = month は通っている
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        // GET の query には apply が無い (コンパイル時に保証されている)
        assert!(!RecalcRequest::default().apply);
    }

    #[test]
    fn the_page_size_is_clamped() {
        assert_eq!(DEFAULT_MAX_FOLD_DRIVERS.clamp(1, MAX_MAX_FOLD_DRIVERS), 100);
        assert_eq!(0_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 1);
        assert_eq!(999_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 150);
    }
}
