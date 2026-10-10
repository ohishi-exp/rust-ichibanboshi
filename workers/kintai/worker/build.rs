// 勤怠 Worker の「応答を形づくるコード」の版 `KINTAI_WORKER_OUTPUT_SHA` を焼く (Refs ohishi-exp/rust-ichibanboshi#322)。
//
// `GET /api/kintai/version` の etag に畳む (オンプレ版の `KINTAI_OUTPUT_SHA` に当たる)。畳み方は repo ルートの build.rs と
// 同じ関数 (`../output_sha.rs` を include!)。対象は workers/kintai の 4 crate (kosoku・logic・mysql・worker) の src の全 .rs —
// 応答を組むコードが worker の src にもあるため worker も入れる。dtako (day-events・worktime) は kosoku-daily と無関係なので
// 入れない。オンプレ版の版とは値が違ってよい (対象のファイルもパスも違う)。

include!("../output_sha.rs");

/// 対象 (dir, ファイル名の接頭辞)。接頭辞は空 = dir の全 .rs (新しいファイルは自動で入る)。
const GLOBS: &[(&str, &str)] = &[
    ("../kosoku/src", ""),
    ("../logic/src", ""),
    ("../mysql/src", ""),
    ("src", ""),
];

/// glob が必ず拾わなければならないファイル。1 つでも欠けたらビルドを落とす (移動・リネームで版から黙って抜けないように)。
const REQUIRED: &[&str] = &[
    "../kosoku/src/anchors.rs",
    "../kosoku/src/kintai_reading_dates.rs",
    "../kosoku/src/kintai_rest_diff.rs",
    "../kosoku/src/kintai_tail_gap_probe.rs",
    "../kosoku/src/kintai_timecard.rs",
    "../kosoku/src/kintai_version.rs",
    "../kosoku/src/kosoku.rs",
    "../kosoku/src/kosoku_daily.rs",
    "../kosoku/src/kosoku_paper.rs",
    "../kosoku/src/lib.rs",
    "../kosoku/src/sql.rs",
    "../kosoku/src/window.rs",
    "../logic/src/common.rs",
    "../logic/src/kosoku_reads.rs",
    "../logic/src/lib.rs",
    "../logic/src/mariadb_reads.rs",
    "../logic/src/mariadb_rows.rs",
    "../mysql/src/bind.rs",
    "../mysql/src/lib.rs",
    "../mysql/src/response.rs",
    "src/conn.rs",
    "src/lib.rs",
    "src/probe.rs",
];

fn main() {
    println!(
        "cargo:rustc-env=KINTAI_WORKER_OUTPUT_SHA={}",
        fold_output_sha(GLOBS, REQUIRED)
    );
    // 対象ファイルの追加・削除も拾うためディレクトリごと監視する
    for (dir, _) in GLOBS {
        println!("cargo:rerun-if-changed={dir}");
    }
    println!("cargo:rerun-if-changed=../output_sha.rs");
}
