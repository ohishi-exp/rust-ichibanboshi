use std::process::Command;

// 版の畳み方 (`fold_output_sha`)。勤怠 Worker の build.rs と共有する (Refs #322)。glob の外に置いてある
include!("workers/kintai/output_sha.rs");

/// `KINTAI_OUTPUT_SHA` の対象 (Refs #191)。
///
/// `/api/kintai/{daily,kosoku-daily,version}` の**応答を形づくるコード**だけを覆う。
/// ディレクトリと接頭辞で glob するので、同じ接頭辞の新しいモジュールは自動で入る
/// (列挙漏れ対策 — 個別列挙にすると新設ファイルを黙って取りこぼす)。
///
/// 拘束・休息の純粋ロジックと MariaDB の SQL は共有 crate `workers/kintai/kosoku` に
/// 移した (Refs #322)。**接頭辞は空 = dir の全 .rs** — lib.rs や切り出し先のファイルも
/// 応答を形づくるので、名前で絞ると版から漏れる。
const KINTAI_OUTPUT_GLOBS: &[(&str, &str)] = &[
    ("src", "kosoku"),
    ("src", "kintai"),
    ("src/routes", "kintai"),
    ("workers/kintai/kosoku/src", ""),
];

/// 上の glob が必ず拾わなければならないファイル。**1 つでも欠けたらビルドを落とす** —
/// リネームや移動で対象から黙って抜けるのが、この仕組みで唯一の「古い値」事故になる。
const KINTAI_OUTPUT_REQUIRED: &[&str] = &[
    "src/kintai_repo.rs",
    "src/kintai_store.rs",
    "src/kintai_version.rs",
    "src/routes/kintai.rs",
    "src/routes/kintai_version.rs",
    // 共有 crate は全ファイルを列挙する (Refs #322)
    "workers/kintai/kosoku/src/anchors.rs",
    "workers/kintai/kosoku/src/kintai_reading_dates.rs",
    "workers/kintai/kosoku/src/kintai_rest_diff.rs",
    "workers/kintai/kosoku/src/kintai_tail_gap_probe.rs",
    "workers/kintai/kosoku/src/kintai_timecard.rs",
    "workers/kintai/kosoku/src/kintai_version.rs",
    "workers/kintai/kosoku/src/kosoku.rs",
    "workers/kintai/kosoku/src/kosoku_daily.rs",
    "workers/kintai/kosoku/src/kosoku_paper.rs",
    "workers/kintai/kosoku/src/lib.rs",
    "workers/kintai/kosoku/src/sql.rs",
    "workers/kintai/kosoku/src/window.rs",
];

/// 対象ファイルをパス順に畳んだ sha256 (先頭 16 文字)。
///
/// `routes/kintai_version.rs` が etag に畳む「コード側の版」。リポジトリ全体の
/// `BUILD_SHA` を使っていた頃は、kintai と無関係なデプロイでも relay の上流キャッシュが
/// 全月無効になっていた (Refs #191 / ohishi-exp/nuxt-dtako-admin#543)。
fn kintai_output_sha() -> String {
    fold_output_sha(KINTAI_OUTPUT_GLOBS, KINTAI_OUTPUT_REQUIRED)
}

// build 時に commit SHA と build 時刻を rustc-env として焼き込む。
// /health がどの build で動いているか識別できるようにするため (Refs #14)。
fn main() {
    // commit SHA: CI が渡す GITHUB_SHA を優先、無ければ git、どちらも無ければ unknown。
    let sha = std::env::var("GITHUB_SHA")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    let short: String = sha.chars().take(12).collect();

    // build 時刻 (UTC ISO8601)。date コマンドに依存 (CI runner / Linux 開発機で利用可)。
    let built_at = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=BUILD_SHA={short}");
    println!("cargo:rustc-env=BUILD_TIME={built_at}");
    println!("cargo:rustc-env=KINTAI_OUTPUT_SHA={}", kintai_output_sha());

    // HEAD が変われば再ビルドして SHA を更新する。
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-changed=.git/HEAD");
    // 対象ファイルの追加・削除も拾うためディレクトリごと監視する (src はどのみち
    // 変更で再ビルドされるので追加コストは無い)
    println!("cargo:rerun-if-changed=src");
    // 共有 crate のロジック (Refs #322)。ここは root の src の外なので別に監視する
    println!("cargo:rerun-if-changed=workers/kintai/kosoku/src");
    println!("cargo:rerun-if-changed=workers/kintai/output_sha.rs");
}
