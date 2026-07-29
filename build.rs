use std::path::PathBuf;
use std::process::Command;

use sha2::{Digest, Sha256};

/// `KINTAI_OUTPUT_SHA` の対象 (Refs #191)。
///
/// `/api/kintai/{daily,kosoku-daily,version}` の**応答を形づくるコード**だけを覆う。
/// ディレクトリと接頭辞で glob するので、同じ接頭辞の新しいモジュールは自動で入る
/// (列挙漏れ対策 — 個別列挙にすると新設ファイルを黙って取りこぼす)。
const KINTAI_OUTPUT_GLOBS: &[(&str, &str)] = &[
    ("src", "kosoku"),
    ("src", "kintai"),
    ("src/routes", "kintai"),
];

/// 上の glob が必ず拾わなければならないファイル。**1 つでも欠けたらビルドを落とす** —
/// リネームや移動で対象から黙って抜けるのが、この仕組みで唯一の「古い値」事故になる。
const KINTAI_OUTPUT_REQUIRED: &[&str] = &[
    "src/kintai_repo.rs",
    "src/kintai_store.rs",
    "src/kintai_version.rs",
    "src/kosoku.rs",
    "src/kosoku_paper.rs",
    "src/routes/kintai.rs",
    "src/routes/kintai_version.rs",
];

/// 対象ファイルをパス順に畳んだ sha256 (先頭 16 文字)。
///
/// `routes/kintai_version.rs` が etag に畳む「コード側の版」。リポジトリ全体の
/// `BUILD_SHA` を使っていた頃は、kintai と無関係なデプロイでも relay の上流キャッシュが
/// 全月無効になっていた (Refs #191 / ohishi-exp/nuxt-dtako-admin#543)。
fn kintai_output_sha() -> String {
    let mut files: Vec<PathBuf> = Vec::new();
    for (dir, prefix) in KINTAI_OUTPUT_GLOBS {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir}: {e}"));
        for entry in entries {
            let path = entry.unwrap_or_else(|e| panic!("read_dir {dir}: {e}")).path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_file() && name.starts_with(prefix) && name.ends_with(".rs") {
                files.push(path);
            }
        }
    }
    // パス区切りは OS で違う (Windows は `\`) ので、比較・畳み込みは `/` に正規化する
    let mut rels: Vec<String> = files
        .iter()
        .map(|p| p.to_string_lossy().replace('\\', "/"))
        .collect();
    rels.sort();

    for required in KINTAI_OUTPUT_REQUIRED {
        assert!(
            rels.iter().any(|r| r == required),
            "KINTAI_OUTPUT_SHA の対象から {required} が消えています。\
             移動・リネームしたなら build.rs の KINTAI_OUTPUT_GLOBS / KINTAI_OUTPUT_REQUIRED を\
             同じ PR で直してください (取りこぼすと relay が古い値を返し続けます、Refs #191)"
        );
    }

    let mut hasher = Sha256::new();
    for rel in &rels {
        let body = std::fs::read(rel).unwrap_or_else(|e| panic!("read {rel}: {e}"));
        // パス名も畳む — 中身が同じファイルの入れ替えを別の版として扱うため
        hasher.update(rel.as_bytes());
        hasher.update([0u8]);
        hasher.update(&body);
        hasher.update([0u8]);
    }
    format!("{:x}", hasher.finalize()).chars().take(16).collect()
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
}
