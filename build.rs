use std::process::Command;

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

    // HEAD が変われば再ビルドして SHA を更新する。
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-changed=.git/HEAD");
}
