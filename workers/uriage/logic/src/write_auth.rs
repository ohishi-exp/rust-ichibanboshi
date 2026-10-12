//! 書き込みの口 (`POST /api/uriage/recalc`) の認可 (Refs ohishi-exp/rust-ichibanboshi#322)。
//! workers/kintai/logic/src/write_auth.rs の写し (別 workspace なので依存しない。名前と失敗の本文だけ売上に合わせた)。
//!
//! - ヘッダー `X-Uriage-Write-Token` を Secrets Store の binding `URIAGE_WRITE_TOKEN` の値と照合する
//! - 無い・違う → 403 `{"ok":false,"stage":"auth","kind":"forbidden"}` (どちらだったかも、secret の長さも出さない)
//! - binding が無い・読めない・空 → 503 `{"ok":false,"stage":"auth","kind":"unconfigured"}` (空の secret と空のヘッダーを一致させない)
//! - 照合は両方を sha256 にしてから 32 バイトを定数時間で比べる (長さの違いで早く抜けない)
//! - 検査の順は **認可 → 入力 → binding → I/O** (認可の前に Query を読んで 400 を返さない)

use sha2::{Digest, Sha256};

use crate::routes::Reply;

/// Secrets Store の binding 名 (wrangler.toml の `[[secrets_store_secrets]]`)。
pub const WRITE_TOKEN_BINDING: &str = "URIAGE_WRITE_TOKEN";

/// 呼び手が secret を載せるヘッダー。
pub const WRITE_TOKEN_HEADER: &str = "X-Uriage-Write-Token";

/// `secret` は binding から読んだ値 (無い・読めないは `None`)、`header` はリクエストのヘッダーの値。
pub fn authorize(secret: Option<&str>, header: Option<&str>) -> Result<(), Reply> {
    let secret = secret
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Reply::failure(503, "auth", "unconfigured"))?;
    let forbidden = || Reply::failure(403, "auth", "forbidden");
    let header = header.ok_or_else(forbidden)?;
    if constant_time_eq(&Sha256::digest(secret), &Sha256::digest(header)) {
        Ok(())
    } else {
        Err(forbidden())
    }
}

/// 同じ長さの 2 つを、途中で抜けずに全バイト比べる。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    a.len() == b.len() && diff == 0
}
