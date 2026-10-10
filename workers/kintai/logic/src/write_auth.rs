//! 書き込みの口 (`POST /api/kintai/timecard`・`POST /api/kintai/wage-snapshot`) の認可 (Refs #322)。
//!
//! ユーザー決定 (2026-10-10): **書き込みの口は Worker が共有 secret を照合する** (読みの口は認可なしのまま)。
//! 読みのために Service Binding を持つ呼び手が POST を転送しても書けないようにするため。
//!
//! - ヘッダー `X-Kintai-Write-Token` を Secrets Store の binding `KINTAI_WRITE_TOKEN` の値と照合する
//! - 無い・違う → 403 (本文は固定文言。どちらだったかも、secret の長さも出さない)
//! - binding が無い・読めない・空 → 503 (空の secret と空のヘッダーを一致させない)
//! - 照合は両方を sha256 にしてから 32 バイトを定数時間で比べる (長さの違いで早く抜けない)
//! - 検査の順は **認可 → 入力 → テナント → DB** (認可の前に本文を読んで 400 を返さない)

use sha2::{Digest, Sha256};

use crate::common::Fail;

/// Secrets Store の binding 名 (wrangler.toml の `[[secrets_store_secrets]]`)。
pub const WRITE_TOKEN_BINDING: &str = "KINTAI_WRITE_TOKEN";

/// 呼び手が secret を載せるヘッダー。
pub const WRITE_TOKEN_HEADER: &str = "X-Kintai-Write-Token";

/// 403 の本文 (固定)。
pub const FORBIDDEN: &str = "書き込みの口には正しい X-Kintai-Write-Token が要ります";

/// 503 の本文 (固定)。
pub const UNCONFIGURED: &str = "書き込みの認可の設定 (KINTAI_WRITE_TOKEN) が読めません";

/// `secret` は binding から読んだ値 (無い・読めないは `None`)、`header` はリクエストのヘッダーの値。
pub fn authorize(secret: Option<&str>, header: Option<&str>) -> Result<(), Fail> {
    let secret = secret
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Fail::new(503, UNCONFIGURED))?;
    let header = header.ok_or_else(|| Fail::new(403, FORBIDDEN))?;
    if constant_time_eq(&Sha256::digest(secret), &Sha256::digest(header)) {
        Ok(())
    } else {
        Err(Fail::new(403, FORBIDDEN))
    }
}

/// 同じ長さの 2 つを、途中で抜けずに全バイト比べる。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
    a.len() == b.len() && diff == 0
}
