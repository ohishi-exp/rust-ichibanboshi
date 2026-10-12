//! raw NDJSON (R2 に置く行) と fingerprint (root の `src/routes/uriage.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! **fingerprint はバイト単位で固定** (`tests/golden.rs`)。オンプレ版が `recalc_jobs` に記録した fingerprint と
//! Worker の再計算が一致しないと、全 bucket が「変化あり」になる。

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::compute::UriageRow;

/// raw rows を JSON-serializable 構造体に変換 (sort 済み列順は repo 側で保証済み、再 sort しない)。
///
/// `unko_date` を含めることで、R2 にある raw NDJSON.gz から後で日次再集計できる。
/// **フィールドの順・名前は fingerprint そのもの** (変えると全 bucket の fingerprint が変わる)。
#[derive(Serialize)]
struct RawRowOut<'a> {
    unko_date: &'a str,
    yokoyoko: i32,
    seikyu_k: i32,
    biko2: &'a str,
    nyuryoku_tanto_c: i32,
    kado_bumon: &'a str,
    kingaku: i64,
    nebiki: i64,
    warimashi: i64,
    jippi: i64,
    yosha_kingaku: i64,
    yosha_nebiki: i64,
    yosha_warimashi: i64,
    yosha_jippi: i64,
    shain_r: &'a str,
    yoshasaki_c: &'a str,
    tokuisaki_key: &'a str,
    tokuisaki_n: &'a str,
    yoshasaki_key: &'a str,
    yoshasaki_n: &'a str,
}

impl<'a> From<&'a UriageRow> for RawRowOut<'a> {
    fn from(r: &'a UriageRow) -> Self {
        Self {
            unko_date: &r.unko_date,
            yokoyoko: r.yokoyoko,
            seikyu_k: r.seikyu_k,
            biko2: &r.biko2,
            nyuryoku_tanto_c: r.nyuryoku_tanto_c,
            kado_bumon: &r.kado_bumon,
            kingaku: r.kingaku,
            nebiki: r.nebiki,
            warimashi: r.warimashi,
            jippi: r.jippi,
            yosha_kingaku: r.yosha_kingaku,
            yosha_nebiki: r.yosha_nebiki,
            yosha_warimashi: r.yosha_warimashi,
            yosha_jippi: r.yosha_jippi,
            shain_r: &r.shain_r,
            yoshasaki_c: &r.yoshasaki_c,
            tokuisaki_key: &r.tokuisaki_key,
            tokuisaki_n: &r.tokuisaki_n,
            yoshasaki_key: &r.yoshasaki_key,
            yoshasaki_n: &r.yoshasaki_n,
        }
    }
}

/// raw rows を NDJSON (改行区切り JSON) bytes にシリアライズし、その bytes の sha256 hex を
/// fingerprint として返す。bytes は gzip 圧縮前の確定形 (row order = repo sort 順)。
pub fn serialize_ndjson_and_fingerprint(rows: &[UriageRow]) -> (Vec<u8>, String) {
    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 200);
    for r in rows {
        let out = RawRowOut::from(r);
        // serde_json::to_writer は trailing \n を付けないので明示的に push する
        let _ = serde_json::to_writer(&mut buf, &out);
        buf.push(b'\n');
    }
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let fingerprint = format!("{:x}", hasher.finalize());
    (buf, fingerprint)
}

/// NDJSON bytes を gzip 圧縮する (`Compression::default()`、ヘッダは flate2 の既定 = mtime 0)。
/// オンプレ版はこれをファイルに書き、Worker は R2 に置く。
pub fn gzip(ndjson: &[u8]) -> std::io::Result<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(ndjson)?;
    enc.finish()
}

/// 1 (month, eigyosho_id) の raw のファイル名 (`eigyosho-{id}.ndjson.gz`)。
pub fn raw_file_name(eigyosho_id: i64) -> String {
    format!("eigyosho-{}.ndjson.gz", eigyosho_id)
}

/// R2 の key (`uriage/{month}/eigyosho-{id}.ndjson.gz`)。nuxt の r2-sync が置いてきた key と同じ。
pub fn r2_key(month: &str, eigyosho_id: i64) -> String {
    format!("uriage/{month}/{}", raw_file_name(eigyosho_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_ndjson_empty_rows() {
        let (bytes, fp) = serialize_ndjson_and_fingerprint(&[]);
        assert!(bytes.is_empty());
        // 空 bytes でも sha256 は固定値
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            fp,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn serialize_ndjson_deterministic_for_same_rows() {
        let rows = vec![
            UriageRow {
                kingaku: 1000,
                biko2: "売上 山﨑".to_string(),
                ..UriageRow::default()
            },
            UriageRow {
                kingaku: 2000,
                biko2: "売上 青井".to_string(),
                ..UriageRow::default()
            },
        ];
        let (b1, fp1) = serialize_ndjson_and_fingerprint(&rows);
        let (b2, fp2) = serialize_ndjson_and_fingerprint(&rows);
        assert_eq!(b1, b2);
        assert_eq!(fp1, fp2);
        // 2 行 → 改行で分かれている
        assert_eq!(b1.iter().filter(|&&c| c == b'\n').count(), 2);
    }

    #[test]
    fn serialize_ndjson_changes_when_row_changes() {
        let r1 = vec![UriageRow {
            kingaku: 1000,
            ..UriageRow::default()
        }];
        let r2 = vec![UriageRow {
            kingaku: 1001,
            ..UriageRow::default()
        }];
        let (_, fp1) = serialize_ndjson_and_fingerprint(&r1);
        let (_, fp2) = serialize_ndjson_and_fingerprint(&r2);
        assert_ne!(fp1, fp2);
    }
}
