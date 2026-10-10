// 勤怠の「応答を形づくるコード」の版の畳み方 (Refs ohishi-exp/rust-ichibanboshi#191 / #322)。
//
// repo ルートの build.rs (オンプレ版・Cloud Run 版の KINTAI_OUTPUT_SHA) と workers/kintai/worker/build.rs (勤怠 Worker の
// KINTAI_WORKER_OUTPUT_SHA) の両方が `include!` する。畳み方を 2 実装にしないため、ここにだけ置く。
// **このファイルはどちらの glob にも入らない場所に置く** (crate の src の外)。glob に入れると、畳み方を直しただけで
// 自分の版が変わる。使う側は `sha2::{Digest, Sha256}` を build-dependencies に持つこと。

/// `globs` (`(ディレクトリ, ファイル名の接頭辞)`。接頭辞が空 = その dir の全 .rs) が拾う .rs を、パス (`/` 区切り) の順に
/// `パス \0 中身 \0` で sha256 に畳んだ先頭 16 文字。パスは build script の cwd (= package の dir) からの相対。
///
/// `required` のどれかが拾えなければ panic (= ビルドを落とす) — リネームや移動で対象から黙って抜けると、
/// 応答が変わっても版が変わらず、呼び手のキャッシュが古い値を返し続ける (この仕組みで唯一の「古い値」事故)。
fn fold_output_sha(globs: &[(&str, &str)], required: &[&str]) -> String {
    use sha2::{Digest, Sha256};

    let mut rels: Vec<String> = Vec::new();
    for (dir, prefix) in globs {
        let entries = std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir}: {e}"));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|e| panic!("read_dir {dir}: {e}"))
                .path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_file() && name.starts_with(prefix) && name.ends_with(".rs") {
                // パス区切りは OS で違う (Windows は `\`) ので、比較・畳み込みは `/` に正規化する
                rels.push(path.to_string_lossy().replace('\\', "/"));
            }
        }
    }
    rels.sort();

    for want in required {
        assert!(
            rels.iter().any(|r| r == want),
            "版の対象から {want} が消えています。移動・リネームしたなら build.rs の glob と必須の一覧を\
             同じ PR で直してください (取りこぼすと呼び手が古い値を返し続けます、Refs #191)"
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
    format!("{:x}", hasher.finalize())
        .chars()
        .take(16)
        .collect()
}
