# workers/uriage — 担当者別売上 (`/api/uriage/*`) の Worker 化

Refs ohishi-exp/rust-ichibanboshi#322。オンプレ版 rust の売上の口を Cloudflare Worker に移す。
**いまは純粋部分の crate `uriage-logic` (`logic/`) だけ**。Worker の crate・D1・R2・口は次の PR で足す。

## uriage-logic

root (オンプレ版) の `src/routes/uriage.rs`・`src/repo.rs`・`src/cakephp.rs`・`src/sqlite.rs` から移した。
root は path 依存で `pub use` する (写さない)。tiberius・worker・axum・reqwest・rusqlite に依存せず、時計も読まない。

| ファイル | 中身 |
|---|---|
| `compute.rs` | 担当者振替 (PHP `computePersonSum()` の写経。**リファクタ禁止**)・日次と取引先別の集計・`cal_total` 等 |
| `sqlserver.rs` | 一番星 SQL Server の `[運転日報明細]` を読む SQL (`uriage_rows_sql`)・列番号・`Cells` と `uriage_row` |
| `raw.rs` | raw ndjson と fingerprint (sha256)・gzip・ファイル名と R2 の key |
| `period.rs` | 月 (`YYYY-MM`) の月初・月末・日数 |
| `verify.rs` | PHP `print-json.sum` の読み方と PHP / Rust の差分 |
| `cakephp.rs` | 社内 CakePHP (`masters-json`・`editable-months`・`print-json`) の応答の型 (path の組み立てと通信は呼び手) |
| `api.rs` | 口の Query と応答の型・store から読んだ行の型・r2/pending の verify gate |

## 変えてはいけないもの

- **SQL の文字列** — `logic/tests/golden.rs` が代表的な入力の sha256 を固定している
- **raw ndjson のバイト列と fingerprint** — 同じく固定。オンプレ版が `recalc_jobs` に記録した fingerprint と
  Worker の再計算が一致しないと、全 bucket が「変化あり」になる
- **`uriage_rows_sql` の呼び方の順序** — 部門が空かは接続の前、`None` (英数字に絞って空) は接続の後に見る

## 試験と gate

```
cargo test --manifest-path workers/uriage/Cargo.toml -p uriage-logic
cargo llvm-cov --manifest-path workers/uriage/Cargo.toml -p uriage-logic --text > cov.txt
bash scripts/check_coverage_100.sh --config workers/uriage/coverage_100.toml --use-cache cov.txt
cargo build --manifest-path workers/uriage/Cargo.toml --target wasm32-unknown-unknown
```

CI は `.github/workflows/worker-uriage.yml`。`compute.rs` は到達不能な 1 行があるので登録簿に載せない (理由は
`coverage_100.toml`)。root の HTTP の試験は `tests/uriage_test.rs` に残る。
