# workers/ichiban

一番星 (CAPE#01) SQL Server への到達確認 PoC (Refs #322)。一番星の読み出しを Worker + Workers VPC + 既存の Tunnel へ移す
2 段目の最初の関門「Workers VPC (TCP のみ) → Tunnel → SQL Server に TDS でログインできるか」だけを確かめる。
本番には出さない (CI は build / clippy / 公開範囲の検査 / `wrangler deploy --dry-run` まで。deploy job なし)。

| 口 | 中身 |
|---|---|
| `POST /probe` | ログインして `SELECT 1`。成功 200 `{"ok":true}`、失敗 502 `{"ok":false,"stage":"secret\|connect\|login\|query","kind":"…"}`。認可なし・データは返さない。エラー本文と資格情報は出さない |

method 違いは 405、他の path は 404 (本文 `{"ok":false}`)。

## binding (`worker/wrangler.toml`)

- `ICHIBAN_VPC` — Workers VPC の VPC Service (TCP)。宛先 host:port は Service 側で固定。`service_id` は作成後に差し替える (いまはプレースホルダ)
- `ICHIBAN_SQL` — Secrets Store の secret。JSON `{"user":…,"pass":…}`
- 外から届かない: `workers_dev = false` / `preview_urls = false` / route・env なし。`scripts/check-exposure.sh` が CI で検査し、`check-exposure-test.sh` が陰性対照

tiberius は `EncryptionLevel::NotSupported`・`database("CAPE#01")`。`port` / `instance_name` は呼ばない
(SQL Browser の UDP は Worker から出せない)。

## 構成

`worker/src/`: `lib.rs` (fetch) / `probe.rs` / `repo.rs` (資格情報・ログイン) / `transport.rs` (socket) / `tcp.rs` (VPC の `connect()` extern) /
`probe_logic.rs` (経路・stage・応答・資格情報の検証)。`workers/kyuyo` から**意図して写している** (共通化は本実装の段で決める。
特に公開範囲の検査スクリプト 2 本は kyuyo と片方だけ直さないこと)。独立した workspace で、repo ルートの package・coverage gate とは無関係。

## ローカル検証

`workers/kyuyo/README.md` のローカル検証の節に準ずる。`.dev.vars` (commit しない) に `LOCAL_ICHIBAN_SQL_JSON` と `LOCAL_SQL_ADDR` を置く。
`wrangler.toml` の vars には置かない。`wrangler.toml` のまま `wrangler dev` を打たない (`vpc_services` が remote で API に繋ぎにいく)。
実接続は VPC Service・`ICHIBAN_SQL`・宛先の FW を用意してから `wrangler dev --remote` で `POST /probe`。

## 罠

- FW が cloudflared のホストを許していないと `stage=login` / `kind=io:Other` になり、資格情報の誤りに見える
- オンプレ版は instance 名 + SQL Browser でポートを引いている。VPC Service のポートが SQL Server の実ポートと違う・動的ポートだと `stage=connect` か `login` で落ちる
