# rust-ichibanboshi

一番星 (売上・運賃)・勤怠の読みを提供する Cloudflare Worker 群 (`workers/`) と、
RDP 中継 `rdp-relay` (root package、オンプレの systemd) の repo。

## 構成

```
workers/uriage・workers/ichiban・workers/kintai 等  … 売上・一番星・勤怠の口 (CF Workers)
root package (src/)                                 … rdp-relay だけ (/health・/rdp・/defaults)
```

- オンプレ版 `ichibanboshi` (売上 API) は Worker へ移して撤去した (#322)。root に売上・勤怠を戻さない
- `rdp-relay` は nuxt-dtako-admin の RemoteApp が使う。main への merge で `deploy` ジョブが
  rdp-relay だけをオンプレへ運び、health (:3390) を確かめる (`scripts/deploy-remote.sh`)

## 売上集計の必須ルール (workers/ の売上)

- 月計テーブルと一致させる集計では **必ず `税抜金額 + 税抜割増 + 税抜実費 - 値引`**
  (自車) / `税抜傭車金額 + 税抜傭車割増 + 税抜傭車実費 - 傭車値引` (傭車) を使うこと。
  `金額` カラムは実費の税処理(内税/外税/非課税)で消費税の含み方が異なるため **使わない**。
- 自車/傭車判定: `傭車先C` は空白ではなく **`'000000'`**（6桁ゼロ）で判定する。

## カバレッジ 100% gate

`coverage_100.toml` に登録したファイルは **100% 行カバレッジを維持**する。CI (ci.yml の
test job) が毎 PR で `scripts/check_coverage_100.sh` を回し、1 行でも落ちたら fail する。

- 確認は `make cov-check`。未達ファイルの一覧は `make cov-not100`、特定ファイルの
  未カバー行は `make cov-file F=cf_access`。テストは DB も環境変数も不要
- **gate 対象ファイルで `tracing` マクロを複数行にしない。** フォーマット文字列が
  独立行になると (手書きでも rustfmt の 100 桁折り返しでも) その行は llvm-cov の行
  カバレッジに乗らず gate が落ちる。メッセージを短くして必ず 1 行に収める
  (rust-alc-api の PR #399 / #400 で 2 回踏んだ罠)。`format!` 等の他のマクロも同様
- **100% に到達したファイルは登録簿に足す。** 登録漏れは gate が守っていないのと同じ
- **登録簿にあるのに計測データに現れないファイルは fail する** (スキップしない)。
  実行行 0 の `lib.rs` は登録できない

## migration

`migrations/` は **#205 の勤怠 (kintai) スキーマ専用**で相手は別 DB。適用済み
migration は絶対に変更しない (checksum 照合で loud fail)。適用 `make kintai-migrate` /
検証 `make kintai-rls-verify`。

詳細 (Worker ごとの口・デプロイ手順・rdp-relay の運用・ワークスペース構成) は
`rust-ichibanboshi-map` skill を参照。
