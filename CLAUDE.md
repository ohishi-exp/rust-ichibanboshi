# rust-ichibanboshi

一番星 SQL Server (CAPE#01) の売上データを取得し REST API で提供する Linux サービス。

## アーキテクチャ

```
ブラウザ → https://nuxt-ichibanboshi.m-tama-ramu.workers.dev (CF Workers)
         → /api/sales/* (server route, CF Access Service Token 付き)
         → https://rust-ichiban.mtamaramu.com (Cloudflare Tunnel)
         → ohishi-data:3100 (systemd)
         → 172.18.21.102\softec (SQL Server, CAPE#01)
```

## 売上集計の必須ルール

- 月計テーブルと一致させる集計では **必ず `税抜金額 + 税抜割増 + 税抜実費 - 値引`**
  (自車) / `税抜傭車金額 + 税抜傭車割増 + 税抜傭車実費 - 傭車値引` (傭車) を使うこと。
  `金額` カラムは実費の税処理(内税/外税/非課税)で消費税の含み方が異なるため **使わない**。
- 自車/傭車判定: `傭車先C` は空白ではなく **`'000000'`**（6桁ゼロ）で判定する。

## Cloudflare Access

- CF Access Service Token (`CF-Access-Client-Id/Secret`) が無いリクエストは **403**。

## migration (`migrations/` — kintai スキーマ / Supabase)

売上は SQL Server で migration を持たない。`migrations/` は **#205 の勤怠 (kintai)
スキーマ専用**で、相手は別 DB (専用 Supabase / secret `kintai-database-url`)。

適用は `make kintai-migrate` (= `scripts/migrate_kintai.sh`、psql)。ledger は
sqlx-postgres 0.8 と同形の `_sqlx_migrations` (version / description / SHA-384
checksum) なので、postgres client を入れた後は `sqlx::migrate!` がそのまま
引き継げる — **どちらで適用しても状態が割れない**。

規範は `rust-alc-api` と同じ:

- **適用済み migration は絶対に変更しない** (checksum 照合で loud fail)。修正は
  新規ファイルの追加で行う。`make kintai-rls-verify` がこの fail も検証している
- `SECURITY DEFINER` 関数を作るなら `SET search_path = kintai` を必須にする
  (001 には該当する関数が無い)
- RLS policy に `WITH CHECK (true)` を書かない。`FOR ALL` で `WITH CHECK` を
  省略すれば `USING` 式がそのまま適用されるので、そちらに倒す
- 既存データへの `INSERT` / `UPDATE` をハードコードしない (`WHERE EXISTS` で書く)
- **パスワードを migration に書かない。** ロールは認証情報無しで作り、付与は
  Supabase 側で `ALTER ROLE ... PASSWORD` → secret へ

RLS は `postgres` では素通りするので、検証は必ず `kintai_reader`
(`NOBYPASSRLS`) で繋いで行う。`make kintai-rls-verify` が使い捨ての docker
postgres でこれをやり、CI (`ci.yml` の migration job) が毎 PR で同じ検証を回す。

## カバレッジ 100% gate

`coverage_100.toml` に登録したファイルは **100% 行カバレッジを維持**する。CI (ci.yml の
test job) が毎 PR で `scripts/check_coverage_100.sh` を回し、1 行でも落ちたら fail する。

- 確認は `make cov-check`。未達ファイルの一覧は `make cov-not100`、特定ファイルの
  未カバー行は `make cov-file F=kosoku`。テストは DB も環境変数も不要
- **gate 対象ファイルで `tracing` マクロを複数行にしない。** フォーマット文字列が
  独立行になると (手書きでも rustfmt の 100 桁折り返しでも) その行は llvm-cov の行
  カバレッジに乗らず gate が落ちる。メッセージを短くして必ず 1 行に収める
  (rust-alc-api の PR #399 / #400 で 2 回踏んだ罠)。`format!` 等の他のマクロも同様
- **100% に到達したファイルは登録簿に足す。** 登録漏れは gate が守っていないのと同じ
- **登録簿にあるのに計測データに現れないファイルは fail する** (スキップしない)。
  実行行 0 の `mod.rs` や cfg(windows) の `src/service.rs` は登録できない

詳細 (担当者別売上のデータ流れ・燃料サーチャージ・集計ロジックの完全条件・SQL Server 接続・
デプロイ手順・フロントエンド・ワークスペース構成) は `rust-ichibanboshi-map` skill を参照。
