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

詳細 (担当者別売上のデータ流れ・燃料サーチャージ・集計ロジックの完全条件・SQL Server 接続・
デプロイ手順・フロントエンド・ワークスペース構成) は `rust-ichibanboshi-map` skill を参照。
