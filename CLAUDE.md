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

## 一番星 売上データ集計ロジック（検証済み）

### 月計テーブルとの完全一致条件

月計テーブル（種別別月計、部門別月計、得意先別月計）の `自車売上 + 傭車売上` は、
運転日報明細から以下の条件で集計した値と **全得意先で完全一致** する。

| 項目 | 値 |
|------|-----|
| **日付カラム** | `売上年月日`（運行年月日・管理年月日ではない） |
| **請求区分** | `請求K IN ('0', '2')`（請求+非請求。請求のみ(1)は除外） |
| **自車売上** | `税抜金額 + 税抜割増 + 税抜実費 - 値引` |
| **傭車売上** | `税抜傭車金額 + 税抜傭車割増 + 税抜傭車実費 - 傭車値引` |

### 注意: `金額` カラムは使わない

`金額` カラムは実費の税処理（内税/外税/非課税）によって消費税の含み方が異なるため、
単純な `金額+割増+実費-値引` では月計と一致しない。
必ず `税抜金額 + 税抜割増 + 税抜実費 - 値引` を使うこと。

### 実費の税処理K（実費内訳ﾏｽﾀ）

| 実費K | 名称 | 税処理K | 説明 |
|-------|------|---------|------|
| 1 | 高速料金 | 3 | 内税（税込） |
| 2 | 保険料 | 1 | 非課税 |
| 3 | 高速（課税） | 0 | 外税 |
| 4 | 高速料金調整 | 0 | 外税 |
| 5 | 橋通行料 | 3 | 内税 |
| 6 | フェリー代 | 3 | 内税 |
| 7 | 手数料 | 0 | 外税 |
| 8 | ﾌｪﾘｰ代調整金 | 3 | 内税 |
| 9 | 計量代 | 0 | 外税 |

### 消費税調整

- `基本事項.消費税調整K = 1` — 得意先単位で消費税を再計算・丸め調整
- 調整行: 品名C=9003「消費税調整」、品名C=9998「端数調整(消費税調整の為)」
- 一括調整行: 品名N「※　請求一括調整明細　※」「※　傭車一括調整明細　※」（金額0、消費税カラムのみ）

### 自車/傭車の判定

- `傭車先C = '000000'` → 自車
- `傭車先C ≠ '000000'` → 傭車
- ※ `傭車先C` は空白ではなく `'000000'`（6桁ゼロ）

### 月計テーブルと日報明細のずれ

- 月計テーブルは締め処理時点のスナップショット
- 締め後に日報明細が遡り修正されても月計は再集計されない
- 例: 2026年1月分の日報が3月に修正 → 月計との差額 ~412万円
- 2月（締め直後）は差額 ~5,500円（消費税丸めのみ）

### 基本事項

| 項目 | 値 |
|------|-----|
| 運用月 | `基本事項.運用月`（現在の処理月） |
| 輸送締日 | 31（月末） |
| 消費税調整K | 1（得意先単位丸め） |
| インボイス適用 | 2023-09-01 |

## SQL Server 接続

- **ホスト**: `172.18.21.102` (ohishi-srv)
- **インスタンス**: `softec`（名前付きインスタンス、`using_named_connection()` 必須）
- **データベース**: `CAPE#01`
- **ユーザー**: `pbi` / `test`
- **暗号化**: `EncryptionLevel::NotSupported`（PHP の `encrypt=optional` 相当）
- **文字コード**: Shift_JIS (CP932)

## デプロイ

```bash
# ohishi-data (Ubuntu) に systemd サービスとしてデプロイ
./deploy.sh

# musl スタティックリンク（GLIBC バージョン不一致回避）
cargo build --release --target x86_64-unknown-linux-musl
```

- **実行先**: `ohishi-data.tailea945d.ts.net` (ubuntu / Ohishi55)
- **インストール先**: `/opt/ichibanboshi/`
- **サービス**: `systemctl status ichibanboshi`

## Cloudflare Access

- **トンネル**: `rust-ichiban.mtamaramu.com` → `http://172.18.21.35:3100`
- **Service Token**: CF-Access-Client-Id / CF-Access-Client-Secret
- トークンなしのリクエストは 403

## フロントエンド

- **リポジトリ**: `ohishi-exp/nuxt-ichibanboshi`
- **URL**: `https://nuxt-ichibanboshi.m-tama-ramu.workers.dev`
- **テナント制限**: `NUXT_ALLOWED_TENANT_ID` (wrangler secret)
- **認証**: rust-alc-api の Google OAuth → JWT
