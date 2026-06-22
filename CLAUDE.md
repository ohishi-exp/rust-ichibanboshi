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

## 燃料サーチャージ基礎データ (`/api/surcharge/base`、Refs #12)

調査 #12 で確定した「`運転日報明細` の単一行に完了条件の全項目が揃う」結論に基づく
基礎データ endpoint。請求のみ行 (`請求K`='1') を中心に、各行を
**得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定)** に展開して返す。

- query: `from` / `to` (売上年月 YYYY-MM、半開区間)、`kind` (`billing_only` default / `transport` / `all`)、`limit` (1..=10000、default 2000)
- 県正規化: `地域ﾏｽﾀ.地域N` の先頭を都道府県へ。`北海道` のみ 4 文字、他は最初の `県`/`府`/`都` まで。
  未マップ (`発地域C`='000000' 等) は `"?"`。ロジックは `routes/surcharge.rs::normalize_prefecture` (純粋関数)
- 運賃 = `金額 + 割増 + 実費` (#12 確定式。月計一致用の税抜カラムとは別物なので混同しない)
- **残課題は scope 外** (新規構築/外部取込が必要): 燃費 km/L マスタ / 県庁間距離マスタ (47×47) /
  週次全国平均軽油価格の取込 / サーチャージ対象得意先の識別。これらは本 endpoint では扱わない

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

### CI 自動デプロイ (推奨、Refs #14)

`main` への merge で `ci.yml` の `deploy` job が musl binary を build →
**Cloudflare Tunnel SSH 経由**で `ohishi-data` に deploy → PathModified 自動 restart →
`/health` 200 を確認する。GitHub Actions runner は Tailscale 網に居ないため、
Tailscale ではなく `cloudflared access ssh` を `ProxyCommand` にした SSH で到達し、
**CF Access service token** で認証する。

- trigger: `push: branches: [main]` (= merge で本番反映)、`needs: [test]`
- 実 deploy ロジックは手動 `deploy.sh` と `scripts/deploy-remote.sh` を共有
  (host 名・proxy・鍵を env 化)
- deploy 失敗 (build / SSH / health != 200) は job が **loud fail** する

必要な GitHub secrets / variables (ohishi-exp repo or org):

| 名前 | 種別 | 用途 |
|---|---|---|
| `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET` | secret | CF Access service token (SSH 経路の認証) |
| `DEPLOY_SSH_KEY` | secret | CI 専用 SSH 秘密鍵 (host の authorized_keys に公開鍵を登録) |
| `DEPLOY_SSH_HOST` | variable | `ssh-rust-ichiban.mtamaramu.com` 等 (CF Tunnel SSH ingress hostname) |

host 側 (一度きり): `cloudflared` Tunnel に SSH ingress
(`ssh-rust-ichiban.mtamaramu.com` → `ssh://localhost:22`) を追加 → CF Access app +
Service Auth ポリシーで保護 (CI 専用 token のみ許可) → deploy ユーザー
(`ubuntu`) の `~/.ssh/authorized_keys` に CI 公開鍵を登録。

### 手動 fallback (Tailscale 経路)

```bash
# musl build → Tailscale SSH で deploy → 自動 restart
./deploy.sh
```

- **実行先**: `ohishi-data.tailea945d.ts.net` (ubuntu / Ohishi55)
- **インストール先**: `/opt/ichibanboshi/`（ubuntu 所有、sudo 不要）
- **サービス**: `systemctl status ichibanboshi`
- **自動再起動**: `ichibanboshi-watcher.path` (systemd PathModified) がバイナリ変更を検知 → 自動 restart
- **ビルド**: musl スタティックリンク（GLIBC バージョン不一致回避）
- `deploy.sh` / `scripts/deploy-remote.sh` の流れ: `cargo build --release --target x86_64-unknown-linux-musl` → `scp /tmp` → `mv`（アトミック） → PathModified で自動 restart → `/health` 疎通確認

## Cloudflare Access

- **トンネル**: `rust-ichiban.mtamaramu.com` → `http://172.18.21.35:3100`
- **Service Token**: CF-Access-Client-Id / CF-Access-Client-Secret
- トークンなしのリクエストは 403

## フロントエンド

- **リポジトリ**: `ohishi-exp/nuxt-ichibanboshi`
- **URL**: `https://nuxt-ichibanboshi.m-tama-ramu.workers.dev`
- **テナント制限**: `NUXT_ALLOWED_TENANT_ID` (wrangler secret)
- **認証**: rust-alc-api の Google OAuth → JWT

## ワークスペース構成

- `nuxt-ichibanboshi/` → `/home/yhonda/js/nuxt-ichibanboshi` (symlink)
- `.vscode/settings.json` で `git.scanRepositories` にフロントエンドリポジトリを登録
