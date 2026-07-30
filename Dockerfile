# rust-ichibanboshi 実行イメージ (GCP Artifact Registry へ publish する成果物)。
#
# ⚠️ このイメージは **Cloud Run では起動できない**。現時点では「オンプレで走る
#    バイナリを、GCP から pull できる形で不変・SHA 付きに固めたもの」でしかない。
#    理由は .github/workflows/gcp-image.yml の冒頭コメント (4 つの阻害要因) を読むこと。
#    要約すると (1) 起動時に SQL Server へ `SELECT 1` を投げて失敗したら exit する
#    (src/db.rs::create_pool)、(2) その SQL Server はオンプレ private で GCP から
#    到達できない (ohishi-exp/rust-ichibanboshi#205 の 04「push 方向のみ」)。
#
# build context は repo root ではなく、workflow が組み立てる ctx/ (musl static
# binary + この Dockerfile だけ)。rust-alc-api の Dockerfile と同じ流儀 —
# ビルド済みバイナリを COPY するだけで、イメージ内で cargo を回さない。
FROM debian:trixie-slim

# tiberius / reqwest は rustls だが、native root store 経由になる経路のために
# ca-certificates は入れておく (rust-alc-api の Dockerfile と同じ)。
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# x86_64-unknown-linux-musl の static binary。glibc に依存しないので base image の
# 版ずれで壊れない (= deploy.sh が musl を選んでいるのと同じ理由)。
COPY ichibanboshi /usr/local/bin/ichibanboshi

# 設定ファイルはイメージに焼かない。config/ichibanboshi.default.toml には
# DB host / user / password が入っており、焼くと rust-alc-api CLAUDE.md の規範
# 「値のハードコード禁止 — Secret Manager + secretKeyRef」に真っ向から反する。
#
# 代わりに **環境変数が TOML と対等な入力経路**になっている (src/config.rs の
# Config::apply_env_overrides)。優先順位は CLI 引数 > 環境変数 > TOML > 既定値。
# Cloud Run では secretKeyRef が入れる env var だけで起動できる:
#   DATABASE_ENABLED=false          … SQL Server (CAPE#01) を使わないと宣言する
#   MARIADB_* / KYUYO_* / JWT_SECRET … 使うバックエンドの秘匿値
#
# ⚠️ DATABASE_ENABLED を宣言しないと既定 (= true、オンプレの形) のまま
#    localhost の SQL Server を探しに行き、起動時接続テストに失敗して終了する。
#    これは意図した挙動 — 「使うと宣言したものに繋がらなければ落ちる」。
#    実行形態の宣言はイメージではなく **deploy 側の設定**が持つべきものなので、
#    ここには焼かない。
#
# PORT / BIND_ADDR は ENV で既定値だけ与える。CMD の CLI 引数として渡すと
# **CLI が env より強い**ため Cloud Run が注入する PORT を握り潰してしまう。
ENV PORT=8080
ENV BIND_ADDR=0.0.0.0
EXPOSE 8080
CMD ["ichibanboshi", "--console"]
