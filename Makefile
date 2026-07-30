# rust-ichibanboshi 開発用 Makefile
#
# テストは全て MockRepo / wiremock / 純粋関数で完結しており、SQL Server も
# MariaDB も環境変数も要らない。したがって rust-alc-api にある db-up / itest /
# cov-check-unit / cov-check-mock のような「DB 有無で分ける」ターゲットは無い。

.PHONY: test test-file fmt clippy cov cov-check cov-dl cov-summary cov-not100 cov-file

# --- テスト (DB 不要) ---

test:
	cargo test --locked

# 特定テストファイル: make test-file T=kosoku_daily_test
test-file:
	cargo test --test $(T)

# --- Lint ---

fmt:
	cargo fmt --check --all

clippy:
	cargo clippy --all-targets -- -D warnings

# --- カバレッジ ---

# ローカルで計測して gate を回す (CI の "Coverage regression check" と同じ)
cov-check:
	bash scripts/check_coverage_100.sh

# ローカル計測結果を安定パスに残す (以降の cov-summary 等が --use-cache で使える)
COV_CACHE := /tmp/llvm-cov-cache/text-$(shell printf '%s' "$(CURDIR)" | md5sum | cut -c1-8).txt

cov:
	@mkdir -p /tmp/llvm-cov-cache
	cargo llvm-cov --text > $(COV_CACHE)
	@echo "Coverage text written to $(COV_CACHE)"

cov-summary: cov
	bash scripts/parse_coverage.sh summary "" $(COV_CACHE)

# 100% 未達のファイル一覧 (次に埋める対象探し)
cov-not100: cov
	bash scripts/parse_coverage.sh not-100 "" $(COV_CACHE)

# 特定ファイルの未カバー行: make cov-file F=kosoku
cov-file: cov
	bash scripts/parse_coverage.sh file $(F) $(COV_CACHE)

# --- CI の artifact を落として使う (再ビルド不要) ---

CI_COV := /tmp/llvm-cov-cache/ci-latest.txt

cov-dl:
	@mkdir -p /tmp/llvm-cov-cache
	rm -rf /tmp/llvm-cov-dl
	gh run download --name llvm-cov-text --dir /tmp/llvm-cov-dl 2>/dev/null || \
		gh run download $$(gh run list --workflow ci.yml --status success \
			--json databaseId -q '.[0].databaseId') \
			--name llvm-cov-text --dir /tmp/llvm-cov-dl
	mv /tmp/llvm-cov-dl/*.txt $(CI_COV)
	rm -rf /tmp/llvm-cov-dl
	@echo "Downloaded to $(CI_COV)"
