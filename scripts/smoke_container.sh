#!/usr/bin/env bash
# 両形態のコンテナ smoke test (ohishi-exp/rust-ichibanboshi#205 の G9)
#
# 検証するのは「同じイメージが設定だけで 2 つの実行形態になる」こと。#208 で実機
# 確認した挙動を CI に固定する:
#
#   | 形態              | 与える設定                        | 期待                                    |
#   |-------------------|-----------------------------------|-----------------------------------------|
#   | GCP (Cloud Run)   | DATABASE_ENABLED=false + PORT 注入 | 起動成功 / /health 200 / sales 503      |
#   | オンプレ          | 既定 (DATABASE_ENABLED 未設定)     | **起動失敗** (exit 非 0、listener 無し) |
#
# ── なぜ `cargo run` ではなくコンテナなのか ─────────────────────────────────
# #208 で見つかった実バグは **Dockerfile 層**にあった (CMD に --port を書いていた
# ため CLI > env の優先順位で Cloud Run が注入する PORT を握り潰す)。バイナリを
# 直に起動する形ではこのバグは再現しない。src/config.rs / src/routes/health.rs
# 側の挙動は tests/config_env_test.rs と tests/health_backends_test.rs が PR 時に
# 見ているので、ここは **イメージでしか検証できない層だけ**を担当する:
#   - ENV PORT / ENV BIND_ADDR の既定値が効くこと (case B)
#   - 注入された PORT が ENV より強いこと = CMD に --port が復活していないこと (case A)
#   - musl static binary が debian:trixie-slim で実際に exec できること
#
# ── 「起動失敗を期待する」検証が誤って緑にならないための担保 ────────────────
# 1. case A / B が **同じイメージの起動成功**を先に証明する。イメージが壊れて
#    いれば case C の前に赤くなるので、case C が「何も起動しない理由」で通ること
#    はない (vacuous pass の排除)
# 2. exit code が非 0 なだけでは通さず、docker 層の失敗 (125 = daemon エラー /
#    126 = 実行不能 / 127 = コマンド無し) を明示的に **失敗**として弾く
# 3. 落ちた理由をログで確認する (`DB connection failed` / SQL Server 接続テスト)。
#    別の理由で落ちたら赤
# 4. 待っている間ずっと /health を叩き、一度でも 200 が返ったら赤
#    (= 「起動していないこと」を listener の不在で直接確認する)
#
# ── 「正常な失敗」で赤くならないための担保 ──────────────────────────────────
# case C の期待は非 0 exit。`set -e` に拾わせないため、コンテナは常に `-d` で
# 起動し、終了状態は `docker inspect` で **事後に** 読む (`docker run` 自体は
# 成功する)。curl も専用ヘルパ経由で、接続拒否を戻り値ではなく HTTP コード
# `000` として扱う。
#
# usage: scripts/smoke_container.sh <image-ref>
#   例:  scripts/smoke_container.sh ghcr.io/ohishi-exp/rust-ichibanboshi:abc1234
#
# ローカルで回す場合は Makefile の `make smoke-image` を使うこと (musl build →
# ctx/ 組み立て → docker build → 本スクリプト)。
set -euo pipefail

IMAGE="${1:-}"
if [ -z "${IMAGE}" ]; then
  echo "usage: $0 <image-ref>" >&2
  exit 2
fi

# 起動待ちの上限 (秒)。固定 sleep はしない — 下の wait_health は 0.25 秒間隔で
# poll し、コンテナが死んだ時点で即座に諦める。
READY_TIMEOUT_SECS="${SMOKE_READY_TIMEOUT_SECS:-60}"
# 「起動失敗すること」の確認に使う上限 (秒)。create_pool は bb8 の
# connection_timeout(30s) を持つので、それより十分長く取る。
FAIL_TIMEOUT_SECS="${SMOKE_FAIL_TIMEOUT_SECS:-90}"

# 実在しないダミー資格情報。MariaDB の pool は lazy で起動時検証をしないため
# (src/server.rs のコメント参照)、繋がらない値でも `/health` は "declared" を
# 返す。G9 が見たいのは **宣言が /health に出ること**なので、これで足りる。
# 逆に本物を要求すると smoke test が秘匿値と社内網に依存してしまう。
readonly DUMMY_MARIADB_HOST="mariadb.invalid"
readonly DUMMY_MARIADB_DATABASE="kintai_smoke"
readonly DUMMY_MARIADB_PASSWORD="not-a-real-password"
# HTTP 読み先も実接続はしない (client は lazy)。到達しない host で足りる
readonly DUMMY_KINTAI_BASE_URL="http://alc-api.invalid"
readonly DUMMY_KINTAI_TENANT_ID="00000000-0000-0000-0000-000000000000"

CONTAINERS=()
NETWORKS=()
cleanup() {
  local c n
  for c in "${CONTAINERS[@]+"${CONTAINERS[@]}"}"; do
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
  for n in "${NETWORKS[@]+"${NETWORKS[@]}"}"; do
    docker network rm "$n" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

fail() {
  echo "::error::smoke: $*" >&2
  echo "SMOKE FAILED: $*" >&2
  exit 1
}

note() { echo "==> $*"; }

# 一意な名前 (同一 runner で並走しても衝突しない)
name_for() { echo "ichibanboshi-smoke-$1-$$"; }

# docker が割り当てた host 側の 127.0.0.1:port を返す
host_addr() {
  local container="$1" cport="$2" mapped
  mapped="$(docker port "$container" "${cport}/tcp" 2>/dev/null | head -n 1)" \
    || fail "docker port ${container} ${cport} failed"
  [ -n "$mapped" ] || fail "no host port mapped for ${container}:${cport}"
  echo "$mapped"
}

# HTTP status code を stdout に出す。接続できなければ 000 (curl の既定動作)。
# set -e に拾わせないため常に成功で返る。
http_code() {
  local url="$1" code
  # curl は接続できなくても %{http_code} に 000 を書くが、終了 status が非 0 に
  # なるので `|| true` で set -e から守る (ここで死ぬと「起動待ち中」が失敗になる)。
  code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" 2>/dev/null)" || true
  echo "${code:-000}"
}

# body を stdout に出す (失敗時は空)
http_body() {
  local url="$1"
  curl -s --max-time 5 "$url" 2>/dev/null || true
}

container_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo false)" = "true" ]
}

dump_logs() {
  echo "--- docker logs $1 ---" >&2
  docker logs "$1" 2>&1 | tail -n 40 >&2 || true
  echo "--- end logs ---" >&2
}

# /health が 200 を返すまで poll する。固定 sleep ではなく期限付きループで、
# コンテナが途中で死んだら待たずに失敗させる。
wait_health() {
  local container="$1" url="$2" deadline code
  deadline=$(( $(date +%s) + READY_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! container_running "$container"; then
      dump_logs "$container"
      fail "${container} exited before /health became ready (expected it to start)"
    fi
    code="$(http_code "$url")"
    if [ "$code" = "200" ]; then
      return 0
    fi
    sleep 0.25
  done
  dump_logs "$container"
  fail "${container}: /health did not return 200 within ${READY_TIMEOUT_SECS}s (last code=${code:-none})"
}

# 使い捨て Postgres が接続を受け付けるまで poll する (container-internal
# pg_isready を叩くので host 側にポートを publish しなくてよい)。
wait_postgres() {
  local container="$1" deadline
  deadline=$(( $(date +%s) + READY_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! container_running "$container"; then
      dump_logs "$container"
      fail "${container} exited before postgres became ready"
    fi
    if docker exec "$container" pg_isready -U postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  dump_logs "$container"
  fail "${container}: postgres did not become ready within ${READY_TIMEOUT_SECS}s"
}

# JSON の 1 フィールドを取り出して期待値と照合する
assert_json() {
  local body="$1" filter="$2" want="$3" got
  got="$(printf '%s' "$body" | jq -r "$filter" 2>/dev/null || echo '<unparsable>')"
  [ "$got" = "$want" ] || fail "expected ${filter} == ${want}, got ${got} (body: ${body})"
  note "ok: ${filter} == ${want}"
}

note "image = ${IMAGE}"
docker image inspect "$IMAGE" >/dev/null 2>&1 || fail "image ${IMAGE} is not present locally"

# ---------------------------------------------------------------------------
# case A — GCP の形 (Cloud Run が PORT を注入する状況を再現)
# ---------------------------------------------------------------------------
# コンテナ内 port を 8080 (Dockerfile の ENV PORT) から**ずらす**のが要点。
# CMD に `--port 8080` が書かれていると CLI > env で注入した PORT が無視され、
# 8080 で listen してしまうので、この mapping には何も来ず case A が赤くなる。
# = #208 で直したバグの回帰テスト。
CASE_A_PORT=9090
C_A="$(name_for gcp)"
CONTAINERS+=("$C_A")
note "case A: GCP form (DATABASE_ENABLED=false, injected PORT=${CASE_A_PORT})"
docker run -d --name "$C_A" \
  -e DATABASE_ENABLED=false \
  -e PORT="$CASE_A_PORT" \
  -e MARIADB_HOST="$DUMMY_MARIADB_HOST" \
  -e MARIADB_DATABASE="$DUMMY_MARIADB_DATABASE" \
  -e MARIADB_PASSWORD="$DUMMY_MARIADB_PASSWORD" \
  -e KINTAI_EVENTS_SOURCE=http \
  -e KINTAI_EVENTS_BASE_URL="$DUMMY_KINTAI_BASE_URL" \
  -e KINTAI_EVENTS_TENANT_ID="$DUMMY_KINTAI_TENANT_ID" \
  -p 127.0.0.1::"$CASE_A_PORT" \
  "$IMAGE" >/dev/null || fail "case A: docker run failed"

BASE_A="http://$(host_addr "$C_A" "$CASE_A_PORT")"
note "case A: base = ${BASE_A}"
wait_health "$C_A" "${BASE_A}/health"
note "ok: injected PORT=${CASE_A_PORT} is honored (CMD does not pin --port)"

BODY_A="$(http_body "${BASE_A}/health")"
note "case A: /health = ${BODY_A}"
assert_json "$BODY_A" '.status' 'ok'
# 宣言していない → pool を作らない → "disabled" (静かな degraded を作らない)
assert_json "$BODY_A" '.backends.sqlserver' 'disabled'
# ダミー資格情報でも「使うと宣言した」ところまでは出る (pool は lazy)
assert_json "$BODY_A" '.backends.mariadb' 'declared'
# 給与大臣は env を与えていないので disabled
assert_json "$BODY_A" '.backends.kyuyo' 'disabled'
# 生イベントの読み先 (Refs #211)。宣言ではなく**実際に注入された実装**が出るので、
# ここが "mariadb" のままなら GCP の形なのにオンプレの読み先を掴んでいる
assert_json "$BODY_A" '.backends.kintai_events' 'http'

# SQL Server 依存ルートは fail-closed。空配列を返して「0 件」に見せない。
CODE_A_SALES="$(http_code "${BASE_A}/api/sales/monthly")"
[ "$CODE_A_SALES" = "503" ] \
  || fail "case A: /api/sales/monthly expected 503, got ${CODE_A_SALES}"
note "ok: /api/sales/monthly == 503 (fail-closed)"

docker rm -f "$C_A" >/dev/null

# ---------------------------------------------------------------------------
# case B — Dockerfile の ENV 既定値だけで外から到達できること
# ---------------------------------------------------------------------------
# PORT / BIND_ADDR を一切与えない。コード側の既定は port 3100 / bind 127.0.0.1
# (src/config.rs の default_port / default_bind_addr) なので、8080 に外から
# 届いた時点で **Dockerfile の ENV PORT=8080 と ENV BIND_ADDR=0.0.0.0 の両方が
# 効いている**ことが確定する。ENV が消えれば 3100 か loopback で赤くなる。
CASE_B_PORT=8080
C_B="$(name_for env)"
CONTAINERS+=("$C_B")
note "case B: Dockerfile ENV defaults (no PORT / BIND_ADDR given)"
docker run -d --name "$C_B" \
  -e DATABASE_ENABLED=false \
  -p 127.0.0.1::"$CASE_B_PORT" \
  "$IMAGE" >/dev/null || fail "case B: docker run failed"

BASE_B="http://$(host_addr "$C_B" "$CASE_B_PORT")"
note "case B: base = ${BASE_B}"
wait_health "$C_B" "${BASE_B}/health"
note "ok: ENV PORT=${CASE_B_PORT} / ENV BIND_ADDR=0.0.0.0 are in effect"

BODY_B="$(http_body "${BASE_B}/health")"
note "case B: /health = ${BODY_B}"
# case A で mariadb が "declared" だったのは **env で渡したから**であって既定では
# ないことの裏取り。ここが declared になったら宣言判定が壊れている。
assert_json "$BODY_B" '.backends.mariadb' 'disabled'

docker rm -f "$C_B" >/dev/null

# ---------------------------------------------------------------------------
# case C — オンプレの形 (SQL Server 不在) は **起動しない**
# ---------------------------------------------------------------------------
# DATABASE_ENABLED を渡さない = 既定 true = 従来どおりのオンプレの形。
# コンテナ内に SQL Server は居ないので create_pool の起動時 SELECT 1 が失敗し、
# server::run に到達せず listener が立たないまま異常終了するのが正解。
C_C="$(name_for onprem)"
CONTAINERS+=("$C_C")
note "case C: on-prem form (DATABASE_ENABLED unset -> default true), no SQL Server"
docker run -d --name "$C_C" \
  -p 127.0.0.1::8080 \
  "$IMAGE" >/dev/null || fail "case C: docker run failed"

# 期待は「落ちること」。落ちる前に listener が立たないことも同時に見る。
BASE_C="http://$(host_addr "$C_C" 8080)"
DEADLINE=$(( $(date +%s) + FAIL_TIMEOUT_SECS ))
EXITED=0
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if ! container_running "$C_C"; then
    EXITED=1
    break
  fi
  CODE_C="$(http_code "${BASE_C}/health")"
  if [ "$CODE_C" = "200" ]; then
    dump_logs "$C_C"
    fail "case C: /health returned 200 — the on-prem form started without SQL Server. \
Either [database] enabled stopped defaulting to true, or the startup connection test \
(src/db.rs::create_pool) stopped failing hard."
  fi
  sleep 0.25
done

[ "$EXITED" = "1" ] \
  || { dump_logs "$C_C"; fail "case C: container was still running after ${FAIL_TIMEOUT_SECS}s — expected it to exit non-zero"; }

EXIT_CODE="$(docker inspect -f '{{.State.ExitCode}}' "$C_C")"
note "case C: exit code = ${EXIT_CODE}"
[ "$EXIT_CODE" != "0" ] \
  || { dump_logs "$C_C"; fail "case C: exited 0 — expected a non-zero exit (fail fast on the declared backend)"; }

# docker 層の失敗を「期待どおりの起動失敗」と取り違えない。
case "$EXIT_CODE" in
  125) dump_logs "$C_C"; fail "case C: exit 125 = docker daemon error, not an app-level startup failure" ;;
  126) dump_logs "$C_C"; fail "case C: exit 126 = entrypoint not executable (broken image), not an app-level startup failure" ;;
  127) dump_logs "$C_C"; fail "case C: exit 127 = command not found (binary missing from image), not an app-level startup failure" ;;
esac

# 落ちた理由まで確認する。ここを見ないと「たまたま別の理由で落ちた」を通してしまう。
LOGS_C="$(docker logs "$C_C" 2>&1 || true)"
if ! printf '%s' "$LOGS_C" | grep -qE 'DB connection failed|SQL Server connection test FAILED'; then
  dump_logs "$C_C"
  fail "case C: exited ${EXIT_CODE} but the logs do not show the SQL Server startup connection failure — it died for some other reason"
fi
note "ok: exited ${EXIT_CODE} with the expected SQL Server startup failure"

# 終了後に listener が残っていないこと (念押し)
CODE_C_AFTER="$(http_code "${BASE_C}/health")"
[ "$CODE_C_AFTER" != "200" ] \
  || fail "case C: something is still serving /health after the container exited"
note "ok: no listener (last /health code = ${CODE_C_AFTER})"

docker rm -f "$C_C" >/dev/null

# ---------------------------------------------------------------------------
# case D — 宣言したのに設定が欠けていたら **起動しない** (Refs #211)
# ---------------------------------------------------------------------------
# KINTAI_EVENTS_SOURCE=http と言いながら base_url / tenant_id を与えない。
# ここで黙って MariaDB 読みに落ちると、GCP で「動いているが読み先が違う」= 遅く
# ならず静かに間違う状態になる。case C と同じ「宣言したら loud に倒れる」規則。
C_D="$(name_for kintai-misconfig)"
CONTAINERS+=("$C_D")
note "case D: KINTAI_EVENTS_SOURCE=http without base_url / tenant_id"
docker run -d --name "$C_D" \
  -e DATABASE_ENABLED=false \
  -e KINTAI_EVENTS_SOURCE=http \
  -p 127.0.0.1::8080 \
  "$IMAGE" >/dev/null || fail "case D: docker run failed"

BASE_D="http://$(host_addr "$C_D" 8080)"
DEADLINE_D=$(( $(date +%s) + FAIL_TIMEOUT_SECS ))
EXITED_D=0
while [ "$(date +%s)" -lt "$DEADLINE_D" ]; do
  if ! container_running "$C_D"; then
    EXITED_D=1
    break
  fi
  CODE_D="$(http_code "${BASE_D}/health")"
  if [ "$CODE_D" = "200" ]; then
    dump_logs "$C_D"
    fail "case D: /health returned 200 — declaring kintai_events=http without base_url \
must fail startup. A silent fall back to MariaDB reads is exactly the 'not slow, just \
quietly wrong' failure mode this declaration style exists to prevent."
  fi
  sleep 0.25
done

[ "$EXITED_D" = "1" ] \
  || { dump_logs "$C_D"; fail "case D: container was still running after ${FAIL_TIMEOUT_SECS}s — expected it to exit non-zero"; }

EXIT_D="$(docker inspect -f '{{.State.ExitCode}}' "$C_D")"
note "case D: exit code = ${EXIT_D}"
[ "$EXIT_D" != "0" ] \
  || { dump_logs "$C_D"; fail "case D: exited 0 — declaring http without base_url must fail startup, not fall back to MariaDB"; }
case "$EXIT_D" in
  125|126|127) dump_logs "$C_D"; fail "case D: exit ${EXIT_D} = docker/image level failure, not the config validation we are testing" ;;
esac
LOGS_D="$(docker logs "$C_D" 2>&1 || true)"
if ! printf '%s' "$LOGS_D" | grep -qiE 'kintai_events|base_url|tenant_id'; then
  dump_logs "$C_D"
  fail "case D: exited ${EXIT_D} but the logs do not name the missing kintai_events setting — it died for some other reason"
fi
note "ok: exited ${EXIT_D} naming the missing kintai_events setting"

docker rm -f "$C_D" >/dev/null

# ---------------------------------------------------------------------------
# case E — 本番の形 (GCP #205-26): MariaDB 無し + KINTAI_PUSH_ENABLED=true で
# 打刻の読み先が Supabase 読み返し (http+pg) に切り替わること
# ---------------------------------------------------------------------------
# case A は「GCP の形」を名乗りながら DUMMY_MARIADB_* を宣言しており、実際の本番
# (Cloud Run) には MariaDB が無い形態を一度も起動していなかった (#205-26)。
# 現在の Cloud Run revision の実測 env (2026-07-31) はこれ:
#   DATABASE_ENABLED=false / SQLITE_PATH=(空) / KINTAI_PUSH_ENABLED=true /
#   KINTAI_EVENTS_SOURCE=http / KINTAI_EVENTS_BASE_URL=<alc の Cloud Run URL> /
#   KINTAI_EVENTS_TENANT_ID=<uuid> / KINTAI_EVENTS_AUTH_TOKEN_METADATA=true /
#   secret kintai-push-database-url (= KINTAI_PUSH_DATABASE_URL)
# `KINTAI_PUSH_TENANT_ID` は設定しない (#222 の決定 — 空なら受け口が
# X-Tenant-ID から決める。設定すると [kintai_events] tenant_id との一致検査に
# 巻き込まれるだけで本番はそうしていない)。
#
# src/server.rs の build_kintai_events_repo は「MariaDB 無し + kintai_push 有効」
# のときだけ backends.kintai_events を "http+pg" にする (それ以外は "http" のまま
# 打刻が読めない、または "disabled")。ここが本タスクの中核の assert。
#
# KintaiPgStore::connect は lazy ではなく起動時に実際に 1 本繋ぐので、使い捨ての
# Postgres を container 間ネットワークで用意する (host にポートは publish しない
# — pg_isready は docker exec で container 内から確認する)。
NET_E="$(name_for 205-26-net)"
docker network create "$NET_E" >/dev/null || fail "case E: docker network create failed"
NETWORKS+=("$NET_E")

readonly CASE_E_PG_PASSWORD="smoke-205-26-not-a-real-password"
PG_E="$(name_for 205-26-pg)"
CONTAINERS+=("$PG_E")
note "case E: starting throwaway postgres (${PG_E}) for KINTAI_PUSH_DATABASE_URL"
docker run -d --name "$PG_E" --network "$NET_E" \
  -e POSTGRES_PASSWORD="$CASE_E_PG_PASSWORD" \
  postgres:17 >/dev/null || fail "case E: postgres docker run failed"
wait_postgres "$PG_E"

C_E="$(name_for 205-26-prod-shape)"
CONTAINERS+=("$C_E")
note "case E: production shape (GCP, no MariaDB, KINTAI_PUSH_ENABLED=true)"
docker run -d --name "$C_E" --network "$NET_E" \
  -e DATABASE_ENABLED=false \
  -e SQLITE_PATH= \
  -e KINTAI_PUSH_ENABLED=true \
  -e KINTAI_PUSH_DATABASE_URL="postgres://postgres:${CASE_E_PG_PASSWORD}@${PG_E}:5432/postgres" \
  -e KINTAI_EVENTS_SOURCE=http \
  -e KINTAI_EVENTS_BASE_URL="$DUMMY_KINTAI_BASE_URL" \
  -e KINTAI_EVENTS_TENANT_ID="$DUMMY_KINTAI_TENANT_ID" \
  -e KINTAI_EVENTS_AUTH_TOKEN_METADATA=true \
  -p 127.0.0.1::8080 \
  "$IMAGE" >/dev/null || fail "case E: docker run failed"

BASE_E="http://$(host_addr "$C_E" 8080)"
note "case E: base = ${BASE_E}"
wait_health "$C_E" "${BASE_E}/health"

BODY_E="$(http_body "${BASE_E}/health")"
note "case E: /health = ${BODY_E}"
assert_json "$BODY_E" '.status' 'ok'
assert_json "$BODY_E" '.backends.sqlserver' 'disabled'
assert_json "$BODY_E" '.backends.mariadb' 'disabled'
# 本タスク (#205-26) の中核: MariaDB 無し + KINTAI_PUSH 有効なら打刻の読み先が
# Supabase 読み返しに切り替わる。"http" のままなら打刻が読めておらず、
# "disabled" なら kintai_events 自体の設定が届いていない。
assert_json "$BODY_E" '.backends.kintai_events' 'http+pg'

docker rm -f "$C_E" "$PG_E" >/dev/null
docker network rm "$NET_E" >/dev/null

echo
echo "SMOKE OK — both forms behave as declared:"
echo "  GCP form     : starts, /health 200, sqlserver=disabled, mariadb=declared, kintai_events=http, sales 503, injected PORT honored"
echo "  ENV defaults : reachable on 8080 from outside the container (ENV PORT / BIND_ADDR in effect)"
echo "  on-prem form : refuses to start without SQL Server (exit ${EXIT_CODE}, no listener)"
echo "  misconfig    : refuses to start when a declared backend is missing its settings (exit ${EXIT_D})"
echo "  prod shape   : no MariaDB + KINTAI_PUSH_ENABLED=true -> kintai_events=http+pg (#205-26)"
