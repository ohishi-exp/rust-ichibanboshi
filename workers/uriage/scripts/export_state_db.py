#!/usr/bin/env python3
"""オンプレ版の売上 (uriage) の state.db と raw を、D1 と R2 へ移すための SQL と適用手順を作る (Refs #322)。

入口は export-state-db.sh (ssh で取得 → このファイルの `generate` を回す)。python3 の標準ライブラリだけ。

  backup   オンプレ上で動く。state.db を読み取り専用で開き、conn.backup() で一時ファイルへ写してそのパスを出す。
           (ssh <host> python3 - backup <db> < このファイル、で流す。古い python3 でも動くよう構文を保守的にしてある)
  generate 手元で動く。写しを開き、次を --out に書く (D1/R2 には書かない):
             000-delete.sql          4 表の DELETE FROM (再実行のため。VIEW は触らない)
             NNN-<表>.sql            列名を明示した INSERT。1 文 1 行、500 文ごとに分割
             NNN-repair.sql          オンプレ版が起動時に流す修復 UPDATE 2 本 (冪等)
             apply.sh                D1 の execute と R2 の put を順に打つコマンド列 (親が見てから流す)
             verify.sql / expected.txt  D1 に流す照合 SQL と、手元の写しに流した期待値

設計の要点:
  - 本番 (オンプレ) の列順は migration 0001 と違う (ALTER で後から足した列が末尾)。SELECT * や位置指定の INSERT は
    使わず、列名を必ず明示する。
  - 値の文字列化は SQLite 自身の quote() に任せる (氏名・取引先名に引用符が入り得る。自前でエスケープを書かない)。
  - recalc_jobs.raw_path はオンプレのファイルパスから R2 の key (uriage/{month}/eigyosho-{id}.ndjson.gz) に置き換える。

PR-F (オンプレ版の売上の撤去) でこのスクリプトも消す。
"""
import argparse
import os
import shlex
import sqlite3
import sys
import tempfile
from urllib.parse import quote as urlquote

D1_DATABASE = "ichibanboshi-uriage"
R2_BUCKET = "ichibanboshi-uriage"
WRANGLER = "wrangler@4.144.0"
CHUNK = 500

# オンプレの既定値 (src/config.rs)
DEFAULT_DB = "/opt/ichibanboshi/state.db"
DEFAULT_RAW_DIR = "/opt/ichibanboshi/raw"

# (表, migration 0001 の列名の順, 並び替えに使う主キー)
TABLES = [
    (
        "uriage_person_daily",
        [
            "unko_date", "month", "person_name", "eigyosho_id", "cal",
            "kingaku", "yosha_kingaku", "kensuu",
            "kingaku_y0", "yosha_kingaku_y0", "kensuu_y0", "calculated_at",
        ],
        ["unko_date", "person_name", "eigyosho_id", "cal"],
    ),
    (
        "uriage_person_partner_daily",
        [
            "unko_date", "month", "person_name", "eigyosho_id", "cal",
            "partner_kind", "partner_code", "partner_name",
            "kingaku", "kensuu", "kingaku_y0", "kensuu_y0", "calculated_at",
        ],
        ["unko_date", "person_name", "eigyosho_id", "cal", "partner_kind", "partner_code"],
    ),
    (
        "recalc_jobs",
        [
            "month", "eigyosho_id", "status", "fingerprint_before", "fingerprint_after",
            "raw_path", "created_at", "computed_at", "fingerprint_changed_at",
            "r2_synced_at", "last_error",
        ],
        ["month", "eigyosho_id"],
    ),
    (
        "verify_jobs",
        [
            "unko_date", "eigyosho_id", "cal", "month", "ok", "skipped_reason",
            "diff_json", "row_count", "elapsed_ms", "ran_at",
        ],
        ["unko_date", "eigyosho_id", "cal"],
    ),
]

# オンプレ版が起動時に流す修復 (src/sqlite.rs)。本番には既に当たっているが冪等
REPAIRS = [
    "UPDATE recalc_jobs SET fingerprint_changed_at = computed_at "
    "WHERE fingerprint_changed_at IS NULL AND computed_at IS NOT NULL;",
    "UPDATE recalc_jobs SET status = 'r2_synced' "
    "WHERE status = 'computed' AND r2_synced_at IS NOT NULL;",
]

# 照合 (1 文 1 行、結果は (k, v))。D1 と手元の写しの両方に同じものを流す。
# group_concat の ORDER BY (SQLite 3.44〜) は使わず、並べた副問い合わせの上で連結する
_FP = "(SELECT fingerprint_after FROM recalc_jobs ORDER BY month, eigyosho_id)"
VERIFY = [
    ("count.uriage_person_daily", "SELECT 'count.uriage_person_daily' AS k, COUNT(*) AS v FROM uriage_person_daily;"),
    ("count.uriage_person_partner_daily", "SELECT 'count.uriage_person_partner_daily' AS k, COUNT(*) AS v FROM uriage_person_partner_daily;"),
    ("count.recalc_jobs", "SELECT 'count.recalc_jobs' AS k, COUNT(*) AS v FROM recalc_jobs;"),
    ("count.verify_jobs", "SELECT 'count.verify_jobs' AS k, COUNT(*) AS v FROM verify_jobs;"),
    ("sum.uriage_person_daily.kingaku", "SELECT 'sum.uriage_person_daily.kingaku' AS k, SUM(kingaku) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_daily.yosha_kingaku", "SELECT 'sum.uriage_person_daily.yosha_kingaku' AS k, SUM(yosha_kingaku) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_daily.kensuu", "SELECT 'sum.uriage_person_daily.kensuu' AS k, SUM(kensuu) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_daily.kingaku_y0", "SELECT 'sum.uriage_person_daily.kingaku_y0' AS k, SUM(kingaku_y0) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_daily.yosha_kingaku_y0", "SELECT 'sum.uriage_person_daily.yosha_kingaku_y0' AS k, SUM(yosha_kingaku_y0) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_daily.kensuu_y0", "SELECT 'sum.uriage_person_daily.kensuu_y0' AS k, SUM(kensuu_y0) AS v FROM uriage_person_daily;"),
    ("sum.uriage_person_partner_daily.kingaku", "SELECT 'sum.uriage_person_partner_daily.kingaku' AS k, SUM(kingaku) AS v FROM uriage_person_partner_daily;"),
    ("sum.uriage_person_partner_daily.kensuu", "SELECT 'sum.uriage_person_partner_daily.kensuu' AS k, SUM(kensuu) AS v FROM uriage_person_partner_daily;"),
    ("sum.uriage_person_partner_daily.kingaku_y0", "SELECT 'sum.uriage_person_partner_daily.kingaku_y0' AS k, SUM(kingaku_y0) AS v FROM uriage_person_partner_daily;"),
    ("sum.uriage_person_partner_daily.kensuu_y0", "SELECT 'sum.uriage_person_partner_daily.kensuu_y0' AS k, SUM(kensuu_y0) AS v FROM uriage_person_partner_daily;"),
    ("sum.verify_jobs.ok", "SELECT 'sum.verify_jobs.ok' AS k, SUM(ok) AS v FROM verify_jobs;"),
    ("sum.verify_jobs.row_count", "SELECT 'sum.verify_jobs.row_count' AS k, SUM(row_count) AS v FROM verify_jobs;"),
    ("recalc_jobs.status", "SELECT 'recalc_jobs.status.' || status AS k, COUNT(*) AS v FROM recalc_jobs GROUP BY status ORDER BY status;"),
    ("recalc_jobs.raw_path_not_null", "SELECT 'recalc_jobs.raw_path_not_null' AS k, COUNT(raw_path) AS v FROM recalc_jobs;"),
    ("recalc_jobs.fingerprint_after.count", "SELECT 'recalc_jobs.fingerprint_after.count' AS k, COUNT(fingerprint_after) AS v FROM recalc_jobs;"),
    ("recalc_jobs.fingerprint_after.length", "SELECT 'recalc_jobs.fingerprint_after.length' AS k, length(group_concat(fingerprint_after, ',')) AS v FROM " + _FP + ";"),
    ("recalc_jobs.fingerprint_after.head", "SELECT 'recalc_jobs.fingerprint_after.head' AS k, substr(group_concat(fingerprint_after, ','), 1, 64) AS v FROM " + _FP + ";"),
    ("recalc_jobs.fingerprint_after.tail", "SELECT 'recalc_jobs.fingerprint_after.tail' AS k, substr(group_concat(fingerprint_after, ','), -64) AS v FROM " + _FP + ";"),
]


class ExportError(Exception):
    pass


# ── backup (オンプレ上で動く) ──────────────────────────────────────────────

def cmd_backup(src):
    """src を読み取り専用で開いて一時ファイルへ写し、そのパスを標準出力へ出す。"""
    uri = "file:%s?mode=ro" % urlquote(src)
    fd, dst = tempfile.mkstemp(prefix="uriage-state-", suffix=".db")
    os.close(fd)
    try:
        source = sqlite3.connect(uri, uri=True)
        try:
            dest = sqlite3.connect(dst)
            try:
                source.backup(dest)
            finally:
                dest.close()
        finally:
            source.close()
    except Exception:
        os.unlink(dst)
        raise
    print(dst)


# ── generate (手元で動く) ─────────────────────────────────────────────────

def table_columns(conn, table):
    return [r[1] for r in conn.execute('PRAGMA table_info("%s")' % table)]


def sql_lines_for_table(conn, table, columns, order_by, key_for_raw_path):
    """INSERT 文のリストと、(recalc_jobs なら) 置き換えた raw_path の (key, 元のパス) を返す。"""
    present = table_columns(conn, table)
    missing = [c for c in columns if c not in present]
    if missing:
        raise ExportError("%s に列が無い: %s" % (table, ", ".join(missing)))
    extra = ["raw_path", "month", "eigyosho_id"] if table == "recalc_jobs" else []
    select = ", ".join(['quote("%s")' % c for c in columns] + ['"%s"' % c for c in extra])
    sql = 'SELECT %s FROM "%s" ORDER BY %s' % (select, table, ", ".join('"%s"' % c for c in order_by))
    head = "INSERT INTO %s (%s) VALUES (" % (table, ", ".join(columns))
    n = len(columns)
    stmts = []
    raw_files = []
    raw_idx = columns.index("raw_path") if table == "recalc_jobs" else -1
    for row in conn.execute(sql):
        values = list(row[:n])
        if table == "recalc_jobs":
            raw_path, month, eigyosho_id = row[n], row[n + 1], row[n + 2]
            if raw_path is not None:
                key = key_for_raw_path(raw_path, month, eigyosho_id)
                values[raw_idx] = conn.execute("SELECT quote(?)", (key,)).fetchone()[0]
                raw_files.append((key, raw_path))
        # quote() が改行を出すのは文字列リテラルの中だけ。1 文 1 行にするため式に分ける
        values = [v.replace("\n", "'||char(10)||'").replace("\r", "'||char(13)||'") for v in values]
        stmts.append(head + ", ".join(values) + ");")
    return stmts, raw_files


def make_key_resolver(remote_raw_dir):
    prefix = remote_raw_dir.rstrip("/") + "/"

    def resolve(raw_path, month, eigyosho_id):
        if not raw_path.startswith(prefix):
            raise ExportError("raw_path が raw_dir (%s) の下に無い: %s" % (remote_raw_dir, raw_path))
        key = "uriage/" + raw_path[len(prefix):]
        expected = "uriage/%s/eigyosho-%s.ndjson.gz" % (month, eigyosho_id)
        if key != expected:
            raise ExportError("raw_path から作った key が (month, eigyosho_id) の key と違う: %s != %s" % (key, expected))
        return key

    return resolve


def write_text(path, text, mode=0o644):
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    os.chmod(path, mode)


def run_verify(conn):
    """VERIFY を流して `k<TAB>v` の行のリストにする。"""
    lines = []
    for _name, sql in VERIFY:
        for k, v in conn.execute(sql):
            lines.append("%s\t%s" % (k, "NULL" if v is None else v))
    return lines


def cmd_generate(db, raw_dir, remote_raw_dir, out, chunk=CHUNK):
    conn = sqlite3.connect("file:%s?mode=ro" % urlquote(db), uri=True)
    try:
        resolve = make_key_resolver(remote_raw_dir)
        files = []  # 適用順のファイル名
        idx = 0

        def emit(label, stmts):
            nonlocal idx
            name = "%03d-%s.sql" % (idx, label)
            write_text(os.path.join(out, name), "\n".join(stmts) + "\n")
            files.append(name)
            idx += 1

        emit("delete", ["DELETE FROM %s;" % t for t, _c, _o in TABLES])
        raw_files = []
        for table, columns, order_by in TABLES:
            stmts, raws = sql_lines_for_table(conn, table, columns, order_by, resolve)
            raw_files.extend(raws)
            for i in range(0, len(stmts), chunk):
                emit(table, stmts[i:i + chunk])
        emit("repair", REPAIRS)

        # raw: apply.sh に書く前に、手元にあることを確かめる
        puts = []
        seen = set()
        for key, raw_path in raw_files:
            if key in seen:
                continue
            seen.add(key)
            rel = raw_path[len(remote_raw_dir.rstrip("/")) + 1:]
            local = os.path.join(raw_dir, rel)
            if not os.path.isfile(local):
                raise ExportError("raw が手元に無い: %s (key %s)" % (local, key))
            puts.append((key, os.path.relpath(local, out)))

        lines = [
            "#!/usr/bin/env bash",
            "# 生成物。親が内容を見てから流す (本番の D1 と R2 に書く)。このスクリプトを作った側は何も書いていない。",
            "# D1 は 000 (DELETE) → INSERT → repair の順。途中で止まったら 000 から流し直せる (再実行できる)。",
            "set -euo pipefail",
            'cd "$(dirname "$0")"',
        ]
        for name in files:
            lines.append("npx %s d1 execute %s --remote --file %s" % (WRANGLER, D1_DATABASE, shlex.quote(name)))
        for key, rel in puts:
            lines.append(
                "npx %s r2 object put %s --file %s --remote"
                % (WRANGLER, shlex.quote("%s/%s" % (R2_BUCKET, key)), shlex.quote(rel))
            )
        write_text(os.path.join(out, "apply.sh"), "\n".join(lines) + "\n")

        write_text(os.path.join(out, "verify.sql"), "\n".join(sql for _n, sql in VERIFY) + "\n")
        write_text(os.path.join(out, "expected.txt"), "\n".join(run_verify(conn)) + "\n")
    finally:
        conn.close()
    return files, len(puts)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("backup")
    b.add_argument("src")
    g = sub.add_parser("generate")
    g.add_argument("--db", required=True, help="手元に持ってきた state.db の写し")
    g.add_argument("--raw-dir", required=True, help="手元に持ってきた raw ディレクトリ")
    g.add_argument("--remote-raw-dir", default=DEFAULT_RAW_DIR, help="オンプレの raw_dir (raw_path の接頭辞。既定はオンプレの既定値)")
    g.add_argument("--out", required=True)
    a = p.parse_args(argv)
    try:
        if a.cmd == "backup":
            cmd_backup(a.src)
        else:
            files, nraw = cmd_generate(a.db, a.raw_dir, a.remote_raw_dir, a.out)
            print("generated %d sql files, %d raw files -> %s" % (len(files), nraw, a.out))
    except ExportError as e:
        print("error: %s" % e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
