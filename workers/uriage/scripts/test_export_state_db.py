"""export_state_db.py のテスト (python3 -m unittest workers/uriage/scripts/test_export_state_db.py)。

標準ライブラリだけ。手で作った小さい SQLite (本番と同じ「ずれた列順」・引用符入りの名前・NULL・raw_path の有/無) から
SQL を生成し、migration 0001 を流した別の空 SQLite に適用して、元と行が一致することを確かめる。
"""
import os
import re
import sqlite3
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import export_state_db as ex  # noqa: E402

MIGRATION = os.path.join(HERE, "..", "worker", "migrations", "0001_uriage.sql")
REMOTE_RAW = "/opt/ichibanboshi/raw"

# 本番のオンプレの列順 (src/sqlite.rs): calculated_at が 9 番目、y0 の 3 列は ALTER で末尾。recalc_jobs は
# fingerprint_changed_at が末尾。partner と verify_jobs は 0001 と同じ
ONPREM_SCHEMA = """
CREATE TABLE uriage_person_daily (
    unko_date TEXT NOT NULL, month TEXT NOT NULL, person_name TEXT NOT NULL, eigyosho_id INTEGER NOT NULL,
    cal INTEGER NOT NULL, kingaku INTEGER NOT NULL, yosha_kingaku INTEGER NOT NULL, kensuu INTEGER NOT NULL,
    calculated_at TEXT NOT NULL,
    kingaku_y0 INTEGER NOT NULL DEFAULT 0, yosha_kingaku_y0 INTEGER NOT NULL DEFAULT 0, kensuu_y0 INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (unko_date, person_name, eigyosho_id, cal)
);
CREATE TABLE uriage_person_partner_daily (
    unko_date TEXT NOT NULL, month TEXT NOT NULL, person_name TEXT NOT NULL, eigyosho_id INTEGER NOT NULL,
    cal INTEGER NOT NULL, partner_kind TEXT NOT NULL, partner_code TEXT NOT NULL, partner_name TEXT NOT NULL,
    kingaku INTEGER NOT NULL, kensuu INTEGER NOT NULL, kingaku_y0 INTEGER NOT NULL DEFAULT 0,
    kensuu_y0 INTEGER NOT NULL DEFAULT 0, calculated_at TEXT NOT NULL,
    PRIMARY KEY (unko_date, person_name, eigyosho_id, cal, partner_kind, partner_code)
);
CREATE TABLE recalc_jobs (
    month TEXT NOT NULL, eigyosho_id INTEGER NOT NULL, status TEXT NOT NULL, fingerprint_before TEXT,
    fingerprint_after TEXT, raw_path TEXT, created_at TEXT NOT NULL, computed_at TEXT, r2_synced_at TEXT,
    last_error TEXT, fingerprint_changed_at TEXT,
    PRIMARY KEY (month, eigyosho_id)
);
CREATE TABLE verify_jobs (
    unko_date TEXT NOT NULL, eigyosho_id INTEGER NOT NULL, cal INTEGER NOT NULL, month TEXT NOT NULL,
    ok INTEGER NOT NULL, skipped_reason TEXT, diff_json TEXT, row_count INTEGER NOT NULL,
    elapsed_ms INTEGER NOT NULL, ran_at TEXT NOT NULL,
    PRIMARY KEY (unko_date, eigyosho_id, cal)
);
"""

NAMES = ["O'Brien 太郎", 'A "B" 次郎', "改行\nを含む\r\n名前", "山田;DROP TABLE x;--", "通常 花子"]


def raw_path(month, eid):
    return "%s/%s/eigyosho-%d.ndjson.gz" % (REMOTE_RAW, month, eid)


def build_source(path, days=3):
    """本番と同じ列順の小さい state.db。"""
    c = sqlite3.connect(path)
    c.executescript(ONPREM_SCHEMA)
    for d in range(days):
        for i, name in enumerate(NAMES):
            date = "2026-07-%02d" % (d + 1)
            c.execute(
                "INSERT INTO uriage_person_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (date, "2026-07", name, 1 + i % 2, i % 2, 1000 * (i + 1) + d, 10 * i, i + 1,
                 "2026-08-01T00:00:00Z", 7 * (i + 1), 3 * i, i),
            )
            c.execute(
                "INSERT INTO uriage_person_partner_daily VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (date, "2026-07", name, 1 + i % 2, i % 2, "customer" if i % 2 else "subcontractor",
                 "C%03d" % i, "株式会社 '%s' \"店\"" % name, 500 * (i + 1), i + 1, 5 * i, i, "2026-08-01T00:00:00Z"),
            )
            c.execute(
                "INSERT INTO verify_jobs VALUES (?,?,?,?,?,?,?,?,?,?)",
                (date, 1 + i % 2, i, "2026-07", i % 2, None if i % 2 else "skip'ped",
                 None if i % 2 == 0 else '{"a":"it\'s","b":[1,2]}', i * 3, 12 + i, "2026-08-01T01:00:00Z"),
            )
    # raw_path 有 (r2_synced / computed) と無 (failed、NULL のまま)
    c.execute("INSERT INTO recalc_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              ("2026-06", 1, "r2_synced", "fpb1", "fpa1", raw_path("2026-06", 1), "2026-07-01T00:00:00Z",
               "2026-07-01T00:01:00Z", "2026-07-01T00:02:00Z", None, "2026-07-01T00:01:00Z"))
    c.execute("INSERT INTO recalc_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              ("2026-07", 2, "computed", None, "fpa2", raw_path("2026-07", 2), "2026-07-02T00:00:00Z",
               "2026-07-02T00:01:00Z", None, "エラー'\"", None))
    c.execute("INSERT INTO recalc_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              ("2026-07", 3, "failed", None, None, None, "2026-07-03T00:00:00Z", None, None, "boom\nline2", None))
    c.commit()
    c.close()


def make_raw(raw_dir, keys):
    for rel in keys:
        p = os.path.join(raw_dir, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "wb") as f:
            f.write(b"\x1f\x8b dummy " + rel.encode())


def migrated_target():
    t = sqlite3.connect(":memory:")
    with open(MIGRATION, encoding="utf-8") as f:
        t.executescript(f.read())
    return t


def table_rows(conn, table):
    cols = [c for t, c, _o in ex.TABLES if t == table][0]
    order = [o for t, _c, o in ex.TABLES if t == table][0]
    return conn.execute("SELECT %s FROM %s ORDER BY %s" % (", ".join(cols), table, ", ".join(order))).fetchall()


class ExportStateDbTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = self._tmp.name
        self.src = os.path.join(self.tmp, "state.db")
        self.raw = os.path.join(self.tmp, "out", "raw")
        self.out = os.path.join(self.tmp, "out")
        os.makedirs(self.out)
        build_source(self.src)
        make_raw(self.raw, ["2026-06/eigyosho-1.ndjson.gz", "2026-07/eigyosho-2.ndjson.gz"])

    def generate(self, chunk=ex.CHUNK):
        return ex.cmd_generate(self.src, self.raw, REMOTE_RAW, self.out, chunk)

    def apply_all(self, target, files):
        for name in files:
            with open(os.path.join(self.out, name), encoding="utf-8") as f:
                target.executescript(f.read())

    def test_rows_match_despite_shifted_column_order(self):
        """列順のずれ・引用符・改行・NULL を越えて、0001 の表に適用した行が元と一致する。"""
        files, _ = self.generate()
        target = migrated_target()
        self.apply_all(target, files)
        src = sqlite3.connect(self.src)
        for table in ("uriage_person_daily", "uriage_person_partner_daily", "verify_jobs"):
            self.assertEqual(table_rows(target, table), table_rows(src, table), table)
        # y0 の 3 列と calculated_at が入れ替わっていないこと
        row = target.execute(
            "SELECT calculated_at, kingaku_y0, yosha_kingaku_y0, kensuu_y0 FROM uriage_person_daily "
            "WHERE person_name = ? AND unko_date = '2026-07-01'", (NAMES[1],)).fetchone()
        self.assertEqual(row, ("2026-08-01T00:00:00Z", 14, 3, 1))
        self.assertEqual(target.execute("SELECT COUNT(*) FROM uriage_person_daily").fetchone()[0], 15)
        names = {r[0] for r in target.execute("SELECT person_name FROM uriage_person_daily")}
        self.assertEqual(names, set(NAMES))

    def test_null_stays_null_and_quotes_survive(self):
        files, _ = self.generate()
        target = migrated_target()
        self.apply_all(target, files)
        self.assertEqual(
            target.execute("SELECT skipped_reason, diff_json FROM verify_jobs WHERE cal = 1 LIMIT 1").fetchone(),
            (None, '{"a":"it\'s","b":[1,2]}'))
        self.assertEqual(
            target.execute("SELECT last_error FROM recalc_jobs WHERE eigyosho_id = 3").fetchone()[0], "boom\nline2")

    def test_raw_path_becomes_r2_key_and_null_stays(self):
        files, nraw = self.generate()
        self.assertEqual(nraw, 2)
        target = migrated_target()
        self.apply_all(target, files)
        got = dict(target.execute("SELECT eigyosho_id, raw_path FROM recalc_jobs").fetchall())
        self.assertEqual(got, {
            1: "uriage/2026-06/eigyosho-1.ndjson.gz",
            2: "uriage/2026-07/eigyosho-2.ndjson.gz",
            3: None,
        })
        # fingerprint_changed_at は 0001 の位置ではなく名前で入る。修復 UPDATE で computed_at の NULL ぶんが埋まる
        row = target.execute("SELECT fingerprint_changed_at, computed_at FROM recalc_jobs WHERE eigyosho_id = 2").fetchone()
        self.assertEqual(row, ("2026-07-02T00:01:00Z", "2026-07-02T00:01:00Z"))

    def test_statements_are_one_per_line_and_use_explicit_columns(self):
        files, _ = self.generate()
        for name in files:
            with open(os.path.join(self.out, name), encoding="utf-8") as f:
                for line in f.read().splitlines():
                    self.assertRegex(line, r"^(INSERT INTO \w+ \([a-z_0-9, ]+\) VALUES \(|DELETE FROM |UPDATE recalc_jobs )")
                    self.assertTrue(line.endswith(";"), line)

    def test_file_split_and_order(self):
        files, _ = self.generate(chunk=4)
        self.assertEqual(files[0], "000-delete.sql")
        self.assertTrue(files[-1].endswith("-repair.sql"))
        self.assertEqual(files, sorted(files))
        # 15 行 / 4 = 4 ファイル (daily)、同 4 (partner)、recalc_jobs 1、verify 4 → delete + 13 + repair
        self.assertEqual(len(files), 1 + 4 + 4 + 1 + 4 + 1)
        with open(os.path.join(self.out, files[0])) as f:
            self.assertEqual(f.read().count("DELETE FROM"), 4)
        target = migrated_target()
        self.apply_all(target, files)
        self.assertEqual(table_rows(target, "uriage_person_daily"), table_rows(sqlite3.connect(self.src), "uriage_person_daily"))

    def test_rerun_is_idempotent(self):
        files, _ = self.generate()
        target = migrated_target()
        self.apply_all(target, files)
        self.apply_all(target, files)
        self.assertEqual(target.execute("SELECT COUNT(*) FROM uriage_person_daily").fetchone()[0], 15)

    def test_apply_sh_lists_d1_and_r2_commands_and_never_applies(self):
        files, _ = self.generate()
        with open(os.path.join(self.out, "apply.sh"), encoding="utf-8") as f:
            lines = [ln for ln in f.read().splitlines() if ln.startswith("npx ")]
        d1 = [ln for ln in lines if " d1 execute " in ln]
        r2 = [ln for ln in lines if " r2 object put " in ln]
        self.assertEqual([ln.rsplit(" ", 1)[1] for ln in d1], files)
        self.assertTrue(all(ln.startswith("npx wrangler@4.144.0 d1 execute ichibanboshi-uriage --remote --file ") for ln in d1))
        self.assertEqual(len(r2), 2)
        self.assertIn(
            "npx wrangler@4.144.0 r2 object put ichibanboshi-uriage/uriage/2026-06/eigyosho-1.ndjson.gz "
            "--file raw/2026-06/eigyosho-1.ndjson.gz --remote", r2)
        self.assertEqual(os.stat(os.path.join(self.out, "apply.sh")).st_mode & 0o111, 0)

    def test_expected_matches_target_after_apply(self):
        files, _ = self.generate()
        target = migrated_target()
        self.apply_all(target, files)
        with open(os.path.join(self.out, "verify.sql"), encoding="utf-8") as f:
            stmts = [ln for ln in f.read().splitlines() if ln]
        got = []
        for s in stmts:
            for k, v in target.execute(s):
                got.append("%s\t%s" % (k, "NULL" if v is None else v))
        with open(os.path.join(self.out, "expected.txt"), encoding="utf-8") as f:
            self.assertEqual(f.read().splitlines(), got)
        self.assertIn("count.uriage_person_daily\t15", got)
        self.assertIn("recalc_jobs.status.r2_synced\t1", got)
        # 修復 UPDATE で computed → r2_synced になるのは r2_synced_at のある行だけ。ここでは 0
        self.assertIn("recalc_jobs.status.computed\t1", got)

    def test_raw_path_outside_raw_dir_fails(self):
        c = sqlite3.connect(self.src)
        c.execute("UPDATE recalc_jobs SET raw_path = '/elsewhere/2026-06/eigyosho-1.ndjson.gz' WHERE eigyosho_id = 1")
        c.commit()
        c.close()
        with self.assertRaisesRegex(ex.ExportError, "raw_dir"):
            self.generate()

    def test_raw_path_not_matching_month_and_id_fails(self):
        c = sqlite3.connect(self.src)
        c.execute("UPDATE recalc_jobs SET raw_path = ? WHERE eigyosho_id = 1", (raw_path("2026-05", 1),))
        c.commit()
        c.close()
        with self.assertRaisesRegex(ex.ExportError, "key"):
            self.generate()

    def test_missing_raw_file_fails(self):
        os.remove(os.path.join(self.raw, "2026-07", "eigyosho-2.ndjson.gz"))
        with self.assertRaisesRegex(ex.ExportError, "手元に無い"):
            self.generate()

    def test_missing_column_fails(self):
        c = sqlite3.connect(self.src)
        c.execute("ALTER TABLE verify_jobs RENAME COLUMN diff_json TO diff")
        c.commit()
        c.close()
        with self.assertRaisesRegex(ex.ExportError, "diff_json"):
            self.generate()

    def test_backup_copies_read_only(self):
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            ex.cmd_backup(self.src)
        dst = buf.getvalue().strip()
        self.addCleanup(os.unlink, dst)
        self.assertTrue(re.match(r"^/[A-Za-z0-9_./-]+$", dst), dst)
        self.assertEqual(table_rows(sqlite3.connect(dst), "uriage_person_daily"),
                         table_rows(sqlite3.connect(self.src), "uriage_person_daily"))

    def test_verify_runs_on_empty_migrated_db(self):
        # 空の D1 に流しても失敗しない (NULL が返る)
        t = migrated_target()
        lines = ex.run_verify(t)
        self.assertIn("count.recalc_jobs\t0", lines)


if __name__ == "__main__":
    unittest.main()
