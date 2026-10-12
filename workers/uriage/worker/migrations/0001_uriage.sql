-- 担当者別売上 (/api/uriage/*) の D1 (binding URIAGE_DB、database ichibanboshi-uriage) の表の定義の正本 (Refs ohishi-exp/rust-ichibanboshi#322)。
-- オンプレ版 (root の src/sqlite.rs の migrate) の 4 表・index・VIEW 3 本と同じ定義。オンプレ版が ALTER で後から足した列
-- (uriage_person_daily の y0 列・recalc_jobs の fingerprint_changed_at) は表の中に並べて書く。オンプレ版の「毎起動の
-- VIEW の DROP/CREATE」・列の後付け・修復の UPDATE は持たない。
-- 適用済みの migration は変えない (表を変えるなら 0002_*.sql を足す)。uriage-logic の d1.rs が include_str! で読む。

-- 担当者 × 日 × 営業所 × cal の集計。*_y0 は横横=0 (自社運行 or sql_from_other) だけの集計値
CREATE TABLE IF NOT EXISTS uriage_person_daily (
    unko_date         TEXT    NOT NULL,
    month             TEXT    NOT NULL,
    person_name       TEXT    NOT NULL,
    eigyosho_id       INTEGER NOT NULL,
    cal               INTEGER NOT NULL,
    kingaku           INTEGER NOT NULL,
    yosha_kingaku     INTEGER NOT NULL,
    kensuu            INTEGER NOT NULL,
    kingaku_y0        INTEGER NOT NULL DEFAULT 0,
    yosha_kingaku_y0  INTEGER NOT NULL DEFAULT 0,
    kensuu_y0         INTEGER NOT NULL DEFAULT 0,
    calculated_at     TEXT    NOT NULL,
    PRIMARY KEY (unko_date, person_name, eigyosho_id, cal)
);
CREATE INDEX IF NOT EXISTS idx_upd_month_eigyosho
    ON uriage_person_daily (month, eigyosho_id, cal);
CREATE INDEX IF NOT EXISTS idx_upd_person_date
    ON uriage_person_daily (person_name, unko_date);

-- 担当者 × 得意先/傭車先の内訳 (uriage_person_daily の粒度に partner_kind (customer/subcontractor) と partner_code を足したもの)
CREATE TABLE IF NOT EXISTS uriage_person_partner_daily (
    unko_date         TEXT    NOT NULL,
    month             TEXT    NOT NULL,
    person_name       TEXT    NOT NULL,
    eigyosho_id       INTEGER NOT NULL,
    cal               INTEGER NOT NULL,
    partner_kind      TEXT    NOT NULL,
    partner_code      TEXT    NOT NULL,
    partner_name      TEXT    NOT NULL,
    kingaku           INTEGER NOT NULL,
    kensuu            INTEGER NOT NULL,
    kingaku_y0        INTEGER NOT NULL DEFAULT 0,
    kensuu_y0         INTEGER NOT NULL DEFAULT 0,
    calculated_at     TEXT    NOT NULL,
    PRIMARY KEY (unko_date, person_name, eigyosho_id, cal, partner_kind, partner_code)
);
CREATE INDEX IF NOT EXISTS idx_uppd_month_eigyosho
    ON uriage_person_partner_daily (month, eigyosho_id, cal);
CREATE INDEX IF NOT EXISTS idx_uppd_person_kind
    ON uriage_person_partner_daily (person_name, month, partner_kind);

-- (month, eigyosho_id) ごとの recalc の状態・fingerprint・R2 への同期の状態
CREATE TABLE IF NOT EXISTS recalc_jobs (
    month                   TEXT    NOT NULL,
    eigyosho_id             INTEGER NOT NULL,
    status                  TEXT    NOT NULL,
    fingerprint_before      TEXT,
    fingerprint_after       TEXT,
    raw_path                TEXT,
    created_at              TEXT    NOT NULL,
    computed_at             TEXT,
    fingerprint_changed_at  TEXT,
    r2_synced_at            TEXT,
    last_error              TEXT,
    PRIMARY KEY (month, eigyosho_id)
);
CREATE INDEX IF NOT EXISTS idx_rj_status
    ON recalc_jobs (status);

-- PHP と Rust の突き合わせ (verify) の履歴
CREATE TABLE IF NOT EXISTS verify_jobs (
    unko_date         TEXT    NOT NULL,
    eigyosho_id       INTEGER NOT NULL,
    cal               INTEGER NOT NULL,
    month             TEXT    NOT NULL,
    ok                INTEGER NOT NULL,
    skipped_reason    TEXT,
    diff_json         TEXT,
    row_count         INTEGER NOT NULL,
    elapsed_ms        INTEGER NOT NULL,
    ran_at            TEXT    NOT NULL,
    PRIMARY KEY (unko_date, eigyosho_id, cal)
);
CREATE INDEX IF NOT EXISTS idx_vj_month_eigyosho
    ON verify_jobs (month, eigyosho_id);
CREATE INDEX IF NOT EXISTS idx_vj_ran_at
    ON verify_jobs (ran_at DESC);

-- 月次の集計は日次の SUM の VIEW (二重に持たない)
CREATE VIEW IF NOT EXISTS uriage_person_monthly AS
SELECT month,
       person_name,
       eigyosho_id,
       cal,
       SUM(kingaku)           AS kingaku,
       SUM(yosha_kingaku)     AS yosha_kingaku,
       SUM(kensuu)            AS kensuu,
       SUM(kingaku_y0)        AS kingaku_y0,
       SUM(yosha_kingaku_y0)  AS yosha_kingaku_y0,
       SUM(kensuu_y0)         AS kensuu_y0,
       MAX(calculated_at)     AS calculated_at
FROM uriage_person_daily
GROUP BY month, person_name, eigyosho_id, cal;

-- (month, eigyosho_id) ごとの verify の集計
CREATE VIEW IF NOT EXISTS verify_coverage AS
SELECT month,
       eigyosho_id,
       COUNT(*)                                AS verified_count,
       SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS verified_ok,
       SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS verified_ng
FROM verify_jobs
GROUP BY month, eigyosho_id;

-- R2 に置けていない bucket (status='computed' AND r2_synced_at IS NULL AND raw_path IS NOT NULL) と verify の集計
CREATE VIEW IF NOT EXISTS r2_pending AS
SELECT rj.month,
       rj.eigyosho_id,
       rj.raw_path,
       rj.fingerprint_after,
       rj.computed_at,
       COALESCE(vc.verified_count, 0) AS verified_count,
       COALESCE(vc.verified_ok,    0) AS verified_ok,
       COALESCE(vc.verified_ng,    0) AS verified_ng
FROM recalc_jobs rj
LEFT JOIN verify_coverage vc
  ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id
WHERE rj.status = 'computed'
  AND rj.r2_synced_at IS NULL
  AND rj.raw_path IS NOT NULL
ORDER BY rj.computed_at ASC;
