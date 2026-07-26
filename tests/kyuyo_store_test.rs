//! KyuyoStore (SQLite derived store、Refs #106) の store 単体テスト。
//! read-through / sync の経路テストは kyuyo_routes_test.rs 側。ここでは
//! 永続化・順序保存・schema 版不一致の drop 再作成を実ファイルで確認する。

use rust_ichibanboshi::kyuyo::store::{KyuyoStore, KyuyoStoreApi};

fn sample_rows() -> Vec<rust_ichibanboshi::kyuyo::logic::PayrollRow> {
    // serde 経由で最小の PayrollRow を作る (フィールド網羅は routes テストの
    // fixture が担う — ここでは順序と復元だけ見る)
    let json = |code: &str| {
        format!(
            r#"{{"employee_code":"{code}","employee_code_key":"{code}","employee_name":"n",
                "department":"d","taikei":1,"month_index":5,"pay_date":"2026-06-15",
                "period_start":"2026-05-01","period_end":"2026-05-31","retired":false,
                "payments":{{}},"deductions":{{}},"base_rate":null,"overtime_rate":null,
                "attendance":{{}},"totals":null}}"#
        )
    };
    // わざと数値順でない順序 — 保存順がそのまま返ることを見る
    ["0002", "0001", "0003"]
        .iter()
        .map(|c| serde_json::from_str(&json(c)).expect("row"))
        .collect()
}

#[tokio::test]
async fn persists_across_reopen_and_preserves_order() {
    let dir = std::env::temp_dir().join(format!("kyuyo-store-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // 親ディレクトリが無くても open が作る (crash-loop 対策の確認)
    let path = dir.join("nested").join("kyuyo_local.sqlite");
    let path_str = path.to_str().unwrap().to_string();

    let rows = sample_rows();
    {
        let store = KyuyoStore::open(&path_str).expect("open");
        store
            .put_payroll(
                "0100",
                "2026-06",
                &rows,
                &["w1".to_string()],
                "2026-07-26T00:00:00Z",
            )
            .await
            .expect("put");
    }
    // 別インスタンスで開き直しても読める (= プロセス再起動相当)
    let store = KyuyoStore::open(&path_str).expect("reopen");
    let cached = store
        .get_payroll("0100", "2026-06")
        .await
        .expect("get")
        .expect("hit");
    let codes: Vec<&str> = cached
        .rows
        .iter()
        .map(|r| r.employee_code.as_str())
        .collect();
    assert_eq!(codes, vec!["0002", "0001", "0003"]);
    assert_eq!(cached.warnings, vec!["w1".to_string()]);
    assert_eq!(cached.synced_at, "2026-07-26T00:00:00Z");
    // 別 scope は miss
    assert!(store
        .get_payroll("0100", "2026-05")
        .await
        .expect("get")
        .is_none());
    assert!(store
        .get_payroll("0200", "2026-06")
        .await
        .expect("get")
        .is_none());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn schema_version_mismatch_drops_and_recreates() {
    let dir = std::env::temp_dir().join(format!("kyuyo-store-ver-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("kyuyo_local.sqlite");
    let path_str = path.to_str().unwrap().to_string();

    {
        let store = KyuyoStore::open(&path_str).expect("open");
        store
            .put_payroll(
                "0100",
                "2026-06",
                &sample_rows(),
                &[],
                "2026-07-26T00:00:00Z",
            )
            .await
            .expect("put");
    }
    // 旧版のファイルを装う (user_version を巻き戻す)
    {
        let conn = rusqlite::Connection::open(&path_str).unwrap();
        conn.execute_batch("PRAGMA user_version = 0;").unwrap();
    }
    // 再 open で drop → 再作成 = キャッシュは空に戻る (derived store の意味論)
    let store = KyuyoStore::open(&path_str).expect("reopen");
    assert!(store
        .get_payroll("0100", "2026-06")
        .await
        .expect("get")
        .is_none());

    let _ = std::fs::remove_dir_all(&dir);
}
