//! The schema: migrations that apply twice, no tenant, and no engine named
//! by a constraint.

mod common;

#[tokio::test]
async fn the_migrations_apply_twice() {
    let db = common::database().await;
    orchestrator_postgres::migrate(&db.pool).await.unwrap();
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM settings")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1, "one row of settings, with the defaults");
}

#[tokio::test]
async fn no_table_has_a_tenant() {
    let db = common::database().await;
    let tenants: Vec<String> = sqlx::query_scalar(
        "SELECT table_name || '.' || column_name FROM information_schema.columns
          WHERE table_schema = 'public' AND column_name ILIKE '%tenant%'",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert!(tenants.is_empty(), "{tenants:?}");
}

#[tokio::test]
async fn no_check_names_an_engine() {
    let db = common::database().await;
    // Every CHECK on a column that holds an engine's name: none may list
    // values, only check the name's form.
    let checks: Vec<(String, String)> = sqlx::query_as(
        "SELECT c.conrelid::regclass::text, pg_get_constraintdef(c.oid)
           FROM pg_constraint c
          WHERE c.contype = 'c' AND c.connamespace = 'public'::regnamespace",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert!(!checks.is_empty());
    for (table, def) in &checks {
        let on_engine = def.contains("engine") || def.contains("line");
        let lists = def.contains("ANY (ARRAY[") || def.contains(" IN (");
        assert!(!(on_engine && lists), "{table}: {def}");
    }
    // Any well-formed name is taken.
    sqlx::query(
        "INSERT INTO engines (engine, line, engine_version, port, kinds)
         VALUES ('an-engine-nobody-knows', '-', '1', 1, '[]')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let bad = sqlx::query(
        "INSERT INTO engines (engine, line, engine_version, port, kinds)
         VALUES ('Not An Engine', '-', '1', 1, '[]')",
    )
    .execute(&db.pool)
    .await;
    assert!(bad.is_err(), "the name's form is checked");
}
