use sqlx::{PgPool, Row};
use tracing::{error, info};


pub async fn pred_tables_exist(pg_con_pool: &PgPool, table_names: &Vec<&str>) -> Result<bool, anyhow::Error> {
    info!("Checking whether PostgreSQL tables exist");    
    
    let table_query: &str = r#"select table_name from information_schema.tables;"#;

    let rows = sqlx::query(table_query)
        .fetch_all(pg_con_pool)
        .await
        .map_err(|err| {
            error!("Failed to execute query: {}", err);
            anyhow::Error::new(err)
        })?;

    let pg_table_names: Vec<String> = rows.into_iter().map(|row| row.get(0)).collect();
    let all_tables_exist = table_names.iter().all(|table_name| pg_table_names.contains(&table_name.to_string()));


    Ok(all_tables_exist)
}


pub async fn create_tables(pg_con_pool: &PgPool) -> Result<(), anyhow::Error> {
    info!("Creating PostgreSQL tables");

    let create_tables_queries = vec![
        "CREATE TABLE IF NOT EXISTS patients (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resource JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS specimen (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resource JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS conditions (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resource JSONB NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS observations (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            resource JSONB NOT NULL
        )",
        "CREATE OR REPLACE FUNCTION update_last_updated()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.last_updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;",
        "CREATE TRIGGER update_last_updated_trigger
            BEFORE UPDATE ON patients
            FOR EACH ROW
            EXECUTE PROCEDURE update_last_updated();",
        "CREATE TRIGGER update_last_updated_trigger
            BEFORE UPDATE ON specimen
            FOR EACH ROW
            EXECUTE PROCEDURE update_last_updated();",
        "CREATE TRIGGER update_last_updated_trigger
            BEFORE UPDATE ON conditions
            FOR EACH ROW
            EXECUTE PROCEDURE update_last_updated();",
        "CREATE TRIGGER update_last_updated_trigger
            BEFORE UPDATE ON observations
            FOR EACH ROW
            EXECUTE PROCEDURE update_last_updated();",
    ];

    for query in create_tables_queries {
        sqlx::query(query)
            .execute(pg_con_pool)
            .await?;
    }

    Ok(())
}
