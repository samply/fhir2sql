use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{error, info};
use anyhow::anyhow;
use reqwest::Client;

pub async fn get_pg_connection_pool(pg_url: &str, num_attempts: u32) -> Result<PgPool, anyhow::Error> {
    info!("Trying to establish a PostgreSQL connection pool");
    
    let mut attempts = 0;
    let mut err: Option<anyhow::Error> = None;
    
    while attempts < num_attempts {
        info!("Attempt to connect to PostgreSQL {} of {}", attempts + 1, num_attempts);
        match PgPoolOptions::new()
            .max_connections(10)
            .connect(&pg_url)
            .await
        {
            Ok(pg_con_pool) => {
                info!("PostgreSQL connection successfull \u{2705}");
                return Ok(pg_con_pool)
            },
            Err(e) => {
                error!("Failed to connect to PostgreSQL. Attempt {} of {}: {}", attempts + 1, num_attempts, e);
                err = Some(anyhow!(e));
            }
        }
        attempts += 1;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await; //@todo: move param somewhere else?
    }
    Err(err.unwrap_or_else(|| anyhow!("Failed to connect to PostgreSQL")))
}

pub async fn check_blaze_connection(blaze_base_url: &str, num_attempts: u32) -> Result<bool, anyhow::Error> {
    info!("Attempting to connect to Blaze");
    
    let mut attempts = 0;
    let mut err: Option<anyhow::Error> = None;
    let client = Client::new();
    
    while attempts < num_attempts {
        info!("Attempt to connect to Blaze {} of {}", attempts + 1, num_attempts);
        match client.get(format!("{}/health", blaze_base_url)).send().await {
            Ok(_) => {
                info!("Blaze connection successfull \u{2705}");
                return Ok(true)
            },
            Err(e) => {
                error!("Failed to connect to Blaze. Attempt {} of {}: {}", attempts + 1, num_attempts, e);
                err = Some(anyhow!(e));
            }
        }
        attempts += 1;
        tokio::time::sleep(std::time::Duration::from_secs(5)).await; //@todo: move param somewhere else?
    }
    Err(err.unwrap_or_else(|| anyhow!("Failed to connect to PostgreSQL")))

}

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
