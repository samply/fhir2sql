mod db_utils;
mod graceful_shutdown;
mod models;

use db_utils::*;
use models::*;

use tokio::select;
use tokio::time::{interval, Duration};
use sqlx::{PgPool, Row};
use std::collections::BTreeMap;
use anyhow::bail;
use anyhow::anyhow;
use reqwest::Client;
use serde_json;
use std::env;
use tracing::{error, info, warn};
use tracing_subscriber;
use dotenv::dotenv;
use chrono::{NaiveTime, Timelike, Utc};

pub struct Config {
    blaze_base_url: String,
    pg_host: String,
    pg_username: String,
    pg_password: String,
    pg_dbname: String,
    pg_port: u16,
    pg_batch_size: u32,
    blaze_page_resource_count: u32,
    blaze_num_connection_attempts: u32,
    target_time: NaiveTime,
}

// read id and version_id from a resource if possible
pub fn get_version(resource: serde_json::Value) -> Option<ResourceVersion> {
    let resource_id = resource.get("id").and_then(|v| v.as_str()).map(|s| s.to_string());
    let version_id = resource.get("meta").and_then(|v| v.get("versionId")).and_then(|v| v.as_str()).map(|s| s.parse::<i64>().ok()).flatten();
    
    if let (Some(resource_id), Some(version_id)) = (resource_id, version_id) {
        Some(ResourceVersion { version_id, resource_id })
    } else {
        None
    }
}

// get BTreeMap of all <resource_id, PgVersion> pairs in pg for the given resource type
pub async fn get_pg_resource_versions(table_name: &str, pg_con_pool: &PgPool) -> Result<BTreeMap<String,PgVersion>, anyhow::Error> {
    let query = format!("SELECT id pk_id, resource::text FROM {}", table_name);
    let rows: Vec<(i32, String)> = sqlx::query_as(&query)
    .fetch_all(pg_con_pool)
    .await?;

    let mut pg_versions = BTreeMap::new();

    for row in rows {
        let pk_id: i32 = row.0;        
        let resource_str: String = row.1;        
        let resource: serde_json::Value = match serde_json::from_str(&resource_str) {
            Ok(resource) => resource,
            Err(_) => continue
        };        

        let res_version = get_version(resource);
        match res_version {
            Some(res_version) => {
                // only if resource_id is found something is inserted into resource_versions                
                pg_versions.insert(res_version.resource_id, PgVersion {pk_id, version_id: res_version.version_id});            
            }        
            None => continue            
        }
    }
    Ok(pg_versions)
}    

//do a batch update of pg rows
pub async fn update_helper(pg_con_pool: &PgPool, items: &[PgUpdateItem], table_name: &str) -> Result<(), sqlx::Error> {

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("({}, $${}$$)", item.id, item.resource))
        .collect();

    let query = format!(
        "UPDATE {} SET resource = data.resource::jsonb FROM (VALUES {}) AS data(id, resource) WHERE data.id = {}.id",
        table_name,
        values.join(","),
        table_name
    );

    sqlx::query(&query)
    .execute(pg_con_pool)
    .await?;

    Ok(())
}

//do a batch insert of pg rows
pub async fn insert_helper(pg_con_pool: &PgPool, items: &[PgInsertItem], table_name: &str) -> Result<(), anyhow::Error> {

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("($${}$$)", item.resource))
        .collect();

    let query = format!(
        "INSERT INTO {} (resource) values {};",
        table_name,
        values.join(",")        
    );

    sqlx::query(&query)
    .execute(pg_con_pool)
    .await?;
    Ok(())
}

//do a batch delete of pg rows
pub async fn delete_helper(pg_con_pool: &PgPool, items: &[i32], table_name: &str) -> Result<(), anyhow::Error> {

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("{}", item))
        .collect();

    let query = format!(
        "DELETE FROM {} where id in ({});",
        table_name,
        values.join(",")        
    );

    sqlx::query(&query)
    .execute(pg_con_pool)
    .await?;

    Ok(())
}

// get the number of rows of a given pg table
pub async fn get_num_rows(pg_con_pool: &PgPool, table_name: &str) -> Result<i64,anyhow::Error> {
    let query = format!("SELECT COUNT(*) FROM {}", table_name);
    let row = sqlx::query(query.as_str())
        .fetch_one(pg_con_pool)
        .await?;
    let count: i64 = row.try_get(0)?;
    Ok(count)
}

pub async fn get_blaze_search_set(url: &str, client: &Client) -> Result<SearchSet, anyhow::Error> {    
    let res = client.get(url).send().await;
    let res = res.inspect_err(|e| error!("Failed to get response from Blaze: {}", e))?;
    let res_text = res.text().await.inspect_err(|e| error!("Failed to get payload: {}", e))?;
    let search_set: SearchSet = serde_json::from_str(&res_text)
        .inspect_err(|e| error!("Could not deserialize JSON to SearchSet: {}", e))?;
    Ok(search_set)
}

/// Synchronizes resources from Blaze to PostgreSQL
///
/// pg_versions holds a BTreeMap that is used to look up resource version_ids currently in pg
/// resource-ids that are found in Blaze but not in pg: will be inserted
/// resource-ids that are found in Blaze with a different version_id than in pg: will be updated
/// resource-ids that are found in pg but not in blaze : will be deleted from pg
/// # Parameters
///
/// * `blaze_base_url`: The base URL of the Blaze API.
/// * `pg_con_pool`: A PostgreSQL connection pool.
/// * `type_arg`: The type of resource to synchronize (e.g. "specimen", "patient", etc.).
/// * `batch_size`: The number of resources to process in each batch.
/// * `page_resource_count`: The number of resources to fetch from Blaze in each page.
///
/// # Returns
///
/// A `Result` indicating whether the synchronization was successful. If an error occurs, it will be returned as an `anyhow::Error`.
async fn sync_blaze_2_pg(
    blaze_base_url: &str, 
    pg_con_pool: &PgPool, 
    type_arg: &str, 
    batch_size: u32, 
    page_resource_count: u32
) -> Result<(), anyhow::Error>{

    // set pg table name to insert into or update
    let table_name = type_arg.to_lowercase();
    let client = Client::new();

    // vectors holding batches of items to insert/update
    let mut update_batch: Vec<PgUpdateItem> = Vec::new();
    let mut insert_batch: Vec<PgInsertItem> = Vec::new();

    let mut pg_versions = get_pg_resource_versions(&table_name, pg_con_pool).await?;

    let mut url: String = format!("{}/fhir/{}?_count={}&_history=current",
        blaze_base_url, type_arg, page_resource_count);
    let mut update_counter: u32 = 0;    
    let mut insert_counter: u32 = 0;
    info!("Attempting to sync: {}", type_arg);    
    loop {
        let search_set = get_blaze_search_set(&url, &client).await?;
        let entries = match search_set.entry {
            Some(entries) => entries,
            None => {
                warn!("Could not read entries from search set.");
                break;
            }
        };                        
        for e in entries {                            

            let blaze_version = get_version(e.resource.clone());
            let blaze_version = match blaze_version {
                Some(v) => v,
                None => {                                    
                    warn!("Could not read resource version from Blaze search set entry.");
                    continue
                }
            };

            let resource_str = serde_json::to_string(&e.resource).unwrap_or("{}".to_string());

            match pg_versions.get(&blaze_version.resource_id) {
                Some(pg_version) => { //Resource is already in pg
                    if pg_version.version_id < blaze_version.version_id ||
                        pg_version.version_id > blaze_version.version_id
                    {
                        //Resource exists in pg but is outdated. 
                        //Add resource for batch update into pg
                        update_counter += 1;                                        
                        update_batch.push(PgUpdateItem {id: pg_version.pk_id, resource: resource_str.clone()});
                        //Remove all entries from pg_versions that can be found in Blaze 
                        //=> remaining ones need to be deleted from pg
                        pg_versions.remove(&blaze_version.resource_id);  
                    } else { //Blaze and pg versions match
                        pg_versions.remove(&blaze_version.resource_id);
                    }                                    
                },
                None => { // current resource not (yet) in pg
                    //Add resource for batch insert into pg
                        insert_counter += 1;                                    
                        insert_batch.push(PgInsertItem {resource: resource_str.clone()});                                    
                }
            }

            if update_counter > 0 && update_counter % batch_size == 0 {
                update_helper(pg_con_pool, &update_batch, &table_name).await?;
                update_batch.clear();

            } else if insert_counter > 0 && insert_counter % batch_size == 0 {
                insert_helper(pg_con_pool, &insert_batch, &table_name).await?;
                insert_batch.clear();
            }

        }
        // extract link to next page if exists or break
        let next_url = search_set.link.iter()
            .find(|link| link.relation == "next")
            .map(|link| link.url.clone());

        if let Some(next_url) = next_url {
            url = next_url;
        } else {
            break;
        }
    }
    //insert or update the last remaining resources
    if update_batch.len() > 0 {
        update_counter += update_batch.len() as u32;
        update_helper(pg_con_pool, &update_batch, &table_name).await?;
    }
    if insert_batch.len() > 0 {
        insert_counter += insert_batch.len() as u32;
        insert_helper(pg_con_pool, &insert_batch, &table_name).await?;       
    }
    //remove rows from pg that were not encountered in blaze
    let delete_ids: Vec<i32> = pg_versions.values().map(|value| value.pk_id).collect();
    if delete_ids.len() > 0 {        
        delete_helper(pg_con_pool, &delete_ids, &table_name).await?;
    }

    info!("Updated {} rows", update_counter);
    info!("Inserted {} rows", insert_counter);
    info!("Deleted {} rows", delete_ids.len() as u32);

    //compare total entry counts between blaze and pg
    let row_count = get_num_rows(pg_con_pool, &table_name).await?;
    let resource_count = get_blaze_search_set(&format!("{}/fhir/{}?_count=0", blaze_base_url, type_arg), &client)
        .await
        .map(|search_set| search_set.total as i64)?;
    if row_count != resource_count {
        warn!("{} entry counts do not match between Blaze and PostgreSQL", type_arg);
    } else {
        info!("{} entry counts match between Blaze and PostgreSQL \u{2705}", type_arg);
    }

    Ok(())
}


pub async fn run_sync(pg_con_pool: &PgPool, config: &Config) -> Result<(), anyhow::Error> {
    let type_args: Vec<&str> = vec!["Specimen", "Patient", "Observation", "Condition"];
    let table_names: Vec<&str> = vec!["specimen", "patient", "observation", "condition"];
    
    //check preconditions for sync    
    let blaze_available = check_blaze_connection(&config.blaze_base_url,
         config.blaze_num_connection_attempts).await?;

    if !blaze_available {
        bail!("Aborting sync run because connection to Blaze could not be established");
    }
         
    let all_tables_exist = pred_tables_exist(pg_con_pool, &table_names).await?;

    if blaze_available && all_tables_exist {
        info!("All tables found as expected");
        for type_arg in type_args {
            sync_blaze_2_pg(
                &config.blaze_base_url,
                pg_con_pool, 
                type_arg,
                config.pg_batch_size, 
                config.blaze_page_resource_count).await?;
        }
    } else if blaze_available && !all_tables_exist {
        create_tables(pg_con_pool).await?;
        for type_arg in type_args {
            sync_blaze_2_pg(
                &config.blaze_base_url,
                pg_con_pool,
                type_arg,
                config.pg_batch_size,
                config.blaze_page_resource_count).await?;
        }
    }
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{

    tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();

    dotenv().ok();

    //@todo: make use of clap crate? 
    let config = Config {
        blaze_base_url: env::var("BLAZE_BASE_URL").expect("BLAZE_BASE_URL must be set"),
        pg_host: env::var("PG_HOST").expect("PG_HOST must be set"),
        pg_username: env::var("PG_USERNAME").expect("PG_USERNAME must be set"),
        pg_password: env::var("PG_PASSWORD").expect("PG_PASSWORD must be set"),
        pg_dbname: env::var("PG_DBNAME").expect("PG_DBNAME must be set"),
        pg_port: 5432,
        pg_batch_size: 10000,
        blaze_page_resource_count: 5000,
        blaze_num_connection_attempts: 20,
        target_time: NaiveTime::from_hms_opt(3, 0, 0)
            .ok_or_else(|| anyhow!("Invalid target time"))?
    };

    info!("fhir2sql started"); //@todo: replace with proper banner
    
    let pg_url = format!("postgresql://{}:{}@{}:{}/{}", 
        config.pg_username, 
        config.pg_password, 
        config.pg_host, 
        config.pg_port, 
        config.pg_dbname);
    let pg_con_pool = get_pg_connection_pool(&pg_url, 10).await?;
    
    info!("Running initial sync");
    match run_sync(&pg_con_pool, &config).await {
        Ok(()) => {
            info!("Sync run successfull");
        },
        Err(err) => {
            error!("Sync run unsuccessfull: {}", err);
        }
    }

    // main loop
    info!("Entering regular sync schedule");
    let mut interval = interval(Duration::from_secs(60)); // execute every 1 minute
    
    loop {
        select! {
            _ = interval.tick() => {                  
                let now = Utc::now().naive_local().time();                                
                if now.hour() == config.target_time.hour() && now.minute() == config.target_time.minute() {
                    info!("Syncing at target time");
                    match run_sync(&pg_con_pool, &config).await {
                        Ok(()) => {
                            info!("Sync run successfull");
                        },
                        Err(err) => {
                            error!("Sync run unsuccessfull: {}", err);
                        }
                    }
                }
            } _ = graceful_shutdown::wait_for_signal() => {                
                break;                
            }
        }
    }

    Ok(())    
}
