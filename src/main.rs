mod db_utils;
use db_utils::*;
use sqlx::{postgres::PgPoolOptions, PgPool};

use std::collections::BTreeMap;
use anyhow::bail;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use std::env;
use tracing::{info, warn, error};
use tracing_subscriber;
use dotenv::dotenv;


#[derive(Deserialize, Serialize)]
struct Entry {    
    resource: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
struct Search {
    mode: String,
}

#[derive(Deserialize, Serialize)]
struct SearchSet {
    id: String,
    #[serde(rename = "type")]
    type_: String,
    entry: Option<Vec<Entry>>,
    link: Vec<Link>,
    total: u32,
    #[serde(rename = "resourceType")]
    resource_type: String,
}

#[derive(Deserialize, Serialize)]
struct Link {
    relation: String,
    url: String,
}

pub struct ResourceVersion {
    resource_id: String,
    version_id: i64
}

pub struct BTreeMapValue {
    pk_id: i32, //pg row primary key. Needed for upsert
    version_id: i64 //resource version_id
}

pub struct PgInsertItem {    
    resource: String
}

pub struct PgUpdateItem {
    id: i32,    
    resource: String
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

// get BTreeMap of all <resource_id, BTreeMapValue> pairs in pg for the given resource type
pub async fn get_pg_resource_versions(table_name: &str, pg_con_pool: &PgPool) -> Result<BTreeMap<String,BTreeMapValue>, anyhow::Error> {
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
                pg_versions.insert(res_version.resource_id, BTreeMapValue {pk_id, version_id: res_version.version_id});            
            }        
            None => continue            
        }
    }
    Ok(pg_versions)
}    

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


/// Synchronizes resources from Blaze to PostgreSQL
///
/// pg_versions holds a BTreeMap that is used to look up resource version_ids currently in pg
/// resource-ids that are found in Blaze but not in pg: will be inserted
/// resource-ids that are found in Blaze with a different version_id than in pg: will be updated
/// resource-ids that are found in pg but not in blaze : will be deleted from pg
/// # Parameters
///
/// * `base_url`: The base URL of the Blaze API.
/// * `pg_client`: A mutable reference to a PostgreSQL client.
/// * `type_arg`: The type of resource to synchronize (e.g. "specimen", "patient", etc.).
/// * `batch_size`: The number of resources to process in each batch.
/// * `page_resource_count`: The number of resources to fetch from Blaze in each page.
///
/// # Returns
///
/// A `Result` indicating whether the synchronization was successful. If an error occurs, it will be returned as an `anyhow::Error`.
async fn sync(
        base_url: &str, 
        pg_con_pool: &PgPool, 
        type_arg: &str, 
        batch_size: u32, 
        page_resource_count: u32
    ) -> Result<(), anyhow::Error>{
    
    // set pg table name to insert into or update
    let table_name = match type_arg.to_lowercase().as_str() {
        "specimen" => "specimen",
        "patient" => "patients",
        "condition" => "conditions",
        "observation" => "observations",
        _ => {
            bail!("Invalid type_arg!");
        }
    };

    // vectors holding batches of items to insert/update
    let mut update_batch: Vec<PgUpdateItem> = Vec::new();
    let mut insert_batch: Vec<PgInsertItem> = Vec::new();

    let mut pg_versions = get_pg_resource_versions(table_name, pg_con_pool).await?;
    
    let client = Client::new();
    let mut url: String = format!("{}{}?_count={}&_history=current", base_url, type_arg, page_resource_count);
    let mut update_counter: u32 = 0;    
    let mut insert_counter: u32 = 0;
    info!("Attempting to sync: {}", &type_arg);    
    loop {
        let res = client.get(&url).send().await;
        match res {            
            Ok(res) => {                
                let res_text = match res.text().await {
                    Ok(text) => text,
                    Err(err) => {
                        error!("Failed to get payload: {}", err);
                        break;
                    }
                };      
                let search_set: Result<SearchSet, serde_json::Error> = serde_json::from_str(&res_text); 
                match search_set {
                    Ok(search_set) => {
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
                                                        
                            let resource_str = match serde_json::to_string(&e.resource) {
                                Ok(s) => s,
                                Err(_) => {
                                    "{}".to_string() 
                                }
                            };

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
                                    }                                    
                                },
                                None => { // current resource not (yet) in pg
                                    //Add resource for batch insert into pg
                                        insert_counter += 1;                                    
                                        insert_batch.push(PgInsertItem {resource: resource_str.clone()});                                    
                                }
                            }

                            if update_counter > 0 && update_counter % batch_size == 0 {
                                update_helper(pg_con_pool, &update_batch, table_name).await?;
                                update_batch.clear();

                            } else if insert_counter > 0 && insert_counter % batch_size == 0 {
                                insert_helper(pg_con_pool, &insert_batch, table_name).await?;
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
                    },
                    Err(err) => {
                        error!("Could not deserialize JSON to SearchSet: {}", err);
                        break;
                    }
                }
            },
            //no valid search response from Blaze
            Err(err) => {                 
                error!("Failed to get response from Blaze: {}", err);
                break;
            }
        }
    }
    //insert or update the last remaining resources
    if update_batch.len() > 0 {
        update_helper(pg_con_pool, &update_batch, table_name).await?;
    }
    if insert_batch.len() > 0 {
        insert_helper(pg_con_pool, &insert_batch, table_name).await?;       
    }
    //remove rows from pg that were not encountered in blaze
    let delete_ids: Vec<i32> = pg_versions.values().map(|value| value.pk_id).collect();
    if delete_ids.len() > 0 {
        delete_helper(pg_con_pool, &delete_ids, table_name).await?;
    }    
    Ok(())
}


//@todo: add check whether to number of blaze resources exactly matches the number of resources in pg after inserts/updates/deletes
//@todo: add main loop that performs sync, sleeps and exits gracefully
//@todo: check blaze and pg connections and retry if connection can't be established
pub async fn main_loop(pg_con_pool: &PgPool, blaze_base_url: &str) -> Result<(), anyhow::Error>{
    let type_args: Vec<&str> = vec!["Specimen", "Patient", "Observation", "Condition"];

    let page_resource_count = 1000; //the number of resources to return per page by blaze
    let batch_size = 10000; //the number of resources to insert or update per batch in PostgreSQL
    let table_names: Vec<&str> = vec!["specimen", "patients", "observations", "conditions"];
    
    let all_tables_exist = pred_tables_exist(pg_con_pool, &table_names).await?;
    if all_tables_exist {
        info!("All tables found as expected");
        for type_arg in type_args {
            sync(&blaze_base_url, pg_con_pool, type_arg, batch_size, page_resource_count).await?;
        }
    } else {
        create_tables(pg_con_pool).await?;
        for type_arg in type_args {
            sync(&blaze_base_url, pg_con_pool, type_arg, batch_size, page_resource_count).await?;
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
    let blaze_base_url: String = env::var("BLAZE_BASE_URL").expect("BLAZE_BASE_URL must be set");
    let host_name = env::var("PG_HOST").expect("PG_HOST must be set");
    let user_name = env::var("PG_USERNAME").expect("PG_USERNAME must be set");
    let password = env::var("PG_PASSWORD").expect("PG_PASSWORD must be set");
    let db_name = env::var("PG_DBNAME").expect("PG_DBNAME must be set");        
    let port: u16 = 5432;

    info!("fhir2sql started"); //@todo: replace with proper banner
    
    let pg_url = format!("postgresql://{}:{}@{}:{}/{}", user_name, password, host_name, port, db_name);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let pg_con_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&pg_url)
        .await
        .map_err(|err| {
            error!("Failed to connect to PostgreSQL: {}", err);
            anyhow::Error::new(err)
        })?;


    main_loop(&pg_con_pool, &blaze_base_url).await?;
    Ok(())    
}
