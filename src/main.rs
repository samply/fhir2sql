use std::collections::BTreeMap;
use anyhow::bail;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use dotenv::dotenv;
use tokio_postgres::{Client as PgClient, NoTls};
use tracing::{event, info, warn, error, Level};
use tracing_subscriber;


#[derive(Deserialize, Serialize)]
struct Entry {    
    resource: serde_json::Value, //interpret resource as Value    
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

// get BTreeMap of all <resource_id, version_id> pairs in pg for the given resource type
pub async fn get_pg_resource_versions(table_name: &str, pg_client: &PgClient) -> Result<BTreeMap<String,BTreeMapValue>, anyhow::Error> {
    let rows = pg_client.query(
        &format!("SELECT id pk_id, resource::text FROM {};", table_name), &[]).await?;

    let mut pg_versions = BTreeMap::new();

    for row in rows {
        let pk_id: i32 = row.get("pk_id");        
        let resource_str: String = row.get("resource");        
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

pub async fn update_helper(pg_client: &mut PgClient, items: &[PgUpdateItem], table_name: &str) -> Result<(), anyhow::Error> {
    let tx = pg_client.transaction().await?;

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("({}, $${}$$)", item.id, item.resource))
        .collect();

    //@todo implement this    
    let query = format!(
        "UPDATE {} SET resource = data.resource::jsonb FROM (VALUES {}) AS data(id, resource) WHERE data.id = {}.id",
        table_name,
        values.join(","),
        table_name
    );

    tx.execute(&query, &[]).await?;
    tx.commit().await?;

    Ok(())
}

pub async fn insert_helper(pg_client: &mut PgClient, items: &[PgInsertItem], table_name: &str) -> Result<(), anyhow::Error> {
    let tx = pg_client.transaction().await?;

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("($${}$$)", item.resource))
        .collect();

    let query = format!(
        "INSERT INTO {} (resource) values {};",
        table_name,
        values.join(",")        
    );
    tx.execute(&query, &[]).await?;
    tx.commit().await?;

    Ok(())
}

pub async fn delete_helper(pg_client: &mut PgClient, items: &[i32], table_name: &str) -> Result<(), anyhow::Error> {
    let tx = pg_client.transaction().await?;

    let values: Vec<_> = items
        .into_iter()
        .map(|item| format!("{}", item))
        .collect();

    let query = format!(
        "DELETE FROM {} where id in ({});",
        table_name,
        values.join(",")        
    );
    tx.execute(&query, &[]).await?;
    tx.commit().await?;

    Ok(())
}

async fn sync(
        base_url: &str, 
        pg_client: &mut PgClient, 
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

    let mut pg_versions = get_pg_resource_versions(table_name, &pg_client).await?;
    
    let client = Client::new();
    let mut url: String = format!("{}{}?_count={}", base_url, type_arg, page_resource_count);
    let mut update_counter: u32 = 0;    
    let mut insert_counter: u32 = 0;    
    loop {
        let res = client.get(&url).send().await;
        match res {            
            Ok(res) => {                
                let res_text = match res.text().await {
                    Ok(text) => text,
                    Err(err) => {
                        error!("failed to get payload: {}", err);
                        break;
                    }
                };      
                let search_set: Result<SearchSet, serde_json::Error> = serde_json::from_str(&res_text); 
                match search_set {
                    Ok(search_set) => {
                        let entries = match search_set.entry {
                            Some(entries) => entries,
                            None => break
                        };
                        event!(Level::INFO, "Processing: {}", url);                        
                        for e in entries {                            

                            let blaze_version = get_version(e.resource.clone());
                            let blaze_version = match blaze_version {
                                Some(v) => v,
                                None => {
                                    //@todo: log stuff (to pg?) and
                                    continue
                                }
                            };
                                                        
                            let resource_str = match serde_json::to_string(&e.resource) {
                                Ok(s) => s,
                                Err(e) => {
                                    //@todo: log error
                                    "{}".to_string()  // use empty JSON object 
                                }
                            };

                            match pg_versions.get(&blaze_version.resource_id) {
                                Some(pg_version) => { //resource is already in pg
                                    if pg_version.version_id < blaze_version.version_id {
                                        //Resource exists in pg but is outdated. 
                                        //Add e.resource for batch upsert into pg
                                        update_counter += 1;                                        
                                        update_batch.push(PgUpdateItem {id: pg_version.pk_id, resource: resource_str.clone()});
                                        //remove all entries from pg_versions that can be found in Blaze 
                                        //=> remaining ones need to be deleted from pg
                                        pg_versions.remove(&blaze_version.resource_id);  
                                    }                                    
                                },
                                None => { // current resource not (yet) in pg
                                    //@todo: e.resource for batch insert into pg
                                        insert_counter += 1;                                    
                                        insert_batch.push(PgInsertItem {resource: resource_str.clone()});                                    
                                }
                            }

                            if update_counter > 0 && update_counter % batch_size == 0 {
                                update_helper(pg_client, &update_batch, table_name).await?;
                                update_batch.clear();

                            } else if insert_counter > 0 && insert_counter % batch_size == 0 {
                                insert_helper(pg_client, &insert_batch, table_name).await?;
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
                    Err(error) => {
                        bail!("Error deserializing JSON: {}", error);  //@todo: log
                    }
                }
            },
            //no valid search response from Blaze
            Err(err) => {                 
                eprintln!("Failed to get response from Blaze: {}", err);  //@todo: log
                break;
            }
        }
    }
    //insert or update the last remaining resources
    if update_batch.len() > 0 {
        update_helper(pg_client, &update_batch, table_name).await?;
    }
    if insert_batch.len() > 0 {
        insert_helper(pg_client, &insert_batch, table_name).await?;       
    }
    //remove rows from pg that were not encountered in blaze
    let delete_ids: Vec<i32> = pg_versions.values().map(|value| value.pk_id).collect();
    if delete_ids.len() > 0 {
        delete_helper(pg_client, &delete_ids, table_name).await?;
    }
    
    Ok(())
}

//@todo: add check whether to number of blaze resources exactly matches the number of resources in pg after inserts

#[tokio::main]
async fn main() -> Result<(), anyhow::Error>{
    // Initialize tracing subscriber
    tracing_subscriber::fmt()
    .with_max_level(tracing::Level::INFO)
    .init();

    //let args: Vec<String> = env::args().collect();
    /*
    if args.len() != 3 || args[1] != "upsert" {
        eprintln!("Usage: blaze2pg upsert <type>. \n
        <type> must be one of observation, patient, specimen, or condition");
        return;
    }
    let type_arg = &args[2];
    // Call your upsert function with the type argument
    upsert(type_arg);
    */
    //@todo: add error handling and stuff
    let type_arg = "Specimen";

    dotenv().ok();

    let blaze_base_url: String = dotenv::var("BLAZE_BASE_URL").expect("BLAZE_BASE_URL must be set");
    let host = dotenv::var("PG_HOST").expect("PG_HOST must be set");
    let port = dotenv::var("PG_PORT").expect("PG_PORT must be set");
    let username = dotenv::var("PG_USERNAME").expect("PG_USERNAME must be set");
    let password = dotenv::var("PG_PASSWORD").expect("PG_PASSWORD must be set"); //@todo: move out of .env
    let dbname = dotenv::var("PG_DBNAME").expect("PG_DBNAME must be set");        
    let batch_size = match dotenv::var("PG_BATCH_SIZE") {
        Ok(val) => match val.parse::<u32>() {
            Ok(num) => num,
            Err(_) => {
                eprintln!("PG_BATCH_SIZE must be a positive number. Using default value.");
                10000 // default value if parsing fails
            }
        },
        Err(_) => {
            eprintln!("PG_BATCH_SIZE must be set. Using default value.");
            10000 // default value if env var is not set
        }
    };
    let page_resource_count = match dotenv::var("BLAZE_PAGE_RESOURCE_COUNT") {
        Ok(val) => match val.parse::<u32>() {
            Ok(num) => num,
            Err(_) => {
                eprintln!("BLAZE_PAGE_RESOURCE_COUNT not set. Using default value.");
                100 // default value if parsing failsf
            }
        },
        Err(_) => {
            eprintln!("BLAZE_PAGE_RESOURCE_COUNT not set. Using default value.");
            100 // default value if env var is not set
        }
    };

    let con_str = &format!("host={} port={} user={} password={} dbname={}", host, port, username, password, dbname);
    let (mut client, connection) =
    tokio_postgres::connect(&con_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            //eprintln!("connection error: {}", e);
            event!(Level::ERROR, "Could not connect to PostgreSQL");
        }
    });
    
    sync(&blaze_base_url, &mut client, type_arg, batch_size, page_resource_count).await?;
    Ok(())    
}
