pub mod schema;
pub mod models;
//use std::env;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json;
use diesel::prelude::*;
use diesel::PgConnection;
use diesel::{dsl::sql, sql_types::*};
use std::borrow::Cow;
use dotenv::dotenv;


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
    entry: Vec<Entry>,
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

// get BTreeMap of all <resource_id, version_id> pairs in pg for the given resource type
/*
async fn get_pg_resource_versions(type_arg: &str, pg_con: PgConnection){
    
    let table_name = match type_arg {
        "specimen" => {let results = ,
        "patient" => Cow::Borrowed("patients"),
        "condition" => Cow::Borrowed("conditions"),
        _ => return Ok(Vec::new()), // Return an empty vector if the type_arg doesn't match any table
    };
    

    match query {
        Ok(rows) => Ok(rows),
        Err(e) => {
            eprintln!("Error occurred: {}", e);
            Ok(Vec::new())
        }
    }
}    
*/

async fn upsert(base_url: &str, pg_con: PgConnection, type_arg: &str) {
    //get_pg_resource_versions(type_arg, pg_con);

    let client = Client::new();
    let mut url: String = format!("{}{}", base_url, type_arg);    
    loop {
        let res = client.get(&url)
            .send()
            .await
            .expect("failed to get response");

        let res_text = res.text().await.expect("failed to get payload");

        let search_set: SearchSet = match serde_json::from_str(&res_text) {
            Ok(search_set) => search_set,
            Err(error) => {
                eprintln!("Error deserializing JSON: {}", error);
                break;
            }
        };

        println!("test");
        for e in search_set.entry {
            // if Entry.id and Entry.resource.meta.versionId == what's in pg BTreeMap do nothing
            // else add Entry to insert_batch
            // let new_condition = NewCondition {
            //  entry: Entry.resource
            // };
            // if batch_chunk_size reached insert into resource type table
            println!("{:?}", "do stuff");
        }

        let next_url = search_set.link.iter()
            .find(|link| link.relation == "next")
            .map(|link| link.url.clone());

        if let Some(next_url) = next_url {
            url = next_url;
        } else {
            break;
        }
    }
}

#[tokio::main]
async fn main() {
    //let args: Vec<String> = env::args().collect();
    /*
    if args.len() != 3 || args[1] != "upsert" {
        eprintln!("Usage: blaze2pg upsert <type>");
        return;
    }
    let type_arg = &args[2];
    // Call your upsert function with the type argument
    upsert(type_arg);
    */
    //@todo: add error handling and stuff
    dotenv().ok();

    let host = dotenv::var("PG_HOST").expect("PG_HOST must be set");
    let port = dotenv::var("PG_PORT").expect("PG_PORT must be set");
    let username = dotenv::var("PG_USERNAME").expect("PG_USERNAME must be set");
    let password = dotenv::var("PG_PASSWORD").expect("PG_PASSWORD must be set"); //@todo: move out of .env
    let database = dotenv::var("PG_DATABASE").expect("PG_DATABASE must be set");

    let mut pg_con = PgConnection::establish(&format!(
        "host={} user={} password={} port={} dbname={}",
        host, username, password, port, database
    )).expect("Failed to connect to database");
    let base_url = "http://localhost:8082/fhir/".to_string();  //take from .env    
    let type_arg = "Observation";
    let _res = upsert(&base_url, pg_con, type_arg);

}
