use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Entry {    
    pub resource: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
pub struct Search {
    pub mode: String,
}

#[derive(Deserialize, Serialize)]
pub struct SearchSet {
    pub id: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub entry: Option<Vec<Entry>>,
    pub link: Vec<Link>,
    pub total: u32,
    #[serde(rename = "resourceType")]
    pub resource_type: String,
}

#[derive(Deserialize, Serialize)]
pub struct Link {
    pub relation: String,
    pub url: String,
}

pub struct ResourceVersion {
    pub resource_id: String,
    pub version_id: i64
}

pub struct PgVersion {
    pub pk_id: i32, //pg row primary key. Needed for upsert
    pub version_id: i64 //resource version_id
}

pub struct PgInsertItem {    
    pub resource: String
}

pub struct PgUpdateItem {
    pub id: i32,    
    pub resource: String
}