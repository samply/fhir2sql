use diesel::prelude::*;

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = crate::schema::conditions)]
pub struct Condition {
    pub entry: serde_json::Value
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = patients)]
pub struct Patient {
    pub entry: serde_json::Value
}

#[derive(Queryable, Selectable, Insertable)]
#[diesel(table_name = specimen)]
pub struct Specimen {
    pub entry: serde_json::Value
}