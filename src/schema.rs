// @generated automatically by Diesel CLI.

diesel::table! {
    conditions (id) {
        id -> Int4,
        created_at -> Timestamp,
        last_updated_at -> Timestamp,
        resource -> Jsonb,
    }
}

diesel::table! {
    observations (id) {
        id -> Int4,
        created_at -> Timestamp,
        last_updated_at -> Timestamp,
        resource -> Jsonb,
    }
}

diesel::table! {
    patients (id) {
        id -> Int4,
        created_at -> Timestamp,
        last_updated_at -> Timestamp,
        resource -> Jsonb,
    }
}

diesel::table! {
    specimen (id) {
        id -> Int4,
        created_at -> Timestamp,
        last_updated_at -> Timestamp,
        resource -> Jsonb,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    conditions,
    observations,
    patients,
    specimen,
);
