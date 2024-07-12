Here is a concise and catchy README.md for your code base:

**FHIR2SQL**
=====================

A Rust-based application that synchronizes FHIR (Fast Healthcare Interoperability Resources) data with a PostgreSQL database.

**Overview**
------------

This application connects to a FHIR server, retrieves data, and syncs it with a PostgreSQL database. It uses the `sqlx` library for database interactions and `reqwest` for making HTTP requests to the FHIR server. The application is designed to run continuously, syncing data at regular intervals.

**Features**
------------

* Connects to a PostgreSQL database and creates necessary tables and triggers if they don't exist
* Retrieves FHIR data from a specified server and syncs it with the PostgreSQL database
* Supports regular syncing at a specified interval
* Handles errors and retries connections to the FHIR server and PostgreSQL database
* Supports graceful shutdown on SIGTERM and SIGINT signals

**Components**
--------------

* `main.rs`: The main application entry point
* `db_utils.rs`: Database utility functions for connecting to PostgreSQL and creating tables and triggers
* `models.rs`: Data models for FHIR resources and database interactions
* `graceful_shutdown.rs`: Functions for handling graceful shutdown on SIGTERM and SIGINT signals

**Getting Started**
-------------------

To use this application, you'll need to:

1. Install Rust and the required dependencies
2. Configure the application by setting environment variables for the FHIR server URL, PostgreSQL connection details, and syncing interval
3. Run the application using `cargo run`