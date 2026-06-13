# Airline Data Warehouse Pipeline (Snowflake + Airflow)

## Project Overview

This project implements a small scale Data Warehouse (DWH) on Snowflake, built around the medallion architecture (Bronze, Silver, Gold), with the end to end pipeline orchestrated by Apache Airflow running in Docker.

The goal of this project was to gain hands on experience with Snowflake as a columnar cloud database, including its staging, loading, transformation, and security features, while building an ETL pipeline that processes raw airline passenger and booking data into analytics ready tables.

This was completed as part of a Big Data Engineering internship task, with a focus on learning core data warehousing concepts that are widely used in real world data engineering roles.

---

## Objectives

- Design a multi layer DWH (Bronze, Silver, Gold) with 5 to 10 tables
- Load data into Snowflake using a staged COPY INTO process
- Build a main ETL pipeline (Stage 1 to Stage 2 to Stage 3) using stored procedures, streams, and Airflow orchestration
- Implement an audit logging process to track rows affected by each pipeline run
- Write Snowflake Time Travel queries (DDL and DML examples)
- Implement a Secure View with Row Level Security on a fact table

---

## Architecture

```
CSV Source (Airline Dataset)
        |
        v
   Internal Stage (Snowflake)
        |  COPY INTO
        v
RAW (Bronze)
  raw_airline_data
        |  Stream (CDC) + stored procedure
        v
STAGING (Silver)
  staging_passengers
  staging_airports
  staging_flight_bookings
        |  stored procedure (aggregation)
        v
CORE (Gold)
  dim_passengers, dim_airports
  fact_bookings (+ secure view)
        |
        v
Audit Log / Reporting / BI Tools
```

All three layer transitions are encapsulated as Snowflake stored procedures, which are called sequentially by an Airflow DAG, giving the pipeline scheduling, retries, dependency management, and observability.

---

## Project Structure

```
.
|-- 01_setup_and_tables.sql     Database, warehouse, schemas, tables, stage, stream
|-- 02_procedures.sql           Stored procedures for Bronze to Silver to Gold processing
|-- 03_security_and_views.sql   Row level security policy, secure view, Time Travel examples
|-- dwh_pipeline.py              Airflow DAG orchestrating the pipeline
|-- docker-compose.yaml          Local Airflow environment (Celery executor)
|-- requirements.txt             Python/Airflow provider dependencies
|-- Airline_Dataset.csv           Source dataset
|-- README.md
```

---

## Data Model

### Bronze (Raw), RAW schema

| Table | Purpose |
|---|---|
| raw_airline_data | 1 to 1 copy of the source CSV, no transformations, used for traceability and reprocessing |

### Silver (Staging), STAGING schema

| Table | Purpose |
|---|---|
| staging_passengers | Deduplicated passenger entities |
| staging_airports | Deduplicated airport/location entities |
| staging_flight_bookings | Booking level transactional records |

### Gold (Core), CORE schema

| Table | Purpose |
|---|---|
| dim_passengers | Passenger dimension (surrogate key passenger_key) |
| dim_airports | Airport dimension (surrogate key airport_key) |
| fact_bookings | Fact table referencing dimensions, with booking level measures |

### Audit, AUDIT schema

| Table | Purpose |
|---|---|
| audit_log | Records procedure name, rows affected, and execution timestamp for each pipeline run |
| user_continents | Maps roles to continents, used by the row access policy |

---

## Pipeline Logic

### 1. Bronze: raw.load_raw_data()

Bulk loads the source CSV from an internal stage into raw_airline_data using COPY INTO. No cleaning or transformation happens here. The goal is a faithful, traceable copy of the source data that can be reprocessed at any time if downstream logic changes.

### 2. Silver: staging.process_silver_layer()

- Reads only new rows from raw_to_staging_stream (Snowflake's change tracking object), captured once into a temporary table for a consistent snapshot.
- Splits the flat raw structure into normalized entities (passengers, bookings).
- Uses MERGE (upsert) for passengers to avoid duplicating dimension like entities (for example, a passenger who has booked more than one flight).
- Inserts new booking records into staging_flight_bookings.
- Wrapped in a transaction with rollback on error, so the layer never ends up in a partially updated state.

### 3. Gold: core.process_gold_layer()

- Aggregates staging data into business facing metrics: total bookings per passenger and total revenue per flight.
- Uses MERGE to upsert results into the corresponding fact tables.
- Writes a record to audit.audit_log capturing the procedure name and execution time.
- Wrapped in a transaction with rollback on error.

---

## Airflow Orchestration

The pipeline is orchestrated by a single DAG (dwh_pipeline), running daily:

```
load_raw_data -> process_silver_layer -> process_gold_layer
```

Each task is a SnowflakeOperator calling its corresponding stored procedure via the snowflake_default Airflow connection. This gives the pipeline:

- Automatic daily scheduling
- Enforced task dependencies, so Gold never runs if Silver fails
- Automatic retries on transient failures
- Centralized logging and run history via the Airflow UI

---

## Security: Row Level Security and Secure Views

A row access policy (audit.continent_access_policy) restricts which rows of core.fact_bookings_secure_view a user can see, based on the continent associated with each booking. Access is controlled via the audit.user_continents mapping table, with ACCOUNTADMIN having unrestricted access. This demonstrates a basic data governance pattern that is relevant when handling data containing personal information, such as passenger names and nationalities.

---

## Time Travel Examples

Snowflake's Time Travel feature is demonstrated with the following query types (see 03_security_and_views.sql):

- DML, query historical data: select data as it existed at a point in the past using AT(OFFSET => ...)
- DML, restore rows: re insert rows lost to a bad statement using BEFORE(STATEMENT => '...')
- DDL, undrop a table: restore an accidentally dropped table with UNDROP TABLE
- DDL, clone at a timestamp: create a new table as a clone of another table's state at a specific point in time

---

## Running the Project Locally

### Prerequisites

- Docker and Docker Compose
- A Snowflake account (free trial is sufficient)
- A configured Airflow connection named snowflake_default pointing to your Snowflake account

### Setup Steps

1. Start Airflow:
   ```bash
   docker-compose up airflow-init
   docker-compose up
   ```

2. Set up the Snowflake environment by running the SQL scripts in order against your Snowflake account:
   ```sql
   -- 1. Database, schemas, tables, stage, stream
   01_setup_and_tables.sql

   -- 2. Stored procedures for each layer
   02_procedures.sql

   -- 3. Security policy and secure view
   03_security_and_views.sql
   ```

3. Upload the dataset to the Snowflake stage. Upload Airline_Dataset.csv to @raw.airline_data_stage (via SnowSQL, the Snowflake UI, or an external stage).

4. Configure the Airflow connection. In the Airflow UI (http://localhost:8080), add a connection with ID snowflake_default pointing to your Snowflake account, warehouse, and credentials.

5. Trigger the DAG. Enable and trigger dwh_pipeline from the Airflow UI to run the full Bronze to Silver to Gold pipeline.

---

## Key Concepts Demonstrated

- Medallion architecture (Bronze, Silver, Gold)
- Snowflake stages and bulk loading with COPY INTO
- Change Data Capture with Snowflake Streams
- Upsert patterns with MERGE
- Transaction management and exception handling in stored procedures
- Pipeline orchestration with Airflow DAGs and the SnowflakeOperator
- Audit logging for pipeline observability
- Row level security and secure views
- Time Travel for data recovery and historical querying

---

## Known Limitations / Future Improvements

This project is a learning exercise and has some known gaps that would need to be addressed before production use:

- The silver procedure inserts booking_id, flight_id, and price into staging_flight_bookings, and the gold procedure reads flight_id and price from that same table and writes to core.fact_passenger_bookings and core.fact_flight_revenue. None of these columns or tables currently exist in 01_setup_and_tables.sql, which defines staging_flight_bookings with passenger_id, airport_name, departure_date, flight_status, ticket_type, and defines core.fact_bookings instead. These need to be reconciled so the DDL and the procedures reference the same schema.
- The gold procedure inserts into audit.audit_log using a created_at column, but audit_log is defined with execution_time (which has a default value). This column name needs to be aligned.
- Dimension tables (dim_passengers, dim_airports) are defined but not yet populated by a dedicated procedure. In a complete star schema, dimensions should be loaded before, or alongside, the fact table load so that fact rows can reference valid surrogate keys.
- audit_log.rows_affected is declared and inserted as total_rows, but total_rows is never actually calculated from SQLROWCOUNT or similar, so it is always logged as 0.
- Credentials are managed via Airflow connections, which is good practice, but should be further secured using a secrets backend in a production deployment.

---

## Author

Built by Gareth as part of a Big Data Engineering internship project, focused on learning Snowflake's columnar architecture and building production style ETL pipelines orchestrated with Airflow.
