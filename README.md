# Airline Data Warehouse Pipeline (Snowflake + Airflow)

## Project Overview

A small-scale Data Warehouse (DWH) implemented on Snowflake using the medallion architecture (Bronze → Silver → Gold), with end-to-end pipeline orchestration via Apache Airflow running in Docker. Built as part of a Big Data Engineering internship to gain hands-on experience with Snowflake staging, loading, transformation, and security, processing raw airline passenger and booking data into analytics-ready tables.

---

## Objectives

- Design a multi-layer DWH (Bronze, Silver, Gold) with tables across four schemas
- Load data into Snowflake via a staged `COPY INTO` process
- Build a sequential ETL pipeline (Bronze → Silver → Gold) using stored procedures, streams, and Airflow orchestration
- Implement audit logging to track rows affected per pipeline run
- Demonstrate Snowflake Time Travel queries (DML and DDL)
- Implement a Secure View with Row-Level Security on the fact table

---

## Architecture

```
CSV Source (Airline Dataset)
        │
        ▼
   Internal Stage (Snowflake)
        │  COPY INTO
        ▼
RAW (Bronze)
  raw_airline_data
        │  Stream (CDC) + stored procedure
        ▼
STAGING (Silver)
  staging_passengers
  staging_airports
  staging_flight_bookings
        │  stored procedure (upsert + aggregation)
        ▼
CORE (Gold)
  dim_passengers, dim_airports
  fact_bookings
  agg_daily_booking_metrics
        │
        ▼
Audit Log / BI Tools
```

---

## Project Structure

```
.
├── 01_setup_and_tables.sql     Database, warehouse, schemas, tables, stage, all streams
├── 02_procedures.sql           Stored procedures for Bronze → Silver → Gold processing
├── 03_security_and_views.sql   Row-level security policy, secure view, Time Travel examples
├── dwh_pipeline.py             Airflow DAG orchestrating the pipeline
├── docker-compose.yaml         Local Airflow environment (Celery executor)
├── requirements.txt            Python/Airflow provider dependencies
└── Airline_Dataset.csv         Source dataset
```

---

## Data Model

### Bronze — `RAW` schema

| Table | Purpose |
|---|---|
| `raw_airline_data` | 1:1 copy of the source CSV; no transformations; preserved for traceability and reprocessing |

### Silver — `STAGING` schema

| Table | Purpose |
|---|---|
| `staging_passengers` | Deduplicated passenger entities |
| `staging_airports` | Deduplicated airport/location entities |
| `staging_flight_bookings` | Booking-level transactional records |

### Gold — `CORE` schema

| Table | Purpose |
|---|---|
| `dim_passengers` | Passenger dimension (surrogate key `passenger_key`) |
| `dim_airports` | Airport dimension (surrogate key `airport_key`) |
| `fact_bookings` | Fact table referencing both dimensions, with booking-level measures |
| `agg_daily_booking_metrics` | Pre-aggregated daily totals by `departure_date`, `flight_status`, and `ticket_type` |

### Audit — `AUDIT` schema

| Table | Purpose |
|---|---|
| `audit_log` | Records `procedure_name`, `rows_affected`, and `execution_time` per pipeline run |
| `user_continents` | Maps roles to continents for the row access policy |

---

## Pipeline Logic

### 1. Bronze — `raw.load_raw_data()`

Bulk-loads the source CSV from an internal stage into `raw_airline_data` using `COPY INTO`. No cleaning or transformation occurs at this layer; the goal is a faithful, traceable copy of the source data that can be reprocessed if downstream logic changes.

### 2. Silver — `staging.process_silver_layer()`

- Reads only new rows from `raw_to_staging_stream` (Snowflake's CDC object), captured once into a temp table for a consistent snapshot.
- Splits the flat raw structure into three normalised entities: passengers, airports, and bookings.
- Uses `MERGE` (upsert) on passengers and airports to prevent duplication of dimension-like entities.
- Appends new booking records to `staging_flight_bookings`.
- Wrapped in a transaction with `ROLLBACK` on error to prevent partial updates.

### 3. Gold — `core.process_gold_layer()`

- Reads incremental inserts from three staging streams (`staging_passengers_stream`, `staging_airports_stream`, `staging_flight_bookings_stream`) to upsert `dim_passengers`, `dim_airports`, and insert into `fact_bookings`, resolving surrogate keys via joins on the dimension tables.
- Builds `agg_daily_booking_metrics` by merging an aggregation of `fact_bookings` grouped by `departure_date`, `flight_status`, and `ticket_type`.
- Captures `SQLROWCOUNT` after the fact table insert and writes it to `audit.audit_log`.
- Wrapped in a transaction with `ROLLBACK` on error.

---

## Airflow Orchestration

The pipeline is orchestrated by a single DAG (`dwh_pipeline`), running daily:

```
load_raw_data → process_silver_layer → process_gold_layer
```

Each task is a `SnowflakeOperator` calling its corresponding stored procedure via the `snowflake_default` Airflow connection, providing automatic scheduling, enforced task dependencies, automatic retries on transient failures, and centralised logging via the Airflow UI.

---

## Security: Row-Level Security and Secure Views

A row access policy (`audit.continent_access_policy`) restricts which rows of `core.fact_bookings_secure_view` a given role can read, based on the continent associated with each booking. Access is controlled via the `audit.user_continents` mapping table; `ACCOUNTADMIN` retains unrestricted access. This demonstrates a data governance pattern relevant to datasets containing personal information such as passenger names and nationalities.

---

## Time Travel Examples

Demonstrated in `03_security_and_views.sql`:

- **DML — historical query:** retrieve data as it existed at a past offset using `AT(OFFSET => ...)`
- **DML — row restoration:** re-insert rows lost to a bad statement using `BEFORE(STATEMENT => '...')`
- **DDL — undrop:** restore an accidentally dropped table with `UNDROP TABLE`
- **DDL — point-in-time clone:** create a table as a clone of another table's state at a specific timestamp

---

## Running the Project Locally

### Prerequisites

- Docker and Docker Compose
- A Snowflake account (free trial is sufficient)
- An Airflow connection named `snowflake_default` configured for your Snowflake account

### Setup Steps

1. **Start Airflow:**
   ```bash
   docker-compose up airflow-init
   docker-compose up
   ```

2. **Initialise Snowflake** by running the SQL scripts in order:
   ```sql
   -- 1. Database, schemas, tables, stage, all streams
   01_setup_and_tables.sql

   -- 2. Stored procedures for each layer
   02_procedures.sql

   -- 3. Security policy and secure view
   03_security_and_views.sql
   ```

3. **Upload the dataset** — load `Airline_Dataset.csv` to `@raw.airline_data_stage` via SnowSQL or the Snowflake UI.

4. **Configure the Airflow connection** — in the Airflow UI (`http://localhost:8080`), add a connection with ID `snowflake_default` pointing to your Snowflake account, warehouse, and credentials.

5. **Trigger the DAG** — enable and manually trigger `dwh_pipeline` to run the full Bronze → Silver → Gold pipeline.

---

## Key Concepts Demonstrated

- Medallion architecture (Bronze, Silver, Gold)
- Snowflake stages and bulk loading with `COPY INTO`
- Change Data Capture with Snowflake Streams
- Upsert patterns with `MERGE`
- Transaction management and exception handling in stored procedures
- Pre-aggregated metrics tables
- Pipeline orchestration with Airflow DAGs and `SnowflakeOperator`
- Audit logging for pipeline observability (`SQLROWCOUNT`)
- Row-level security and secure views
- Time Travel for data recovery and historical querying

---

## Known Limitations / Future Improvements

- **Credentials management.** Credentials are handled via Airflow connections, which is acceptable for development but should use a dedicated secrets backend (e.g., HashiCorp Vault, AWS Secrets Manager) before any production deployment.

---

## Author

Built by Gareth as part of a Big Data Engineering internship project, focused on Snowflake's columnar architecture and production-style ETL pipelines orchestrated with Airflow.