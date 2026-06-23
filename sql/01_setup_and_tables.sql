-- 1. Create the Environment
CREATE DATABASE IF NOT EXISTS dev_db
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Development database for implementation';
CREATE WAREHOUSE IF NOT EXISTS dev_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 30
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for development and testing';
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS AUDIT;

-- Stage 1: Raw (Bronze)
USE SCHEMA RAW;
CREATE OR REPLACE STAGE airline_data_stage;
CREATE TABLE raw_airline_data (
    unnamed_col VARCHAR,
    passenger_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    age INT,
    nationality VARCHAR,
    airport_name VARCHAR,
    airport_country_code VARCHAR,
    country_name VARCHAR,
    airport_continent VARCHAR,
    continents VARCHAR,
    departure_date VARCHAR,
    arrival_airport VARCHAR,
    pilot_name VARCHAR,
    flight_status VARCHAR,
    ticket_type VARCHAR,
    passenger_status VARCHAR
);

create or replace stream raw_to_staging_stream on table raw_airline_data;

-- Stage 2: Staging (Silver)
USE SCHEMA STAGING;
create or replace table staging_passengers(
    passenger_id varchar,
    first_name varchar,
    last_name varchar,
    gender varchar,
    age INT,
    nationality varchar
);

create or replace table staging_airports(
    airport_name varchar,
    country_code varchar,
    country_name varchar,
    continent varchar
);

create or replace table staging_flight_bookings(
    passenger_id varchar,
    airport_name varchar,
    departure_date date,
    flight_status varchar,
    ticket_type varchar
);

-- Streams for Staging to Core
create or replace stream staging_passengers_stream on table staging_passengers;
create or replace stream staging_airports_stream on table staging_airports;
create or replace stream staging_flight_bookings_stream on table staging_flight_bookings;

-- Stage 3: Core (Gold)
USE SCHEMA CORE;
create or replace table dim_passengers (
    passenger_key INT AUTOINCREMENT,
    passenger_id VARCHAR,
    full_name VARCHAR,
    nationality VARCHAR
);

create or replace table dim_airports (
    airport_key INT AUTOINCREMENT,
    airport_name VARCHAR,
    country_name VARCHAR,
    continent VARCHAR
);

create or replace table fact_bookings (
    booking_key INT AUTOINCREMENT,
    passenger_key INT,
    airport_key INT,
    departure_date DATE,
    flight_status VARCHAR,
    ticket_type VARCHAR
);

create or replace table agg_daily_booking_metrics (
    departure_date  DATE,
    flight_status   VARCHAR,
    ticket_type     VARCHAR,
    total_bookings  INT,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Stage 4: Audit
USE SCHEMA AUDIT;

create or replace table audit_log (
    log_id INT AUTOINCREMENT,
    procedure_name VARCHAR,
    rows_affected INT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

create or replace table user_continents (
    role_name VARCHAR,
    continent_access VARCHAR
);