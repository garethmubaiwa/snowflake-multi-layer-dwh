-- Demonstrates Snowflake Time Travel for both DML and DDL operations
USE DATABASE dev_db;

-- DML 1: Query historical state of a table
-- Purpose: see fact_bookings as it looked 1 hour ago
SELECT * 
FROM core.fact_bookings 
AT(OFFSET => -3600);   -- -3600 seconds = 1 hour ago

-- DML 2: Recover rows deleted by a bad statement
-- Step 1 — simulate a bad DELETE
DELETE FROM staging.staging_passengers 
WHERE nationality = 'Germany';

-- Step 2 — grab the query ID of the DELETE just made
SELECT 
    query_id,
    query_text,
    start_time
FROM TABLE(information_schema.query_history_by_session())
WHERE query_text ILIKE '%DELETE FROM staging.staging_passengers%'
ORDER BY start_time DESC
LIMIT 1;

-- Step 3 — restore using the query ID from Step 2
-- Replace 'query_id' with the actual ID from the result above
INSERT INTO staging.staging_passengers
SELECT * 
FROM staging.staging_passengers 
BEFORE(STATEMENT => 'query_id');


-- DDL 1: Undrop an accidentally dropped table
-- Step 1 — drop the table
DROP TABLE core.dim_airports;

-- Step 2 — verify it's gone (this will error)
-- SELECT * FROM core.dim_airports;

-- Step 3 — recover it
UNDROP TABLE core.dim_airports;

-- Step 4 — verify it's back
SELECT COUNT(*) AS row_count FROM core.dim_airports;

-- DDL 2: Clone a table at a specific past timestamp
-- Purpose: create a dev/audit snapshot of fact_bookings as it existed at a specific point in time

-- First check what timestamp needed to get back to.
SELECT MIN(last_updated) AS earliest_record 
FROM core.agg_daily_booking_metrics;

-- Then clone at that point (replace the timestamp with one from above)
CREATE OR REPLACE TABLE core.dev_fact_bookings
CLONE core.fact_bookings
AT(TIMESTAMP => '2026-06-06 12:00:00'::TIMESTAMP);

-- Verify the clone exists and has data
SELECT COUNT(*) AS cloned_rows FROM core.dev_fact_bookings;