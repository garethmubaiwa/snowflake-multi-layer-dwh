/* 
Procedure to load raw data into the bronze layer
Procedure to process silver layer (staging) 
    - Merge passengers (deduplication)
    - Insert bookings (append-only)
    - FIXED: added flight_id, price to staging_flight_bookings
Procedure to process gold layer (core)
    - Passenger bookings (count)
    - Flight revenue (sum of price)
    - Audit logging (insert into audit_log)
*/

-- procedure 1: load raw data
CREATE OR REPLACE PROCEDURE raw.load_raw_data()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO raw.raw_airline_data
    FROM '@raw.airline_data_stage/Airline Dataset.csv'
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );

    COMMIT;

    RETURN 'Stage 1 loaded (Raw)';
END;
$$;

-- Procedure 2: process silver layer (staging)
CREATE OR REPLACE PROCEDURE staging.process_silver_layer()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_rows INT DEFAULT 0;
BEGIN

    BEGIN TRANSACTION;
    -- Create a temporary table to hold the new stream data
    CREATE OR REPLACE TEMP TABLE tmp_stream AS
    SELECT *
    FROM raw.raw_to_staging_stream
    WHERE metadata$action = 'INSERT';

    -- Passengers (FIXED: added passenger_id to staging_passengers)
    MERGE INTO staging.staging_passengers t
    USING (
        SELECT passenger_id, first_name, last_name
        FROM tmp_stream
    ) s
    ON t.passenger_id = s.passenger_id
    WHEN NOT MATCHED THEN
        INSERT (passenger_id, first_name, last_name)
        VALUES (s.passenger_id, s.first_name, s.last_name);

    -- Bookings (FIXED: added flight_id, price)
    INSERT INTO staging.staging_flight_bookings (
        booking_id,
        passenger_id,
        flight_id,
        price
    )
    SELECT
        booking_id,
        passenger_id,
        flight_id,
        price
    FROM tmp_stream;

    COMMIT;

    RETURN 'OK';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;

-- Procedure 3: process gold layer (core)
CREATE OR REPLACE PROCEDURE core.process_gold_layer()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    total_rows INT DEFAULT 0;
BEGIN

    BEGIN TRANSACTION;

    -- Passenger bookings
    MERGE INTO core.fact_passenger_bookings t
    USING (
        SELECT passenger_id, COUNT(*) AS total_bookings
        FROM staging.staging_flight_bookings
        GROUP BY passenger_id
    ) s
    ON t.passenger_id = s.passenger_id
    WHEN MATCHED THEN
        UPDATE SET total_bookings = s.total_bookings
    WHEN NOT MATCHED THEN
        INSERT (passenger_id, total_bookings)
        VALUES (s.passenger_id, s.total_bookings);

    -- Flight revenue
    MERGE INTO core.fact_flight_revenue t
    USING (
        SELECT flight_id, SUM(price) AS total_revenue
        FROM staging.staging_flight_bookings
        GROUP BY flight_id
    ) s
    ON t.flight_id = s.flight_id
    WHEN MATCHED THEN
        UPDATE SET total_revenue = s.total_revenue
    WHEN NOT MATCHED THEN
        INSERT (flight_id, total_revenue)
        VALUES (s.flight_id, s.total_revenue);

    -- Audit
    INSERT INTO audit.audit_log (
        procedure_name,
        rows_affected,
        created_at
    )
    VALUES (
        'process_gold_layer',
        total_rows,
        CURRENT_TIMESTAMP()
    );

    COMMIT;

    RETURN 'Gold processed successfully';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;