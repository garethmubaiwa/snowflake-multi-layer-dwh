/* 
Procedure to load raw data into the bronze layer
Procedure to process silver layer (staging) 
    - Merge passengers (deduplication)
    - Insert bookings (append-only)
Procedure to process gold layer (core)
    - Build dimensions and fact table
    - Audit logging
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
    )
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;  -- FIX

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

    CREATE OR REPLACE TEMP TABLE tmp_stream AS
    SELECT *
    FROM raw.raw_to_staging_stream
    WHERE metadata$action = 'INSERT';

    -- Passengers
    MERGE INTO staging.staging_passengers t
    USING (
        SELECT 
            passenger_id, 
            first_name, 
            last_name,
            gender,
            age,
            nationality
        FROM tmp_stream
        WHERE passenger_id IS NOT NULL   
    ) s
    ON t.passenger_id = s.passenger_id
    WHEN NOT MATCHED THEN
        INSERT (
            passenger_id, 
            first_name, 
            last_name,
            gender,
            age,
            nationality
        )
        VALUES (
            s.passenger_id, 
            s.first_name, 
            s.last_name,
            s.gender,
            s.age,
            s.nationality
        );

    -- Airports
    MERGE INTO staging.staging_airports t
    USING (
        SELECT 
            airport_name,
            airport_country_code,
            country_name,
            airport_continent
        FROM tmp_stream
    ) s
    ON t.airport_name = s.airport_name
    WHEN NOT MATCHED THEN
        INSERT (
            airport_name,
            country_code,
            country_name,
            continent
        )
        VALUES (
            s.airport_name,
            s.airport_country_code,
            s.country_name,
            s.airport_continent
        );

    -- Bookings
    INSERT INTO staging.staging_flight_bookings (
        passenger_id,
        airport_name,
        departure_date,
        flight_status,
        ticket_type
    )
    SELECT
        passenger_id,
        airport_name,
        TRY_TO_DATE(departure_date),   
        flight_status,
        ticket_type
    FROM tmp_stream
    WHERE passenger_id IS NOT NULL;  

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

    -- dim_passengers
    MERGE INTO core.dim_passengers t
    USING (
        SELECT DISTINCT 
            passenger_id,
            first_name || ' ' || last_name AS full_name,
            nationality
        FROM staging.staging_passengers
    ) s
    ON t.passenger_id = s.passenger_id
    WHEN NOT MATCHED THEN
        INSERT (passenger_id, full_name, nationality)
        VALUES (s.passenger_id, s.full_name, s.nationality);

    -- dim_airports
    MERGE INTO core.dim_airports t
    USING (
        SELECT DISTINCT
            airport_name,
            country_name,
            continent
        FROM staging.staging_airports
    ) s
    ON t.airport_name = s.airport_name
    WHEN NOT MATCHED THEN
        INSERT (airport_name, country_name, continent)
        VALUES (s.airport_name, s.country_name, s.continent);

    -- fact_bookings
    INSERT INTO core.fact_bookings (
        passenger_key,
        airport_key,
        departure_date,
        flight_status,
        ticket_type
    )
    SELECT
        p.passenger_key,
        a.airport_key,
        b.departure_date,
        b.flight_status,
        b.ticket_type
    FROM staging.staging_flight_bookings b
    JOIN core.dim_passengers p 
        ON b.passenger_id = p.passenger_id
    JOIN core.dim_airports a 
        ON b.airport_name = a.airport_name
    WHERE NOT EXISTS (
        SELECT 1 
        FROM core.fact_bookings f
        WHERE f.passenger_key = p.passenger_key
          AND f.airport_key = a.airport_key
          AND f.departure_date = b.departure_date
    );

    -- Audit
    SELECT COUNT(*) INTO total_rows
    FROM staging.staging_flight_bookings;

    INSERT INTO audit.audit_log (
        procedure_name,
        rows_affected
    )
    VALUES (
        'process_gold_layer',
        total_rows
    );

    COMMIT;

    RETURN 'Gold layer processed successfully';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;