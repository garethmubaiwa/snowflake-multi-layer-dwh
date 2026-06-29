-- procedure 1: load raw data
CREATE OR REPLACE PROCEDURE raw.load_raw_data()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO raw.raw_airline_data
    FROM '@raw.airline_data_stage/Airline_Dataset.csv'
    FILE_FORMAT = (
        TYPE = 'CSV',
        SKIP_HEADER = 1,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    )
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;

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
    passenger_rows  INT DEFAULT 0;
    airport_rows    INT DEFAULT 0;
    booking_rows    INT DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    CREATE OR REPLACE TEMP TABLE tmp_stream AS
    SELECT *
    FROM raw.raw_to_staging_stream
    WHERE metadata$action = 'INSERT';

    -- Passengers
    MERGE INTO staging.staging_passengers t
    USING (
        SELECT passenger_id, first_name, last_name, gender, age, nationality
        FROM tmp_stream
        WHERE passenger_id IS NOT NULL   
    ) s
    ON t.passenger_id = s.passenger_id
    
    WHEN MATCHED THEN 
        UPDATE SET 
            t.first_name = s.first_name,
            t.last_name = s.last_name,
            t.gender = s.gender,
            t.age = s.age,
            t.nationality = s.nationality
    
    WHEN NOT MATCHED THEN
        INSERT (passenger_id, first_name, last_name, gender, age, nationality)
        VALUES (s.passenger_id, s.first_name, s.last_name, s.gender, s.age, s.nationality);

    passenger_rows := SQLROWCOUNT; 

    -- Airports
    MERGE INTO staging.staging_airports t
    USING (
        SELECT airport_name, airport_country_code, country_name, airport_continent
        FROM tmp_stream
    ) s
    ON t.airport_name = s.airport_name
    
    WHEN MATCHED THEN
        UPDATE SET
            t.country_code = s.airport_country_code,
            t.country_name = s.country_name,
            t.continent = s.airport_continent

    WHEN NOT MATCHED THEN
        INSERT (airport_name, country_code, country_name, continent)
        VALUES (s.airport_name, s.airport_country_code, s.country_name, s.airport_continent);

    airport_rows := SQLROWCOUNT;

    -- Bookings (Append Only from Stream)
    INSERT INTO staging.staging_flight_bookings (
        passenger_id, airport_name, departure_date, flight_status, ticket_type
    )
    SELECT
        passenger_id,
        airport_name,
        TRY_TO_DATE(departure_date),   
        flight_status,
        ticket_type
    FROM tmp_stream
    WHERE passenger_id IS NOT NULL;

    booking_rows := SQLROWCOUNT;

    INSERT INTO audit.audit_log (procedure_name, rows_affected)
    VALUES
        ('process_silver_layer:passengers', passenger_rows),
        ('process_silver_layer:airports',   airport_rows),
        ('process_silver_layer:bookings',   booking_rows);

    COMMIT;
    RETURN 'Silver OK — passengers: ' || passenger_rows 
        || ', airports: ' || airport_rows 
        || ', bookings: ' || booking_rows;

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
    affected_fact_rows INT DEFAULT 0;
BEGIN
    BEGIN TRANSACTION;

    -- dim_passengers: Incremental read from stream
    MERGE INTO core.dim_passengers t
    USING (
        SELECT DISTINCT 
            passenger_id,
            first_name || ' ' || last_name AS full_name,
            nationality
        FROM staging.staging_passengers_stream
        WHERE metadata$action = 'INSERT'
    ) s
    ON t.passenger_id = s.passenger_id

    WHEN MATCHED THEN
        UPDATE SET 
            t.full_name    = s.full_name,
            t.nationality  = s.nationality

    WHEN NOT MATCHED THEN
        INSERT (passenger_id, full_name, nationality)
        VALUES (s.passenger_id, s.full_name, s.nationality);

    -- dim_airports: Incremental read from stream
    MERGE INTO core.dim_airports t
    USING (
        SELECT DISTINCT airport_name, country_name, continent
        FROM staging.staging_airports_stream
        WHERE metadata$action = 'INSERT'
    ) s
    ON t.airport_name = s.airport_name

    WHEN MATCHED THEN
        UPDATE SET 
            t.country_name = s.country_name,
            t.continent = s.continent
    
    WHEN NOT MATCHED THEN
        INSERT (airport_name, country_name, continent)
        VALUES (s.airport_name, s.country_name, s.continent);

    -- fact_bookings: Incremental read from stream
    INSERT INTO core.fact_bookings (
        passenger_key, airport_key, departure_date, flight_status, ticket_type
    )
    SELECT
        p.passenger_key,
        a.airport_key,
        b.departure_date,
        b.flight_status,
        b.ticket_type
    FROM staging.staging_flight_bookings_stream b
    JOIN core.dim_passengers p ON b.passenger_id = p.passenger_id
    JOIN core.dim_airports a ON b.airport_name = a.airport_name
    WHERE b.metadata$action = 'INSERT';

    -- capture number of affected rows
    affected_fact_rows := SQLROWCOUNT;

    -- Build Business Metrics 
    MERGE INTO core.agg_daily_booking_metrics t
    USING (
        SELECT 
            departure_date, 
            flight_status, 
            ticket_type, 
            COUNT(*) as total_bookings
        FROM core.fact_bookings
        GROUP BY departure_date, flight_status, ticket_type
    ) s
    ON t.departure_date = s.departure_date 
       AND t.flight_status = s.flight_status 
       AND t.ticket_type = s.ticket_type
    WHEN MATCHED THEN
        UPDATE SET 
            t.total_bookings = s.total_bookings,
            t.last_updated = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (departure_date, flight_status, ticket_type, total_bookings)
        VALUES (s.departure_date, s.flight_status, s.ticket_type, s.total_bookings);

    -- Audit Logging
    INSERT INTO audit.audit_log (procedure_name, rows_affected)
    VALUES ('process_gold_layer', affected_fact_rows);

    COMMIT;
    RETURN 'Gold layer processed successfully';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'FAILED: ' || SQLERRM;
END;
$$;