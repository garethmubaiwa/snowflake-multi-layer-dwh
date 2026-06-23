/*
This script demonstrates the use of row access policies and secure views in Snowflake to manage data access based on user roles and attributes. 
It also includes examples of time travel queries for both DML and DDL operations.
*/

use schema audit;
-- table to manage user access to continents
create or replace row access policy continent_access_policy
as (continent_val varchar) returns boolean ->
    current_role() = 'ACCOUNTADMIN'
    or exists (
        select 1
        from audit.user_continents
        where role_name = current_role()
          and continent_access = continent_val
    );

-- secure view for fact_bookings with continent information
use schema core;

create or replace secure view fact_bookings_secure_view as
select 
    f.*, 
    a.continent 
from fact_bookings f
join dim_airports a on f.airport_key = a.airport_key;

alter view fact_bookings_secure_view
add row access policy audit.continent_access_policy on continent;

--time travel query examples

-- DML 1: Query data as it looked 1 hour ago
-- SELECT * FROM core.fact_bookings AT(OFFSET => -3600);

-- DML 2: Restore rows before a bad statement
-- INSERT INTO staging.staging_passengers
-- SELECT * FROM staging.staging_passengers BEFORE(STATEMENT => 'your_query_id');

-- DDL 1: Undrop a deleted table
-- UNDROP TABLE core.dim_airports;

-- DDL 2: Clone table at a timestamp
-- CREATE TABLE core.dev_fact_bookings 
-- CLONE core.fact_bookings 
-- AT(TIMESTAMP => '2026-06-06 12:00:00');
