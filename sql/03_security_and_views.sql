/*
This script demonstrates the use of row access policies and secure views in Snowflake to manage data access based on user roles and attributes. 
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