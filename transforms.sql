-- Creates a table of events and where and when they happened as well as interesting facts such as damage in crops, deaths and injuries they caused.
CREATE TABLE storm_events_modeled.StormEvents AS
SELECT *
FROM 
(SELECT 
event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2013
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2012
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2011
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2010)
ORDER BY event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops

--Create Date as an entity and create a primary key.

CREATE TABLE storm_events_modeled.Date AS
SELECT *
FROM 
(SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2013
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2012
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2011
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2010)
ORDER BY date
                 

                 
--Creates a table with locations as primary keys and can be mapped to county name given state fips and county fips.

CREATE TABLE storm_events_modeled.Locations AS
SELECT *
FROM 
(SELECT  distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2013
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2012
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2011
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended) as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2010 
WHERE begin_lat_begin_lon_appended is not null)
ORDER BY location,cz_name, state_fips, cz_fips
                 
                 

--Creates a table with all unique state FIPS code along with the name of the state or territory.
--The state_fips being the primary key
CREATE TABLE storm_events_modeled.State AS
SELECT *
FROM 
(SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2013
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2012
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2011
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2010
WHERE state_fips IS NOT null OR state IS NOT null)
ORDER BY state_fips           
                 
