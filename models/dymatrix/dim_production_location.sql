{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with production_locations as
(
  select * FROM {{ ref('production_locations' )}}
  
) 
SELECT distinct country_code,
production_location_id,
production_id,
ifnull(theatre_id,'DE - _N/A' )theatre_id,
last_update
FROM production_locations
where country_code = 'DE'