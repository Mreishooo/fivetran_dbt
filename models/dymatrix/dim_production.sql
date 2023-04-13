{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with productions as
(
  select * FROM {{ ref('productions' )}}
  
) 
SELECT distinct
country_code,
production_id,
production_name, 
production_code,
is_expired, 
last_update
FROM productions
where country_code = 'DE'