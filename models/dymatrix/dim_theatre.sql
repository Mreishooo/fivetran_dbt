{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with theatres as
(
  select * FROM {{ ref('theatres' )}}
  
) 
SELECT distinct
country_code,
theatre_id,
theatre_code,
theatre_name,
last_update,
FROM theatres
where country_code = 'DE'