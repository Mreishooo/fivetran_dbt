{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with price_types as
(
  select * FROM {{ ref('price_types' )}}
  
) 
SELECT distinct
price_type_id,
price_type_name,
price_type_group,
last_update,
FROM price_types
