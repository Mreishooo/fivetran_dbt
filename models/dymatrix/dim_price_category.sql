{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with price_categories as
(
  select * FROM {{ ref('price_categories' )}}
  
) 
SELECT distinct
country_code,
price_category_id,
price_category_name,
price_category_group, 
sort_code,
last_update,
FROM price_categories
where country_code = 'DE'