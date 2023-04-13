{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with article_types as
(
  select * FROM {{ ref('article_types' )}}
  
) 
SELECT distinct
article_type_code article_type_id,
article_type_name,
last_update
FROM article_types
