{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with distributions as
(
  select * FROM {{ ref('distributions' )}}
  
) 
SELECT distinct
country_code,
distribution_id ,
distribution_point,
distribution_channel_code,
distribution_channel_name,
local_sales_channel_1,
channel_type_code,

current_timestamp() last_update
FROM distributions
where country_code = 'DE'