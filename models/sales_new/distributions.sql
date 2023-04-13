
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','distributions_distributions') }}

)

SELECT 
country_code,
distribution_id,
distribution_channel_code,
distribution_channel_name,
local_sales_channel_1,
local_sales_channel_2,
distribution_point,
commission_percentage,
kickback_percentage,
distribution_owner,
channel_type_code,
channel_type_name,
is_distribution_owned_by_stage,
discount_percentage,
providing_customer_data_code,
providing_customer_data_name,
_fivetran_synced last_update,
from seed_date
order by _line