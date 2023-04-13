
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','price_types_price_types') }}

)

SELECT 
price_type_id,
price_type_name,
price_type_group,
_fivetran_synced last_update,
from seed_date
order by _line