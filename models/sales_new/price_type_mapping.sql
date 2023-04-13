
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','price_type_mapping_price_type_mapping') }}

)

SELECT  
upper(country_code)country_code, 
upper(source_code) source_code, 
upper(source_price_type) source_price_type, 
upper(source_price_type_id) source_price_type_id, 
upper(price_type_id) price_type_id,
_fivetran_synced last_update,
from seed_date
order by _line