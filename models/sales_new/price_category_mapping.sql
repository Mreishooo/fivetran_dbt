
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','price_category_mapping_price_category_mapping') }}

)

SELECT  
upper(country_code) country_code, 
upper(source_code) source_code, 
upper(source_price_category_id) source_price_category_id, 
upper(production_location_id) production_location_id,
upper( price_category_id) price_category_id, 
_fivetran_synced last_update,
from seed_date
order by _line