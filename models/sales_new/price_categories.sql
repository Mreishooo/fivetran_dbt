
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT  distinct *  
   FROM {{ source( 'ft_sales_seeds','price_categories_price_categories') }}

)

SELECT 
country_code,
price_category_id,
price_category_name,
price_category_group, 
sort_code,
_fivetran_synced last_update,
from seed_date
order by _line