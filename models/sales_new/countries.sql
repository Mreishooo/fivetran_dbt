
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','countries_countries') }}

)

SELECT 
country_code,
country_name,	
currency_code,
ticket_price_adjustment_factor,	
vat,	
vat_high,

_fivetran_synced last_update,
from seed_date
order by _line