
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','exchange_rates_exchange_rate') }}

)


SELECT  
upper(from_currency_code) from_currency_code,
upper(to_currency_code) to_currency_code,
date(valid_from) valid_from,
rate,
_fivetran_synced last_update,
from seed_date
order by _line