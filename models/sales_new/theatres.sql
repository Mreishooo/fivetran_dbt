
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','theatres_theatres') }}

)

SELECT   
upper(country_code) country_code,
theatre_id,
theatre_code,
theatre_name,
address,
postcode,
city_code,
city,
owner,
max_capacity,
is_theatre_owned_by_stage,
_fivetran_synced last_update,
from seed_date
order by _line