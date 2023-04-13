
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','productions_productions') }}

)

SELECT  
country_code,
production_id,
production_name, 
production_code,
licensor, 
license_type, 
license_name, 
license_classification, 
is_expired, 
_fivetran_synced last_update,
from seed_date
order by _line