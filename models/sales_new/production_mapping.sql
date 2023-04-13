
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','production_mapping_production_mapping') }}

)


SELECT   
upper(country_code) country_code,
upper(source_code) source_code,
upper(source_location) source_location,
upper(source_production) source_production,

upper(article_type_code) article_type_code,
upper(production_location_id) production_location_id,
upper(theatre_id) theatre_id,
delete_from_bi,
_fivetran_synced last_update,
from seed_date
order by _line
