
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT  distinct *  
   FROM {{ source( 'ft_sales_seeds','data_import_limitation_data_import_limitation') }}

)

SELECT  
upper(country_code) country_code,
upper(source_type) source_type,
upper(source_code) source_code,
client_id client_id,
include_data, 
_fivetran_synced last_update, 
from seed_date
order by _line
