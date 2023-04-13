
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','article_types_article_types') }}

)

SELECT 
article_type_code,
article_type_name,
_fivetran_synced last_update,
from seed_date
order by _line