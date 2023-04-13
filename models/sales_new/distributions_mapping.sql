
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
 SELECT distinct *  
   FROM {{ source( 'ft_sales_seeds','distribution_mapping_distributions_mapping') }}

)


SELECT  
upper(country_code) country_code,	
distribution_id,	
Prioriy,	
upper(source_code) source_code,	
source_sales_partner_class,	
source_distribution_channel,	
source_distribution_point_id,
source_client_id,	
source_customer_code,	
source_promotion_code,	
source_promotion_id,
source_promotion_name,	
source_price_type_id_from,
source_price_type_id_to,
source_price_type_name,
_fivetran_synced last_update,
from seed_date
order by _line


