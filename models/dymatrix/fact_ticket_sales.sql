{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with ticket_sales as
(
  select * FROM {{ ref('tickets' )}}
  
) 
 
select distinct
country_code,
ticket_id fact_ticket_sales_id ,
production_location_id,
performance_id,
price_type_id,
distribution_id,
source_customer_id customer_id,
price_category_id,
article_type_code article_type_id,
booking_date,
source_promotion_name,
tpt_value_eur ticket_price_value_eur,
article_count,
source_distribution_point_id,
source_distribution_channel,
_loaded_at last_update_date,
_last_update insert_date,
_rebooking
FROM ticket_sales
where country_code = 'DE'

