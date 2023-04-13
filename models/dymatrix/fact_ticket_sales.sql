{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with ticket_sales as
(
  select * FROM {{ ref('ticket_sales' )}}
  
) 
 
select distinct
country_code,
cast(fact_ticket_sales_id as string) fact_ticket_sales_id ,
production_location_id,
concat(production_location_id,' - ',performance_date ,' - ',performance_time) performance_id,
price_type_id,
upper( concat (country_code, ' - ', distribution_owner ,' - ' ,distribution_point)) distribution_id,
dim_golden_customer_id golden_customer_id,
price_category_id,
article_type_code article_type_id,
booking_date,
source_promotion_name,
ticket_price_value_eur,
article_count,
source_distribution_point_id,
source_distribution_channel,
current_timestamp last_update_date,
current_timestamp insert_date,
FROM ticket_sales
where country_code = 'DE'