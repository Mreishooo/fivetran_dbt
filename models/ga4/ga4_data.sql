{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link','type':'enriched'},
)}}

with  
  ga4_source AS (
   SELECT 'Germany' as country , *  
   FROM {{ source( 'analytics_272653220','events_202210*') }}
   --union all 
   --SELECT 'Netherlands' as country , *  
   --FROM {{ source( '97634084','ga_sessions_2022*') }}
  ),

  ga_page_groups AS ( 
    SELECT *
    FROM {{ ref('ga_page_groups') }}
  )

SELECT 
PARSE_DATE("%Y%m%d",event_date)  date ,
TIMESTAMP_MICROS(event_timestamp)  event_timestamp,
TIMESTAMP_MICROS(user_first_touch_timestamp)  user_first_touch_timestamp,
event_name,
event_params ,
user_pseudo_id ,
struct ( device.category, device.operating_system, device.web_info.hostname) as device,
struct ( geo.continent, geo.country, geo.region, geo.city) as geo,
traffic_source
platform,
struct ( ecommerce.transaction_id, 
ecommerce.unique_items,
ecommerce.purchase_revenue, 
ecommerce.total_item_quantity) as ecommerce
from ga4_source
 -- where  fullVisitorId = '3190936853180529794'
