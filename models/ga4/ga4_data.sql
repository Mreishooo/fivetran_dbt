{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga4', 'refresh': 'daily','connection':'ga_link','type':'enriched'},
)}}

with  
  ga4_source AS (
   SELECT 'Germany' as country , *  
   FROM {{ source( 'analytics_272653220','events_2023*') }}
   union all 
   SELECT 'Spain' as country , *  
   FROM {{ source( 'analytics_272654930','events_2023*') }}
   union all 
   SELECT 'Netherlands' as country , *  
   FROM {{ source( 'analytics_272676813','events_2023*') }}
   union all 
   SELECT 'France' as country , *  
   FROM {{ source( 'analytics_272685101','events_2023*') }}
   
  )

, ga4_date as (
SELECT  country,
PARSE_DATE("%Y%m%d",event_date)  date ,
TIMESTAMP_MICROS(event_timestamp)  event_timestamp,
TIMESTAMP_MICROS(user_first_touch_timestamp)  user_first_touch_timestamp,
user_pseudo_id ,
ga_session_id.value.int_value session_id,
event_name,
event_params ,
struct ( device.category, device.operating_system, device.web_info.hostname) as device,
struct ( geo.continent, geo.country, geo.region, geo.city) as geo,
traffic_source
platform,
struct ( ecommerce.transaction_id, 
ecommerce.unique_items,
ecommerce.purchase_revenue, 
ecommerce.total_item_quantity) as ecommerce,
items
from ga4_source  
left join unnest (event_params) ga_session_id on ga_session_id.key = 'ga_session_id' 
where PARSE_DATE("%Y%m%d",event_date) >= '2022-10-10'
qualify  row_number() OVER (PARTITION BY country ,date ,user_pseudo_id ,event_name ,event_timestamp ORDER BY  event_timestamp DESC )  = 1 
 -- where  fullVisitorId = '3190936853180529794'
)

,landing_page as (
  SELECT country,
    user_pseudo_id,
    session_id,
    page_location.value.string_value page_location,
    page_referrer.value.string_value page_referrer
 FROM ga4_date 
 left join unnest (event_params) page_location on page_location.key = 'page_location' 
 left join unnest (event_params) page_referrer on page_referrer.key = 'page_referrer' 
 
 WHERE 
 true
 --and  date = "2023-01-20"
 and event_name='session_start'
 and session_id is not null 
 qualify  row_number() OVER (PARTITION BY country  ,user_pseudo_id ,session_id  ORDER BY  event_timestamp )  = 1 


)

select * from ga4_date left join landing_page using (country,user_pseudo_id,session_id )