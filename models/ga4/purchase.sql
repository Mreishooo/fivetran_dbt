{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga4', 'refresh': 'daily','connection':'ga4_link','type':'mart'},
)}}

with  ga4_data AS ( 
    SELECT *
    FROM {{ ref('ga4_data') }}
    qualify  row_number() OVER (PARTITION BY country, ecommerce.transaction_id ORDER BY  event_timestamp DESC ,  user_pseudo_id  NULLS last )  =  1
  )



select country ,
date ,
event_timestamp
user_pseudo_id,
session_id,
event_name,
transaction_id.value.string_value transaction_id ,
show.value.string_value show ,
value.value.double_value revenue ,
page_referrer.value.string_value page_referrer ,
affiliation.value.string_value affiliation ,
page_location.value.string_value page_location ,
ignore_referrer.value.string_value ignore_referrer ,
location.value.string_value location ,
engagement_time_msec.value.int_value engagement_time_msec ,
count ( distinct items.item_id) unique_items,
sum (items.quantity ) items_quantity,
ARRAY_AGG( STRUCT ( items.item_id as item_id,
items.item_name as item_name ,
items.item_brand as item_brand,
items.item_variant as item_variant,
items.item_category as item_category,
items.price as price,
items.quantity as quantity,
items.item_revenue  as revenue )) items ,

  FROM ga4_data
  left join unnest(items) items
  left join unnest (event_params) page_referrer on  page_referrer.key = 'page_referrer' 
  left join unnest (event_params) affiliation on   affiliation.key = 'affiliation'   
  left join unnest (event_params) page_location on page_location.key = 'page_location'   
  left join unnest (event_params) ignore_referrer on ignore_referrer.key = 'ignore_referrer'   
  left join unnest (event_params) transaction_id on transaction_id.key = 'transaction_id'   
  left join unnest (event_params) show on show.key = 'show'   
  left join unnest (event_params) value on value.key = 'value'      
  left join unnest (event_params) location on location.key = 'location'     
  left join unnest (event_params) engagement_time_msec on engagement_time_msec.key = 'engagement_time_msec'  
  where event_name = 'purchase' 
  --qualify  row_number() OVER (PARTITION BY ecommerce.transaction_id ORDER BY  event_timestamp DESC ,  user_pseudo_id  NULLS last )  =  1
 -- and items.price <> value.value.double_value
 --and ecommerce.transaction_id  = '2041962412'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14