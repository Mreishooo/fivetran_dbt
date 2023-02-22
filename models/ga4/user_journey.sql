{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga4', 'refresh': 'daily','connection':'ga_link','type':'mart'},
)}}

with  
  ga4_data AS ( 
    SELECT *
    FROM {{ ref('ga4_data') }}
  )


, journey_events as (
    SELECT distinct  country, date, user_pseudo_id,-- session_id ,
      show.value.string_value show , event_name,page_location landing_page,
    FROM ga4_data 
        left join unnest (event_params) show on show.key = 'show' 
    WHERE date >= "2023-02-1"
      and event_name in (
      'ticketshop',
      'ticketshop_tabtwo',
      'search_api',
      'add_to_cart',
      'begin_checkout', 
      'login', 
      'add_shipping_info', 
      'add_payment_info', 
      'checkout_summary', 
      'purchase') 
   )

 select country, date , show,landing_page,
  countif(event_name ='ticketshop' )  calendar, 
 --count ( distinct (if (event_name ='ticketshop' ,user_pseudo_id, null  )))hh,
  countif(event_name ='ticketshop_tabtwo' ) seat_map,
  countif(event_name ='search_api' ) search_api,
  countif(event_name ='add_to_cart' ) add_to_cart,
  countif(event_name ='begin_checkout' ) begin_checkout,
  countif(event_name ='login' ) login,
  countif(event_name ='add_shipping_info' ) add_shipping_info,
  countif(event_name ='add_payment_info' ) add_payment_info,
  countif(event_name ='checkout_summary' ) checkout_summary,
  countif(event_name ='purchase' ) purchase,
 from journey_events

 group by 1 , 2,3,4