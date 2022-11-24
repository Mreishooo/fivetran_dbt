{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "booking_date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'sales', 'refresh': 'view','connection':'mdb','type':'mart'},
)}}

with sales AS ( 
    select *
    FROM {{ ref('ticket_sales') }}) 


select 
    country_code,
    country_name country ,
    booking_date, 
    booking_date_struct.week,
    if( production_location_id LIKE'NL - HGIM22%','NL - HGIM22',production_location_id) production_location_id,  
    Production_name,
    article_type_code,
    price_type_name,
    fact_ticket_sales_id,
    web_order_number,
    cancellation_status,
    article_count,
    euro_paid_price

from sales
where 
distribution_point in ( 'stage-entertainment.nl','stage-entertainment.fr' ,'musicals.de' ,'ElReyLeon.es','Tina.es') 
and booking_date>='2021-01-01'

  
