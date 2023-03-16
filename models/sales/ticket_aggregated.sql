{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "booking_date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with  
  ticket_sales AS ( 
    SELECT *
    FROM {{ ref('ticket_sales') }}
  )


SELECT  country_name ,
booking_date,
booking_date_struct.month month, 
booking_date_struct.week	iso_week,
booking_date_struct.month_name month_name,
booking_date_struct.year year,
concat (booking_date_struct.month_name ,' - ' ,booking_date_struct.year) month_year,
production_location_id,
production_name,
Cancellation_status,
theatre_city,
theatre_name,

sum(article_count) articles ,
sum(if (article_type_code= 'TICKET',  article_count , 0 )) tickets,
sum(euro_paid_price ) total_euro_paid_price_article,
sum(if (article_type_code= 'TICKET',  euro_paid_price , 0 )) total_euro_paid_price_ticket,

 FROM ticket_sales
 group by 1 , 2 ,3 , 4,5,6,7,8,9,10,11,12


 