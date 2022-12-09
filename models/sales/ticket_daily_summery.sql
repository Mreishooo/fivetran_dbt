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


SELECT booking_date, country_name ,
cast( FORMAT_DATE("%Y-%m-%d", booking_date) as string ) booking_date_string,
booking_date_struct.week	iso_week,
sum(article_count) articles , 
sum(if (article_type_code= 'TICKET',  article_count , 0 )) tickets,
sum(if (article_type_code= 'TICKET',  euro_paid_price , 0 )) total_euro_paid_price_ticket,
sum(euro_paid_price ) total_euro_paid_price_article,

SAFE_DIVIDE (sum(if (article_type_code= 'TICKET',  euro_paid_price , 0 )) , sum(if (article_type_code= 'TICKET',  article_count , 0 ))) avg_ticket_euro_paid_price,
SAFE_DIVIDE (sum(euro_paid_price) , sum(article_count) ) avg_article_price 

 FROM ticket_sales
 group by 1 , 2 ,3 , 4



 