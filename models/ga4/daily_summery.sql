{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga4_ga', 'refresh': 'daily','connection':'ga_link','type':'enriched'},
)}}

with  
  ga4_data AS ( 
    SELECT *
    FROM {{ ref('ga4_data') }}
  ),
  purchase_date as (
  select * 
  from {{ref('purchase')}}
  ),
  
  ga_daily_sessions as (
  select * 
  from {{ref('ga_daily_sessions')}}
  --where date < '2022-10-10'
  ),


  traffic as (
    SELECT  
    date   , 
    country ,
    COUNTIF( event_name = 'session_start' )  sessions ,
    COUNT ( DISTINCT  user_pseudo_id  ) unique_visitors ,
    COUNT ( distinct ( IF (event_name in ('first_open' , 'first_visit'), user_pseudo_id, null)))  new_vistor ,
    COUNTIF( event_name = 'page_view' )  pageviews,
    COUNTIF( event_name = 'newsletter_signup' )  newsletter_signup,
    COUNTIF( event_name = 'newsletter_unsubscribe' )  newsletter_unsubscribe,
    COUNTIF( event_name = '404_error' )  error_pages,
    COUNTIF( event_name = 'cookie_consent' )  optin
    from ga4_data
    group by 1,2
  ),

  purchase as (
    select country , 
    date , 
    count (transaction_id ) transactions,
    sum (revenue) revenue, 
    sum (unique_items) unique_articles, 
    sum (items_quantity) articles 
    from purchase_date
    group by 1,2
  )

SELECT  
* ,
transactions/sessions  as converstion 
FROM traffic
  join purchase using (country,date)
where true --and date = '2022-10-01'
union all
select country, date ,visitors, unique_visitors, new_vistor, pageviews, newsletter,null , null, optin, transactions, total_transaction_revenue , null, null ,converstion
from ga_daily_sessions


