{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))',
  'timestamp(date_sub(current_date, interval 2 day))'
] %}

{{ config(
    materialized='incremental',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link','type':'mart'},
    incremental_strategy = 'insert_overwrite',
)}}

with ga_data AS (
   SELECT *  
   FROM {{ ref( 'ga_data') }}
  )

SELECT  
date , 
country ,
COUNT ( DISTINCT  concat (full_visitor_id , visit_id )) visitors ,
COUNT ( DISTINCT full_visitor_id ) unique_visitors ,
COUNT (DISTINCT IF(totals.new_visits, full_visitor_id, NULL))  new_vistor ,
sum (totals.pageviews) pageviews,
sum ( totals.transactions) transactions,
sum ( totals.total_transaction_revenue) total_transaction_revenue,
sum (totals.time_on_site) time_on_site,
COUNT(DISTINCT IF(totals.transactions >0, full_visitor_id, NULL))   /  count  ( distinct full_visitor_id ) converstion ,
COUNT(DISTINCT IF(optin, full_visitor_id, NULL))   optin ,
COUNT(DISTINCT IF(newsletter, full_visitor_id, NULL))  newsletter ,
COUNT(DISTINCT IF(ecommerce, full_visitor_id, NULL))  ecommerce ,
FROM ga_data 
where true 
    {% if is_incremental() %}
      and   date >=  current_date() - 2
    {% endif %}
group by 1,2 