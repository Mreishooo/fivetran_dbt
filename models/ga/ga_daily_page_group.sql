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
    labels = {'source': 'enriched_table', 'refresh': 'daily','connection':'ga_link','type':'mart'},
    incremental_strategy = 'insert_overwrite',
)}}

with ga_data as ( select * FROM {{ ref('ga_data' ) }} )

SELECT date,
country,
page_group,
group_desc,
is_show, 
count(distinct  concat (full_visitor_id , visit_id )) Visitors ,
count(distinct full_visitor_id ) unique_visitors ,
COUNT(DISTINCT IF(totals.new_visits, full_visitor_id, NULL))  new_vistor ,
count(*) impressions 
FROM ga_data
join unnest  (hits)  hits     
where true 
    {% if is_incremental() %}
      and   date >=  current_date() - 2
    {% endif %}
and hits.hits_type = 'PAGE' 
group by 1 , 2 , 3 , 4 ,5 
