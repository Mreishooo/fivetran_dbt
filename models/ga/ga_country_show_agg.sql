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

SELECT  country,
page_group,
group_desc,
is_show, 
date,
count (distinct full_visitor_id) distinct_visitor,
countif(new_visits) new_visits , 
count(*) impressions 
FROM {{ ref('ga_data' ) }}    
where true 
    {% if is_incremental() %}
      and   date >=  current_date() - 2
    {% endif %}
and type = 'PAGE' 
group by 1 , 2 , 3 , 4 ,5 
