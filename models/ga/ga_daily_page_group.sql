/*{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))',
  'timestamp(date_sub(current_date, interval 2 day))'
] %}*/

{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'enriched_table', 'refresh': 'daily','connection':'ga_link','type':'mart'}
)}}

with ga_data as 
( select * FROM {{ ref('ga_data' ) }} ),


mpl AS ( 
    select *
    FROM {{ ref('sales_production_location') }})

SELECT date,
country,
page_group,
group_desc,
is_show, 
mpl.production_name ,
count(distinct  concat (full_visitor_id , visit_id )) visitors ,
count(distinct full_visitor_id ) unique_visitors ,
count(DISTINCT IF(totals.new_visits, full_visitor_id, NULL))  new_vistor ,
count(*) impressions 
FROM ga_data
join unnest  (hits)  hits  
left join mpl on  page_group = Production_Location_Id   
where true 
and hits.hits_type = 'PAGE' 
group by 1 , 2 , 3 , 4 ,5 ,6
