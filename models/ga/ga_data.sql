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
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link','type':'enriched'},
    incremental_strategy = 'insert_overwrite',
)}}

with  
  ga_hostnames AS ( 
    select hostname, country, internal
    from {{ ref('ga_hostnames') }}
  ),
  
  ga_page_groups AS ( 
    select page_group,	is_show,	group_desc
    from {{ ref('ga_page_groups') }}
  ),


 hits_data as (  
  select fullVisitorId , 
    visitId ,
    hits.type as hits_type,
    hits.eventInfo.eventAction as event_action,
    hits.eventInfo.eventCategory  as  event_category,
    hits.eventInfo.eventLabel as event_label,
    hits.TRANSACTION.transactionId as web_order_id,
    {{ fix_google_value ('hits.TRANSACTION.transactionRevenue') }} as revenue,
    hits.TRANSACTION.affiliation as  affiliation,
    hits.sourcePropertyInfo.sourcePropertyTrackingId as source_property_tracking_id,
    TIMESTAMP_SECONDS(visitStartTime + hits.time) AS hits_timestampe,
    hits.time as  time ,
    hits.hitNumber as hit_number,
    hits.isInteraction as is_interaction,
    hits.page.pagePath as page_path,
    hits.page.hostname as  hostname,
    hits.page.pageTitle as page_title,
    {{ get_production_name('hits.page.pagePath') }} as page_group,
    appInfo.screenName as screen_name,
  FROM
  stage-landing.201229008.ga_sessions_20220711
LEFT JOIN UNNEST (hits) hits
  --where  fullVisitorId = '3190936853180529794'
 ),

hit_data_enr as (
select  *,
FIRST_VALUE(country IGNORE NULLS )
          OVER (PARTITION BY fullVisitorId , visitId ORDER BY hit_number ASC  ) visit_first_county
from hits_data
    left join ga_hostnames using (hostname)
    left join ga_page_groups using (page_group)
),

hit_data_enr_agg as (
select fullVisitorId , 
    visitId , 
    visit_first_county,
    ARRAY_AGG( STRUCT (  hits_type,
        event_action,
        event_category,
        event_label,
        web_order_id,
        revenue,
        affiliation,
        source_property_tracking_id,
        hits_timestampe,
        time ,
        hit_number,
        is_interaction,
        page_path,
        hostname, country, internal,
        page_title,
        page_group,	is_show,	group_desc,
        screen_name)) hits
from hit_data_enr
group by 1 , 2 , 3
)
 

SELECT
  PARSE_DATE("%Y%m%d", date) date,
  fullVisitorId full_visitor_id,
  visitId visit_id, 
  visit_first_county,
  TIMESTAMP_SECONDS(visitStartTime) AS visit_start_time,
  (
  SELECT
    AS STRUCT trafficSource.campaign campaign,
    trafficSource.medium medium,
    trafficSource.source  source ) traffic_source,
  (
  SELECT
    AS STRUCT device.deviceCategory device_category,
    device.browser browser) device,
  (
  SELECT
    AS STRUCT geoNetwork.country country,
    geoNetwork.city city,
    geoNetwork.region region ) geo_network,
  channelGrouping channel_grouping,
  (
  SELECT
    AS STRUCT 
    totals.visits visits, 
    totals.hits hits , 
    totals.pageviews pageviews , 
    totals.timeOnSite time_on_site, 
    totals.bounces, 
    ifnull( totals.transactions ,0 ) transactions, 
     {{ fix_google_value ('totals.totalTransactionRevenue') }} total_transaction_revenue , 
    (ifnull( totals.newVisits , 0 )  = 1 ) new_visits , 
     totals.sessionQualityDim session_quality_dim
    ) totals,
  hdea.hits hits
FROM
  stage-landing.201229008.ga_sessions_20220711
LEFT JOIN hit_data_enr_agg hdea using (fullVisitorId,visitId )
 -- where  fullVisitorId = '3190936853180529794'
