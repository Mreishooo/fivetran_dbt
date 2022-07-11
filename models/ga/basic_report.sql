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
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link'},
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

  ga_data AS (
  SELECT
    PARSE_DATE("%Y%m%d",date) date,
    fullVisitorId full_visitor_id,
    visitId visit_id,
    TIMESTAMP_SECONDS(visitStartTime) AS visit_start_time,
    trafficSource.campaign campaign,
    trafficSource.medium medium,
    trafficSource.source source,
    device.deviceCategory device_category,
    device.browser browser,
    geoNetwork.country geo_country,
    geoNetwork.city geo_city,
    geoNetwork.region geo_region,
    hits.type,
    hits.eventInfo.eventAction event_action,
    hits.eventInfo.eventCategory event_category,
    hits.eventInfo.eventLabel event_label,
    hits.TRANSACTION.transactionId web_order_id,
    {{ fix_google_value ('hits.TRANSACTION.transactionRevenue') }}  revenue,  
    hits.TRANSACTION.affiliation affiliation,
    hits.TRANSACTION TRANSACTION,
    hits.sourcePropertyInfo.sourcePropertyTrackingId source_property_tracking_id,
    TIMESTAMP_SECONDS(hits.time) AS hits_time,
    hits.hitNumber hit_number,
    hits.isInteraction is_interaction,
    appInfo.screenName screen_name,
    totals.newVisits new_visits,
    hits.page.pagePath page_path,
    hits.page.hostname hostname,
    hits.page.pageTitle page_title,
    {{ get_production_name('hits.page.pagePath') }}page_group,
    channelGrouping channel_grouping
  FROM
    {{ source('201229008', 'ga_sessions_2022*') }} 
  LEFT JOIN
    UNNEST (hits) hits
    where true 
    {% if is_incremental() %}
      and   _TABLE_SUFFIX >= format_date("%m%d",current_date()-2)
    {% endif %}
  )
  
SELECT
  *
FROM
  ga_data
  left join ga_hostnames using (hostname)
  left join ga_page_groups using (page_group)