{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link','type':'enriched'},
)}}

with  
  ga_source AS (
   SELECT 'Germany' as country , *  
   FROM {{ source( '75566045','ga_sessions_202*') }}
   union all 
   SELECT 'Netherland' as country , *  
   FROM {{ source( '97634084','ga_sessions_2022*') }}
  ),

  ga_page_groups AS ( 
    SELECT *
    FROM {{ ref('ga_page_groups') }}
  ),

  ga_event_type AS ( 
    SELECT *
    FROM {{ ref('ga_event_types') }}
  ),

 hits_data as (  
  SELECT  country,
    fullVisitorId , 
    visitId ,
    hits.type as hits_type,
    hits.eventInfo.eventAction as event_action,
    hits.eventInfo.eventCategory  as  event_category,
    hits.eventInfo.eventLabel as event_label,
    hits.TRANSACTION.transactionId as web_order_id,
    {{ fix_google_value ('hits.TRANSACTION.transactionRevenue') }} as revenue,
    hits.TRANSACTION.affiliation as  affiliation,
    hits.sourcePropertyInfo.sourcePropertyTrackingId as source_property_tracking_id,
    TIMESTAMP_SECONDS(visitStartTime + cast (hits.time/1000 as int64)) AS hits_timestampe,
    hits.time as  time ,
    hits.hitNumber as hit_number,
    hits.isInteraction as is_interaction,
    hits.page.pagePath as page_path,
    hits.page.hostname as  hostname,
    hits.page.pageTitle as page_title,
    {{ get_production_name('country','hits.page.pagePath') }} as page_group,
    appInfo.screenName as screen_name,
    hits.experiment
  FROM ga_source
LEFT JOIN UNNEST (hits) hits
  --where  fullVisitorId = '3190936853180529794'
 ),

hit_data_enr as (
SELECT  *
FROM hits_data
    --left join ga_hostnames using (hostname)
    left join ga_page_groups using (page_group)
),

hit_data_enr_agg as (
SELECT  
    fullVisitorId , 
    visitId , 
    country,
    ARRAY_AGG( STRUCT (   hits_type,
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
        hostname,  
        page_title,
        page_group,	
        is_show,	
        page_group_desc,
        page_group_type,
        screen_name
        )) hits
from hit_data_enr
group by 1 , 2 , 3
),

landing_page as (
SELECT  distinct
    fullVisitorId , 
    visitId , 
    country,
    page_path landing_page_path,
    page_group landing_page_group,
    page_group_desc landing_page_group_desc,
    page_group_type landing_page_group_type
    from hit_data_enr
    where hit_number = 1 
    
),

hits_data_experiment
as (
SELECT country,
    fullVisitorId , 
    visitId , 
    ARRAY_AGG( STRUCT (  ex.experimentid as experiment_id,
    ex.experimentvariant as experiment_variant))  experiment
from hit_data_enr , unnest ( experiment ) ex
group by 1 , 2 ,3
--qualify  row_number() OVER (PARTITION BY fullVisitorId ,visitId   ORDER BY  time DESC NULLS first )  = 1  
),


ga_optin_sessions as (
  select distinct country , fullVisitorId ,visitId ,true optin
  from hits_data
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='optin'
),

ga_newsletter_sessions as (
  select distinct country , fullVisitorId, visitId  ,true newsletter
  from hits_data 
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='newsletter'
),

ga_ecommerce_sessions as (
  select distinct country , fullVisitorId ,visitId ,true ecommerce
  from hits_data
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='ecommerce'
)
 

SELECT 
  PARSE_DATE("%Y%m%d", date) date,
  fullVisitorId full_visitor_id,
  visitId visit_id, 
  country,
  TIMESTAMP_SECONDS(visitStartTime) AS visit_start_time,
  (
  SELECT
    AS STRUCT  
    trafficSource.campaign campaign,
    trafficSource.medium medium,
    trafficSource.source  source,
    trafficSource.adwordsClickInfo.adNetworkType ad_network_type ) traffic_source,
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
    ifnull( optin, false) optin ,
    ifnull( newsletter, false)  newsletter ,
    ifnull( ecommerce, false)  ecommerce ,
  hdea.hits hits,
  experiment,
  --experiment_id,
  --experiment_variant
  ( SELECT
    AS STRUCT
    landing_page_path,
    landing_page_group,
    landing_page_group_desc,
    landing_page_group_type
  ) landing_page
FROM ga_source
LEFT JOIN hit_data_enr_agg hdea using (country , fullVisitorId,visitId )
left join ga_optin_sessions using (country , fullVisitorId,visitId )
left join ga_newsletter_sessions using (country , fullVisitorId,visitId )
left join ga_ecommerce_sessions using (country , fullVisitorId,visitId )
left join hits_data_experiment using  (country , fullVisitorId,visitId )
left join Landing_page using  (country , fullVisitorId,visitId )
 -- where  fullVisitorId = '3190936853180529794'
