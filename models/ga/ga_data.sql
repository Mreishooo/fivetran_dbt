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
   SELECT 'Germany' as country  ,PARSE_DATE("%Y%m%d", date) date , * except (date)  
   FROM {{ source( '75566045','ga_sessions_202*') }}
   union all 
   SELECT 'Netherlands' as country ,PARSE_DATE("%Y%m%d", date) date , * except (date)  
   FROM {{ source( '97634084','ga_sessions_2022*') }}
   union all 
   SELECT 'France' as country  ,PARSE_DATE("%Y%m%d", date) date , * except (date)  
   FROM {{ source( '198014170','ga_sessions_2022*') }}
   union all 
   SELECT 'Spain' as country  ,PARSE_DATE("%Y%m%d", date) date , * except (date)  
   FROM {{ source( '198328733','ga_sessions_2022*') }}
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
    date,
    fullVisitorId , 
    visitId , 
    ifnull(totals.transactions,0)   totals_transactions,
    hits.type as hits_type,
    hits.eventInfo.eventAction as event_action,
    hits.eventInfo.eventCategory  as  event_category,
    hits.eventInfo.eventLabel as event_label,
    hits.TRANSACTION.transactionId as transaction_id,
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
    hits.experiment,
    hits.product
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
    date,
    ARRAY_AGG( STRUCT (   hits_type,
        event_action,
        event_category,
        event_label,
        transaction_id,
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
group by 1 , 2 , 3 ,4
),

landing_page as (
SELECT  distinct
    fullVisitorId , 
    visitId , 
    country,
    date,
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
    date,
    ARRAY_AGG( STRUCT (  ex.experimentid as experiment_id,
    ex.experimentvariant as experiment_variant))  experiment
from hit_data_enr , unnest ( experiment ) ex
group by 1 , 2 ,3 ,4
--qualify  row_number() OVER (PARTITION BY fullVisitorId ,visitId   ORDER BY  time DESC NULLS first )  = 1  
),


hits_products as (
SELECT country,
    fullVisitorId , 
    visitId , 
    date ,
    count(distinct product.v2ProductName ) unique_articles,
    sum(product.productQuantity) total_articles,
    ARRAY_AGG( STRUCT ( transaction_id as transaction_id , 
                        product.v2ProductName as product_name, 
                        {{ fix_google_value ('product.productRevenue') }} as product_revenue ,  
                        {{ fix_google_value ('product.productPrice') }}  as product_price ,
                        product.productQuantity as product_quantity ,
                        {{ get_production_name('country','product.v2ProductName')}} as production_location_id ))  product
from hit_data_enr ,  unnest ( product ) product 
where transaction_id is not null 
and ifnull(totals_transactions,0) > 0 
group by 1 , 2 ,3 ,4
--qualify  row_number() OVER (PARTITION BY transaction_id  ORDER BY  time DESC NULLS first )  = 1  
),

ga_optin_sessions as (
  select distinct country , fullVisitorId ,visitId ,date ,true optin
  from hits_data
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='optin'
),

ga_newsletter_sessions as (
  select distinct country , fullVisitorId, visitId ,date  ,true newsletter
  from hits_data 
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='newsletter'
),

ga_ecommerce_sessions as (
  select distinct country , fullVisitorId ,visitId ,date ,true ecommerce
  from hits_data
  join ga_event_type using ( event_category)
WHERE  hits_type = 'EVENT' and event_type ='ecommerce'
)
 

SELECT 
  date,
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
    ifnull({{ fix_google_value ('totals.totalTransactionRevenue') }},0) total_transaction_revenue , 
    (ifnull( totals.newVisits , 0 )  = 1 ) new_visits , 
     totals.sessionQualityDim session_quality_dim,
    ifnull( unique_articles,0 ) unique_articles,
    ifnull(total_articles,0 ) total_articles
    ) totals,
    ifnull( optin, false) optin ,
    ifnull( newsletter, false)  newsletter ,
    ifnull( ecommerce, false)  ecommerce ,
  hdea.hits hits,
  experiment,
  product,
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
LEFT JOIN hit_data_enr_agg hdea using (country , fullVisitorId,visitId,date )
left join ga_optin_sessions using (country , fullVisitorId,visitId,date  )
left join ga_newsletter_sessions using (country , fullVisitorId,visitId ,date )
left join ga_ecommerce_sessions using (country , fullVisitorId,visitId,date  )
left join hits_data_experiment using  (country , fullVisitorId,visitId ,date )
left join hits_products using  (country , fullVisitorId,visitId,date  )
left join Landing_page using  (country , fullVisitorId,visitId,date  )
 -- where  fullVisitorId = '3190936853180529794'
