{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'google_ads', 'refresh': 'daily','connection':'bq_transfer','type':'enriched'},
)}}
with google_ad_basic as
(
  select  'Germany' as country , *  FROM {{ source('GoogleAd', 'AdBasicStats_9940526481') }}  
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'AdBasicStats_8396768808') }}  
) ,

google_campaign as
(
  select   'Germany' as country ,*  FROM {{ source('GoogleAd', 'Campaign_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Campaign_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_group_ads as 
(
  select   'Germany' as country , *  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'AdGroup_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
) 

SELECT 'google_ads' platform , 
country,
AdvertisingChannelType account_name ,
CampaignName campaign_name,
AdGroupName ad_group_name,
date , 
clicks clicks ,
impressions impressions , 
conversions  conversions , 
conversionValue conversions_value, 
 {{ fix_google_value ('cost') }}  as spend, 
interactions interactions
FROM google_ad_basic 
left join  google_campaign   using ( CampaignId,country)
left join  google_group_ads  using ( AdGroupId ,country)
--where date >='2022-05-22'
 