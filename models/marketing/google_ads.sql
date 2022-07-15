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
  select *  FROM {{ source('GoogleAd', 'AdBasicStats_9940526481') }}  
) ,

google_campaign as
(
  select *  FROM {{ source('GoogleAd', 'Campaign_9940526481') }} 
) 

SELECT 'google_ads' platform ,
AdvertisingChannelType account_name ,
date , 
clicks clicks ,
impressions impressions , 
conversions  conversions , 
conversionValue conversions_value, 
 {{ fix_google_value ('cost') }}  as spend, 
interactions interactions
FROM google_ad_basic 
left join  google_campaign   using ( CampaignId)
--where date >='2022-05-22'
 