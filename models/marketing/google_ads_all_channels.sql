{{ config(
    materialized='table',
      partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'google_ads', 'refresh': 'daily','connection':'bq_transfer','type':'enriched'},
)}}

with 
google_campaign as
(
  select * FROM {{ source('GoogleAd', 'Campaign_9940526481') }}    
  where  _LATEST_DATE = _DATA_DATE
) ,

google_group_ads as 
(
  select *  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }}     
  where  _LATEST_DATE = _DATA_DATE
) ,
google_ads as 
(
  select *  FROM {{ source('GoogleAd', 'Ad_9940526481') }}   
  where  _LATEST_DATE = _DATA_DATE
) ,
google_customer as 
(
  select *  FROM {{ source('GoogleAd', 'Customer_9940526481') }}   
  where  _LATEST_DATE = _DATA_DATE
  
),

google_ad_stats as
(
  select *  FROM {{ source('GoogleAd', 'AdBasicStats_9940526481') }}  
) ,

VideoStats as
(
  SELECT * FROM {{ source('GoogleAd', 'VideoBasicStats_9940526481') }}  
),
VideoNonClickStats as
  (
  SELECT  * FROM {{ source('GoogleAd', 'VideoNonClickStats_9940526481') }}   
  )

  
SELECT distinct   'google_ads' platform ,
CreativeId ad_id,
AdType ad_type,
status ad_satatus,CombinedApprovalStatus approval_status,
ads.ExternalCustomerId customer_id ,
ads.CampaignID  campaign_id,
ads.adGroupid  ad_group_id,
CampaignName campaign_name,
AdGroupName ad_group_name,
AdNetworkType1 ad_network_type1,
AdNetworkType2 ad_network_type2,
AccountDescriptiveName account_name,
AccountCurrencyCode currency_code, 
CampaignTrialType Campaign_Type,
Status ad_status,
AdGroupType ad_group_type	, 
AdGroupStatus ad_group_status,
Date date, 
StartDate campaign_start_date, 
EndDate campaign_end_date, 
CampaignStatus campaign_status,
google_campaign.BiddingStrategyType  bidding_strategy_type,
device ,
CampaignTrialType campaign_trial_type, 
AdvertisingChannelType advertising_channel_type,
ad_stats.clicks,
ad_stats.impressions ,
{{ fix_google_value ('cost') }}  cost  ,
ad_stats.conversions , 
ad_stats.ConversionValue conversion_value, 
engagements,
VideoQuartile25Rate video_quartile_25rate, 
VideoQuartile50Rate video_quartile_50rate, 
VideoQuartile75Rate video_quartile_75rate, 
VideoQuartile100Rate video_quartile_100rate, 
VideoViews video_views
FROM google_ads   ads
left join google_customer using (ExternalCustomerId)
left join  google_campaign   using (CampaignId)
left join  google_group_ads  using ( AdGroupId,CampaignId)
left join google_ad_stats ad_stats  using ( AdGroupId,CampaignId,CreativeId) 
--left join VideoStats    using ( AdGroupId,CampaignId,CreativeId)  
left join  VideoNonClickStats  using ( CreativeId,AdGroupId,CampaignId,AdNetworkType1,AdNetworkType2,Date,Device)
