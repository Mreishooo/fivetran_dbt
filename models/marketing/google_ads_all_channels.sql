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
  select 'Germany' as country ,* FROM {{ source('GoogleAd', 'Campaign_9940526481') }}    
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'Campaign_8396768808') }}    
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'Campaign_1713416990') }}    
  where  _LATEST_DATE = _DATA_DATE
) ,

google_group_ads as 
(
  select'Germany' as country , *  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }}     
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'AdGroup_8396768808') }}    
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'AdGroup_1713416990') }}    
  where  _LATEST_DATE = _DATA_DATE
  
) ,
google_ads as 
(
  select 'Germany' as country ,*  FROM {{ source('GoogleAd', 'Ad_9940526481') }}   
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'Ad_8396768808') }}    
  where  _LATEST_DATE = _DATA_DATE 
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'Ad_1713416990') }}    
  where  _LATEST_DATE = _DATA_DATE 
) ,
google_customer as 
(
  select 'Germany' as country , *  FROM {{ source('GoogleAd', 'Customer_9940526481') }}   
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'Customer_8396768808') }}    
  where  _LATEST_DATE = _DATA_DATE
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'Customer_1713416990') }}    
  where  _LATEST_DATE = _DATA_DATE
  
),

google_ad_stats as
(
  select 'Germany' as country ,*  FROM {{ source('GoogleAd', 'AdBasicStats_9940526481') }}  
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'AdBasicStats_8396768808') }}
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'AdBasicStats_1713416990') }}    
 
) ,

VideoStats as
(
  SELECT 'Germany' as country , * FROM {{ source('GoogleAd', 'VideoBasicStats_9940526481') }}  
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'VideoBasicStats_8396768808') }} 
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'VideoBasicStats_1713416990') }}    

),
VideoNonClickStats as
  (
  SELECT 'Germany' as country ,  * FROM {{ source('GoogleAd', 'VideoNonClickStats_9940526481') }}   
  union all 
  select 'France' as country ,* FROM {{ source('google_ad_fr', 'VideoNonClickStats_8396768808') }}   
  union all 
  select 'Spain' as country ,* FROM {{ source('google_ad_sp', 'VideoNonClickStats_1713416990') }}   

  )

  
SELECT distinct   'google_ads' platform ,
country,
CreativeId ad_id,
AdType ad_type,
CombinedApprovalStatus approval_status,
ads.ExternalCustomerId customer_id ,
ads.CampaignID  campaign_id,
ads.adGroupid  ad_group_id,
CampaignName campaign_name,
SPLIT(CampaignName,'_') [safe_OFFSET(3)] campaign_type_split,
case country 
when 'Spain' then 
  SPLIT(CampaignName,'-') [safe_OFFSET(2)]
else SPLIT(CampaignName,'_') [safe_OFFSET(4)] 
end show,
SPLIT(CampaignName,'_') [safe_OFFSET(5)] theatre_location,
SPLIT(CampaignName,'_') [safe_OFFSET(12)] free_text,
AdGroupName ad_group_name,
AdNetworkType1 ad_network_type1,
AdNetworkType2 ad_network_type2,
AccountDescriptiveName account_name,
AccountCurrencyCode currency_code, 
CampaignTrialType campaign_type,
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
{{ fix_google_value ('cost') }}  spend  ,
ad_stats.conversions , 
ad_stats.ConversionValue conversion_value, 
engagements,
VideoQuartile25Rate video_quartile_25rate, 
VideoQuartile50Rate video_quartile_50rate, 
VideoQuartile75Rate video_quartile_75rate, 
VideoQuartile100Rate video_quartile_100rate, 
VideoViews video_views
FROM google_ads   ads
left join google_customer using (ExternalCustomerId,country)
left join  google_campaign   using (CampaignId,country)
left join  google_group_ads  using ( AdGroupId,CampaignId,country)
left join google_ad_stats ad_stats  using ( AdGroupId,CampaignId,CreativeId,country) 
--left join VideoStats    using ( AdGroupId,CampaignId,CreativeId)  
left join  VideoNonClickStats  using ( CreativeId,AdGroupId,CampaignId,AdNetworkType1,AdNetworkType2,Date,Device,country)
