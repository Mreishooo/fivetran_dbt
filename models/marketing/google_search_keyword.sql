{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'google_ads', 'refresh': 'daily','connection':'bq_transfer','type':'enriched'},
)}}
with google_keyword as
(
  select 'Germany' as country , *  FROM {{ source('GoogleAd', 'Keyword_9940526481') }}  
   where _LATEST_DATE = _DATA_DATE
   and  ApprovalStatus = 'APPROVED'
   -- and CriterionId = 300545487483
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Keyword_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  and  ApprovalStatus = 'APPROVED'
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Keyword_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
  and  ApprovalStatus = 'APPROVED'
    
) ,

google_campaign as
(
  select  'Germany' as country ,*  FROM {{ source('GoogleAd', 'Campaign_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Campaign_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Campaign_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_group_ads as 
(
  select  'Germany' as country ,*  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'AdGroup_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'AdGroup_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_Criteria as 
(
  select  'Germany' as country ,*  FROM {{ source('GoogleAd', 'Criteria_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Criteria_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Criteria_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_customer as 
(
  select  'Germany' as country ,*  FROM {{ source('GoogleAd', 'Customer_9940526481') }}  
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Customer_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Customer_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_search_stat as
(
  select  'Germany' as country ,*  FROM {{ source('GoogleAd', 'SearchQueryStats_9940526481') }}  
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'SearchQueryStats_8396768808') }} 
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'SearchQueryStats_1713416990') }} 
)

SELECT distinct  'google_ads' platform ,
country ,
CreativeId ad_id,
st.CriterionId keyword_id,
key.Criteria keyword ,
st.ExternalCustomerId customer_id ,
st.CampaignID  campaign_id,
st.adGroupid  ad_group_id,
CampaignName campaign_name,
AdGroupName ad_group_name,
AdNetworkType1 ad_network_type_1,
AdNetworkType2 ad_network_type_2,
AccountCurrencyCode currency_code ,
AccountDescriptiveName account_name ,
key.qualityScore quality_score,
CreativeQualityScore creative_quality_score ,
status,
KeywordMatchType keyword_match_type,
device,
Query query, 
QueryMatchTypeWithVariant query_match_type_with_variant, 
QueryTargetingStatus query_targeting_status ,
date, 
 {{ fix_google_value ('cost') }} spend , 
clicks, 
impressions ,
conversions , 
ConversionValue conversion_value,
AllConversions all_conversions, 
AllConversionValue all_conversion_value, 
AllConversionRate all_conversion_rate, 
ConversionRate conversion_rate, 
AbsoluteTopImpressionPercentage  absolute_top_impression_percentage, 
TopImpressionPercentage top_impression_percentage,
FROM google_search_stat st
left join google_customer  using (ExternalCustomerId,country)
join google_Keyword key using (CriterionId , CampaignId , AdGroupId,country)
--left join google_Criteria using ( CriterionId,CampaignId ,AdGroupId)
left join  google_campaign   using ( CampaignId,country)
left join  google_group_ads  using ( AdGroupId,country)
