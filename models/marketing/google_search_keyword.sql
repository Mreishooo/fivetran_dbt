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
  select *  FROM {{ source('GoogleAd', 'Keyword_9940526481') }}  
   where _LATEST_DATE = _DATA_DATE
   and  ApprovalStatus = 'APPROVED'
   -- and CriterionId = 300545487483
    
) ,

google_campaign as
(
  select *  FROM {{ source('GoogleAd', 'Campaign_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_group_ads as 
(
  select *  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_Criteria as 
(
  select *  FROM {{ source('GoogleAd', 'Criteria_9940526481') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_customer as 
(
  select *  FROM {{ source('GoogleAd', 'Customer_9940526481') }}  
  where  _LATEST_DATE = _DATA_DATE
) ,

google_search_stat as
(
   select *  FROM {{ source('GoogleAd', 'SearchQueryStats_9940526481') }}  
)

SELECT distinct 'google_ads' platform ,
st.CriterionId Keyword_id,
key.Criteria Keyword ,
st.ExternalCustomerId Customer_id ,
st.CampaignID  campaign_id,
st.adGroupid  ad_group_id,
CampaignName campaign_name,
AdGroupName ad_group_name,
AdNetworkType1 ad_network_type_1,
AdNetworkType2 ad_network_type_2,
AccountCurrencyCode currency_code ,
AccountDescriptiveName account_name ,
key.qualityScore quality_score,
status,
KeywordMatchType keyword_match_type,
device,
Query query, 
QueryMatchTypeWithVariant query_match_type_with_variant, 
QueryTargetingStatus query_targeting_status ,
date, 
 {{ fix_google_value ('cost') }} cost , 
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
left join google_customer  using (ExternalCustomerId)
join google_Keyword key using (CriterionId , CampaignId , AdGroupId)
--left join google_Criteria using ( CriterionId,CampaignId ,AdGroupId)
left join  google_campaign   using ( CampaignId)
left join  google_group_ads  using ( AdGroupId)
