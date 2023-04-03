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
   union all
   select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Keyword_1713416990') }} 
   where  _LATEST_DATE = _DATA_DATE
    
) ,

google_campaign as
(
  select 'Germany' as country , *  FROM {{ source('GoogleAd', 'Campaign_9940526481') }} 
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
  select  'Germany' as country , *  FROM {{ source('GoogleAd', 'AdGroup_9940526481') }} 
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
  select  'Germany' as country , *  FROM {{ source('GoogleAd', 'Criteria_9940526481') }} 
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
  select 'Germany' as country , *  FROM {{ source('GoogleAd', 'Customer_9940526481') }}  
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Customer_8396768808') }} 
  where  _LATEST_DATE = _DATA_DATE
  union all
  select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'Customer_1713416990') }} 
  where  _LATEST_DATE = _DATA_DATE
) ,

google_keyword_cross as
(
   select 'Germany' as country ,*  FROM {{ source('GoogleAd', 'KeywordCrossDeviceStats_9940526481') }}  
   union all
   select 'France' as country , *  FROM {{ source('google_ad_fr', 'KeywordCrossDeviceStats_8396768808') }}
   union all
   select 'Spain' as country , *  FROM {{ source('google_ad_sp', 'KeywordCrossDeviceStats_1713416990') }} 

)

SELECT distinct  'google_ads' platform ,  
country ,
st.CriterionId keyword_id,
key.Criteria keyword ,
st.ExternalCustomerId customer_id ,
st.CampaignID  campaign_id,
st.adGroupid  ad_group_id,
CampaignName campaign_name,
AdGroupName ad_group_name,
AdNetworkType1 ad_network_type_1,
AdNetworkType2 ad_network_type_2,
 date,
AccountCurrencyCode currency_code ,
AccountDescriptiveName account_name ,
SearchImpressionShare search_impression_share,
SearchAbsoluteTopImpressionShare search_absolute_top_impression_share,
SearchBudgetLostAbsoluteTopImpressionShare search_budget_lost_absolute_top_impression_share,
SearchBudgetLostTopImpressionShare search_budget_lost_top_impression_share,
SearchExactMatchImpressionShare search_exact_match_impression_share,
SearchRankLostAbsoluteTopImpressionShare search_rank_lost_absolute_top_impression_share,
SearchRankLostImpressionShare search_rank_lost_impression_share,
SearchRankLostTopImpressionShare search_rank_lost_top_impression_share,
SearchTopImpressionShare search_top_impression_share,
TopImpressionPercentage top_impression_percentage,
AbsoluteTopImpressionPercentage  absolute_top_impression_percentage

FROM google_keyword_cross st
left join google_customer  using (ExternalCustomerId,country)
join google_Keyword key using (CriterionId , CampaignId , AdGroupId,country)
--left join google_Criteria using ( CriterionId,CampaignId ,AdGroupId)
left join  google_campaign   using ( CampaignId,country)
left join  google_group_ads  using ( AdGroupId,country)
