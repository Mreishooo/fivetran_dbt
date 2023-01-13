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

google_keyword_cross as
(
   select *  FROM {{ source('GoogleAd', 'KeywordCrossDeviceStats_9940526481') }}  
)

SELECT distinct  'google_ads' platform ,
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
left join google_customer  using (ExternalCustomerId)
join google_Keyword key using (CriterionId , CampaignId , AdGroupId)
--left join google_Criteria using ( CriterionId,CampaignId ,AdGroupId)
left join  google_campaign   using ( CampaignId)
left join  google_group_ads  using ( AdGroupId)
