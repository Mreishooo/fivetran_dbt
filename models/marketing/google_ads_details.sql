{{ config(
    materialized='table',
    labels = {'source': 'google_ads', 'refresh': 'daily','connection':'bq_transfer','type':'enriched'},
)}}
with google_ad as
(
  select 'Germany' as country , *  FROM {{ source('GoogleAd', 'Ad_9940526481') }}  
   where _LATEST_DATE = _DATA_DATE
   -- and CriterionId = 300545487483
   union all 
  select 'France' as country , *  FROM {{ source('google_ad_fr', 'Ad_8396768808') }}  
   where _LATEST_DATE = _DATA_DATE
   -- and CriterionId = 300545487483
    
) 
select 
country,
CreativeId ad_id,
ExternalCustomerId customer_id  ,
AdType ad_type,
headline headline,
HeadlinePart1 headline_part1,
HeadlinePart2 headline_part2,
Description description,
Description1 description1,
Description2 description2,
--ExpandedTextAdDescription1 expanded_text_ad_description1,
ExpandedTextAdDescription2 expanded_text_ad_description2,
ImageAdUrl image_ad_url,
CreativeFinalUrls creative_final_urls,
regexp_extract( CreativeFinalUrls,'/[^/]+/') domain_url,
status,
ResponsiveSearchAdPath1 responsive_search_ad_path1,
ResponsiveSearchAdPath2 responsive_search_ad_path2,
ShortHeadline short_headline,
CallOnlyPhoneNumber call_only_phone_number,
CallToActionText call_to_action_text,
PolicySummary policy_summary

from google_ad

--and     _LATEST_DATE = _DATA_DATE
--and CallToActionText is not null
--limit 10