{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'facebook_ads', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with facebook_region_click as
(
  select  * FROM {{ source('ft_facebook_ad', 'region_click') }}
) ,

facebook_region_click_actions as
(
  select  * from {{ source('ft_facebook_ad', 'region_click_actions') }}  
) ,

fb_actions_pivot as 
(
  SELECT * 
      FROM(
        select _fivetran_id,ad_id, date, action_type , ra.value  
        from facebook_region_click_actions ra 
        where true
      and action_type in ( 'omni_add_to_cart',  'omni_view_content','omni_purchase','post_engagement','page_engagement','like','comment','post_reaction','post','omni_initiated_checkout','landing_page_view','video_view') )
      PIVOT( sum(value) as value FOR action_type IN  ( 'omni_add_to_cart',  'omni_view_content','omni_purchase','post_engagement','page_engagement','like','comment','post_reaction','post','omni_initiated_checkout','landing_page_view','video_view')  ) as v 
)




SELECT 'facebook_ads' platform , 
{{get_facebook_ad_country('account_id')}} as country,
account_name,
campaign_name,
SPLIT(campaign_name,'_') [safe_OFFSET(3)] campaign_type_split,
SPLIT(campaign_name,'_') [safe_OFFSET(4)] show,
SPLIT(campaign_name,'_') [safe_OFFSET(5)] theatre_location,
SPLIT(campaign_name,'_') [safe_OFFSET(12)] free_text,
adset_name,
ad_name,
date,
account_id,
campaign_id,
adset_id,
ad_id,
region,
value_omni_purchase conversions,
reach,
impressions,
frequency,
account_currency currency,
spend,
attribution_setting ,
inline_link_clicks link_clicks,
clicks clicks ,
value_page_engagement,
value_like page_Likes,
value_comment post_comments,
value_post_engagement post_engagement,
value_post_reaction post_reactions,
value_post post_shares,
value_video_view video_plays_3s,
-- v25.value video_plays_at_25,
-- v50.value video_plays_at_50,
-- v75.value video_plays_at_75,
-- v95.value video_plays_at_95,
-- v100.value video_plays_at_100,
value_omni_add_to_cart adds_to_cart , 
value_omni_initiated_checkout checkouts_initiated,
value_omni_view_content content_views,
value_landing_page_view landing_page_view ,

FROM facebook_region_click
left join  fb_actions_pivot as  ra using (ad_id, date,_fivetran_id ) 
where true --and date ='2022-12-27'

