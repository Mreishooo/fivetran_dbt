{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'facebook_ads', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with facebook_ad_basic as
(
  select * FROM {{ source('ft_facebook_ads', 'basic_report') }}
) ,

facebook_basic_report_actions as
(
  select * from {{ source('ft_facebook_ads', 'basic_report_actions') }}  
) ,

facebook_basic_report_action_values as
(
  select * from {{ source('ft_facebook_ads', 'basic_report_action_values') }}  
) ,

fb_actions_pivot as 
(
  SELECT * 
      FROM(
        select ad_id, date, action_type ,   ra.value  
        from facebook_basic_report_actions ra 
        where true
      and action_type in ( 'omni_add_to_cart',  'omni_view_content','omni_purchase') )
      PIVOT( sum(value) FOR action_type IN ( 'omni_add_to_cart',  'omni_view_content','omni_purchase')  ) as v 
),

fb_action_values_pivot as 
(
SELECT * 
   FROM(
      select 
      ad_id,date,action_type, omni_view_content view_content  ,omni_add_to_cart add_to_cart  ,omni_purchase conversions ,rav.value 
      from fb_actions_pivot
      left join  facebook_basic_report_action_values    rav  using (ad_id, date  ) 
      where true
      --and '23850523304830668' = ad_id
      --and date >='2022-07-31'
      )
    PIVOT( sum(value) FOR action_type IN ( 'omni_add_to_cart'  ,'omni_purchase')  
    ) as v 
)

 
SELECT 'facebook_ads' platform , 
account_name ,
campaign_name campaign_name,
adset_name,
ad_name,
date,
inline_link_clicks clicks ,
impressions impressions , 
spend as spend, 
reach interactions,
view_content,
add_to_cart ,
omni_add_to_cart add_to_cart_value,
conversions,
omni_purchase conversions_value 
FROM facebook_ad_basic
 join  fb_action_values_pivot ra using (ad_id, date ) 
where true-- and date ='2022-09-11'
 