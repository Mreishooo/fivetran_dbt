{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'facebook_ads', 'refresh': 'daily','connection':'fivetran'},
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
) 



SELECT 'facebook_ads' platform , 
account_name ,date ,
inline_link_clicks clicks ,
impressions impressions , 
ra.value  conversions , 
rav.value conversions_value, 
spend as spend, 
reach interactions
FROM facebook_ad_basic
left join  facebook_basic_report_actions ra using (ad_id, date ) 
left join  facebook_basic_report_action_values    rav  using (ad_id, date ,action_type )
where true --and date ='2022-06-10'
and ifnull(  action_type,'omni_purchase') = 'omni_purchase'
