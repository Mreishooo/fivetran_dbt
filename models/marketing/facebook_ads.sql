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
  select * FROM {{ source('ft_facebook_ad', 'basic_report') }}
) ,

facebook_basic_report_actions as
(
  select * from {{ source('ft_facebook_ad', 'basic_report_actions') }}  
) ,

facebook_basic_report_action_values as
(
  select * from {{ source('ft_facebook_ad', 'basic_report_action_values') }}  
) ,

fb_actions_pivot as 
(
  SELECT * 
      FROM(
        select _fivetran_id,ad_id, date, action_type , ra._1_d_view view_1 , ra._7_d_click click_7, ra.value  
        from facebook_basic_report_actions ra 
        where true
      and action_type in ( 'omni_add_to_cart',  'omni_view_content','omni_purchase') )
      PIVOT( sum(view_1) as view_1 , sum(click_7) as click_7 ,sum(value) as value FOR action_type IN ( 'omni_add_to_cart',  'omni_view_content','omni_purchase')  ) as v 
),


fb_action_values_pivot as 
(
SELECT * 
   FROM(
      select 
      _fivetran_id,ad_id,date,action_type, 
      ifnull(view_1_omni_view_content,0) view_content_1view  ,
      ifnull(click_7_omni_view_content,0) view_content_7click  ,
      ifnull(value_omni_view_content,0) view_content  ,
      ifnull(view_1_omni_add_to_cart,0) add_to_cart_1view  ,
      ifnull(click_7_omni_add_to_cart,0) add_to_cart_7click  ,
      ifnull(value_omni_add_to_cart,0) add_to_cart  ,
      ifnull(view_1_omni_purchase,0) conversions_1view,  
      ifnull(click_7_omni_purchase,0) conversions_7click,
      ifnull(value_omni_purchase,0) conversions  ,
      ifnull(rav.value,0)value,
      ifnull(rav._1_d_view,0) value_1view, 
      ifnull(rav._7_d_click,0) value_7click,
      from fb_actions_pivot _1view
      left join  facebook_basic_report_action_values    rav  using (_fivetran_id,ad_id, date  ) 
      where true
      --and '23850523304830668' = ad_id
      --and date ='2022-12-13'
      )
    PIVOT( sum(value) as value , sum(value_1view) as _1view , sum(value_7click) as _7click  FOR action_type IN ( 'omni_add_to_cart'  ,'omni_purchase')  
    ) as v 
)

 
SELECT 'facebook_ads' platform , 
{{get_facebook_ad_country('account_id')}} as country, 
account_name ,
campaign_name campaign_name,
SPLIT(campaign_name,'_') [safe_OFFSET(3)] campaign_type_split,
SPLIT(campaign_name,'_') [safe_OFFSET(4)] show,
SPLIT(campaign_name,'_') [safe_OFFSET(5)] theatre_location,
SPLIT(campaign_name,'_') [safe_OFFSET(12)] free_text,
adset_name,
ad_name,
date,
inline_link_clicks clicks ,
impressions impressions , 
spend as spend, 
reach interactions,
view_content_1view,
view_content_7click,
view_content,
add_to_cart_1view ,
add_to_cart_7click,
add_to_cart,
_1view_omni_add_to_cart add_to_cart_1view_value,
_7click_omni_add_to_cart add_to_cart_7click_value,
value_omni_add_to_cart add_to_cart_value ,
conversions_1view ,
conversions_7click,
conversions,
_1view_omni_purchase conversions_1view_value,
_7click_omni_purchase conversions_7click_value,
value_omni_purchase  conversion_value 


FROM facebook_ad_basic
 left join  fb_action_values_pivot ra using (_fivetran_id,ad_id, date) 
where true-- and date ='2022-09-11')
--and account_id in (   641810463098719 , 2441671829467106 )

