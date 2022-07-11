{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'google_dv360', 'refresh': 'daily','connection':'fivetran'},
)}}

with google_dv_360 as
(
  select *  FROM {{ source('ft_google_display_and_video_360', 'basic_report') }}  
) 

SELECT 'google_dv360' platform, 
advertiser account_name ,
date,
clicks clicks ,
impressions impressions , 
post_click_conversions  conversions , 
cm_360_post_click_revenue conversions_value, 
media_cost_advertiser_currency as spend, 
clicks interactions
FROM  google_dv_360
