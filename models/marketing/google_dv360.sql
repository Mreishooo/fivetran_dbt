{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'google_dv360', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with google_dv_360 as
(
  select 'Germany' as country ,*  FROM {{ source('ft_google_display_and_video_360', 'basic_report') }}  
) 

SELECT 'google_dv360' platform, 
country,
advertiser account_name ,
campaign campaign_name,
date,
clicks clicks ,
impressions impressions , 
post_click_conversions  conversions , 
cm_360_post_click_revenue conversion_value, 
media_cost_advertiser_currency as spend, 
clicks interactions
FROM  google_dv_360
