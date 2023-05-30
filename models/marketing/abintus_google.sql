with google_ads_all_channels as 
( select * from {{ref('google_ads_all_channels')}})
, 
Google_All_Campaigns_Update as
( select *    FROM {{ source('upload', 'Google_All_Campaigns_Update') }}   )
SELECT 
country	
, ifnull( string_field_2, show) show
,campaign_name
,advertising_channel_type	
,ad_type
,string_field_1 campaign_type_split
,string_field_3 theater_location
,date	 
,sum(impressions) impressions 
,sum(clicks) clicks	
,sum( spend) spend
,sum(video_views) video_views,
 FROM google_ads_all_channels
 left join Google_All_Campaigns_Update on campaign_name= string_field_0
 where date >= '2023-01-01'
 and country <> 'Netherlands'
 group by 1,2,3,4,5,6 ,7,8 


 