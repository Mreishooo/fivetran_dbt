with google_ads_all_channels as 
( select * from {{ref('google_ads_all_channels')}})

SELECT 
country	
,show
,campaign_name
,advertising_channel_type	
,ad_type
,date	 
,sum(impressions) impressions 
,sum(clicks) clicks	
,sum( spend) spend
,sum(video_views) video_views,
 FROM google_ads_all_channels
 where date >= '2023-01-01'
 and country <> 'Netherlands'
 group by 1,2,3,4,5,6