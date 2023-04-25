with facebook_region_click as 
( select * from {{ref('facebook_region_click')}} ) 

select country
,campaign_name
,date	
,show
,sum(impressions)impressions	
,sum( clicks	) clicks
,sum(post_reactions	) post_reactions
,sum(content_views)	content_views
,sum( spend)spend
from facebook_region_click
where date >= '2023-01-01'
and country <> 'Netherlands'
group by 1,2,3,4
order by  date desc