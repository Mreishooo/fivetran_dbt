with facebook_region_click as 
( select * from {{ref('facebook_region_click')}} ) 
, 
meta_all_campaigns_update as
( select *    FROM {{ source('upload', 'Meta_All_Campaigns_Update') }}   )
, 
meta_target_audience as
(  SELECT 
string_field_0 adset_name , RIGHT(string_field_0, STRPOS(REVERSE(string_field_0), '_') - 1) target_audience 
 FROM {{ source('upload', 'Meta_Adsets_with_extraction_of_free_text_as_Target_Group') }}   )

select country
, ifnull( string_field_2, show) show
,campaign_name
,string_field_1 campaign_type_split
,string_field_3 theater_location
,null target_audience
,date	
,sum(impressions)impressions	
,sum( clicks) clicks
,sum(post_reactions	) post_reactions
,sum(content_views)	content_views
,sum( spend)spend
from facebook_region_click
left join meta_all_campaigns_update on campaign_name= string_field_0
left join meta_target_audience using (adset_name)
where date >= '2023-01-01'
and country <> 'Netherlands'
group by 1,2,3,4 ,5,6,7
order by  date desc
 