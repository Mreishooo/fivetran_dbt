{{ config(
    materialized='table',
    labels = {'source': 'media_agency', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
    Description = 'Latest loaded file of Radio_Plan_Daily'
)}}

with mind_share_data as
(
  select * FROM {{ source('ft_media_data', 'mind_share') }}
) 


select distinct _file ,_line,
country,
market_region,
{{fix_md_production_name('production')}}  production,
campaign_name,
date,
time,
start_date,
end_date,
no_of_days,
radio_channel,
radio_operator,
media_agency,
media_channel_level_1,
media_channel_level_2,
direct_buy_y_n,
target_audience,
target_audience_size,
added_value_barter_value,
remarks_comments,
local_currency,
planned_actual,
ratecard_gross_cost,
{{fix_md_value('negotiated_media_discount_')}} negotiated_media_discount,
gross_negotiated_net_cost,
{{fix_md_value('agency_commission_')}} agency_commission,
net_cost_net_net_cost,
{{fix_md_value('agency_fees_')}} agency_fees,
agency_fees_local_currency_ agency_fees_local_currency,
production_costs_if_applicable_ production_costs_if_applicable,
ad_serving_tracking_costs_if_applicable_ ad_serving_tracking_costs_if_applicable,
{{fix_md_value('taxes_')}} taxes,
final_cost_to_client,
ta_universe,
ta_grps,
copy_length,
_20_ta_grps,
_30_ta_grps,
_18_65_grps,
drive_time_definition,
null share_of_drive_time_from_ratings,
null affinity_index_targeting,
number_of_spots_bought



FROM mind_share_data
--where _file = ( select `stage-landing.ft_mdb_dbo.get_latest_file_name`('Radio_Plan_Daily') )
{{ get_mindshare_latest_file('Radio_Plan_Daily')  }}
