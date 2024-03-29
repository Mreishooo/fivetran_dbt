{{ config(
    materialized='table',
    labels = {'source': 'media_agency', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
    Description = 'Latest loaded file of OLAP_OOH'
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
start_date,
end_date,
no_of_days,
media_agency,
media_channel_level_1,
media_channel_level_2,
direct_buy_y_n,
target_audience,
planned_gross_contacts_overall_in_mio_tg_a_14_ planned_gross_contacts_overall_in_mio_tg_a_14,
planned_net_reach_in_mio_planned_tg_ planned_net_reach_in_mio_planned_tg,
planned_net_media_cost_in_000_ planned_net_media_cost_in_000,
booked_media_cost_in_000_ booked_media_cost_in_000,
final_gross_contacts_overall_in_mio_tg_a_14_ final_gross_contacts_overall_in_mio_tg_a_14,
final_net_reach_in_mio_within_planned_tg_ final_net_reach_in_mio_within_planned_tg,
final_net_media_cost_in_000_ final_net_media_cost_in_000,
final_cpm,
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
agency_fees_local_currency_ agency_fees_local_currency ,
production_costs_if_applicable_ production_costs_if_applicable,
ad_serving_tracking_costs_if_applicable_ ad_serving_tracking_costs_if_applicable,
{{fix_md_value('taxes_')}} taxes,
final_cost_to_client,
ta_universe,
reach_ reach,
null number_of_sitespanels,
null share_of_illuminated_sites_panels


FROM mind_share_data
--where _file = ( select `stage-landing.ft_mdb_dbo.get_latest_file_name`('OLAP_OOH') )
{{ get_mindshare_latest_file('OLAP_OOH')  }}
