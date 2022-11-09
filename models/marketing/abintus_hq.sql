{{ config(
    materialized='table',
    labels = {'source': 'abintus', 'refresh': 'monthly','connection':'fivetran','type':'enriched'},
)}}

{% set kpis  = ["_30_ta_grps_20_in_spain",
"ad_serving_tracking_costs_if_applicable",
"added_value_eur",
"agency_fees_local_currency",
"booked_media_cost_in_000",
"final_cost_to_client",
"final_gross_contacts_overall_in_mio_tg_a_14",
"final_net_media_cost_in_000",
"final_net_reach_in_mio_within_planned_tg",
"frequency_ots",
"gross_negotiated_net_cost",
"impressions",
"net_cost_net_net_cost",
"number_of_clicks",
"number_of_comments",
"number_of_likes",
"number_of_shares",
"planned_gross_contacts_overall_in_mio_tg_a_14",
"planned_net_media_cost_in_000",
"planned_net_reach_in_mio_planned_tg",
"production_costs_if_applicable",
"ratecard_gross_cost",
"ta_grps",
"viewable_impressions",
"views"] %}


with abintus as
(
  select *  FROM {{ source('ft_abintus', 'abintus_hq') }}  
),

abintus_data_latest 
as 
( 
SELECT _file,_line 
          , _30_ta_cpp 
          , _30_ta_grps_20_in_spain_ _30_ta_grps_20_in_spain
          , ad_serving_tracking_costs_if_applicable_ ad_serving_tracking_costs_if_applicable
          , added_value_eur_ added_value_eur
          , affinity_index_targeting_ affinity_index_targeting
          , agency_client
          , {{fix_md_valye('agency_commission_')}} agency_commission
          , {{fix_md_valye('agency_fees_')}} agency_fees
          , agency_fees_local_currency_ agency_fees_local_currency
          , average_grp_spot
          , safe_cast (booked_media_cost_in_000_ as float64) booked_media_cost_in_000
          , campaign
          , click_through_rate
          , copy_length
          , cost_per_cinema_site
          , cost_per_click
          , cost_per_insertion
          , cost_per_screen
          , cost_per_site_panel
          , country
          , cpm
          , direct_buy_y_n
          , PARSE_DATE("%d/%m/%Y" , end_date ) end_date
          , engagement_rate
          , final_cost_to_client
          , final_cpm
          , final_gross_contacts_overall_in_mio_tg_a_14_ final_gross_contacts_overall_in_mio_tg_a_14
          , safe_cast (final_net_media_cost_in_000_ as float64) final_net_media_cost_in_000
          , final_net_reach_in_mio_within_planned_tg_ final_net_reach_in_mio_within_planned_tg
          , frequency_ots
          , safe_cast (gross_negotiated_net_cost as float64) gross_negotiated_net_cost
          , impressions
          , local_currency
          , market_region
          , media_channel_level_1
          , media_channel_level_2
          , {{fix_md_valye('negotiated_media_discount_')}} negotiated_media_discount
          , safe_cast (net_cost_net_net_cost as float64)  net_cost_net_net_cost
          , no_of_days
          , number_of_clicks
          , number_of_comments
          , number_of_insertions
          , number_of_likes
          , number_of_screens
          , number_of_shares
          , number_of_sites_cinemas
          , number_of_sites_panels
          , number_of_spots_bought
          , planned_actual
          , planned_gross_contacts_overall_in_mio_tg_a_14_ planned_gross_contacts_overall_in_mio_tg_a_14
          , safe_cast (planned_net_media_cost_in_000_ as float64)  planned_net_media_cost_in_000
          , planned_net_reach_in_mio_planned_tg_ planned_net_reach_in_mio_planned_tg
          , planned_target_group
          , premium_position_definitions
          , prime_time_drive_time_definition
          , production
          , production_costs_if_applicable_ production_costs_if_applicable
          , safe_cast ( ratecard_gross_cost as float64)  ratecard_gross_cost
          , reach_
          , remarks_comments
          , share_of_digital
          , share_of_drive_time_from_ratings_ share_of_drive_time_from_ratings
          , share_of_premium_positions_from_ratings_ share_of_premium_positions_from_ratings
          , share_of_prime_peak_time_from_ratings_ share_of_prime_peak_time_from_ratings
          , share_of_right_hand_side
          , PARSE_DATE("%d/%m/%Y" , start_date )start_date
          , ta_grps
          , ta_universe
          , taxes_ taxes
          , view_through_rate
          , viewability_definition
          , viewability_rate
          , viewable_impressions
          , views
          , impacts
FROM  abintus
qualify  row_number() OVER (PARTITION BY _line ORDER BY _modified DESC)  = 1 
)

 ,abintus_add_days as
(
  select * , DATE_DIFF( end_date,start_date, day) + 1 days 
  FROM abintus_data_latest  
) 

SELECT  _file,_line,
      start_date ,
      end_date,
      days,
      day,
      campaign
          , _30_ta_cpp
          , _30_ta_grps_20_in_spain
          , ad_serving_tracking_costs_if_applicable
          , added_value_eur
          , affinity_index_targeting
          , agency_client
          , agency_commission
          , agency_fees
          , agency_fees_local_currency
          , average_grp_spot
          , booked_media_cost_in_000
          , click_through_rate
          , copy_length
          , cost_per_cinema_site
          , cost_per_click
          , cost_per_insertion
          , cost_per_screen
          , cost_per_site_panel
          , country
          , cpm
          , direct_buy_y_n
          , engagement_rate
          , final_cost_to_client
          , final_cpm
          , final_gross_contacts_overall_in_mio_tg_a_14
          , final_net_media_cost_in_000
          , final_net_reach_in_mio_within_planned_tg
          , frequency_ots
          , gross_negotiated_net_cost
          , impressions
          , local_currency
          , market_region
          , media_channel_level_1
          , media_channel_level_2
          , negotiated_media_discount
          , net_cost_net_net_cost
          , no_of_days
          , number_of_clicks
          , number_of_comments
          , number_of_insertions
          , number_of_likes
          , number_of_screens
          , number_of_shares
          , number_of_sites_cinemas
          , number_of_sites_panels
          , number_of_spots_bought
          , planned_actual
          , planned_gross_contacts_overall_in_mio_tg_a_14
          , planned_net_media_cost_in_000
          , planned_net_reach_in_mio_planned_tg
          , planned_target_group
          , premium_position_definitions
          , prime_time_drive_time_definition
          , production
          , production_costs_if_applicable
          , ratecard_gross_cost
          , reach_
          , remarks_comments
          , share_of_digital
          , share_of_drive_time_from_ratings
          , share_of_premium_positions_from_ratings
          , share_of_prime_peak_time_from_ratings
          , share_of_right_hand_side
          , ta_grps
          , ta_universe
          , taxes
          , view_through_rate
          , viewability_definition
          , viewability_rate
          , viewable_impressions
          , views
          , impacts
{% for kpi in kpis %}
    ,SAFE_DIVIDE( {{kpi}} , days ) as {{kpi}}_per_day
{% endfor %}
FROM abintus_add_days  join 
UNNEST(GENERATE_DATE_ARRAY(start_date, (end_date), INTERVAL 1 DAY)) as day  
on day between  (start_date) and  (end_date)
--where _line between 10 and 15
order by 1