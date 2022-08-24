{{ config(
    materialized='table',
    labels = {'source': 'abintus', 'refresh': 'monthly','connection':'fivetran','type':'enriched'},
)}}

with abintus_data as
(
  select *  FROM {{ source('ft_abintus', 'abintus_hq') }}  
) 

SELECT _line
          , _30_ta_cpp
          , _30_ta_grps_20_in_spain_
         -- , _fivetran_synced
         -- , _modified
          , ad_serving_tracking_costs_if_applicable_
          , added_value_eur_
          , affinity_index_targeting_
          , agency_client
          , agency_commission_
          , agency_fees_
          , agency_fees_local_currency_
          , average_grp_spot
          , booked_media_cost_in_000_
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
          , PARSE_DATE("%m/%d/%Y" , end_date ) end_date
          , engagement_rate
          , final_cost_to_client
          , final_cpm
          , final_gross_contacts_overall_in_mio_tg_a_14_
          , final_net_media_cost_in_000_
          , final_net_reach_in_mio_within_planned_tg_
          , frequency_ots
          , gross_negotiated_net_cost
          , impressions
          , local_currency
          , market_region
          , media_channel_level_1
          , media_channel_level_2
          , negotiated_media_discount_
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
          , planned_gross_contacts_overall_in_mio_tg_a_14_
          , planned_net_media_cost_in_000_
          , planned_net_reach_in_mio_planned_tg_
          , planned_target_group
          , premium_position_definitions
          , prime_time_drive_time_definition
          , production
          , production_costs_if_applicable_
          , ratecard_gross_cost
          , reach_
          , remarks_comments
          , share_of_digital
          , share_of_drive_time_from_ratings_
          , share_of_premium_positions_from_ratings_
          , share_of_prime_peak_time_from_ratings_
          , share_of_right_hand_side
          , PARSE_DATE("%m/%d/%Y" , start_date )start_date
          , ta_grps
          , ta_universe
          , taxes_
          , view_through_rate
          , viewability_definition
          , viewability_rate
          , viewable_impressions
          , views
FROM  abintus_data
qualify  row_number() OVER (PARTITION BY _line ORDER BY _modified DESC)  = 1 