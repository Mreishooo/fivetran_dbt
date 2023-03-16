{{ config(
    materialized='table',
    on_schema_change='fail',
    post_hook= '{{create_rebooking_proc()}}',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}

select 
current_timestamp() run_at,
current_date() working_date
 

 