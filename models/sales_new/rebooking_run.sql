
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},
   pre_hook= '{{create_rebooking_proc()}}',
   post_hook= 'CALL sales_stg.ticket_rebook_handeling(null,null)', 
)}}



 SELECT current_timestamp as rebooking_last_run
   
