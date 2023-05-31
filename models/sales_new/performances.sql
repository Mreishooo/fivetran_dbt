
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
    SELECT *  
   FROM {{ source( 'ft_sales_seeds','performances_performances') }}

),
tickets as 
(
    SELECT *  
    FROM {{ ref( 'tickets_mapped') }}
 
)
,performance as 
(
SELECT 
performance_id
,production_location_id
,Date(performance_date) performance_date 
,time(performance_date) performance_time
,from_booking_date	
,is_active_performance	
,is_buyout	
,is_high_tax_rate	
,is_relocated	
,performance_day_ofweek	
,performance_description	
,performance_status	
,performance_type	
,performance_weekday_time	
,production_location_capacity	
,production_location_running_day	
,production_location_show_number	
,promotion_type	
,sales_floor_plan	
,season	setup	
,show	
,showcode	
,_fivetran_synced last_update
from seed_date

union all
SELECT  distinct
 concat(production_location_id,' - ', Date(performance_date) ,' - ', time(performance_date))  performance_id
,production_location_id
,Date(performance_date) performance_date 
,time(performance_date) performance_time,
cast (null as timestamp),
cast (null as int64),
cast (null as bool),
cast (null as int64),
cast (null as bool),
cast (null as string),
cast (null as string),
cast (null as string),
cast (null as string),
cast (null as string),
cast (null as int64),
cast (null as int64),
null,
cast (null as string),
cast (null as string),
cast (null as string),
cast (null as string),
cast (null as string),
current_timestamp
from  tickets 
where concat(production_location_id,' - ', Date(performance_date) ,' - ', time(performance_date))  not in ( select production_location_id from seed_date)
)

select * from performance
QUALIFY ROW_NUMBER() OVER (PARTITION BY performance_id ORDER BY last_update DESC) = 1 
 