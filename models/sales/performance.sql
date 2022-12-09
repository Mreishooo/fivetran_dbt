{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimperformance AS (
   SELECT *  
   FROM {{ source( 'ft_mdb6_dbo','dimperformance') }}
   where {{ ft_filter(none) }} 
  )


SELECT
  dimperformanceid dim_performance_id,
  dimperformancedateid dim_performance_date_id,
  dimperformancetimeid dim_performance_time_id,
  dimproductionlocationid dim_production_location_id, 
  dimrelocatedtoperformancedateid dim_relocated_to_performance_date_id,
  dimrelocatedtoperformanceid dim_relocated_to_performance_id,
  dimrelocatedtoperformancetimeid dim_relocated_to_performance_time_id ,
  frombookingdate from_booking_date, 
  isactiveperformance is_active_performance,
  isbuyout is_buyout,
  ishightaxrate is_high_tax_rate,
  isrelocated is_relocated,
  performancedate performance_date,
  performancedatetime performance_date_time,
  performancedayofweek performance_day_ofweek,
  performancedescription performance_description,
  performanceid performance_id,
  performancestatus performance_status,
  performancetime performance_time, 
  performancetype performance_type,
  performanceweekdaytime performance_weekday_time,
  productionlocationcapacity production_location_capacity,
  productionlocationrunningday production_location_running_day,
  productionlocationshownumber production_location_show_number,
  promotiontype promotion_type,
  salesfloorplan sales_floor_plan,
  season,
  setup,
  show,
  showcode,
  version
FROM
  dimperformance