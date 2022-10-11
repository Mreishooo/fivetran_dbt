{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "booking_date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with  
  fts AS (
   SELECT *  
   FROM {{ ref('ticket_sales_raw') }}
  ),

  dpl AS ( 
    select *
    FROM {{ ref('sales_production_location') }}
  ),
  
  dat AS ( 
    SELECT *
    FROM {{ ref('sales_article_type') }}
  ),
  
  dp AS ( 
    SELECT *
    FROM {{ ref('sales_production') }}
  ),
  
  dt AS ( 
    SELECT *
    FROM {{ ref('sales_theatre') }}
  ),

  da AS ( 
    SELECT *
    FROM {{ ref('sales_age') }}
  ),
  
  dpf AS ( 
    SELECT *
    FROM {{ ref('sales_performance') }}
  ),
  
  ddis AS ( 
    SELECT *
    FROM {{ ref('sales_distribution') }}
  ),

  dgc AS ( 
    SELECT *
    FROM {{ ref('golden_customer') }}
  ),

  dpt AS ( 
    SELECT *
    FROM {{ ref('sales_price_type') }}
  ),

  dpc AS ( 
    SELECT *
    FROM {{ ref('sales_price_category') }}
  ),

  c AS ( 
    SELECT *
    FROM {{ ref('sales_country') }}
  )



SELECT
    fts.fact_ticket_sales_id
	  ,fts.country_id
    ,fts.country_code
      ,c.country_name
    ,fts.source_code
    ,fts.web_order_number
    ,fts.main_order_number
    ,fts.sub_order_number
    ,fts.bar_code
    -- Transaction / status
    ,fts.transaction_type
    ,fts.cancellation_status
    ,fts.is_replaced
    ,fts.is_replacement
    ,fts.replacement_type
    ,fts.is_replaced_cancellation
    --Booking Date
    ,fts.dim_booking_date_id
    ,fts.booking_date
    ,fts.booking_date_struct 
  -- ,fts.InflowBookingCodeGT180 inflow_booking_code_gt180
    --Performance Date
    ,fts.dim_performance_date_id
    ,fts.performance_date
       ,dpf.performance_time 
       ,dpf.show  
    ,fts.performance_date_struct 
 	  ,fts.dim_performance_time_id
      ,dpf.performance_date_time
      ,dpf.performance_weekday_time
  -- ,.  AS lead_weeks_performance_number
  -- ,fts.LeadDaysPerformanceNumber   AS lead_days_performance_number
    -- additional dates and times
      ,dpl.production_location_premiere_date
      ,dpl.sales_start_date
    -- Production / Prerformance
	  ,fts.dim_production_id
	  ,fts.dim_article_type_id
      ,dat.article_type_code
      ,dpl.production_location_id
	  ,fts.dim_production_location_id
      ,dp.production_name
      ,dp.license_name production_license_name
      ,dpf.performance_status
      ,DATE_DIFF(fts.booking_date,fts.performance_date, day) lead_days_perfomance 
      --,DATE_DIFF(fts.booking_date, date(sales_start_date), day) lead_day_sales_start
	  ,fts.dim_theatre_id
      ,dt.theatre_code
      ,dt.theatre_name
      ,dt.City_Code                      AS theatre_city_Code
      ,dt.City                          AS theatre_city
   --- ,fts.production_location_performance_year
    -- Distribution
    ,fts.source_distribution_channel
    ,fts.source_distribution_channel_name
    ,fts.source_client_id
    ,fts.source_sales_partner
      ,ddis.local_sales_channel_1
      ,ddis.local_sales_channel_2
      ,ddis.distribution_owner
      ,ddis.distribution_point
    -- additional Source information
    ,fts.source_promotion_id
    ,fts.source_promotion_name
    ,fts.source_promotion_code
    ,fts.source_promotion_advertising_partner_id
    ,fts.source_customer_code
    --GoldenCustomer
	  ,fts.dim_golden_customer_id
      ,dgc.current_age as golden_customer_current_age
      ,dgc.gender as golden_customer_gender
      ,dgc.customer_country_code as golden_customer_country_code
      ,dgc.postcode as golden_customer_postal_code
      ,LENGTH(dgc.postcode) as golden_customer_postalcodelength

    ---,fts.is_returning_customer_flag
    ---,fts.is_returning_customer
    ,fts.dim_golden_customer_age_at_booking_id 
      ,da.Age_Bin_A                      AS golden_customer_age_bin_A
      ,da.Age_Bin_B                      AS golden_customer_age_bin_B
      ,da.Age_Bin_C                      AS golden_customer_age_bin_C
      ,da.Age_Bin_D                      AS golden_customer_age_bin_D
      ,da.Age_Decenia_Bin_Code           AS golden_customer_age_decenia_bin_code
      ,da.Age_Decenia_Bin_Name           AS golden_customer_age_decenia_bin_name
    -- Seat
    ,fts.source_seat
    ,fts.source_seat_row
    ,fts.source_seat_number
    --PriceType
    ,fts.source_price_category_id
      ,dpc.price_category_name
      ,dpc.price_category_group
      ,dpc.sort_code as price_category_sort_code
	  ,fts.source_price_type_name
      ,dpt.price_type_name
      ,dpt.price_type_group
    --sums
    ,fts.article_count
    ,fts.net_price_value_eur
    ,fts.net_net_price_value_eur
    ,fts.ticket_price_value_eur
    ,fts.customer_price_value_eur
    ,fts.tpt_de_value_eur
    ,fts.euro_paid_price
    ,fts.euro_customer_face_value

FROM  fts
left join  c using (country_code)
left join  dpl  using (dim_production_location_id)
left join  dat using (dim_article_Type_Id )
left join  dp on (dp.dim_production_id=fts.dim_production_id )
left join  dt using (dim_theatre_id) 
left join  da  on (da.dim_age_id = fts.dim_golden_customer_age_at_booking_id)
left join  dpf on (dpf.dim_Performance_id =fts.dim_Performance_id )
left join  ddis using (dim_distribution_id)
left join  dgc using (dim_golden_customer_id)
left join  dpt using (dim_price_type_id)
left join  dpc using (dim_price_category_id)


 