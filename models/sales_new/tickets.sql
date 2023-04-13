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
   FROM {{ ref('tickets_mapped_distributions') }}
  ),

  dpl AS ( 
    select *
    FROM {{ ref('production_locations') }}
    --FROM {{ source( 'ft_mdb7_dbo','dimproductionlocation') }}
    -- where {{ ft_filter(none) }}
  ),

  dat AS ( 
    SELECT *
    FROM {{ ref('article_types') }}
    
  ),
  
  dp AS ( 
    SELECT *
    FROM {{ ref('productions') }}
    --FROM {{ source( 'ft_mdb7_dbo','dimproduction') }}
     --where {{ ft_filter(none) }} 
  ),
  
  dt AS ( 
    SELECT *
    FROM {{ ref('theatres') }}
  ),

/*  da AS ( 
    SELECT *
    FROM {{ ref('sales_age') }}
  ),
  
  dpf AS ( 
    SELECT *
    FROM {{ ref('performance') }}
    --FROM {{ source( 'ft_mdb7_dbo','dimperformance') }}
    --where {{ ft_filter(none) }} 
  ),*/
  
  ddis AS ( 
    SELECT *
    FROM {{ ref('distributions') }}
    --FROM {{ source( 'ft_mdb7_dbo','dimdistribution') }}
    -- where {{ ft_filter(none) }} 
  ),

  dgc AS ( 
    SELECT *
    FROM {{ ref('golden_customer') }}
  ),

  dpt AS ( 
    SELECT *
    FROM {{ ref('price_types') }}
  --- FROM {{ source( 'ft_mdb7_dbo','dimpricetype') }}
    -- where {{ ft_filter(none) }}
  ),

  dpc AS ( 
    SELECT *
    FROM {{ ref('price_categories') }}
   -- FROM {{ source( 'ft_mdb7_dbo','dimpricecategory') }}
    --where {{ ft_filter(none) }}
  ),

  c AS ( 
    SELECT *
    FROM {{ ref('countries') }}
  )

/*  ,distinct_orders AS ( 
    SELECT distinct source_code,country_code,booking_date, main_order_number,dim_golden_customer_id, 
       -- row_number()over (PARTITION BY dim_golden_customer_id order by booking_date ) order_rank
    from fts
    where transaction_type ='Sale' 
    and  not is_replaced and not is_replacement and not is_replaced_cancellation and not cancellation_status and replacement_type <> 'Original'
  )
  ,orders_rank AS ( 
    SELECT * , row_number()over (PARTITION BY dim_golden_customer_id order by booking_date ) order_rank
    from distinct_orders
  )
*/


SELECT

    fts.ticket_id
    ,fts.country_code
      ,c.country_name
    ,fts.source_code
    ,fts.web_order_number
    ,fts.main_order_number
    ,fts.sub_order_number
    ,fts.barcode
    ,fts.barcode_13
    -- Transaction / status
    ,fts.transaction_type
    ,fts.cancellation_status
    ,fts.is_replaced
    ,fts.is_replacement
    ,fts.replacement_type
    ,fts.is_replaced_cancellation
    --Booking Date
  
    ,fts.booking_date
    ,fts.booking_timestamp
  -- ,fts.InflowBookingCodeGT180 inflow_booking_code_gt180
    --Performance Date
    ,concat(production_location_id,' - ', Date(performance_date) ,' - ', time(performance_date)) performance_id
    ,fts.performance_date  performance_timestamp
    ,Date(performance_date) performance_date
    ,time(performance_date) performance_time
       --,dpf.performance_time  here 
       --,dpf.show show  here 

  -- ,.  AS lead_weeks_performance_number
  -- ,fts.LeadDaysPerformanceNumber   AS lead_days_performance_number
    -- additional dates and times
      ,dpl.production_location_premiere_date
      ,dpl.sales_start_date
      ,dpl.show_in_group_sales_reporting  
    -- Production / Prerformance
	  
	  ,fts.article_type_code
      ,dat.article_type_name
     
	  ,fts.production_location_id
      ,dpl.production_id
      ,dp.production_name
      ,dp.license_name production_license_name
     -- ,dpf.performance_status here 
      ,DATE_DIFF(fts.booking_date,date(fts.performance_date), day) lead_days_perfomance 
      --,DATE_DIFF(fts.booking_date, date(sales_start_date), day) lead_day_sales_start
	  ,fts.theatre_id
      ,dt.theatre_code
      ,dt.theatre_name
      ,dt.City_Code                      AS theatre_city_Code
      ,dt.City                          AS theatre_city
   --- ,fts.production_location_performance_year
    -- Distribution
    ,fts.source_distribution_point_id
    ,fts.source_distribution_point 
    ,fts.source_distribution_channel
    ,fts.source_distribution_channel_name
    ,fts.source_client_id
    ,fts.source_sales_partner
    ,fts.distribution_id
      ,ddis.local_sales_channel_1
      ,ddis.local_sales_channel_2
      ,ddis.distribution_owner
      ,ddis.distribution_point
    -- additional Source information
    ,fts.source_promotion_id
    ,fts.source_production
    ,fts.source_promotion_name
    ,fts.source_promotion_code
    ,fts.source_promotion_advertising_partner_id
    ,fts.source_customer_code
    --GoldenCustomer
	  /* -- here 
    ,fts.dim_golden_customer_id here 
      ,dgc.current_age as golden_customer_current_age
      ,dgc.gender as golden_customer_gender
      ,dgc.customer_country_code as golden_customer_country_code
      ,dgc.postcode as golden_customer_postal_code
      ,LENGTH(dgc.postcode) as golden_customer_postalcodelength  
      here */

    ---,fts.is_returning_customer_flag
    ---,fts.is_returning_customer
    -- ,fts.dim_golden_customer_age_at_booking_id 
    /*  ,da.Age_Bin_A                      AS golden_customer_age_bin_A
      ,da.Age_Bin_B                      AS golden_customer_age_bin_B
      ,da.Age_Bin_C                      AS golden_customer_age_bin_C
      ,da.Age_Bin_D                      AS golden_customer_age_bin_D
      ,da.Age_Decenia_Bin_Code           AS golden_customer_age_decenia_bin_code
      ,da.Age_Decenia_Bin_Name           AS golden_customer_age_decenia_bin_name*/
    -- Seat
    ,fts.source_seat
    ,fts.source_seat_row
    ,fts.source_seat_number
    --PriceType
    ,fts.source_price_category_id
      ,dpc.price_category_id
      ,dpc.price_category_name
      ,dpc.price_category_group
      ,dpc.sort_code as price_category_sort_code
	  ,fts.source_price_type_name
      ,dpt.price_type_id
      ,dpt.price_type_name
      ,dpt.price_type_group
    --sums
    ,fts.article_count
    ,fts.net_price_euro
    ,fts.net_net_price_euro
    ,fts.ticket_price_euro
    ,fts.customer_price_euro
    ,fts.tpt_de_value_eur
    ,fts.paid_price_euro
    ,fts.customer_facevalue_euro
    ,fts.paid_price_euro = 0 is_free 
  --  ,order_rank
    ,fts._last_update
    ,fts._loaded_at
    ,fts._run_at
    ,fts._source 
    

FROM  fts
left join  c using (country_code)
left join  dpl  using (production_location_id)
left join  dat using (article_type_code )
left join  dp on (dp.production_id=dpl.production_id )
left join  dt on (dt.theatre_id = fts.theatre_id) 
--left join  da  on (da.dim_age_id = fts.dim_golden_customer_age_at_booking_id)
--left join  dpf  on (dpf.Performance_id =fts.Performance_id )
left join  ddis on (ddis.distribution_id = fts.distribution_id)
--left join  dgc using (golden_customer_id)
left join  dpt using (price_type_id)
left join  dpc using (price_category_id)
--left join orders_rank using( booking_date, main_order_number,dim_golden_customer_id )


 