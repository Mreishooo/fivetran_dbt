{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "booking_date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  fts AS (
   SELECT *  
   FROM {{ source( 'ft_mdb1_se','bi_selectforexport_bq') }}
  ),

  dpl AS ( 
    select *
    FROM {{ ref('mdb_production_location') }}
  ),
  
  dat AS ( 
    SELECT *
    FROM {{ ref('mdb_article_type') }}
  ),
  
  dp AS ( 
    SELECT *
    FROM {{ ref('mdb_production') }}
  ),
  
  dt AS ( 
    SELECT *
    FROM {{ ref('mdb_theatre') }}
  ),

  da AS ( 
    SELECT *
    FROM {{ ref('mdb_age') }}
  )

SELECT
    fts.FactTicketSalesId fact_ticket_sales_id
	  ,fts.DimCountryId country_id
    ,fts.CountryCode country_code
    ,fts.SourceCode source_code
    ,fts.WebOrderNumber web_order_number
    ,fts.MainOrderNumber main_order_number
    ,fts.SubOrderNumber sub_order_number
    ,fts.BarCode bar_code
    -- Transaction / status
    ,fts.TransactionType transaction_type
    ,fts.CancellationStatus cancellation_status
    ,fts.IsReplaced is_replaced
    ,fts.IsReplacement is_replacement
    ,fts.ReplacementType replacement_type
    ,CASE
       WHEN fts.TransactionType = 'Cancellation' AND IsReplaced  AND fts.CancellationStatus  THEN true
       ELSE false
     END                              AS  is_replaced_cancellation
    --Booking Date
    ,fts.DimBookingDateId dim_booking_date_id
    , PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string )) AS booking_date
    , struct ( {{ date_struct( 'PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string ))' ) }}  ) as booking_date_struct 
    ,fts.InflowBookingCodeGT180 inflow_booking_code_gt180
    --Performance Date
    ,fts.DimPerformanceDateId dim_performance_date_id
    , PARSE_DATE("%Y%m%d", cast(DimPerformanceDateId as string )) AS performance_date
    , struct ( {{ date_struct( 'PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string ))' ) }}  ) as performance_date_struct 
 	  ,fts.DimPerformanceTimeId dim_performance_time_id
    ,fts.PerformanceDateTime performance_date_time
    ,fts.PerformanceWeekdayTime performance_weekday_time
    ,fts.LeadWeeksPerformanceNumber  AS lead_weeks_performance_number
    ,fts.LeadDaysPerformanceNumber   AS lead_days_performance_number
    -- additional dates and times
      ,dpl.production_location_premiere_date
      ,dpl.sales_start_date
    -- Production / Prerformance
	  ,fts.DimProductionId dim_production_id
	  ,fts.DimArticleTypeId dim_article_type_id
      ,dat.article_type_code
      ,dpl.production_location_id
	  ,fts.DimProductionLocationId dim_production_location_id
      ,dp.production_name
    ,fts.PerformanceStatus performance_status
	  ,fts.DimTheatreId
      ,dt.theatre_code
      ,dt.theatre_name
      ,dt.City_Code                      AS theatre_city_Code
      ,dt.City                          AS theatre_city
    ,fts.ProductionLocationPerformanceYear production_location_performance_year
    -- Distribution
    ,fts.SourceDistributionChannel source_distribution_channel
    ,fts.SourceDistributionChannelName source_distribution_channel_name
    ,fts.SourceClientId source_client_id
    ,fts.SourceSalesPartner source_sales_partner
    ,fts.LocalSalesChannel1 local_sales_channel1
    ,fts.LocalSalesChannel2 local_sales_channel2
    ,fts.DistributionOwner distribution_owner
    ,fts.DistributionPoint distribution_point
    -- additional Source information
    ,fts.SourcePromotionId source_promotion_id
    ,fts.SourcePromotionName source_promotion_name
    ,fts.SourcePromotionCode source_promotion_code
    ,fts.SourcePromotionAdvertisingPartnerId source_promotion_advertising_partner_id
    ,fts.SourceCustomerCode source_customer_code
    --GoldenCustomer
	  ,fts.DimGoldenCustomerId dim_golden_customer_id
    ,fts.GoldenCustomerCurrentAge golden_customer_current_age
    ,fts.GoldenCustomerGender golden_customer_gender
    ,fts.GoldenCustomerCountryCode golden_customer_country_code
    ,fts.GoldenCustomerPostalCode golden_customer_postal_code
    ,fts.GoldenCustomerPostalCodeLength golden_customer_postalcodelength
    ,fts.IsReturningCustomerFlag is_returning_customer_flag
    ,fts.IsReturningCustomer is_returning_customer
    ,fts.DimGoldenCustomerAgeAtBookingId dim_golden_customer_age_tbooking_id 
      ,da.Age_Bin_A                      AS golden_customer_age_bin_A
      ,da.Age_Bin_B                      AS golden_customer_age_bin_B
      ,da.Age_Bin_C                      AS golden_customer_age_bin_C
      ,da.Age_Bin_D                      AS golden_customer_age_bin_D
      ,da.Age_Decenia_Bin_Code           AS golden_customer_age_decenia_bin_code
      ,da.Age_Decenia_Bin_Name           AS golden_customer_age_decenia_bin_name
    --Customer
    ,fts.CustomerCurrentAge customer_current_age
    ,fts.CustomerGender  customer_gender
    ,fts.CustomerCountryCode customer_country_code
    ,fts.CustomerPostalCode customer_postal_code
    ,fts.CustomerPostalCodeLength customer_postal_code_length
    -- Seat
    ,fts.SourceSeat source_seat
    ,fts.SourceSeatRow source_seat_row
    ,fts.SourceSeatNumber source_seat_number
    --PriceType
    ,fts.SourcePriceCategoryId source_price_category_id
    ,fts.PriceCategoryName price_category_name
    ,fts.PriceCategoryGroup price_category_group
    ,fts.PriceCategorySortCode price_category_sort_code
	  ,fts.SourcePriceTypeName source_price_type_name
    ,fts.PriceTypeName price_type_name
    ,fts.PriceTypeGroup price_type_group
    --sums
    ,fts.ArticleCount article_count
    ,fts.NetPriceValueEUR net_price_value_eur
    ,fts.NetNetPriceValueEUR net_net_price_value_eur
    ,fts.TicketPriceValueEUR ticket_price_value_eur
    ,fts.CustomerPriceValueEUR customer_price_value_eur
    ,fts.TPT_DEValueEUR tpt_de_value_eur
FROM  fts
left join  dpl  on (dpl.dim_production_location_id =  fts.DimProductionLocationId)
left join   dat on (dat.dim_article_Type_Id =  fts.dimArticleTypeId )
left join  dp on (dp.dim_production_id=fts.DimProductionId )
left join  dt  on (dt.dim_theatre_id =fts.DimTheatreId) 
left join  da   on (da.dim_age_id = fts.DimGoldenCustomerAgeAtBookingId)


