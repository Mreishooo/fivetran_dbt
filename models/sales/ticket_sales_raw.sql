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
   FROM {{ source( 'ft_mdb4_se','fact_ticket_sales_bq') }}
   where {{ ft_filter('FactTicketSalesId') }} 
   union all
   SELECT *  
   FROM {{ source( 'ft_mdb_dbo','factticketsales') }}
   where {{ ft_filter('FactTicketSalesId') }} 
   and dimbookingdateid between 20160101 and 20181231
  )


SELECT
    fts.FactTicketSalesId fact_ticket_sales_id
	  ,fts.DimCountryId country_id
    ,fts.SourceCountryCode country_code
    ,fts.SourceCode source_code
    ,fts.WebOrderNumber web_order_number
    ,fts.MainOrderNumber main_order_number
    ,fts.SubOrderNumber sub_order_number
    ,fts.BarCode bar_code
    -- Transaction / status
    ,fts.TransactionType transaction_type
    ,fts.CancellationStatus cancellation_status
    ,fts.Rebooked is_replaced
    ,fts.Replacement is_replacement
    ,fts.ReplacementType replacement_type
    ,CASE
       WHEN fts.TransactionType = 'Cancellation' AND Rebooked    AND fts.CancellationStatus   THEN true
       ELSE false
     END                              AS  is_replaced_cancellation
    --Booking Date
    ,fts.DimBookingDateId dim_booking_date_id
    , PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string )) AS booking_date
    , struct ( {{ date_struct( 'PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string ))' ) }}  ) as booking_date_struct 
    --Performance Date
    ,fts.dimPerformanceId dim_Performance_id
    ,fts.DimPerformanceDateId dim_performance_date_id
    , PARSE_DATE("%Y%m%d", cast(DimPerformanceDateId as string )) AS performance_date
    , struct ( {{ date_struct( 'PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string ))' ) }}  ) as performance_date_struct 
 	  ,fts.DimPerformanceTimeId dim_performance_time_id
    ,fts.DimProductionId dim_production_id
	  ,fts.DimArticleTypeId dim_article_type_id
	  ,fts.DimProductionLocationId dim_production_location_id
	  ,fts.DimTheatreId dim_theatre_id
    -- Distribution
    ,fts.DimDistributionId dim_distribution_id
    ,fts.sourcedistributionpointid source_distribution_point_id
    ,fts.sourcedistributionpoint source_distribution_point 
    ,fts.SourceDistributionChannel source_distribution_channel
    ,fts.SourceDistributionChannelName source_distribution_channel_name
    ,fts.SourceClientId source_client_id
    ,fts.SourceSalesPartner source_sales_partner
    
    -- additional Source information
    ,fts.SourcePromotionId source_promotion_id
    ,fts.SourcePromotionName source_promotion_name
    ,fts.SourcePromotionCode source_promotion_code
    ,fts.SourcePromotionAdvertisingPartnerId source_promotion_advertising_partner_id
    ,fts.SourceCustomerCode source_customer_code
    --GoldenCustomer
	  ,fts.DimGoldenCustomerId dim_golden_customer_id
    ,fts.DimGoldenCustomerAgeAtBookingId dim_golden_customer_age_at_booking_id 
    -- Seat
    ,fts.SourceSeat source_seat
    ,fts.SourceSeatRow source_seat_row
    ,fts.SourceSeatNumber source_seat_number
    --PriceType
    ,fts.DimPriceTypeId dim_price_type_id
    ,fts.DimPriceCategoryId dim_price_category_id
    ,fts.SourcePriceCategoryId source_price_category_id
	  ,fts.SourcePriceTypeName source_price_type_name
    --sums
    ,fts.ArticleCount article_count
    ,fts.eurpaidprice  euro_paid_price
    ,fts.eurcustomerfacevalue euro_customer_face_value
    ,fts.EURNetPriceOld net_price_value_eur
    ,fts.EURNetNetPriceOld net_net_price_value_eur
    ,fts.EURTicketPriceOld ticket_price_value_eur
    ,fts.EURCustomerPriceOld customer_price_value_eur
    ,fts.EURTicketPriceOld * 1.07937 tpt_de_value_eur
FROM  fts



