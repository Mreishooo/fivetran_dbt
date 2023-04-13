{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',

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
   FROM {{ source( 'ft_mdb7_se','fact_ticket_sales_bq') }}
   where {{ ft_filter('FactTicketSalesId') }} 
  

   --union all
   --SELECT *  
   --FROM {{ source( 'ft_mdb_dbo','factticketsales') }}
   
  )


Select distinct 
    concat( fts.sourcecountrycode ,'-',fts.SourceCode,'-' ,fts.BarCode,'-',upper(TransactionType) ) fact_ticket_sales_id 

    ,fts.sourcecountrycode country_code
    ,'EUR' currency_code --fix
    ,fts.SourceCode source_code
    ,fts.WebOrderNumber web_order_number
    ,fts.MainOrderNumber main_order_number
    ,fts.SubOrderNumber sub_order_number
    ,fts.BarCode barcode
    ,substr(fts.BarCode, 0 ,13) barcode_13
    -- Transaction / status
    ,fts.TransactionType transaction_type
    ,cast(fts.CancellationStatus as boolean)  cancellation_status
    ,fts.rebooked is_replaced --here
    ,fts.replacement is_replacement--here
    ,fts.replacementtype replacement_type --here
    ,CASE
       WHEN fts.TransactionType = 'Cancellation'   AND cancellationstatus  THEN true
       ELSE false
     END                              AS  is_replaced_cancellation
    --Booking Date
      , PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string )) AS booking_date 
    , timestamp(PARSE_DATE("%Y%m%d", cast(DimBookingDateId as string ))) AS  booking_timestampe
    --Performance Date
    --fix
    , timestamp(PARSE_DATE("%Y%m%d", cast(DimPerformanceDateId as string ))) AS performance_date
   
	  ,'fts.TheatreId' theatre_id -- FIX
    -- Distribution
    
    --,fts.DimDistributionId dim_distribution_id --here 
     ,fts.sourcedistributionpointid source_distribution_point_id
    ,fts.sourcedistributionpoint source_distribution_point 
    ,fts.SourceDistributionChannel source_distribution_channel
    ,fts.SourceDistributionChannelName source_distribution_channel_name
    ,fts.SourceClientId source_client_id
    ,fts.SourceSalesPartner source_sales_partner
    ,fts.sourceSalesPartnerClass source_sales_partner_class

    -- additional Source information

    ,fts.SourcePromotionId source_promotion_id
    ,fts.sourceproduction source_production
    ,fts.SourcePromotionName source_promotion_name
    ,fts.SourcePromotionCode source_promotion_code
    ,fts.SourcePromotionAdvertisingPartnerId source_promotion_advertising_partner_id
    ,fts.SourceCustomerCode source_customer_code
    
    ---GoldenCustomer
	  --,fts.DimGoldenCustomerId dim_golden_customer_id
    --,fts.DimGoldenCustomerAgeAtBookingId dim_golden_customer_age_at_booking_id 
    
    -- Seat
    ,fts.SourceSeat source_seat
    ,fts.sourceseatrow source_seat_row
    ,fts.sourceSeatNumber source_seat_number
    --PriceType
    ,fts.SourcePriceCategoryId source_price_category_id
    ,fts.SourcePriceTypeId source_price_type_id
	  ,fts.SourcePriceTypeName source_price_type_name
    --_time
    ,fts._fivetran_synced _last_update
    ,fts._fivetran_synced _loaded_at
    ,current_timestamp() _run_at
    ,"MDB" _source 
    --sums
    ,fts.articlecount 
    ,fts.TicketPriceOld orignal_ticket_price
    ,fts.TicketPriceOld ticket_price
    ,fts.PaidPriceOld paid_price --FOR ES, A COMMISSION ADJUSTMENT IS ADDED TO THE PAID PRICE
    ,fts.netpriceold net_price
    ,fts.netnetpriceold net_net_price
    ,fts.CustomerPriceOld customer_price
    ,fts.customerFacevalue customer_Face_value

    ,fts.EURTicketPriceOld   as ticket_price_euro
    ,fts.eurpaidprice  as paid_price_euro
    ,fts.EURNetPriceOld  as net_price_euro
    ,fts.EURNetNetPriceOld  as net_net_price_euro
    ,fts.EURCustomerPriceOld  as customer_price_euro
    ,fts.eurcustomerfacevalue as customer_Face_value_euro
    ,fts.EURTicketPriceOld  * 1.07937 as tpt_de_value_eur


FROM  fts



