
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},

)}}

{% set prices  = ["ticket_price","paid_price","net_price","net_net_price","customer_price","customer_facevalue"] %}


with tickets_mapped as
(
 SELECT *  
   FROM {{ ref( 'tickets_mapped') }}
   where booking_date >='2023-01-01'

),

 tickets_mdb as
(
 SELECT *  
   FROM {{ ref( 'tickets_mdb') }}
   where booking_date <'2023-01-01'
),


exchange_rate_daily as
(
 SELECT *  
   FROM {{ ref( 'exchange_rate_daily') }}

) ,



t1 as ( 
  
 select 
    concat( fts.Country_Code ,'-',fts.Source_Code,'-' ,fts.BarCode,'-',transaction_type) fact_ticket_sales_id 

    ,fts.Country_Code country_code
    ,currency_code
    ,fts.source_code
    ,fts.web_order_number
    ,fts.main_order_number
    ,fts.sub_order_number
    ,fts.barcode
    ,substr(fts.BarCode, 0 ,13) barcode_13
    -- Transaction / status
    ,fts.transaction_type
    ,cast(fts.cancellation_status as boolean)  cancellation_status
    ,false is_replaced --here
    ,false is_replacement--here
    ,cast (null as string) replacement_type --here
    ,CASE
       WHEN fts.transaction_type = 'Cancellation'   AND fts.cancellation_status = 0  THEN true
       ELSE false
     END                              AS  is_replaced_cancellation
    --Booking Date
    , Date(booking_date) AS booking_date
    , booking_date booking_timestamp
    --Performance Date
    , performance_date 
   
	  ,fts.theatre_id
    -- Distribution
    
   -- ,fts.distribution_id --here 
    ,fts.source_distribution_point_id
    ,fts.source_distribution_point 
    ,fts.source_distribution_channel
    ,fts.source_distribution_channel_name
    ,fts.source_client_id
    ,fts.source_sales_partner
    ,fts.source_sales_partner_class
    -- additional Source information

    ,fts.source_promotion_id
    ,fts.source_production
    ,fts.source_promotion_name
    ,fts.source_promotion_code
    ,cast (fts.source_promotion_advertising_partner_id as string) source_promotion_advertising_partner_id
    ,fts.source_customer_code
    
    
    /*--GoldenCustomer
	  ,fts.DimGoldenCustomerId dim_golden_customer_id
    ,fts.DimGoldenCustomerAgeAtBookingId dim_golden_customer_age_at_booking_id 
    */
    -- Seat
    ,fts.source_seat source_seat
    ,fts.seat_row source_seat_row
    ,fts.seat_number source_seat_number
    --PriceType
    ,fts.source_price_category_id
    ,fts.source_price_type_id
	  ,fts.source_price_type_name
    --sums
    ,fts._last_update
    ,fts._loaded_at
    ,current_timestamp _run_at
    ,"BQ" _source 

    ,fts.article_count 
  
    ,ticket_price_old orignal_ticket_price
    ,ticket_price_old ticket_price
    ,Paid_Price_Old paid_price --FOR ES, A COMMISSION ADJUSTMENT IS ADDED TO THE PAID PRICE
    ,if  ( Country_Code = 'DE' ,ifnull(ticket_price_old,0)*(1-0.099-0.006)   , ticket_price_old  )   net_price
    ,if  ( Country_Code = 'DE' ,ifnull(ticket_price_old,0)*(1-0.099-0.006)   , ticket_price_old  )   / (1+ ifnull (vat,0))   net_net_price
    ,Customer_Price_Old customer_price
    ,Paid_Price_Old - Outside_Commissions  customer_facevalue
FROM
  tickets_mapped AS fts

)

select t1.* 
{% for price in prices %}
    ,{{price}} *   ifnull(rate,1)  as {{price}}_euro
{% endfor %} 
, ticket_price *   ifnull(rate,1)   * 1.07937 as tpt_de_value_eur

from  t1 left join exchange_rate_daily on booking_date = date and currency_code = from_currency_code


union all 
select * from tickets_mdb 
 