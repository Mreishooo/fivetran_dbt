
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},
)}}

-- union all sources 

with cts_tickts as
(
 select *  
   from {{ ref( 'cts_tickets') }}
   where booking_date >='2023-01-01'

)
,
elektra_tickets as
(
 select *  
   from {{ ref( 'elektra_tickets') }}
   where booking_date >='2023-01-01'

)
,
eci_tickets as
(
 select *  
   from {{ ref( 'eci_tickets') }}
   where booking_date >='2023-01-01'

)
SELECT
  concat( ts.Country_Code ,'-',ts.Source_Code,'-' ,ts.BarCode,'-',transaction_type) ticket_id,
  ts .source_code,
  ts .barcode,
  ts .customer_id source_customer_id,
  ts .client_id source_client_id,
  ts .subclient_id source_subclient_id,
  ts .subclient source_subclient,
  ts .customer_code source_customer_code,
  ts .main_order_number,
  ts .sub_order_number,
  ts .reservation_date,
  ts .booking_date,
  ts .cancellation_date,
  ifnull(trim ( ts.source_cancellation_reason), '_n/a') as source_cancellation_reason,
  ts .source_payment_status_id,
  ts .source_print_status_id,
  ts .cancellation_status,
  ts .transaction_type,
  ts .source_production,
  ts .performance_date,
  ts .source_location,
  ts .source_seat,
  ts .seat_row,
  ts .seat_number,
  ts .end_price,
  ts .source_price_category_id,
  ts .source_price_type_id,
  ts .source_price_type_name,
  ts .source_distribution_channel,
  ts .source_distribution_channel_name,
  ifnull(trim(ts .source_delivery_method),'_n/a') as source_delivery_method,
  ts .source_promotion_id,
  ts .source_promotion_name,
  ts .source_promotion_advertising_partner_id,
  ts .source_referer,
  ts .source_distribution_point_id,
  ts .source_distribution_point,
  ts .venue_city,
  ifnull(trim(ts.source_payment_method), '_n/a') as source_payment_method,
  ts .ticket_fee1,
  ts .web_order_number,
  ts .free_ticket_reason,
  ts .country_code,
  ts .file_id,
  ts .source_campaign_code,
  ts .delta_reservation_booking,
  /******* price analysis 1.0 ********/ 
  ts .ticket_price_old,
  ts .customer_price_old,
  ts .paid_price_old,
  ts .net_price_old,
  /******* price analysis 2.0 ********/ 
  ts .paid_price,
  ts .outside_commissions,
  ts .customer_facevalue,
  ts .inside_commissions,
  ts .inside_commissions_resellers,
  ts .ticket_price,
  ts .gbor_net,
  ts .deductions,
  ts .nbor,
  /******* end price analysis********/ 
  ts .source_promotion_code,
  ts .source_sales_partner,
  ts .source_sales_partner_class_id,
  ts .source_sales_partner_class,
  ts .source_agency_id,
  ts .source_device_type,
  ts .source_package_id,
  ts._last_update,
  _loaded_at,
  current_timestamp _run_at
FROM
  cts_tickts ts

UNION ALL
  /*Add Elektra*/
SELECT
  concat( Country_Code ,'-',Source_Code,'-',barcode,'-', Cancellation_Status,'-', Transaction_Type ) ticket_id,
  Source_Code,
  BarCode,
  Customer_Id,
  Client_Id,
  SubClient_Id,
  SubClient,
  Customer_Code,
  cast(Main_Order_Number as string) Main_Order_Number,
  Sub_Order_Number,
  timestamp(date(Reservation_Date)),
  timestamp(date(Booking_Date)),
  timestamp(date(Cancellation_Date)),
  '_N/A' AS Source_Cancellation_Reason,
  IFNULL(Payment_Status_Id, - 1) AS SourcePaymentStatusId,
  Source_Print_Status_Id,
  Cancellation_Status,
  Transaction_Type,
  Source_Production,
  timestamp(Performance_Date),
  Source_Location,
  Source_Seat,
  Seat_Row,
  Seat_Number,
  End_Price,
  Source_Price_Category_Id,
  Source_Price_Type_Id,
  Source_Price_Type_Name,
  Source_Distribution_Channel,
  Source_Distribution_Channel_Name,
  IFNULL(Source_Delivery_Method,  '_N/A') AS SourceDeliveryMethod,
  Source_Promotion_Id,
  Source_Promotion_Name,
  cast (null as int64) Source_Promotion_Advertising_Partner_Id,
  Source_Referer,
  Source_Distribution_Point_Id,
  Source_Distribution_Point,
  Venue_City,
  IfNULL(Payment_Type, '_N/A') AS SourcePaymentMethod,
  Ticket_Fee1,
  Web_Order_Number,
  Free_Ticket_Reason,
  Country_Code,
  File_Id, 
  Source_Campaign_Code,
  Delta_Reservation_Booking,
  /******* Price analysis 1.0 ********/  
  Ticket_Price_Old,
  Customer_Price_Old,
  Paid_Price_Old,
  Net_Price_Old,
  /******* Price analysis 2.0 ********/ 
  Paid_Price,
  Outside_Commissions,
  Customer_FaceValue,
  Inside_Commissions,
  Inside_Commissions_Resellers,
  Ticket_Price,
  GBOR_Net,
  Deductions,
  NBOR,
  Source_Promotion_Code,
  Source_Sales_Partner, 
  Source_Sales_Partner_Class_Id,
  Source_Sales_Partner_Class,
  Source_Agency_Id,
  Source_Device_Type,
  Source_Package_ID,
  _last_update,
  _loaded_at,
  current_timestamp _run_at
FROM
 elektra_tickets

union all

Select
concat( country_code ,'-',source_code,'-',barcode,'-', cancellation_status,'-', transaction_type ) ticket_id,
  source_code,
  barcode,
  customer_id,
  client_id,
  subclient_id,
  subclient,
  customer_code,
  main_order_number,
  sub_order_number,
  timestamp(reservation_date),
  booking_timestamp,
  timestamp(cancellation_date),
  '_n/a' as sourcecancellationreason,
  null as sourcepaymentstatusid,
  null as sourceprintstatusid,
  cancellation_status,
  transaction_type,
  source_production,
  performance_date_time,
  source_location,
  source_seat,
  seat_row,
  seat_number,
  end_price,
  source_price_category_id,
  source_price_type_id,
  source_price_type_name,
  /*additionalfeeotheramount, */ 
  source_distribution_channel,
  source_distribution_channel_name,
  '_n/a' as sourcedeliverymethod,
  '_n/a' as source_promotion_id,
  source_promotion_name,
  cast ( null as int64) source_promotion_advertising_partner_id,
  source_referer,
  source_distribution_point_id,
  source_distribution_point,
  venue_city,
  '_n/a' as source_payment_method,
  ticket_fee1,
  web_order_number,
  free_ticket_reason,
  country_code,
  file_id,
  source_campaign_code,
  cast(- 1 as int) as delta_reservation_booking,
  /******* price analysis 1.0 ********/ 
  ticket_price_old,
  customer_price_old,
  paid_price_old,
  net_price_old,
  /******* price analysis 2.0 ********/ 
  paid_price,
  outside_commissions,
  customer_facevalue,
  inside_commissions,
  inside_commissions_resellers,
  ticket_price,
  gbor_net,
  deductions,
  nbor,
  source_promotion_code,
  source_sales_partner,
  source_sales_partner_class_id,
  source_sales_partner_class,
  source_agency_id,
  source_device_type,
  -1 source_package_id,
  _last_update,
  _loaded_at,
  current_timestamp _run_at
  from eci_tickets