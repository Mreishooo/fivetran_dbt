
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'cts', 'refresh': 'daily','connection':'fivetran','type':'enrich'},
)}}


with elektra as
(
 SELECT *  
   FROM {{ source( 'ft_mdb8_stagingelektrahistorical','ticketsales') }}
   where {{ ft_filter(none) }}
),

elektra_changes as
(
  SELECT
    '' Main_Order_Number,
    '' Barcode,
    0 Total_Charge 

),

elektra_data as (
SELECT 
_fivetran_synced _loaded_at, 
_fivetran_synced   _last_update, 
concat ( 'ELEKTRA-DE-' , TS.ClientId , '-' , CustomerNumberDwh) AS customer_id
,'ELEKTRA' AS source_code
,CONCAT(TS.ClientId, '-', TS .MainOrderNumber, '-', TS .SystemId, '.', TS .SubOrderIdConcatenation, '.', TS .SubOrderLineNumber) AS barcode
,ClientId client_id
,SystemId system_id
,SubOrderIdConcatenation sub_order_id_concatenation
,SubOrderLineNumber sub_order_line_number
,'_N/A' AS subclient,
TS .BoxOfficeNumber AS subclient_id,
TS .CustomerCode customer_code,
TS .MainOrderNumber AS main_order_number,
TS .SubOrderNumber sub_order_number,
IFNULL(TS .TicketReservationDateTime, '1900-01-01') AS reservation_date,
IFNULL( TS .CancellationDate, DATE ('1900-01-01') ) AS cancellation_date,
TS .PaymentStatusId AS payment_status_id,
TS .PrintStatusId AS source_print_status_id,
if( TS .TicketCancellationStatus=0,0,1) AS cancellation_status,
TRIM(TS .ProductionName) AS source_production,
IfNULL(TS .PerformanceDate, '1900-01-01') AS performance_date,
IfNULL(trim(TheatreAddressComplete), '_N/A') AS source_location,
CONCAT(TRIM(TS .SeatLevel), ' ', TRIM(TS .SideName)) AS source_seat,
IFNULL (if (TS .SeatRow = '', null ,TS .SeatRow ),'_N/A') AS seat_row,
IFNULL (if (TS .SeatNumber = '', null ,TS .SeatRow ),'_N/A') AS seat_number,
 /*PRICING*/ 
RealizedPrice AS end_price,
IFNULL(TRIM(TS .PriceCategory),'_N/A') AS source_price_category_id,
IFNULL(TRIM(cast (TS .ReductionId as string) ),'_N/A') AS source_price_type_id,
IFNULL(TRIM(TS .ReductionName),'FP - ELEKTRA') AS source_price_type_name,
TS .AdvancedSalesFee AS presales_fee,
0 AS system_fee,
0 AS additional_fee1_amount,
0 AS additional_fee2_amount,
0 AS additional_fee_other_amount,
IfNULL(cast (TS .CreationUserSalesClassCode as string), '_N/A') AS source_distribution_channel,
TS .DeliveryMethod AS source_delivery_method,
IFNULL(TRIM(cast (TS .PromotionCode as string)),'_N/A') AS source_promotion_id,
IFNULL(TRIM(TS .PromotionName),'_N/A') AS source_promotion_name,
'_N/A' AS source_referer,
TS .City AS venue_city,
TS .PaymentType payment_type,
0 AS fee_calculation,
0 AS ticket_fee1,
TS .MainOrderNumber AS web_order_number,
'_N/A' AS free_ticket_reason,
'_N/A' AS source_campaign_code,
'DE' AS country_code,
- 1 AS etl_file_upload_id,
'_N/A' AS file_id,
IFNULL(TRIM(TS .CreationUserSalesClass),'_N/A') AS source_distribution_channel_name,
IFNULL(TRIM(cast(TS .CustomerSalesClassCode as string)),'_N/A') AS source_distribution_point_id,
IFNULL(TRIM(TS .CustomerSalesClass),'_N/A') AS source_distribution_point,
if ( CustomerNumberDwh LIKE 'EDB%' , TS .Agency , '_N/A') source_sales_partner,
IFNULL(TRIM(TS .SourceSalesPartnerClassId),'_N/A') AS source_sales_partner_class_id,
IFNULL(TRIM(TS .SourceSalesPartnerClass),'_N/A') AS source_sales_partner_class,
'_N/A' AS source_agency_id,
'_N/A' AS source_device_type,
-1 AS source_package_id,
TS .DeltaReservationBooking AS delta_reservation_booking

FROM elektra  TS
where true 

QUALIFY ROW_NUMBER() OVER (PARTITION BY TS.ClientId, TS .MainOrderNumber, TS .SystemId, TS.SubOrderIdConcatenation,  TS.SubOrderLineNumber , TS.TicketCancellationStatus , TS.TicketReservationDateTime ORDER BY _fivetran_synced DESC) = 1
)



select   E.*,
 
    ifnull (E.End_Price, 0) AS ticket_price_old,
    IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0)  AS paid_price_old,
    IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0)  AS customer_price_old,
    IfNULL(E.End_Price, 0) *  (1 - 0.099 - 0.006)  AS net_price_old,

  /******* Price analysis 2.0 ********/ 
    IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0)  AS paid_price,
    IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0)  AS customer_facevalue,
    IfNULL(E.Presales_Fee, 0) + IfNULL(SC.Total_Charge,0)    AS inside_commissions,
    0 AS inside_commissions_resellers,
    IfNULL(E.End_Price, 0) ticket_price,
    0 AS Outside_Commissions,
    0 AS gbor_net,
    0 AS deductions,
    0 AS nbor,
    Reservation_Date AS booking_date,
    'Sale' AS transaction_type,
    '_N/A' AS source_promotion_code /*2018-03-08 - SR-217118*/
FROM
  elektra_data E
LEFT JOIN
  elektra_changes SC
ON
  E.BarCode = SC.Barcode  
     /* A sold ticket in ELEKTRA can have a cancellationstatus = 1 and cancellationdate != null
   For every SALE ticket, which was cancelled, an extra record should be added with transaction type CANCELLATION (union) and bookingdate = cancellationdate */

union all
select   E.*,
 
    (ifnull (E.End_Price, 0) ) * -1 AS ticket_price_old,
    (IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0)) * -1  AS paid_price_old,
    (IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0) ) * -1 AS customer_price_old,
    (IfNULL(E.End_Price, 0) *  (1 - 0.099 - 0.006)) * -1  AS net_price_old,

  /******* Price analysis 2.0 ********/ 
    (IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0) ) * -1 AS paid_price,
    (IfNULL(E.End_Price, 0) + IfNULL(E.Presales_Fee,0) + IfNULL(SC.Total_Charge, 0) ) * -1 AS customer_facevalue,
    (IfNULL(E.Presales_Fee, 0) + IfNULL(SC.Total_Charge,0)  ) * -1  AS inside_commissions,
    0 AS inside_commissions_resellers,
    (IfNULL(E.End_Price, 0)) * -1 ticket_price,
    0 AS Outside_Commissions,
    0 AS gbor_net,
    0 AS deductions,
    0 AS nbor,
    Cancellation_Date AS booking_date,
    'Cancellation' AS transaction_type,
    '_N/A' AS source_promotion_code /*2018-03-08 - SR-217118*/
FROM
  elektra_data E
LEFT JOIN
  elektra_changes SC
ON
  E.BarCode = SC.Barcode  
  WHERE   Cancellation_Status = 1
