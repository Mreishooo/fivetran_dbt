
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'cts', 'refresh': 'daily','connection':'fivetran','type':'enrich'},
)}}


with cts as
(
 SELECT *  
   FROM {{ source( 'ft_sales2','cts_verkauf') }}

),

cts_data as (
SELECT 
_file,
_line,
_fivetran_synced _loaded_at, 
_modified  _last_update, 
current_timestamp _run_at,
Concat ('CTS-',ifnull( V.FIELD_38, substr(V._File,7,2)),'-' , V.VERKAUFSMANDANT ,'-' ,V.SUBMANDAT_ID,'-' ,V.KUNDENNUMMER ) AS customer_id ,
'CTS' AS source_code,
V.KARTEN_ID AS barcode,
V.VERKAUFSMANDANT AS client_id,
V.SUBMANDAT_ID AS subclient_id,
ifnull(V.SUBMANDANT,'_N/A')  AS subclient,
V.KUNDENNUMMER AS customer_code,
CAST(V.AUFTRAGSNUMMER AS String ) AS main_order_number,
V.AUFTRAG_POS_ID AS sub_order_number,
PARSE_TIMESTAMP("%d.%m.%Y %H:%M:%S", ifnull(V.RESERVIERUNGSDATUM,"1900.01.01 00:00:00")) AS reservation_date,
PARSE_TIMESTAMP( "%d.%m.%Y %H:%M:%S", ifnull(V.BUCHUNGSDATUM ,"1900.01.01 00:00:00")) AS booking_date,
STORNOSTATUS as cancellation_status ,
IF( V.STORNOSTATUS = 1 , PARSE_TIMESTAMP( "%d.%m.%Y %H:%M:%S", V.BUCHUNGSDATUM) , 
    PARSE_TIMESTAMP( "%d.%m.%Y %H:%M:%S", replace ( V.STORNODATUM ,  '0001' ,'1900' )))AS cancellation_date,
ifnull (V.STORNOGRUND,'_N/A' ) AS source_cancellation_reason,
if (BUCHUNGSDATUM is null or BUCHUNGSDATUM='' , 0,1 ) as source_payment_status_id,
V.DRUCKSTATUS AS source_print_status_id,
IF( V.STORNOSTATUS = 1 , 'Cancellation' , 'Sale' ) AS transaction_type,
trim(VERANSTALTUNG) AS source_production,
PARSE_TIMESTAMP( "%d.%m.%Y %H:%M:%S", VORSTELLUNGSDATUM)AS performance_date,
trim(V.SAALPLANNAME) AS source_location ,
ifnull( trim(V.BEREICH) ,'_N/A' ) AS source_seat,
ifnull( trim(V.REIHE) ,'_N/A' ) AS seat_row,
ifnull( trim(V.PLATZ) ,'_N/A' ) AS seat_number,
ifnull ( safe_cast (replace( V.BUCHUNGSPREIS,',','.') as NUMERIC) ,0.0 ) AS end_price, 
concat ( trim( cast(V.PREISKLASSE as string )) ,' ', ifnull( trim(V.PREISKLASSENNAME),'') ) AS source_price_category_id,
COALESCE( trim( cast(V.RABATTSTUFE as string )) ,'_N/A' ) as source_price_type_id,
COALESCE (trim(V.RABATTTEXT),'_N/A') AS source_price_type_name,
ifnull ( safe_cast (replace( V.VORVERKAUFSGEBUEHR,',','.') as NUMERIC) ,0.0 )  AS presales_fee,
ifnull ( safe_cast (replace( V.SYSTEMGEBUEHR,',','.') as NUMERIC) ,0.0 ) AS system_fee,
ifnull ( safe_cast (replace( V.ZUSATZGEBUEHR_1,',','.') as NUMERIC) ,0.0 ) AS additional_fee1_amount,
V.ZUSATZGEBUEHR_1_NAME AS additional_fee1_name,
ifnull ( safe_cast (replace( V.ZUSATZGEBUEHR_2,',','.') as NUMERIC) ,0.0 ) AS additional_fee2_amount,
V.ZUSATZGEBUEHR_2_NAME AS additional_fee2_name,
ifnull ( safe_cast (replace( V.ZUSATZGEBUEHR_3_10,',','.') as NUMERIC) ,0.0 ) AS additional_fee_other_amount ,
V.REFUNDIERUNG AS presales_fee_share,
ifnull(TRIM(V.VERTRIEBSWEG),'_N/A') AS source_distribution_channel,
ifnull(TRIM(V.VERSANDART),'_N/A')  AS source_delivery_method,
ifnull(TRIM(cast (V.PROMOTION_ID as string) ),'_N/A') as source_promotion_id,
ifnull(TRIM(V.PROMOTIONCODE ),'_N/A') as source_promotion_code,
ifnull(TRIM(V.PROMOTION ),'_N/A') as source_promotion_name,
V.WERBEPARTNER AS source_promotion_advertising_partner_id,
ifnull(TRIM(V.REFERER_ID ),'_N/A') as source_referer,
ifnull(TRIM(V.AFFILIATE_ID ),'_N/A') as source_distribution_point_id,
ifnull(TRIM(V.FIELD_01 ),'_N/A') as source_distribution_point,
ifnull(TRIM(V.FIELD_02 ),'_N/A') as source_campaign_code /* Only used in SP: linked to campaigns of resellers. */,
ifnull(TRIM(V.FIELD_03 ),'_N/A') as venue_city,
ifnull(TRIM(V.FIELD_05 ),'_N/A') as source_payment_method,
ifnull(TRIM(V.FIELD_04 ),'_N/A') as source_distribution_channel_name,
ifnull(TRIM(V.SUBMANDANT ),'_N/A') as source_sales_partner,
ifnull(TRIM(V.AGENTUR ),'_N/A') as source_agency_id,
'_N/A' as source_sales_partner_class_id,
'_N/A' AS source_sales_partner_class, 
ifnull(TRIM(V.FIELD_39 ),'_N/A') as source_device_type,
FIELD_40 as source_package_id,
ifnull ( safe_cast (replace( V.FIELD_06,',','.') as NUMERIC ) ,0.0 ) as fee_calculation,
ifnull ( safe_cast (replace( V.FIELD_08,',','.') as NUMERIC) ,0.0 ) as ticket_fee1,
ifnull ( safe_cast (replace( V.FIELD_07,',','.') as NUMERIC) ,0.0 ) AS field07 ,
ifnull ( safe_cast (replace( V.FIELD_09,',','.') as NUMERIC) ,0.0 ) AS field09 ,
ifnull ( safe_cast (replace( V.FIELD_10,',','.') as NUMERIC) ,0.0 ) AS field10 ,

--ifnull ( safe_cast (replace( V.ZUSATZGEBUEHR_3_10,',','.') as float64) ,0.0 ) AS Additional_fee_other_amount ,

ifnull((V.FIELD_37 ),-1) as web_order_number,
ifnull(TRIM(V.FIELD_38 ),LEFT(V._file,2))  as country_code,
null AS etl_file_upload_id,
_file file_id,
/********* Price analysis 1.0 **********/

FROM cts  V
where true 
--and _file like '%20230108%' 
and ifnull(KARTEN_ID, '') <> '' 
--and _line = 10
QUALIFY ROW_NUMBER() OVER (PARTITION BY BUCHUNGSDATUM,  KARTEN_ID , STORNOSTATUS ORDER BY _fivetran_synced DESC) = 1
)

, cts_price_analysis as( 
select   * ,
IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ) ) 
- IF ( Cancellation_status =1 , -1 * ABS(Presales_fee ) , ABS(Presales_fee ) )
- IF ( Cancellation_status =1 , -1 * ABS(System_fee ) , ABS(System_fee ) )
- IF ( Cancellation_status =1 , -1 * ABS(Additional_fee_other_amount ) , ABS(Additional_fee_other_amount ) )
- IF ( Cancellation_status =1 , -1 * ABS(Additional_fee2_amount ) , ABS(Additional_fee2_amount ) )
- IF ( Cancellation_status =1 , -1 * ABS(Additional_fee1_amount ) , ABS(Additional_fee1_amount ) ) as ticket_price_old,

IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ) ) 
+ IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ) )
+ IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ) )
+ IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ) )
+ IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 ) ) as paid_price_old,

Case Country_Code
when 'NL' then 
  IF (Cancellation_status = 1, -1 * ABS(End_price ),ABS(End_price )) - Presales_fee - Additional_fee2_amount
when 'ES' then 
  IF (Cancellation_status = 1, -1 * ABS(End_price ),ABS(End_price )) - Presales_fee
when 'RU' then 
  IF (Cancellation_status = 1, -1 * ABS(End_price ),ABS(End_price )) - Presales_fee
else   
  IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ))  
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ))
  + IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 )) 
END  AS customer_price_old,

CASE
when Country_Code IN ('DE', 'RU', 'ES') then
  IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ))  
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ))
  + IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 )) 
when Country_Code IN ('FR') then
  IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ))  
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ))
  + IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 )) 
  - IF ( Cancellation_status =1 , -1 * ABS(Additional_fee1_amount ) , ABS(Additional_fee1_amount )) 
when Country_Code IN ('NL') then
  IF( Cancellation_status =1, -1 * ABS(End_price ) , ABS(End_price ))  
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ))
  + IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 )) 
  - IF ( Cancellation_status =1 , -1 * ABS(Additional_fee1_amount ) , ABS(Additional_fee1_amount )) 
  - IF ( Cancellation_status =1 , -1 * ABS(Additional_fee2_amount ) , ABS(Additional_fee2_amount )) 
End as paid_price,
Case Country_Code
when 'DE' then 
 IF( Cancellation_status =1, -1 * ABS(Presales_fee ) , ABS(Presales_fee ))  
  + IF ( Cancellation_status =1 , -1 * ABS(System_fee ) , ABS(System_fee ))
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee1_amount ) , ABS(Additional_fee1_amount ))
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee2_amount ) , ABS(Additional_fee2_amount ))
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee_other_amount ) , ABS(Additional_fee_other_amount )) 
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD07 ) , ABS(FIELD07 ))
  + IF ( Cancellation_status =1 , -1 * ABS(Ticket_Fee1 ) , ABS(Ticket_Fee1 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD09 ) , ABS(FIELD09 ))
  + IF ( Cancellation_status =1 , -1 * ABS(FIELD10 ) , ABS(FIELD10 )) 
when 'NL' then 
 IF ( Cancellation_status =1 , -1 * ABS(System_fee ) , ABS(System_fee ))
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee_other_amount ) , ABS(Additional_fee_other_amount )) 
when 'ES' then 
 IF ( Cancellation_status =1 , -1 * ABS(System_fee ) , ABS(System_fee ))
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee1_amount ) , ABS(Additional_fee1_amount )) 
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee2_amount ) , ABS(Additional_fee2_amount )) 
  + IF ( Cancellation_status =1 , -1 * ABS(Additional_fee_other_amount ) , ABS(Additional_fee_other_amount ))
when 'FR' then 
   0.0
End as inside_commissions,
Case Country_code
when 'FR' then 
 IF( Cancellation_status =1, -1 * ABS(Presales_fee ) , ABS(Presales_fee ))  
else 
  0.0
End inside_commissions_resellers,

CASE
when Country_code IN ('ES', 'NL', 'RU')  then
   IF( Cancellation_status =1, -1 * ABS(Presales_fee ) , ABS(Presales_fee )) 
else
   0.0
END AS outside_commissions,
0.0 AS deductions
from cts_data)

select *  ,
DATE_DIFF(reservation_date, booking_date, day ) AS delta_reservation_booking,
if(Country_code='DE',Ticket_Price_Old * (1 - 0.099 - 0.006) , -1) net_price_old,  
Paid_Price - Outside_Commissions AS customer_facevalue,
Paid_Price - Outside_Commissions - Inside_Commissions AS ticket_price,  
0.0 gbor_net,
0.0 nbor,
if (Country_code = 'ES' ,
      CASE trim(Source_Price_Type_Name) 
                WHEN 'Reubicación' THEN 'Relocation Ticket' 
                WHEN 'Invitación' THEN 'Invitation Ticket' 
                WHEN 'Invitación Grupos' THEN 'Group Invitation Ticket' 
                WHEN 'Intercambio' THEN 'Exchanged Ticket' 
                ELSE '_N/A' END 
        , '_N/A' ) as free_ticket_reason

from cts_price_analysis
