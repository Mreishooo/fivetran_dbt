
{{ config(
    materialized='table',
    on_schema_change='fail',

    labels = {'source': 'share_point', 'refresh': 'daily','connection':'fivetran','type':'source'},
)}}


with seed_date as
(
    SELECT *  
   FROM {{ source( 'ft_sales_seeds','production_locations_production_locations') }}

),
tickets as 
(
    SELECT *  
    FROM {{ ref( 'tickets_mapped') }}
 
)

SELECT distinct
production_location_id,
country_code ,
production_id ,
theatre_id ,
run ,
Date(Sales_Start_Date)Sales_Start_Date,
Date(Production_Location_Premiere_Date) Production_Location_Premiere_Date,
Date(Production_Location_Start_Date)Production_Location_Start_Date,
Date(Production_Location_End_Date)Production_Location_End_Date,
Date(Production_Location_Derniere_Date)Production_Location_Derniere_Date,
Is_Expired,
Production_Location_Description	,
Production_Location_Type	,
Production_Location_Capacity,
show_In_Group_Sales_Reporting,
show_in_group_sales_reporting_name,
_fivetran_synced last_update,
from seed_date

union all
SELECT  distinct
production_location_id,
SPLIT(production_location_id,' - ') [safe_OFFSET(0)] country_code ,
concat ( SPLIT(production_location_id,'-') [safe_OFFSET(0)] ,' - ', SPLIT(production_location_id,' - ') [safe_OFFSET(1)]) production_id ,
concat ( SPLIT(production_location_id,'-') [safe_OFFSET(0)] ,' - ', SPLIT(production_location_id,' - ') [safe_OFFSET(2)])   theatre_id ,
cast (SPLIT(production_location_id,' - ') [safe_OFFSET(3)] as int64) run ,
cast (null as Date),
cast (null as Date),
cast (null as Date),
cast (null as Date),
cast (null as Date),
cast (null as bool),
cast (null as string),
cast (null as string),
null,
true,
'yes',
cast (null as TIMESTAMP)
from  tickets where production_location_id not in ( select production_location_id from seed_date)
