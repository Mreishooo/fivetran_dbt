
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_attribution',
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
Date(Sales_Start_Date)sales_start_date,
Date(Production_Location_Premiere_Date) production_location_premiere_date,
Date(Production_Location_Start_Date)production_location_start_date,
Date(Production_Location_End_Date)production_location_end_date,
Date(Production_Location_Derniere_Date)production_location_derniere_date,
is_expired,
production_location_description	,
production_location_type	,
production_location_capacity,
show_in_group_sales_reporting,
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
