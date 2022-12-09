{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimproductionlocation AS (
   SELECT *  
   FROM {{ source( 'ft_mdb6_dbo','dimproductionlocation') }}
   where {{ ft_filter(none) }} 
  )


SELECT
dimproductionlocationid dim_production_location_id	,		
dimauditoriumid dim_auditorium_id	,	
dimcountryid	dim_country_id,	
dimproductionid	dim_production_id,	
isexpired	is_expired,		
productionlocationcapacity	production_location_capacity,	
productionlocationdernieredate	production_location_dernieredate,	
productionlocationdescription	production_location_description,	
productionlocationenddate	production_location_end_date,	
productionlocationid	production_location_id,	
productionlocationpremieredate	production_location_premiere_date,	
productionlocationstartdate	production_location_start_date,	
productionlocationtype	production_location_type,	
salesstartdate	sales_start_date,	
showingroupsalesreporting	showing_roupsales_reporting,	
showingroupsalesreportingname	showing_roupsales_reporting_name,	
FROM
  dimproductionlocation