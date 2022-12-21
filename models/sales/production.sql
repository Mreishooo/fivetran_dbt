{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimproduction AS (
   SELECT *  
   FROM {{ source( 'ft_mdb7_dbo','dimproduction') }}
   where {{ ft_filter(none) }} 
  )


SELECT
dimproductionid	dim_production_id,	
dimcountryid dim_country_id	, 	
isexpired	is_expired,
licenseclassification	 license_classification,	
licensename	 license_name,	
licensetype license_type	,	
licensor	 licensor,	
productioncode	 production_code,	
productionid production_id	,	
productionname production_name	,	
FROM
  dimproduction