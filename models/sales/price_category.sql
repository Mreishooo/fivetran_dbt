{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimpricecategory AS (
   SELECT *  
   FROM {{ source( 'ft_mdb6_dbo','dimpricecategory') }}
   where {{ ft_filter(none) }} 
  )


SELECT
dimpricecategoryid dim_price_category_id,	
dimproductionlocationid dim_production_location_id,	
pricecategorygroup price_category_group,	
pricecategoryid price_category_id,	
pricecategoryname price_category_name,	
sortcode sort_code

FROM
  dimpricecategory