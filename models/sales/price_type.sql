{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimpricetype AS (
   SELECT *  
   FROM {{ source( 'ft_mdb7_dbo','dimpricetype') }}
   where {{ ft_filter(none) }} 
  )


SELECT
dimpricetypeid dim_price_type_id	,	
pricetypegroup price_type_group	,	
pricetypeid	 price_type_id,	
pricetypename	price_type_name,
FROM
  dimpricetype