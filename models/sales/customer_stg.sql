{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  customer AS (
   SELECT *  
   FROM {{ source( 'ft_mdb7_dbo','dimcustomer') }}
   where {{ ft_filter('DimCustomerId') }} 

  )


SELECT country_code,
       customerid customer_id,
      DimGoldenCustomerId  dim_golden_customer_id
     
FROM  customer
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY lastmodifiedon DESC) = 1 
 



