{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  customer AS (
   SELECT *  
   FROM {{ ref( 'customers') }}

  )


SELECT country_code,
      customer_id,
      COALESCE( email, phone_number,concat(postcode,customer_name), customer_id )  golden_customer_id
    from customer
 


