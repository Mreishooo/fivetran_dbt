{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with customers_stg as
(
  select * FROM {{ ref('customers_stg' )}}
  
) 
 
select *
FROM customers_stg
where country_code = 'DE'
