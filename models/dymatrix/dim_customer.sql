{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
   
)}}

 --grant_access_to= { 'project':  this.database , 'dataset': 'sales' }

with customer as
(
  select * FROM {{ ref('golden_customer' )}}
  
) 
SELECT distinct
country_code,
dim_golden_customer_id golden_customer_id,
salutation,
first_name,
last_name,
email,
company_name,
birthday,
gender,
customer_type,
mobile_number,
customer_code,
city, 
postcode,
phone_number ,
created_on,
last_modified_on,
FROM customer
where country_code = 'DE'