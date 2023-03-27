{{ config(
    materialized='view',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
    grant_access_to=[
      {'project': 'stage-playground', 'dataset': 'sales'},
      {'project': 'stage-commercial', 'dataset': 'sales'}
    ]
)}}



with customer as
(
  select * FROM {{ ref('golden_customer' )}}
  
) 
SELECT 
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