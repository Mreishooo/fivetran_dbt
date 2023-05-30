{{ config(
    materialized='view',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
    grant_access_to=[
      {'project': 'stage-playground', 'dataset': 'business'},
      {'project': 'stage-commercial', 'dataset': 'business'}
    ]
)}}



with customer as
(
  select * FROM {{ ref('golden_customers' )}}
  
) 
SELECT distinct
country_code,
golden_customer_id,
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