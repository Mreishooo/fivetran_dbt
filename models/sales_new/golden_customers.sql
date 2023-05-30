{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  customer AS (
   SELECT 
   COALESCE( email, phone_number,concat(postcode,customer_name), customer_id ) golden_customer_id, *  
   FROM {{ ref( 'customers') }}
  

  )


SELECT country_code ,golden_customer_id
      , FIRST_VALUE(customer_category IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) customer_category 
      , FIRST_VALUE(customer_type IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) customer_type
      , FIRST_VALUE(company_name IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) company_name
      , FIRST_VALUE(salutation IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) salutation
      , FIRST_VALUE(first_name IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) first_name
      , FIRST_VALUE(last_name IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) last_name
      , FIRST_VALUE(Address IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  address
      , FIRST_VALUE(postcode IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  postcode 
      , FIRST_VALUE(city IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  city
      , FIRST_VALUE(phone_number IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  phone_number
      , FIRST_VALUE(fax_number IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  fax_number
      , FIRST_VALUE(mobile_number IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    )  mobile_number
      , FIRST_VALUE(email IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) email
      , FIRST_VALUE(opt_in_id IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) opt_in_id
      , FIRST_VALUE(opt_in_name IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) opt_in_name
      , FIRST_VALUE(birthday IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) birthday
      , FIRST_VALUE(region IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) region 
      , FIRST_VALUE(gender IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) gender 
      , FIRST_VALUE(customer_code IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) customer_code 
      , last_VALUE(last_modified_on IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) last_modified_on
      , FIRST_VALUE(customer_country_code IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) customer_country_code
      , FIRST_VALUE(current_age IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) current_age
      , FIRST_VALUE(customer_name IGNORE NULLS)  OVER (PARTITION BY golden_customer_id    ORDER BY created_on ASC    ) customer_name
  ,created_on
FROM  customer
  QUALIFY ROW_NUMBER() OVER (PARTITION BY golden_customer_id ORDER BY  created_on ASC)  = 1 
 



