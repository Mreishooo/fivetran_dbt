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


SELECT 
        customerid customer_id 
      , CustomerCode  customer_code
      , ClientId  client_id
      , SubClientId  sub_client_id
      , CountryCode  country_code
      , SubClient  sub_client
      , CustomerCategory customer_category 
      , INITCAP(CustomerType)  customer_type
      , CompanyName  company_name
      , Salutation  salutation
      , if(FirstName='_N/A' , null , FirstName)    first_name
      , if(LastName='_N/A' , null , LastName)     last_name
      , Address address
      , if(Postcode='_N/A' , null , Postcode)  postcode 
      , City  city
      , if(PhoneNumber='_N/A' , null , PhoneNumber)  phone_number
      , FaxNumber   fax_number
      , MobileNumber  mobile_number
      , if(Email='_N/A' , null , email)   email
      , OptInId  opt_in_id
      , OptInName  opt_in_name
      , BirthDay  birthday
      , Region region 
      , CreatedOn created_on 
      , LastModifiedOn  last_modified_on
      , CustomerCountryCode  customer_country_code
      , if ( Date_diff(  current_date,birthday  , YEAR) < 0 , -1 , Date_diff(  current_date,birthday  , YEAR) ) current_age
      , if(Name='_N/A' , null , Name)   customer_name
 
      --, OriginalName  original_name
     -- , OriginalEmail  original_email
      , Gender  gender
      , sourcecode source_code
      , 'Buyer' customer_source  
FROM  customer
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY lastmodifiedon DESC) = 1 
 



