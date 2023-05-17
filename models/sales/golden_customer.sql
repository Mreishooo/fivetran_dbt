{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'enriched'},
)}}

with  
  golden_customer AS (
   SELECT *  
   FROM {{ source( 'ft_mdb7_dbo','dimgoldencustomer') }}
   where {{ ft_filter('DimGoldenCustomerId') }} 
  )


SELECT 
      DimGoldenCustomerId  dim_golden_customer_id
      , createdcustomerid customer_id
      , CustomerCode  customer_code
      , ClientId  client_id
      , SubClientId  sub_client_id
      , CountryCode  country_code
      , SubClient  sub_client
      , CustomerCategory customer_category 
      , INITCAP(CustomerType)  customer_type
      , CompanyName  company_name
      , Salutation  salutation
      , FirstName  first_name
      , LastName last_name
      , Address address
      , Postcode postcode 
      , City  city
      ,PhoneNumber  phone_number
      , FaxNumber   fax_number
      , MobileNumber  mobile_number
      , Email   email
      , OptInId  opt_in_id
      , OptInName  opt_in_name
      , BirthDay  birthday
      , Region region 
      , CreatedOn created_on 
      , LastModifiedOn  last_modified_on
      , CustomerCountryCode  customer_country_code
      , CurrentAge  current_age
      , CreatedCustomerId created_customer_id 
      , Name customer_name
      , NameUPPER name_upper
      , StartTime  start_time
      , EndTime  end_time
      , DimZipCodeId  dim_zip_code_id
      , DimSourceId  dim_source_id
      , DimFileId  dim_file_id
      , OriginalName  original_name
      , OriginalEmail  original_email
      , Gender  gender
      , Cluster2017NielsenStatic cluster_2017_nielsen_static 
      , Cluster2017DlwStatic cluster_2017_dlw_static 
      , Cluster2017DlwStaticId  cluster_2017_dlw_static_id
      , Cluster2017DlwDynamicId cluster_2017_dlw_dynamic_id
      , IsEmailHardBounced  is_email_hard_bounced
      , Cluster2017NielsenStaticId  cluster_2017_nielsen_static_id
      , isGDPRAnonymized is_gdpr_anonymized 
      , GDPRAnonymizedDate  gdpr_anonymized_date
      , GDPRDateOfRequest  gdpr_date_of_request
      , GDPRRequestedByName gdpr_requested_by_name
      , GDPRRequestedByDepartment gdpr_requested_by_department 
FROM  golden_customer



