
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},
)}}

 
with tickets as
(
 SELECT *  
   FROM {{ ref( 'tickets_filtered') }}

),

 
sales_production_mapping as
(
 SELECT *  
   FROM {{ ref( 'production_mapping') }}

),

sales_countries as
(
 SELECT *  
   FROM {{ ref( 'countries') }}

),
price_type_mapping as
(
 SELECT *  
   FROM {{ ref( 'price_type_mapping') }}

),

price_category_mapping as
(
 SELECT *  
   FROM {{ ref( 'price_category_mapping') }}

)


SELECT
 ts.* ,
  CASE WHEN strpos(',', source_payment_method) = 0 THEN source_payment_method ELSE LEFT(source_payment_method, strpos(',', source_payment_method) - 1) END
       AS source_payment_method_screening_payment_method,
  -- join table 
  c.country_name,c.currency_code, c.vat,	c.vat_high,
  PM.article_type_code, 
  ifnull(PM.production_location_id,concat(ts.Country_code, ' - unknown')) production_location_id, 
  ifnull(PM.theatre_id,concat(ts.Country_code, ' - unknown')) theatre_id ,
  if( ifnull( PM.delete_from_bi,0)  = 1 , true , false) delete_from_bi ,
  ifnull(pcm.price_category_id,concat(ts.Country_code ,' - unknown'))  price_category_id,
  ifnull(ptm.price_type_id,concat(ts.Country_code, ' - unknown'))  price_type_id,
  If (Transaction_type = 'Cancellation' , - 1 , 1 )  AS article_count
FROM
  tickets AS ts
 LEFT OUTER JOIN  sales_production_mapping AS PM
ON
      PM.Source_Code =  upper(ts.Source_Code)
  AND PM.Source_Location = upper(ts.Source_Location)
  AND PM.Source_Production = upper(ts.Source_Production)
  AND PM.Country_Code = upper(ts.Country_code)

  left join sales_countries c on upper(ts.Country_code) = c.Country_Code
 
  left join price_type_mapping ptm  on ptm.country_code = upper(ts.Country_code) and  ptm.Source_Code = upper(ts.Source_Code) 
            and  ptm.source_price_type = upper(ts.Source_Price_Type_Name)
            and if (ts.Country_code ='NL' and ts.Source_Price_Type_Name <> '_UNK' and TS.Source_price_type_id <>'_UNK',ptm.source_price_type_id ,  TS.Source_price_type_id) =  TS.Source_price_type_id
 
  left join price_category_mapping  pcm on pcm.country_code = upper(ts.Country_code) and  pcm.Source_Code = upper(ts.Source_Code)
           and pcm.source_price_category_id =  upper(ts.source_price_category_id)  and  PM.production_location_id =  pcm.production_location_id   
            


 
