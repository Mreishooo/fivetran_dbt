
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},
)}}

-- filter extra tickets 
with tickets_all_sources as
(
 SELECT *  
   FROM {{ ref( 'tickets_all_sources') }}

),

sales_data_import_limitation as
(
 SELECT *  
   FROM {{ ref( 'data_import_limitation') }}

),

DataImportIncludeLimitationNullableClientId AS (
  SELECT
    Source_Code ,
    Country_Code , 
    Client_Id  
  FROM
   sales_data_import_limitation
  WHERE
    (Client_Id IS NULL)
    AND (Include_Data = 1)
    AND (Source_Type = 'SALES')),

  DataImportIncludeLimitationNonNullableClientId AS (
  SELECT
    Source_Code ,
    Country_Code ,
    Client_Id  
  FROM
   sales_data_import_limitation
  WHERE
    (Client_Id IS NOT NULL)
    AND (Include_Data = 1)
    AND (Source_Type = 'SALES')),

  DataImportIncludeLimitation AS (
  SELECT
    DISTINCT COALESCE (NC.Source_Code, NNC.Source_Code) AS Source_Code,
    COALESCE (NC.Country_Code, NNC.Country_Code) AS Country_Code,
    (CASE
        WHEN NC.Country_Code IS NOT NULL THEN NC.Client_Id
      ELSE
      NNC.Client_Id
    END
      ) AS Client_Id
  FROM
    DataImportIncludeLimitationNullableClientId AS NC
  FULL OUTER JOIN
    DataImportIncludeLimitationNonNullableClientId AS NNC
  ON
    NC.Source_Code = NNC.Source_Code
    AND NC.Country_Code = NNC.Country_Code),
  
  DataImportExcludeLimitation AS (
  SELECT
    Source_Code ,
    Country_Code ,
    Client_Id  
  FROM
     sales_data_import_limitation
  WHERE
    (Include_Data = 0)
    AND (Source_Type = 'SALES')),
  
  
  FilteredTicketSales AS (
  SELECT
    TS.*
  FROM
    tickets_all_sources AS TS
  INNER JOIN
    DataImportIncludeLimitation AS DIIL
  ON
    TS.Source_Code = DIIL.Source_Code
    AND TS.Country_Code = DIIL.Country_Code
    AND TS.Source_Client_Id = COALESCE (DIIL.Client_Id, TS.Source_Client_Id)
  LEFT OUTER JOIN
    DataImportExcludeLimitation AS DIEL
  ON
    TS.Source_Code = DIEL.Source_Code
    AND TS.Country_Code = DIEL.Country_Code
    AND TS.Source_Client_Id = DIEL.Client_Id
  WHERE
    (DIEL.Country_Code IS NULL))
  

SELECT
 FTS.* 
FROM
  FilteredTicketSales AS FTS
