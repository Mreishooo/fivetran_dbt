
{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'exchange_rate', 'refresh': 'daily','connection':'fivetran','type':'enrich'},
)}}


with exchange_rate as
(
 SELECT *  , date_sub ( LEAD(valid_from) 
    OVER (PARTITION BY from_currency_code ORDER BY valid_from ASC) ,  interval 1 day ) AS valid_to
   FROM {{ ref('exchange_rates') }}

),
gen_date as 
(SELECT GENERATE_DATE_ARRAY('2023-01-01', '2036-12-31') AS dates )

,dates as
(select date from gen_date, unnest (dates) date)


select distinct date, from_currency_code, to_currency_code , rate  from 
dates left join exchange_rate 
on date between valid_from and ifnull( valid_to,date)
