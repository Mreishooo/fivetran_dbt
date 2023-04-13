{{ config(
    materialized='table',
    labels = {'source': 'sales', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with tickets as
(
  select * FROM {{ ref('ticket_sales' )}}
  
) 
SELECT distinct 
country_code,
concat(production_location_id,' - ',performance_date ,' - ',performance_time) performance_id,
production_location_id,
cast ( concat (performance_date,' ', performance_time) as datetime) performance_date_time ,
performance_date,
CAST(performance_time AS TIME)  performance_time,
show 

from tickets
--FROM stage-playground.sales.ticket_sales
where country_code = 'DE'
