/*{% set partitions_to_replace = [
  'timestamp(current_date)',
  'timestamp(date_sub(current_date, interval 1 day))',
  'timestamp(date_sub(current_date, interval 2 day))'
] %}*/

{{ config(
    materialized='table',
    on_schema_change='fail',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    labels = {'source': 'ga', 'refresh': 'daily','connection':'ga_link','type':'mart'}
)}}

with ga_data AS (
   SELECT *  
   FROM {{ ref( 'ga_data') }}
  )
,ticket_sales AS (
   SELECT *  
   FROM {{ ref( 'ticket_sales') }}
  )

  
,ga_transaction 
as ( select distinct 
date,
country, 
full_visitor_id , 
hit.hostname,
cast (hit.web_order_id as int64) web_order_id ,  
hit.revenue	,		
hit.affiliation ,
from ga_data
 , unnest ( hits) hit
 where true 
and  hit.web_order_id  is not null
group by date,country, full_visitor_id,hit.hostname,hit.web_order_id , hit.revenue	, hit.affiliation 
QUALIFY row_number() OVER (PARTITION BY  web_order_id ORDER BY date DESC)  = 1
) 

, mdb_orders as
(select web_order_number
, article_type_code,
 distribution_owner	,distribution_point ,production_location_id ,production_name,
 count(fact_ticket_sales_id) fact_ticket_sales_id , 
 sum (net_price_value_eur) net_price_value_eur	,	
 sum (net_net_price_value_eur) net_net_price_value_eur	,			
 sum ( ticket_price_value_eur)	ticket_price_value_eur,		 	
 sum( customer_price_value_eur) customer_price_value_eur ,
 sum(article_count) article_count
	from ga_transaction  join
	  ticket_sales ts on web_order_id = web_order_number 
	group by 1 ,2 ,3 , 4 , 5, 6
)
 

select  
  date,
  country, 
  full_visitor_id , 
  hostname,
  web_order_id ,  
  revenue	,		
  affiliation ,
  ARRAY_AGG( struct (
    web_order_number
    ,article_type_code
    ,distribution_owner	
    ,distribution_point 
    ,production_location_id 
    ,production_name
    ,fact_ticket_sales_id  
    ,net_price_value_eur	
    ,net_net_price_value_eur				
    ,ticket_price_value_eur	 	
    ,customer_price_value_eur 
    ,article_count)) as ticket_sales
 from ga_transaction left join mdb_orders on web_order_id = web_order_number
 group by 1,2,3,4,5,6,7
