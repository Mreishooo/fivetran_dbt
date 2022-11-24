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
/*,ticket_sales AS (
   SELECT *  
   FROM {{ ref( 'ticket_sales') }}
  )
*/
  
,ga_transaction 
as ( select distinct 
date,
country, 
full_visitor_id , 
cast (hit.transaction_id as int64) transaction_id ,  
hit.revenue	,		
hit.affiliation ,
hit.hits_timestampe,
traffic_source.campaign campaign,
count(distinct product_name) unique_articles,
sum(product_quantity) total_articles,
from ga_data
 , unnest ( hits) hit
 left join  unnest ( product) product using (transaction_id)
 where true 
and  hit.transaction_id is not null
and totals.transactions > 0   
group by date,country, full_visitor_id,hit.transaction_id , hit.revenue	, hit.affiliation , hit.hits_timestampe,traffic_source.campaign 
QUALIFY row_number() OVER (PARTITION BY  transaction_id ORDER BY hit.hits_timestampe )  = 1
) 

/*, mdb_orders as
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
	  ticket_sales ts on transaction_id = web_order_number 
	group by 1 ,2 ,3 , 4 , 5, 6
)
 */

select  distinct 
  date,
  country, 
  full_visitor_id, 
  transaction_id,  
  revenue,
  campaign,
  affiliation ,
  unique_articles,
  total_articles,
 /* ARRAY_AGG( struct (
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
    ,article_count)) as ticket_sales*/
 from ga_transaction --left join mdb_orders on transaction_id = web_order_number
 --group by 1,2,3,4,5,6,7,8
