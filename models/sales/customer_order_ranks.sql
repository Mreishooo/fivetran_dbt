{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'mart'},
)}}

with  
  ts AS (
   SELECT *  
   FROM {{ ref('ticket_sales') }}
  ),

first_orders as
( select  distinct  country_code,country_name, dim_golden_customer_id  golden_customer_id, main_order_number first_order
from ts 
where order_rank =1 ),
second_orders as
( select  distinct  dim_golden_customer_id  golden_customer_id, main_order_number second_order
from ts 
where order_rank =2 ),
thrid_orders as
( select  distinct  dim_golden_customer_id  golden_customer_id, main_order_number thrid_order
from ts 
where order_rank =3 ),
forth_orders as
( select  distinct  dim_golden_customer_id  golden_customer_id, main_order_number forth_order
from ts 
where order_rank =4 ),

last_order as (
select  distinct  dim_golden_customer_id golden_customer_id , main_order_number last_order ,order_rank last_order_rank
from sales.ticket_sales out_t
where true
--where dim_golden_customer_id = 22675
and order_rank = (select max (order_rank) from sales.ticket_sales in_t where in_t.dim_golden_customer_id = out_t.dim_golden_customer_id)
order by 1 
)

select * 
from first_orders 
left join second_orders using (golden_customer_id)
left join thrid_orders using (golden_customer_id)
left join forth_orders using (golden_customer_id)
left join last_order using (golden_customer_id)
