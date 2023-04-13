
{{ config(
    materialized='table',
    on_schema_change='fail',
    schema='sales_stg',
    labels = {'source': 'all_sales', 'refresh': 'daily','connection':'fivetran','type':'row'},
)}}

 
with tickets as
(
 SELECT *  
   FROM {{ ref( 'tickets_union_mdb') }}
--where date (booking_date) = '2023-02-02'
),

distributions_mapping as
(
 SELECT *  
   FROM {{ ref( 'distributions_mapping') }}

)

, dm_splits as
(
  select * except(source_sales_partner_class,source_distribution_point_id,source_client_id,source_customer_code,source_promotion_code,source_promotion_id) 
  ,split(source_sales_partner_class,',') source_sales_partner_classes
  ,split(source_distribution_point_id,',') source_distribution_point_ids 
  ,split(source_client_id,',') source_client_ids 
  ,split(source_customer_code,',') source_customer_codes 
  ,split(source_promotion_code,',') source_promotion_codes 
  ,split(source_promotion_id,',') source_promotion_ids 
  from distributions_mapping
)
, dm_splits_unnest as
(
  select distinct  *  except(source_sales_partner_classes,source_distribution_point_ids,source_client_ids,source_customer_codes,source_promotion_codes,source_promotion_ids)   
  from dm_splits 
  left join  unnest (source_sales_partner_classes)  source_sales_partner_class
  left join  unnest (source_distribution_point_ids) source_distribution_point_id
  left join unnest (source_client_ids) source_client_id
  left join unnest (source_customer_codes) source_customer_code
  left join unnest(source_promotion_codes) source_promotion_code
 left join unnest (source_promotion_ids) source_promotion_id
)

, prioriy_1
as ( 
select distinct t.* 
,m.distribution_id 
from tickets t
left join dm_splits_unnest  m on 
{{distribution_mapping_filter()}}
and Prioriy =1
--QUALIFY row_number() OVER (PARTITION BY barcode , cancellation_status ORDER BY 1 DESC) = 2
)

, prioriy_2
as ( 
select distinct t.* except ( distribution_id ) 
,ifnull(t.distribution_id ,m.distribution_id)  distribution_id
from prioriy_1 t
left join dm_splits_unnest m on 
{{distribution_mapping_filter()}}
and Prioriy = 2  
--QUALIFY row_number() OVER (PARTITION BY barcode , cancellation_status ORDER BY 1 DESC) = 2
)

, prioriy_3
as ( 
select  distinct  t.* except ( distribution_id ) 
,ifnull(t.distribution_id ,m.distribution_id)  distribution_id
from prioriy_2 t
left join dm_splits_unnest m on 
{{distribution_mapping_filter()}}
and Prioriy = 3  
--QUALIFY row_number() OVER (PARTITION BY barcode , cancellation_status ORDER BY 1 DESC) = 2
)
, prioriy_4
as ( 
select  distinct t.* except ( distribution_id ) 
,COALESCE (t.distribution_id ,m.distribution_id, concat(t.country_code,' - _N/A - _N/A'))  distribution_id
from prioriy_3 t
left join dm_splits_unnest m on 
{{distribution_mapping_filter()}}
and Prioriy = 4 
--QUALIFY row_number() OVER (PARTITION BY barcode , cancellation_status ORDER BY 1 DESC) = 2
)


select * from prioriy_4
QUALIFY row_number() OVER (PARTITION BY ticket_id , booking_date ORDER BY 1 DESC) = 1