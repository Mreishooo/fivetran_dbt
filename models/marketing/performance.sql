{{ config(
    materialized='view',
    labels = {'source': 'enriched_table', 'refresh': 'daily','connection':'na'},
)}}

{% set kpis  = ["clicks","impressions","conversions","conversions_value","spend","interactions"] %}

with google_ads as
(
  select *  FROM {{ ref('google_ads') }}  
) ,

google_dv360 as
(
  select *  FROM {{ ref('google_dv360') }}  
)  ,

facebook_ads as
(
  select *  FROM {{ ref('facebook_ads') }}  
)  


SELECT platform , account_name ,date ,
{% for kpi in kpis %}
    sum({{kpi}}) as {{kpi}},
{% endfor %}
from   google_ads
group by 1,2 ,3 

union all

SELECT platform , account_name ,date ,
{% for kpi in kpis %}
    sum({{kpi}}) as {{kpi}},
{% endfor %}
from   google_dv360
group by 1,2 ,3 

union all

SELECT platform , account_name ,date  ,
{% for kpi in kpis %}
    sum({{kpi}}) as {{kpi}},
{% endfor %} 
from facebook_ads 
group by 1,2 ,3 