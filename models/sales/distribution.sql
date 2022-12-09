{{ config(
    materialized='table',
    on_schema_change='fail',
    labels = {'source': 'mdb', 'refresh': 'daily','connection':'fivetran','type':'dim'},
)}}

with  
  dimdistribution AS (
   SELECT *  
   FROM {{ source( 'ft_mdb6_dbo','dimdistribution') }}
   where {{ ft_filter(none) }} 
  )


SELECT
dimdistributionid dim_distribution_id,	
channeltypecode	 channel_typec_ode,	
channeltypename	 channel_type_name,	
commissionpercentage	 commission_percentage,	
dimcountryid dim_countryid,	
dimsourceid dim_sourceid,	
discountpercentage	 discount_percentage,	
distributionchannelcode distribution_channel_code	,	
distributionchannelname	 distribution_channel_name,	
distributionid distribution_id,	
distributionlogic	distribution_logic,	
distributionlogicpriority distribution_logic_priority,	
distributionowner	 distribution_owner,	
distributionpoint	 distribution_point,	
isdistributionownedbystage	is_distribution_owned_by_stage,	
kickbackpercentage kickback_percentage	,	
localsaleschannel1 local_sales_channel_1	,	
localsaleschannel2 local_sales_channel_2	,	
providingcustomerdatacode providing_customer_data_code,	
providingcustomerdataname	 providing_customer_data_name,
FROM
  dimdistribution