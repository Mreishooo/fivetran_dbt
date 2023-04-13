{% macro distribution_mapping_filter() %}

t.source_code =  m.source_code
and t.country_code =  m.country_code
and t.source_distribution_channel = ifnull (m.source_distribution_channel,t.source_distribution_channel)

--in 

and t.source_sales_partner_class  = ifnull( trim(m.source_sales_partner_class),t.source_sales_partner_class)
and t.source_distribution_point_id  = ifnull( trim(m.source_distribution_point_id),t.source_distribution_point_id)
and cast(t.source_client_id as string) = ifnull( trim(m.source_client_id) ,cast(t.source_client_id as string))
and cast(t.source_customer_code as string)  = ifnull( trim(m.source_customer_code),cast(t.source_customer_code as string))
and cast(t.source_promotion_code as string)  = ifnull( trim(m.source_promotion_code),cast(t.source_promotion_code as string))
and cast(t.source_promotion_id as string)  = ifnull( trim(m.source_promotion_id),cast(t.source_promotion_id as string) )

and safe_cast(t.source_price_type_id as int64)  between ifnull( m.source_price_type_id_from ,safe_cast(t.source_price_type_id as int64)  ) and  ifnull( m.source_price_type_id_to ,safe_cast(t.source_price_type_id as int64)  )  

--look at

and  t.source_promotion_name   LIKE concat ('%' , ifnull( m.source_promotion_name, t.source_promotion_name) , '%')
and  t.source_price_type_name   LIKE concat ('%' , ifnull( m.source_price_type_name, t.source_price_type_name) , '%') 
    

--and  ifnull(m.source_price_type_name ,t.source_price_type_name)  LIKE concat ('%' ,  t.source_price_type_name , '%')

{% endmacro %}


 