{% macro date_struct(date_value ) %}
    {{date_value}} as date ,
    extract(DAYOFWEEK from {{date_value}}) as day_of_week  ,
    extract(DAY from {{date_value}}) as day  ,
    FORMAT_DATE("%A" , {{date_value}}) as day_name, 
    FORMAT_DATE("%a" , {{date_value}}) as day_name_short, 
    extract(WEEK from {{date_value}}) as week  ,
    extract(ISOWEEK from {{date_value}}) as iso_week  ,
    extract(MONTH from {{date_value}}) as month  ,
    FORMAT_DATE("%B" , {{date_value}}) as month_name, 
    FORMAT_DATE("%b" , {{date_value}}) as month_name_short, 
    extract(QUARTER from {{date_value}}) as quarter  ,
    extract(YEAR from {{date_value}}) as year  ,
    extract(ISOYEAR from {{date_value}})  as iso_year,
    extract(DAYOFWEEK from {{date_value}}) > 5  as is_weekend
{% endmacro %}


