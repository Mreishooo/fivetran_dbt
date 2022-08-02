{% macro fix_double_value( double_value ) %}
  CAST(REGEXP_REPLACE( {{ double_value }} ,',','.') as FLOAT64 )
{% endmacro %}