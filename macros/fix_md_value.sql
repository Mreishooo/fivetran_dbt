{% macro fix_md_value( string_value ) %}
  cast (replace ( replace( {{ string_value }}, '%','') , ',' , '.' ) as float64)
{% endmacro %}