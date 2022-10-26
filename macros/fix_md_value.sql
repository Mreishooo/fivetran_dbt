{% macro fix_md_valye( string_value ) %}
  cast (replace ( replace( {{ string_value }}, '%','') , ',' , '.' ) as float64)
{% endmacro %}