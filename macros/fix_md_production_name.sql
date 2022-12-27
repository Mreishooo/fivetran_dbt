{% macro fix_md_production_name( string_value ) %}
  replace( {{ string_value }}, '–','-')  
{% endmacro %}