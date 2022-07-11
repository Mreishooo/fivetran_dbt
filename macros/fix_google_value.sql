{% macro fix_google_value( money_value ) %}
  {{ money_value }} / 1000000
{% endmacro %}