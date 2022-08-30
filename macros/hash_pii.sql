{% macro hash_pii( pii ) %}
    {%- if pii == '_N/A' -%}
       pii
    {%- else -%}
       concat( '__',to_hex( sha256 ({{ pii }}) ) )
    {%- endif -%}
{% endmacro %}