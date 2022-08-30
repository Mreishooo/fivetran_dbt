{% macro ft_filter( pk ) %}

    {%- if pk is none -%}
       not _fivetran_deleted
    {%- else -%}
       not _fivetran_deleted
      QUALIFY row_number() OVER (PARTITION BY {{ pk }} ORDER BY _fivetran_synced DESC)  = 1
    {%- endif -%}

{% endmacro %}