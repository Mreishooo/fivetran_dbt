{% macro get_mindshare_latest_file( file_name ) %}

    where _file = ( select `stage-landing.ft_mdb_dbo.get_latest_file_name`('{{file_name}}' )) 
  
{% endmacro %}