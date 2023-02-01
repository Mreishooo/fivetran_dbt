{% macro get_facebook_ad_country( p_account_id ) %}

   Case {{p_account_id}}
   when 641810463098719 then 'Germany'
   when 2441671829467106 then 'Germany'
   when 135351755395794 then 'France'
   when 316290149907771 then 'France'
   else 'Other'
   END 
{% endmacro %}

