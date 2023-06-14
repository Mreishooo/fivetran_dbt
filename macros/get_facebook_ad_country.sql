{% macro get_facebook_ad_country( p_account_id ) %}

   Case {{p_account_id}}
   when 641810463098719 then 'Germany'
   when 2441671829467106 then 'Germany'
   when 135351755395794 then 'France'
   when 316290149907771 then 'France'
   when 1406882756790460 then 'France'
   when 489261026747377 then 'France'
   when 1025814388809084 then 'France'
   when 39334103 then 'Spain'
   when 3315948171865020 then 'Netherlands'
   when 377966129965129 then 'Netherlands'
   when 288163912731956 then 'Netherlands'
   when 2561187820842239 then 'Netherlands' 
   when 216850763524531 then 'Netherlands' 
   else 'Other'
   END 
{% endmacro %}



