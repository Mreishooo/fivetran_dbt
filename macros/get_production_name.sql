{% macro get_production_name( country,  page_path ) %}
   CASE
      WHEN ( {{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%hamilton%') THEN 'DE - HA - OPH - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%mamma%') THEN 'DE - MM - TNF - 1'
      WHEN ({{country}} = 'Germany' AND ( LOWER({{ page_path }}) LIKE '%der%koenig%' 
				OR LOWER({{ page_path }}) LIKE '%der%köenig%' 
				OR LOWER({{ page_path }}) LIKE '%der%könig%') ) THEN 'DE - KDL - THH - 1'
      WHEN ({{country}} = 'Germany' AND (LOWER({{ page_path }}) LIKE '%eiskoenigin%'OR LOWER(  {{ page_path }}) LIKE '%eisköenigin%'
      	OR LOWER({{ page_path }}) LIKE '%eiskönigin%') ) THEN 'DE - EIS - TAE - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%wicked%') THEN 'DE - WI - TNF - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%tina%'AND LOWER({{ page_path }}) LIKE '%hamburg%' ) THEN 'DE - TIN - OPH - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%tina%' AND LOWER({{ page_path }}) LIKE '%stuttgart%' ) THEN 'DE - TIN - APO - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%/tina%' ) THEN 'Tina'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%blue%') THEN 'DE - BMG - BMX - 1'
      WHEN ({{country}} = 'Germany' AND (LOWER({{ page_path }}) LIKE '%kudamm%'
      	OR LOWER( {{ page_path }}) LIKE "%ku\'damm%" )) THEN 'DE - KD - TDW - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%aladdin%') THEN 'DE - ALA - APO - 1'
      WHEN ({{country}} = 'Germany' AND LOWER({{ page_path }}) LIKE '%vampire%') THEN 'DE - TDV - PAL - 3'
     
     
     -- Ntherlands 
      WHEN ({{country}} = 'Netherlands' AND LOWER({{ page_path }}) LIKE '%mamma%') THEN 'NL - MM'
      WHEN ({{country}} = 'Netherlands' AND LOWER({{ page_path }}) LIKE '%tina%' ) THEN 'NL - TINA - BTU - 1'
      WHEN ({{country}} = 'Netherlands' AND LOWER({{ page_path }}) LIKE '%aladdin%') THEN 'NL - ALA - ACT - 1'
      WHEN ({{country}} = 'Netherlands' AND LOWER({{ page_path }}) LIKE '%gelooft%') THEN 'NL - HGIM22'
      WHEN ({{country}} = 'Netherlands' AND LOWER({{ page_path }}) LIKE '%aida%') THEN 'NL - AIDA23 - ACT - 1'
    
    -- for all sites 
      WHEN (LOWER({{ page_path }}) LIKE '%checkout%') THEN 'Checkout page'
      WHEN (LOWER({{ page_path }}) LIKE '%faq%') THEN 'FAQ'
      WHEN (LOWER({{ page_path }}) LIKE '%veelgestelde-vragen%') THEN 'FAQ'

      
      
      WHEN (LOWER({{ page_path }}) LIKE '%cast%')THEN 'Cast Page'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicals-shows/%')THEN 'Musicals shows'
      WHEN (LOWER({{ page_path }}) LIKE '%/musical/%')THEN 'Musicals shows'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicalfestival%')THEN 'Musicals shows'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicals-nederland%')THEN 'Musicals shows'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicaltiendaagse%')THEN 'Musicals shows'

      WHEN (LOWER({{ page_path }}) LIKE '%/nationale-musical-card%')THEN 'Nationale musical card'
      WHEN (LOWER({{ page_path }}) LIKE '%/hotels%')THEN 'Hotels'
      WHEN (LOWER({{ page_path }}) LIKE '%sommer%')THEN 'Summer Shows'
      WHEN (LOWER({{ page_path }}) LIKE '%service%')THEN 'Service'
      WHEN (LOWER({{ page_path }}) LIKE '%/news/%')THEN 'News'
      WHEN (LOWER({{ page_path }}) LIKE '%/crm/%')THEN 'Calendar Pages'
      WHEN (LOWER({{ page_path }}) LIKE '%corporate-benefits%')THEN 'Corporate Benefits'
      WHEN (LOWER({{ page_path }}) LIKE '%404%')THEN '404 Error'
      WHEN (LOWER({{ page_path }}) LIKE '%stage-theater%' 
				OR LOWER({{ page_path }}) LIKE '%stage-theatertheatervermietung%' )THEN 'Stage Theater'
      WHEN (LOWER({{ page_path }}) LIKE '%ueber-uns%')THEN 'Abour Us'
      WHEN (LOWER({{ page_path }}) LIKE '%/indirekt/%')THEN 'Indirekt offer links'
      WHEN (LOWER({{ page_path }}) LIKE '%/vertriebsportal%'
      	OR LOWER({{ page_path }}) LIKE '%/theatervermietung%'
      	OR LOWER({{ page_path }}) LIKE '%/partner-vertrieb%') THEN 'For Company'
      WHEN (LOWER({{ page_path }}) LIKE '%/presse/%') THEN 'Press'
      WHEN (LOWER({{ page_path }}) LIKE '%/gutscheine/%')THEN 'Coupons'
      WHEN (LOWER({{ page_path }}) LIKE '%/disney-musicals%')THEN 'Disney Musicals'
      WHEN (LOWER({{ page_path }}) LIKE '%/promotion%')THEN 'Promotion'
      WHEN (LOWER({{ page_path }}) LIKE '%/home') THEN 'Home Page'
      WHEN (LOWER({{ page_path }}) LIKE '%/homepage') THEN 'Home Page'
      WHEN (LOWER({{ page_path }}) LIKE '%/nl') THEN 'Home Page'
      WHEN (LOWER({{ page_path }}) = '/') THEN 'Home Page'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicals-in-stuttgart') THEN 'Musicals in Stuttgart'
      WHEN (LOWER({{ page_path }}) LIKE '%/musicals-in-hamburg') THEN 'Musicals in Hamburg'
      WHEN (LOWER({{ page_path }}) LIKE '%/afas%') THEN 'Musicals in AFAS'
      WHEN (LOWER({{ page_path }}) LIKE '%/beatrix%') THEN 'Musicals in Beatrix'

      WHEN (LOWER({{ page_path }}) LIKE '%/tickets.html%')THEN 'Checkout'
    ELSE
    'other'
  END
{% endmacro %}