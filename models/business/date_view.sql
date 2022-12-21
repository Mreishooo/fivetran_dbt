{{ config(
    materialized='view',
    labels = {'source': 'generated', 'refresh': 'view','connection':'ns','type':'enriched'},
)}}

 
with gen_date as 
(SELECT GENERATE_DATE_ARRAY('2016-01-01', '2036-12-31') AS dates )

select date , 
    if (extract(DAYOFWEEK from date) - 1 = 0 , 7 , extract(DAYOFWEEK from date) - 1 )  as day_of_week  ,
    extract(DAY from date) as day  ,
    FORMAT_DATE("%A" ,date) as day_name, 
    FORMAT_DATE("%a" , date) as day_name_short, 
    extract(WEEK(MONDAY) from date) as week  ,
    extract(ISOWEEK from date) as iso_week  ,
    extract(MONTH from date) as month  ,
    FORMAT_DATE("%B" , date) as month_name, 
    FORMAT_DATE("%b" , date) as month_name_short, 
    extract(QUARTER from date) as quarter  ,
    extract(YEAR from date) as year  ,
    extract(ISOYEAR from date)  as iso_year,
    extract(DAYOFWEEK from date) > 5  as is_weekend,
    DATE_DIFF(date,  current_date(), day)  as days_to_date,

    DATE_DIFF(date,  current_date(), day) = -2 is_2_days_ago,
    DATE_DIFF(date,  current_date(), day) = -1 is_yesterday,
    DATE_DIFF(date,  current_date(), day) = 0 is_today,
    DATE_DIFF(date,  current_date(), day) = 1 is_tomorrow,

    DATE_DIFF(date,  current_date(), WEEK(MONDAY)) = -2 is_two_weeks_ago,
    DATE_DIFF(date,  current_date(), WEEK(MONDAY)) = -1 is_last_week,
    DATE_DIFF(date,  current_date(), WEEK(MONDAY)) = 0 is_this_week,
    DATE_DIFF(date,  current_date(), WEEK(MONDAY)) = 1 is_next_week,
    
    DATE_DIFF(date,  current_date(), month) = -2 is_two_months_ago,
    DATE_DIFF(date,  current_date(), month) = -1 is_last_month,
    DATE_DIFF(date,  current_date(), month) = 0 is_this_month,
    DATE_DIFF(date,  current_date(), month) = 1 is_next_month,

    DATE_DIFF(date,  current_date(), quarter) = -2 is_two_quarter_ago,
    DATE_DIFF(date,  current_date(), quarter) = -1 is_last_quarter,
    DATE_DIFF(date,  current_date(), quarter) = 0 is_this_quarter,
    DATE_DIFF(date,  current_date(), quarter) = 1 is_next_quarter,

    DATE_DIFF(date,  current_date(), year) = -2 is_two_years_ago,
    DATE_DIFF(date,  current_date(), year) = -1 is_last_year,
    DATE_DIFF(date,  current_date(), year) = 0  is_this_year,
    DATE_DIFF(date,  current_date(), year) = 1  is_next_year,
from gen_date, unnest (dates) date