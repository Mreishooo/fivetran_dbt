{% macro create_rebooking_proc()%}

{% set same_cols =["source_code","country_code","currency_code","web_order_number","main_order_number","sub_order_number","production_location_id",
"barcode","barcode_13","cancellation_status","is_replaced_cancellation", "price_category_id","price_type_id",
"performance_date","theatre_id","source_distribution_point_id","source_distribution_point","source_distribution_channel",
"source_distribution_channel_name","source_client_id","source_sales_partner","source_promotion_id","source_production","source_promotion_name",
"source_promotion_code","source_promotion_advertising_partner_id","source_customer_code","source_seat","source_seat_row","source_seat_number",
"source_price_category_id","source_price_type_name","_loaded_at","_last_update","orignal_ticket_price"] %}

{% set prices_cols  = ["article_count","ticket_price","paid_price","net_price","net_net_price","customer_price",
"customer_facevalue","ticket_price_euro","paid_price_euro","net_price_euro","net_net_price_euro","customer_price_euro","customer_facevalue_euro","tpt_de_value_eur"] %}

{% set temp_table = "sales_stg.working_temp" %}



    CREATE OR REPLACE PROCEDURE sales_stg.ticket_rebook_handeling( start_date date , end_date date )
        BEGIN 
        DECLARE work_date DATE DEFAULT '2023-01-01';
        DECLARE max_date DATE DEFAULT current_date();

        IF (start_date is not null ) THEN
        SET work_date = start_date;
        END IF;

        IF (end_date is not null ) THEN
        SET max_date = end_date;
        END IF;

        --WHILE work_date <= max_date DO
        --insert into admin.rebooking_log
        --select current_timestamp(), work_date;

        
        CREATE OR REPLACE TABLE  {{temp_table}}
        as 
        select distinct  original_ticket.* , 
        ticket_work_date.booking_date working_ticket_booking_date, 
        ticket_work_date.booking_timestamp working_ticket_booking_timestamp, 
        ticket_work_date.transaction_type working_ticket_transaction_type,
        ticket_work_date.barcode ticket_work_date_barcode, 
         if (ticket_work_date.transaction_type = 'Sale' , -1 ,1) transaction_operation 
        from  {{ ref( 'tickets_mapped_distributions') }} original_ticket   join  {{ ref( 'tickets_mapped_distributions') }}    ticket_work_date  
                                        on  ticket_work_date.booking_date between work_date and  max_date
                                              and original_ticket.country_code= ticket_work_date.country_code
                                              and original_ticket.source_code = ticket_work_date.source_code 
                                              and original_ticket.barcode_13 = ticket_work_date.source_promotion_code 
        where    true
                and original_ticket.booking_date < ticket_work_date.booking_date
                and original_ticket.transaction_type ='Sale'
                and original_ticket.replacement_type <> 'Original' ;

         -- update replacement_type for all rebooked tickets 
        update   {{ ref( 'tickets_mapped_distributions') }}  t 
        set t.replacement_type = 'Automatic' 
           ,t.is_replacement = true 
           ,t.tpt_de_value_eur = wt.tpt_de_value_eur * t.article_count
           ,_run_at = current_timestamp
        from  {{temp_table}}  wt 
                where 
                    t.source_promotion_code= wt.barcode_13 
                and t.country_code = wt.country_code
                and t.source_code = wt.source_code
                and t.transaction_type =  wt.working_ticket_transaction_type 
                and t.booking_date = wt.working_ticket_booking_date 
                and t.barcode = wt.ticket_work_date_barcode;



        -- insert new line to correct balance 
        insert into   {{ ref( 'tickets_mapped_distributions') }}   
        (  ticket_id,transaction_type,is_replaced,is_replacement,replacement_type,booking_date,booking_timestamp,_run_at,_source
          {% for same_col in same_cols %} ,{{same_col}}  {% endfor %}  
          {% for prices_col in prices_cols %} ,{{prices_col}}  {% endfor %} )
        select concat( Country_Code ,'-',Source_Code,'-' ,BarCode,'-rebooking-',working_ticket_transaction_type) , concat(working_ticket_transaction_type,'-rebooking'), if(transaction_operation =1 , true , false) , true, 'Original' ,working_ticket_booking_date,working_ticket_booking_timestamp, current_timestamp , "BQ"
          {% for same_col in same_cols %} ,{{same_col}} {% endfor %}  
          {% for prices_col in prices_cols %} ,{{prices_col}} * transaction_operation {% endfor %} 
        --working_ticket_booking_date ,barcode, tpt_de_value_eur * transaction_operation  ,'Original',true, if(transaction_operation =1 , true , false) ,article_count * transaction_operation, concat('rebooking-',working_ticket_transaction_type)
        from {{temp_table}} ;



       -- SET work_date = DATE_ADD(work_date, INTERVAL 1 DAY);
        --END WHILE;

        END;


{% endmacro %} 

