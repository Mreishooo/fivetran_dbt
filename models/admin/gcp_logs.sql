{{ config(
    materialized='view',
    labels = {'source': 'gcp_logs_sink', 'refresh': 'streaming','connection':'logs_sink','type':'enriched'},
)}}

 

with gcp_logs as
(
  select *   
  FROM {{ source( 'gcp_logs','cloudaudit_googleapis_com_data_access') }}
) 

SELECT 
resource.labels.project_id project_id ,
 protopayload_auditlog.authenticationInfo.principalEmail user_email, 
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobConfiguration.query.query query,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobConfiguration.query.statementType statement_type,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatus.state,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.createTime create_time,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.startTime start_time,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.endTime end_time,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.totalprocessedbytes/1000000000 total_processed_gb,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.totalBilledBytes/1000000000 total_billed_gb,
 --protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.billingTier billing_Tier,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.totalSlotMs total_slot_ms,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.referencedTables referenced_tables,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.totalTablesProcessed total_tables_processed,
 protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatistics.queryOutputRowCount row_count
 FROM gcp_logs
 WHERE DATE(timestamp) >= DATE_SUB( current_date ,  INTERVAL 31 DAY   ) 
 and  protopayload_auditlog.servicedata_v1_bigquery.jobGetQueryResultsResponse.job.jobStatus.state = 'DONE'

 