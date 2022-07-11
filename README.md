Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### codegen
https://hub.getdbt.com/dbt-labs/codegen/latest/


 dbt run-operation generate_source --args 'schema_name: ga'

 # for multiple arguments, use the dict syntax
 dbt run-operation generate_source --args "{schema_name: fivetran_log, database_name: stage-landing, include_descriptions: true,generate_columns: true, table_names: [temp]   }"


 dbt run --select basic_report

 dbt run-operation generate_model_yaml --args '{"model_name": "basic_report"}'

 dbt docs generate
 dbt docs serve