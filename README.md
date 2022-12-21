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
 dbt run-operation generate_source --args "{schema_name: sales, database_name: stage-commercial, include_descriptions: true,generate_columns: true, table_names: [ticket_sales]   }"

dbt run-operation generate_source --args "{schema_name: marketing, database_name: stage-playgroung, include_descriptions: true,generate_columns: true }"
dbt run-operation generate_source --args "{schema_name: marketing, include_descriptions: true,generate_columns: true}"

dbt test --select source:*


 dbt run-operation generate_model_yaml --args '{"model_name": "marking_offline"}'
dbt run-operation generate_base_model --args '{"source_name": "ft_mdb7_dbo", "table_name": "dimgoldencustomer"}'
 dbt run-operation generate_model_yaml --args '{"model_name": "dyc_customer"}' -- works

 dbt docs generate
 dbt docs serve


 StageDNA1234

 cd .\fivetran_dbt\
 & c:/dbt/dbt-env/Scripts/Activate.ps1
 policy_tags:
          - 'projects/stage-commercial/locations/eu/taxonomies/7781825326639857630/policyTags/873386705705506136'