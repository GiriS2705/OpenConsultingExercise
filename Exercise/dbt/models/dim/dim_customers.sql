-- dbt/models/dim/dim_customers.sql
{{ config(materialized='view') }}
select
  customer_id, first_name, last_name, email, city, state, country, created_at
from {{ ref('stg_customers') }}
