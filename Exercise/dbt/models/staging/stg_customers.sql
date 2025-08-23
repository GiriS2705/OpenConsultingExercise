-- dbt/models/staging/stg_customers.sql
{{ config(materialized='view') }}
select
  cast(customer_id as int) as customer_id,
  initcap(trim(first_name)) as first_name,
  initcap(trim(last_name))  as last_name,
  lower(trim(email))        as email,
  nullif(trim(city),'')     as city,
  nullif(trim(state),'')    as state,
  nullif(trim(country),'')  as country,
  try_to_timestamp_ntz(created_at) as created_at
from {{ source('raw','CUSTOMERS') }}
