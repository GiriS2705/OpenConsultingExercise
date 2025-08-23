-- dbt/models/staging/stg_products.sql
{{ config(materialized='view') }}
select
  cast(product_id as int) as product_id,
  initcap(product_name)   as product_name,
  initcap(category)       as category,
  initcap(brand)          as brand,
  try_to_decimal(unit_price,10,2) as unit_price,
  try_to_timestamp_ntz(created_at) as created_at
from {{ source('raw','PRODUCTS') }}
