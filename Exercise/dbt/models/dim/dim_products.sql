-- dbt/models/dim/dim_products.sql
{{ config(materialized='view') }}
select
  product_id, product_name, category, brand,
  coalesce(unit_price, 0) as unit_price,
  created_at
from {{ ref('stg_products') }}
