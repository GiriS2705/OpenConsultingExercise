-- dbt/models/staging/stg_orders.sql
{{ config(materialized='view') }}
select
  cast(order_id as int)    as order_id,
  cast(customer_id as int) as customer_id,
  cast(product_id as int)  as product_id,
  cast(date_id as int)     as date_id,
  try_to_number(quantity)  as quantity,
  try_to_decimal(unit_price,10,2) as unit_price,
  try_to_decimal(total_amount,12,2) as total_amount,
  coalesce(order_status,'COMPLETED') as order_status,
  try_to_timestamp_ntz(created_at) as created_at
from {{ source('raw','ORDERS') }}
