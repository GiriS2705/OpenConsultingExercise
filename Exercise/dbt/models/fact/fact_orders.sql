-- dbt/models/fact/fact_orders.sql
{{ config(materialized='view') }}
select
  o.order_id, o.customer_id, o.product_id, o.date_id,
  o.quantity, o.unit_price, o.total_amount, o.order_status, o.created_at
from {{ ref('stg_orders') }} o
