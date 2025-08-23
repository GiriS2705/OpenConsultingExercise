-- dbt/models/mart/fct_daily_sales.sql
{{ config(materialized='table') }}
with o as (select * from {{ ref('fact_orders') }}),
d as (select * from {{ ref('dim_date') }})
select
  o.date_id,
  d.calendar_date,
  sum(coalesce(o.quantity,0)) as total_qty,
  sum(coalesce(o.total_amount, coalesce(o.quantity,0) * coalesce(o.unit_price,0))) as total_sales,
  count(*) as order_count
from o
left join d on d.date_id = o.date_id
group by 1,2
