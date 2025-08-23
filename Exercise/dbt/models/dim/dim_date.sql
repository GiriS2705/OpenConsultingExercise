-- dbt/models/dim/dim_date.sql
{{ config(materialized='view') }}
-- Assume RAW.DATE_DIM contains a full calendar
select
  cast(date_id as int) as date_id,
  calendar_date::date as calendar_date,
  year::int as year,
  month::int as month,
  day::int as day,
  day_of_week::string as day_of_week,
  is_weekend::boolean as is_weekend
from {{ source('raw','DATE_DIM') }}
