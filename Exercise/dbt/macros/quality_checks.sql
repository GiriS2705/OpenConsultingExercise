-- dbt/macros/quality_checks.sql
{% macro assert_non_negative(model, column_name) %}
  select * from {{ model }} where {{ column_name }} < 0
{% endmacro %}

{% macro null_rate(model, column_name) %}
  select
    count_if({{ column_name }} is null) / nullif(count(*),0)::float as null_rate
  from {{ model }}
{% endmacro %}
