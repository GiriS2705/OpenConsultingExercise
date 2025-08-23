# airflow/dags/customer_analytics_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
import os, json

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def dq_advanced_fn(**context):
    """
    Advanced DQ:
      - IQR-based anomaly detection on quantity
      - Z-score on total_amount per product
      - Referential integrity check against dims
      - If issues found: log suggestions and (optionally) fail
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    # 1) IQR anomalies for quantity
    iqr_sql = '''
    with q as (
      select quantity::float as q from ANALYTICS.PUBLIC.FACT_ORDERS
    ),
    stats as (
      select percentile_cont(0.25) within group (order by q) as q1,
             percentile_cont(0.75) within group (order by q) as q3
      from q
    )
    select f.order_id, f.quantity, s.q1, s.q3
    from ANALYTICS.PUBLIC.FACT_ORDERS f, stats s
    where f.quantity < (s.q1 - 1.5*(s.q3 - s.q1))
       or f.quantity > (s.q3 + 1.5*(s.q3 - s.q1))
    '''
    iqr_rows = hook.get_pandas_df(iqr_sql)

    # 2) Z-score on total_amount by product
    z_sql = '''
    with stats as (
      select product_id,
             avg(total_amount)::float as mean_amt,
             stddev_samp(total_amount)::float as std_amt
      from ANALYTICS.PUBLIC.FACT_ORDERS
      group by product_id
    )
    select f.order_id, f.product_id, f.total_amount, s.mean_amt, s.std_amt,
           case when s.std_amt is null or s.std_amt = 0 then 0
                else (f.total_amount - s.mean_amt)/s.std_amt end as zscore
    from ANALYTICS.PUBLIC.FACT_ORDERS f
    join stats s using(product_id)
    where abs(coalesce(
      case when s.std_amt is null or s.std_amt = 0 then 0
           else (f.total_amount - s.mean_amt)/s.std_amt end, 0)) >= 3
    '''
    z_rows = hook.get_pandas_df(z_sql)

    # 3) Referential integrity: missing customers/products
    fk_sql = '''
    with c as (
      select f.order_id
      from ANALYTICS.PUBLIC.FACT_ORDERS f
      left join ANALYTICS.PUBLIC.DIM_CUSTOMERS dc on f.customer_id = dc.customer_id
      where dc.customer_id is null
    ),
    p as (
      select f.order_id
      from ANALYTICS.PUBLIC.FACT_ORDERS f
      left join ANALYTICS.PUBLIC.DIM_PRODUCTS dp on f.product_id = dp.product_id
      where dp.product_id is null
    )
    select 'missing_customer' as issue, order_id from c
    union all
    select 'missing_product' as issue, order_id from p
    '''
    fk_rows = hook.get_pandas_df(fk_sql)

    issues = {
        "iqr_quantity": iqr_rows.to_dict(orient="records"),
        "zscore_amount": z_rows.to_dict(orient="records"),
        "fk_breaks": fk_rows.to_dict(orient="records")
    }

    if any(len(v) > 0 for v in issues.values()):
        # Suggest imputation strategy
        suggestions = []
        for r in issues.get("zscore_amount", []):
            suggestions.append({
                "order_id": r["ORDER_ID"],
                "suggestion": "Recompute TOTAL_AMOUNT as QUANTITY * UNIT_PRICE or replace with product median."
            })
        for r in issues.get("iqr_quantity", []):
            suggestions.append({
                "order_id": r["ORDER_ID"],
                "suggestion": "Clamp quantity to IQR bounds or send to manual review."
            })
        for r in issues.get("fk_breaks", []):
            suggestions.append({
                "order_id": r["ORDER_ID"],
                "suggestion": "Verify upstream dimension load; quarantine row until resolved."
            })
        print("DQ Issues Detected:", json.dumps(issues, indent=2, default=str))
        print("Remediation Suggestions:", json.dumps(suggestions, indent=2))
        # Fail the task to surface in UI (flip to 'False' to only warn)
        should_fail = os.getenv('DQ_FAIL_ON_ISSUE','true').lower() in ('1','true','yes')
        if should_fail:
            raise AirflowException("Advanced DQ checks failed; see logs for details.")
    else:
        print("Advanced DQ checks passed.")

with DAG(
    dag_id="customer_analytics_pipeline",
    default_args=default_args,
    description="Simulated ingestion -> dbt -> advanced DQ -> complete",
    schedule_interval="@daily",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["analytics","snowflake","dbt"]
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command='echo "Raw data ingested"'
    )

    # Option A: Run dbt for real (requires dbt installed and project mounted)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/dbt && dbt run --target dev",
        trigger_rule="all_success"
    )

    # Option B: Create tables and seed demo data (SnowflakeOperators)
    create_tables = SnowflakeOperator(
        task_id="create_tables",
        sql="/opt/airflow/dags/sql/01_ddl_snowflake.sql",
        snowflake_conn_id="snowflake_default"
    )
    load_seed_data = SnowflakeOperator(
        task_id="load_seed_data",
        sql="/opt/airflow/dags/sql/02_inserts_snowflake.sql",
        snowflake_conn_id="snowflake_default"
    )

    dq_advanced = PythonOperator(
        task_id="dq_advanced",
        python_callable=dq_advanced_fn,
        provide_context=True
    )

    def done_msg(**kwargs):
        print("Data pipeline complete.")
    done = PythonOperator(task_id="done", python_callable=done_msg)

    # Dependencies: simulate ingestion -> create tables/seed -> dbt -> DQ -> done
    ingest_raw >> [create_tables >> load_seed_data, dbt_run] >> dq_advanced >> done
